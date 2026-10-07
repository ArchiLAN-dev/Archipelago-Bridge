from __future__ import annotations

import asyncio
import glob
import json
import logging
import os
import sys
from collections.abc import Awaitable, Callable
from typing import Any

from .state import StateManager

_reachable_cache: dict[int, tuple[tuple, dict]] = {}

_reachable_daemons: dict[int, asyncio.subprocess.Process] = {}
_daemon_ready_events: dict[int, asyncio.Event] = {}

# The computation running for each slot, shared by every caller (story 17.28).
_inflight: dict[int, asyncio.Task[tuple[dict | None, str]]] = {}

OUT_OF_TURN = "reachable daemon answered out of turn"

ReachablePublisher = Callable[[int, dict], Awaitable[None]]

# Tells the site a computation that nobody waited for (story 17.28): set once by the bridge.
_hooks: dict[str, ReachablePublisher | None] = {"publisher": None}


def set_reachable_publisher(publisher: ReachablePublisher | None) -> None:
    """Where a fresh result started outside the sweep goes once ready (broadcast, push to the site)."""
    _hooks["publisher"] = publisher


def _result_error(result: object) -> str | None:
    """Return the error message if a reachable.py payload is a structured error, else None.

    reachable.py emits {"error": "..."} (exit 0, valid JSON) when a single per-request compute
    fails - e.g. in --daemon mode. Without this check the bridge would cache that payload and
    hand it back as a successful reachability result (HTTP 200), and the cached error would stick
    until the slot's state changes. Surfacing it as (None, error) lets the caller raise properly.
    """
    if not isinstance(result, dict):
        return "reachable.py answered something that is not a result"
    if "error" in result:
        return str(result["error"])
    if not isinstance(result.get("counts"), dict):
        # Not a computation - typically the daemon's {"ready": true} line read as the answer to a
        # request, the stream one line out of step (story 17.28). Cached, it would be served as
        # the slot's reachability until its state changes.
        return OUT_OF_TURN
    return None


async def _compute_and_publish(
    slot: int,
    state: StateManager,
    semaphore: asyncio.Semaphore,
    log: logging.Logger,
    runtime: Any = None,
) -> tuple[dict | None, str]:
    result, err = await _compute_reachable(slot, state, semaphore, log, runtime)
    publisher = _hooks["publisher"]
    if result is not None and not result.get("cached") and publisher is not None:
        try:
            await publisher(slot, result)
        except Exception as exc:
            log.warning("reachable: publishing slot=%d failed: %s", slot, exc)
    return result, err


def start_reachable(
    slot: int,
    state: StateManager,
    semaphore: asyncio.Semaphore,
    log: logging.Logger,
    runtime: Any = None,
) -> asyncio.Task[tuple[dict | None, str]]:
    """The slot's computation, started now or joined if already running (story 17.28).

    Nobody has to wait for it: once fresh, its result goes to the site through the publisher.
    """
    task = _inflight.get(slot)
    if task is None or task.done():
        task = asyncio.create_task(_compute_and_publish(slot, state, semaphore, log, runtime))
        _inflight[slot] = task

        def _forget(done: asyncio.Task[tuple[dict | None, str]], slot: int = slot) -> None:
            if _inflight.get(slot) is done:
                del _inflight[slot]

        task.add_done_callback(_forget)
    return task


async def compute_reachable_shared(
    slot: int,
    state: StateManager,
    semaphore: asyncio.Semaphore,
    log: logging.Logger,
    runtime: Any = None,
) -> tuple[dict | None, str]:
    """`start_reachable`, awaited. A caller that stops waiting (a timeout, a dropped request)
    cancels only its own wait, never the computation: a daemon start cut short is lost work on a
    big multiworld, and the next caller would start it from scratch again (story 17.28).
    """
    return await asyncio.shield(start_reachable(slot, state, semaphore, log, runtime))


async def _reset_daemon(slot: int, runtime: Any, log: logging.Logger) -> None:
    """Drop a slot's daemon whose answers came out of step, so the next compute starts a fresh one."""
    log.warning("reachable: daemon out of step slot=%d - restarting it", slot)
    if runtime is not None and hasattr(runtime, "reset_reachable"):
        await runtime.reset_reachable(slot)
        return
    _daemon_ready_events.pop(slot, None)
    proc = _reachable_daemons.pop(slot, None)
    if proc is not None and proc.returncode is None:
        try:
            proc.kill()
        except ProcessLookupError:
            pass


async def _start_daemon(slot: int, arch_file: str, log: logging.Logger) -> None:
    """Start reachable.py in --daemon mode for a slot and wait for it to signal ready."""
    event = asyncio.Event()
    _daemon_ready_events[slot] = event
    output_dir = os.environ.get("AP_OUTPUT_DIR", os.environ.get("ARCHIPELAGO_OUTPUT_DIR", "/data/output"))
    yamls_dir = os.environ.get("AP_YAMLS_DIR", os.path.join(os.path.dirname(output_dir), "yamls"))
    cmd = [
        sys.executable, "/reachable/reachable.py",
        "--archipelago", arch_file,
        "--yamls", yamls_dir,
        "--slot", str(slot),
        "--daemon",
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        _reachable_daemons[slot] = proc
        log.info("reachable daemon: started slot=%d pid=%d", slot, proc.pid)
        assert proc.stdout is not None
        line = await asyncio.wait_for(proc.stdout.readline(), timeout=180.0)
        data = json.loads(line.decode())
        if data.get("ready"):
            event.set()
            log.info("reachable daemon: ready slot=%d", slot)
        else:
            log.warning("reachable daemon: unexpected first line slot=%d: %s", slot, line)
    except asyncio.TimeoutError:
        log.error("reachable daemon: startup timeout slot=%d", slot)
    except Exception as exc:
        log.error("reachable daemon: startup failed slot=%d: %s", slot, exc)


async def _compute_reachable(
    slot: int,
    state: StateManager,
    semaphore: asyncio.Semaphore,
    log: logging.Logger,
    runtime: Any = None,
) -> tuple[dict | None, str]:
    """Run reachability for a slot. Returns (result_dict, error_msg).

    Cache keyed on (checks_done, items_received).
    """
    ps = state._states.get(slot)

    output_dir = os.environ.get("AP_OUTPUT_DIR", os.environ.get("ARCHIPELAGO_OUTPUT_DIR", "/data/output"))
    arch_files = sorted(
        glob.glob(f"{output_dir}/*.archipelago") or glob.glob(f"{output_dir}/*.zip"),
        key=os.path.getmtime,
        reverse=True,
    )
    if not arch_files:
        return None, "no .archipelago file"

    yamls_dir = os.environ.get("AP_YAMLS_DIR", os.path.join(os.path.dirname(output_dir), "yamls"))

    checks_done = len(ps._checked_locations) if ps else 0
    items_received = len(ps._received_items) if ps else 0
    cache_key = (checks_done, items_received)

    cached = _reachable_cache.get(slot)
    if cached and cached[0] == cache_key:
        return {**cached[1], "cached": True}, ""

    log.info("reachable: running slot=%d cache_key=%s", slot, cache_key)

    state_payload = json.dumps({
        # A session daemon answers for every slot: each request names its own (story 17.28).
        "slot": slot,
        "checked_locations": list(ps._checked_locations) if ps else [],
        "received_items": list(ps._received_items) if ps else [],
    })

    async with semaphore:
        # Docker mode: delegate to ephemeral AP container via runtime adapter.
        if runtime is not None and hasattr(runtime, "run_reachable"):
            try:
                output = await asyncio.wait_for(
                    runtime.run_reachable(
                        slot=slot,
                        arch_file=arch_files[0],
                        yamls_dir=yamls_dir,
                        state_json=state_payload,
                    ),
                    timeout=120.0,
                )
            except asyncio.TimeoutError:
                return None, "reachability check timed out"
            except Exception as exc:
                return None, str(exc)
            try:
                result = json.loads(output)
            except json.JSONDecodeError:
                return None, "invalid JSON from reachable.py"
            err = _result_error(result)
            if err is not None:
                if err == OUT_OF_TURN:
                    await _reset_daemon(slot, runtime, log)
                return None, err
            _reachable_cache[slot] = (cache_key, result)
            log.info("reachable: docker slot=%d reachable=%d",
                     slot, result.get("counts", {}).get("reachable_now", 0))
            return result, ""

        # Subprocess / daemon path (non-Docker mode).
        state_payload_nl = state_payload + "\n"
        daemon_proc = _reachable_daemons.get(slot)
        daemon_event = _daemon_ready_events.get(slot)
        if (daemon_proc and daemon_proc.returncode is None
                and daemon_event and daemon_event.is_set()):
            try:
                assert daemon_proc.stdin is not None and daemon_proc.stdout is not None
                daemon_proc.stdin.write(state_payload_nl.encode())
                await daemon_proc.stdin.drain()
                resp = await asyncio.wait_for(daemon_proc.stdout.readline(), timeout=10.0)
                result = json.loads(resp.decode())
                err = _result_error(result)
                if err is not None:
                    if err == OUT_OF_TURN:
                        await _reset_daemon(slot, None, log)
                    return None, err
                _reachable_cache[slot] = (cache_key, result)
                log.info("reachable: daemon slot=%d reachable=%d",
                         slot, result.get("counts", {}).get("reachable_now", 0))
                return result, ""
            except asyncio.CancelledError:
                # The answer to this request would be read as the answer to the next one.
                await _reset_daemon(slot, None, log)
                raise
            except Exception as exc:
                log.warning("reachable: daemon failed slot=%d %s - subprocess fallback", slot, exc)
                # Its stream is out of step now: a fresh daemon starts below.
                await _reset_daemon(slot, None, log)

        existing = _reachable_daemons.get(slot)
        if existing is None or existing.returncode is not None:
            asyncio.create_task(_start_daemon(slot, arch_files[0], log))

        cmd = [
            sys.executable, "/reachable/reachable.py",
            "--archipelago", arch_files[0],
            "--yamls", yamls_dir,
            "--slot", str(slot),
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=state_payload_nl.encode()), timeout=120.0
            )
        except asyncio.TimeoutError:
            return None, "reachability check timed out"
        except Exception as exc:
            return None, str(exc)

    if proc.returncode != 0:
        err_msg = ""
        if stdout.strip():
            try:
                err_msg = json.loads(stdout).get("error", "")
            except json.JSONDecodeError:
                pass
        if not err_msg:
            err_msg = (
                stderr.decode("utf-8", errors="replace").strip().splitlines()[-1]
                if stderr.strip() else "reachable.py failed"
            )
        log.warning("reachable: reachable.py failed slot=%d: %s", slot, err_msg)
        return None, err_msg

    try:
        result = json.loads(stdout)
    except json.JSONDecodeError:
        return None, "invalid JSON from reachable.py"

    err = _result_error(result)
    if err is not None:
        return None, err

    _reachable_cache[slot] = (cache_key, result)
    log.info("reachable: done slot=%d reachable=%d",
             slot, result.get("counts", {}).get("reachable_now", 0))
    return result, ""
