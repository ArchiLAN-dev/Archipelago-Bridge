"""Story 17.28: the reachability daemon, lighter and never waited on.

On a big multiworld (18 slots, exact regeneration) every per-slot daemon rebuilt the whole world, and
took long to start. `item-locations` gave up on a start after 8 s, which cancelled it after the
daemon was registered but before its `{"ready": true}` line was read: the next request read that
line as its result, the bridge cached it, and the slot showed `{ready, cached, player}` until its
state changed. These tests pin:

- one daemon per session, handed out only once ready, dropped by a cancelled caller, closed when idle;
- a payload that is not a computation is never cached, and restarts the daemon;
- a request answers within a grace, else 202 « computing », and the result is pushed once ready;
- item-locations never waits.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from bridge.adapters.reachable_socket import ReachableSocketAdapter
from bridge.core import reachable, rest_reachable
from bridge.core.reachable import OUT_OF_TURN, _compute_reachable, set_reachable_publisher, start_reachable
from bridge.core.state import StateManager
from bridge.tests.test_rest_handlers import _make_app


class _Stream:
    """A connection to the reachable host: reads the seed line, answers `{"ready": true}` once
    released, then one result per request, echoing the slot the request named."""

    def __init__(self, ready: asyncio.Event) -> None:
        self._ready = ready
        self._out: asyncio.Queue[bytes] = asyncio.Queue()
        self.closed = False
        self._sent_ready = False
        self.seed: dict[str, Any] | None = None

    async def readline(self) -> bytes:
        if not self._sent_ready:
            await self._ready.wait()
            self._sent_ready = True
            return b'{"ready": true}\n'
        return await self._out.get()

    def write(self, data: bytes) -> None:
        request = json.loads(data)
        if self.seed is None:
            self.seed = request
            return
        answer = {"slot": request.get("slot"), "counts": {"reachable_now": 3}}
        self._out.put_nowait(json.dumps(answer).encode() + b"\n")

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


class _Host:
    """The reachable host of the AP container; records each connection, hence each daemon."""

    def __init__(self) -> None:
        self.ready = asyncio.Event()
        self.streams: list[_Stream] = []

    async def connect(self) -> tuple[_Stream, _Stream]:
        stream = _Stream(self.ready)
        self.streams.append(stream)
        return stream, stream


def _adapter() -> tuple[ReachableSocketAdapter, _Host]:
    adapter = ReachableSocketAdapter(SimpleNamespace(reachable_socket="/data/reachable.sock"))  # type: ignore[arg-type]
    host = _Host()
    adapter._connect = host.connect  # type: ignore[method-assign,assignment]
    return adapter, host


async def _ask(adapter: ReachableSocketAdapter, slot: int) -> dict[str, Any]:
    line = await adapter.run_reachable(slot=slot, arch_file="a", yamls_dir="y", state_json=json.dumps({"slot": slot}))
    result: dict[str, Any] = json.loads(line)
    return result


@pytest.mark.asyncio
async def test_one_session_daemon_answers_every_slot() -> None:
    adapter, host = _adapter()
    host.ready.set()

    assert [(await _ask(adapter, slot))["slot"] for slot in (1, 2, 3)] == [1, 2, 3]
    assert len(host.streams) == 1, "one daemon for the whole session"
    assert host.streams[0].seed == {"archipelago": "a", "yamls": "y"}


@pytest.mark.asyncio
async def test_a_start_cut_short_never_leaves_a_daemon_with_its_ready_line_unread() -> None:
    adapter, host = _adapter()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(_ask(adapter, 1), timeout=0.05)

    assert adapter._daemon is None, "a daemon whose ready line was not read is never handed out"
    assert host.streams[0].closed

    host.ready.set()
    assert (await _ask(adapter, 1))["counts"] == {"reachable_now": 3}, "the next request gets a result, not the ready line"


@pytest.mark.asyncio
async def test_two_callers_share_one_daemon_start() -> None:
    adapter, host = _adapter()

    first = asyncio.create_task(_ask(adapter, 1))
    second = asyncio.create_task(_ask(adapter, 2))
    await asyncio.sleep(0.01)
    host.ready.set()

    assert [r["slot"] for r in await asyncio.gather(first, second)] == [1, 2]
    assert len(host.streams) == 1, "one daemon, started once"


@pytest.mark.asyncio
async def test_a_request_cancelled_midway_drops_the_daemon() -> None:
    adapter, host = _adapter()
    host.ready.set()
    await _ask(adapter, 1)
    daemon = adapter._daemon
    assert daemon is not None

    async def never_answers() -> str:
        await asyncio.sleep(10)
        return ""

    daemon.read_line = never_answers  # type: ignore[method-assign]
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(_ask(adapter, 1), timeout=0.05)

    assert adapter._daemon is None, "its answer would have been read as the next request's"


@pytest.mark.asyncio
async def test_an_idle_daemon_is_closed_and_a_busy_one_kept() -> None:
    adapter, host = _adapter()
    host.ready.set()
    await _ask(adapter, 1)

    assert not await adapter.release_idle(idle_seconds=60), "used just now"
    assert await adapter.release_idle(idle_seconds=0)
    assert adapter._daemon is None and host.streams[0].closed

    await _ask(adapter, 1)
    assert len(host.streams) == 2, "the next request starts another"


class _OutOfStepRuntime:
    def __init__(self) -> None:
        self.resets: list[int] = []

    async def run_reachable(self, *, slot: int, arch_file: str, yamls_dir: str, state_json: str) -> str:
        return json.dumps({"ready": True})

    async def reset_reachable(self, slot: int) -> None:
        self.resets.append(slot)


def _state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> StateManager:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "AP_seed.archipelago").write_bytes(b"x")
    monkeypatch.setenv("AP_OUTPUT_DIR", str(output_dir))
    state = StateManager()
    state.ensure_slot(2)
    return state


@pytest.mark.asyncio
async def test_a_ready_line_read_as_a_result_is_never_cached_and_restarts_the_daemon(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reachable._reachable_cache.clear()
    runtime = _OutOfStepRuntime()

    result, err = await _compute_reachable(2, _state(monkeypatch, tmp_path), asyncio.Semaphore(1), logging.getLogger("test"), runtime=runtime)

    assert (result, err) == (None, OUT_OF_TURN)
    assert 2 not in reachable._reachable_cache
    assert runtime.resets == [2]


@pytest.mark.asyncio
async def test_each_request_names_its_slot(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reachable._reachable_cache.clear()
    sent: list[dict[str, Any]] = []

    class _Runtime:
        async def run_reachable(self, *, slot: int, arch_file: str, yamls_dir: str, state_json: str) -> str:
            sent.append(json.loads(state_json))
            return json.dumps({"counts": {"reachable_now": 1}})

    await _compute_reachable(2, _state(monkeypatch, tmp_path), asyncio.Semaphore(1), logging.getLogger("test"), runtime=_Runtime())

    assert sent[0]["slot"] == 2


@pytest.mark.asyncio
async def test_a_computation_nobody_waited_for_is_published_once_ready(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reachable._reachable_cache.clear()
    release = asyncio.Event()
    published: list[tuple[int, dict]] = []

    class _SlowRuntime:
        async def run_reachable(self, *, slot: int, arch_file: str, yamls_dir: str, state_json: str) -> str:
            await release.wait()
            return json.dumps({"counts": {"reachable_now": 7}})

    async def publish(slot: int, result: dict) -> None:
        published.append((slot, result))

    set_reachable_publisher(publish)
    try:
        task = start_reachable(2, _state(monkeypatch, tmp_path), asyncio.Semaphore(1), logging.getLogger("test"), _SlowRuntime())
        assert start_reachable(2, StateManager(), asyncio.Semaphore(1), logging.getLogger("test")) is task, "joined, not started twice"
        release.set()
        await task
    finally:
        set_reachable_publisher(None)

    assert published == [(2, {"counts": {"reachable_now": 7}})]
    assert reachable._reachable_cache[2][1] == {"counts": {"reachable_now": 7}}


@pytest.mark.asyncio
async def test_a_slow_slot_answers_202_with_its_previous_result_then_200(monkeypatch: pytest.MonkeyPatch) -> None:
    reachable._reachable_cache.clear()
    app, state, _ = _make_app()
    ps = state.ensure_slot(1)
    ps.slot_name = "kionx_C"
    reachable._reachable_cache[1] = ((0, 0), {"counts": {"reachable_now": 4}})
    release = asyncio.Event()

    async def fake_compute(slot: int, *_args: Any) -> tuple[dict | None, str]:
        await release.wait()
        result = {"counts": {"reachable_now": 9}}
        reachable._reachable_cache[slot] = ((1, 0), result)
        return result, ""

    monkeypatch.setattr(reachable, "_compute_reachable", fake_compute)
    monkeypatch.setattr(rest_reachable, "REACHABLE_GRACE_SECONDS", 0.05)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/slots/1/reachable")
        assert resp.status_code == 202
        assert resp.json() == {"computing": True, "previous": {"counts": {"reachable_now": 4}, "cached": True, "player": "kionx_C"}}

        release.set()
        await reachable._inflight[1]

    async def cached(slot: int, *_args: Any) -> tuple[dict | None, str]:
        return {**reachable._reachable_cache[slot][1], "cached": True}, ""

    monkeypatch.setattr(reachable, "_compute_reachable", cached)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/slots/1/reachable")
    assert resp.status_code == 200
    assert resp.json()["counts"] == {"reachable_now": 9}


@pytest.mark.asyncio
async def test_item_locations_never_waits_and_starts_the_missing_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    reachable._reachable_cache.clear()
    app, state, _ = _make_app()
    state.ensure_slot(1)
    state.ensure_slot(2)
    release = asyncio.Event()
    started: list[int] = []

    async def fake_compute(slot: int, *_args: Any) -> tuple[dict | None, str]:
        started.append(slot)
        await release.wait()
        result = {"counts": {"reachable_now": slot}}
        reachable._reachable_cache[slot] = ((0, 0), result)
        return result, ""

    monkeypatch.setattr(reachable, "_compute_reachable", fake_compute)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/item-locations/1")
    assert resp.status_code == 200
    assert resp.json()["locations"] == []
    await asyncio.sleep(0)
    assert sorted(started) == [1, 2], "every missing slot is started, none awaited"

    release.set()
    await asyncio.gather(reachable._inflight[1], reachable._inflight[2])
    assert sorted(reachable._reachable_cache) == [1, 2]
