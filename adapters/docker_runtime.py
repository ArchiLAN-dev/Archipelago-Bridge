from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiodocker

from bridge.core.config import Config

log = logging.getLogger("bridge.adapters.docker")

# The reachability daemon loads the apworld + multidata on startup (can be a few seconds);
# subsequent per-state requests are fast.
_READY_TIMEOUT = 100.0
_REQUEST_TIMEOUT = 30.0


class _ReachableDaemon:
    """A long-lived `reachable.py --daemon` exec'd inside the running AP server container.

    Reads one JSON state line on stdin and writes one JSON result line on stdout, reused
    across sweeps so the apworld/seed stay loaded (no per-compute container).
    """

    def __init__(self, stream: Any, arch_file: str) -> None:
        self.stream = stream
        self.arch_file = arch_file
        self.buf = b""
        self.lock = asyncio.Lock()

    async def read_line(self) -> str:
        # The exec stream multiplexes frames; accumulate stdout (frame type 1) until a newline.
        while b"\n" not in self.buf:
            msg = await self.stream.read_out()
            if msg is None:
                raise RuntimeError("reachable daemon stream closed")
            if msg[0] == 1:  # 1 = stdout, 2 = stderr (daemon logs, ignored)
                self.buf += msg[1]
        line, _, rest = self.buf.partition(b"\n")
        self.buf = rest
        return line.decode().strip()


class DockerRuntimeAdapter:
    """Runs AP reachability via a persistent daemon exec'd **inside the already-running AP
    server container** (no per-compute container churn); save-parse stays a one-shot
    ephemeral container (the AP container may be down at resume time).

    Generation and server lifecycle are handled by the Symfony orchestrator.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._docker: aiodocker.Docker | None = None
        self._daemons: dict[int, _ReachableDaemon] = {}

    def _client(self) -> aiodocker.Docker:
        if self._docker is None:
            self._docker = aiodocker.Docker()
        return self._docker

    def _ap_container(self) -> str:
        # The orchestrateur names the AP server container `ap-server-{sessionId}`
        # (it is the WS host the bridge connects to).
        return f"ap-server-{self._config.session_id}"

    def _volume_bind(self) -> str:
        return f"archilan_session_{self._config.session_id}:/data"

    async def run_reachable(
        self,
        *,
        slot: int,
        arch_file: str,
        yamls_dir: str,
        state_json: str,
    ) -> str:
        """Send one state request to the slot's reachability daemon, return its JSON result line."""
        daemon = await self._ensure_daemon(slot, arch_file, yamls_dir)
        async with daemon.lock:
            try:
                await daemon.stream.write_in((state_json + "\n").encode())
                return await asyncio.wait_for(daemon.read_line(), timeout=_REQUEST_TIMEOUT)
            except Exception:
                # Any I/O hiccup desyncs the request/response stream: drop the daemon so the
                # next sweep re-execs a fresh one (e.g. after an AP container relaunch).
                await self._drop_daemon(slot)
                raise

    async def _ensure_daemon(self, slot: int, arch_file: str, yamls_dir: str) -> _ReachableDaemon:
        existing = self._daemons.get(slot)
        if existing is not None and existing.arch_file == arch_file:
            return existing
        if existing is not None:
            # arch file changed (regeneration): restart the daemon on the new seed.
            await self._drop_daemon(slot)

        container = self._client().containers.container(self._ap_container())
        try:
            exec_obj = await container.exec(
                cmd=[
                    "python", "/reachable/reachable.py",
                    "--archipelago", arch_file,
                    "--yamls", yamls_dir,
                    "--slot", str(slot),
                    "--daemon",
                ],
                stdin=True,
                stdout=True,
                stderr=True,
                tty=False,
                environment={"AP_WORLDS_DIR": self._config.ap_worlds_dir},
            )
            daemon = _ReachableDaemon(exec_obj.start(detach=False), arch_file)
            self._daemons[slot] = daemon
            ready_line = await asyncio.wait_for(daemon.read_line(), timeout=_READY_TIMEOUT)
            if not json.loads(ready_line).get("ready"):
                raise RuntimeError(f"reachable daemon not ready: {ready_line[:200]}")
        except Exception:
            await self._drop_daemon(slot)
            raise

        log.info("reachable daemon: ready slot=%d (exec in %s)", slot, self._ap_container())
        return daemon

    async def _drop_daemon(self, slot: int) -> None:
        daemon = self._daemons.pop(slot, None)
        if daemon is not None:
            try:
                await daemon.stream.close()
            except Exception:
                pass

    async def aclose(self) -> None:
        """Tear down all reachability daemons and the Docker client (bridge shutdown)."""
        for slot in list(self._daemons):
            await self._drop_daemon(slot)
        if self._docker is not None:
            try:
                await self._docker.close()
            except Exception:
                pass
            self._docker = None

    async def run_save_parse(self, *, save_dir: str) -> str:
        """Run read_save.py in an ephemeral AP container and return stdout (JSON).

        Kept as a one-shot container: this runs at resume time when the AP server container
        may not be running yet, so we can't exec into it.
        """
        cmd = ["/readsave/read_save.py", "--save-dir", save_dir]
        container_config: dict[str, Any] = {
            "Image": self._config.ap_image,
            "Entrypoint": ["python"],
            "Cmd": cmd,
            "HostConfig": {
                "Binds": [self._volume_bind()],
            },
        }

        async with aiodocker.Docker() as docker:
            container = await docker.containers.create(config=container_config)
            try:
                await container.start()
                result = await container.wait()
                output_parts: list[str] = await container.log(stdout=True, stderr=False, follow=False)
                if result["StatusCode"] != 0:
                    err_parts: list[str] = await container.log(stdout=False, stderr=True, follow=False)
                    raise RuntimeError("".join(err_parts)[:300])
            finally:
                try:
                    await container.delete(force=True)
                except Exception:
                    pass

        return "".join(output_parts)
