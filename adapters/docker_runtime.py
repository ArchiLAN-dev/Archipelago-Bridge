from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiodocker

from bridge.core.config import Config

log = logging.getLogger("bridge.adapters.docker")

# The session daemon rebuilds the whole multiworld before it is ready (story 17.28): about a minute
# on a big run. Nobody waits on it any more - requests answer « computing » meanwhile - so the
# limits only catch a daemon that hangs. A slot's first request may build its single-player
# fallback world, hence the request limit.
_READY_TIMEOUT = 300.0
_REQUEST_TIMEOUT = 120.0
# A daemon unused this long is closed, its memory given back; the next request starts another.
IDLE_SECONDS = 1800.0


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
        self.last_used = asyncio.get_running_loop().time()

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
        # Story 17.28: one daemon for the whole session - it rebuilds the multiworld once and answers
        # for every slot - instead of one per slot, each rebuilding and holding the same world.
        self._daemon: _ReachableDaemon | None = None
        # One start at a time: a second caller waits for the first one's ready line instead of
        # exec'ing a second daemon.
        self._starting = asyncio.Lock()

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
        """Send one state request (naming its slot) to the session daemon, return its JSON result line."""
        daemon = await self._ensure_daemon(arch_file, yamls_dir)
        async with daemon.lock:
            try:
                await daemon.stream.write_in((state_json + "\n").encode())
                line = await asyncio.wait_for(daemon.read_line(), timeout=_REQUEST_TIMEOUT)
            except BaseException:
                # Any I/O hiccup desyncs the request/response stream: drop the daemon so the
                # next request re-execs a fresh one (e.g. after an AP container relaunch). A
                # cancelled caller too (story 17.28): its answer would otherwise stay in the
                # stream and be read as the answer to the next request.
                await self._drop_daemon(daemon)
                raise
            daemon.last_used = asyncio.get_running_loop().time()
            return line

    async def _ensure_daemon(self, arch_file: str, yamls_dir: str) -> _ReachableDaemon:
        async with self._starting:
            existing = self._daemon
            if existing is not None and existing.arch_file == arch_file:
                return existing
            if existing is not None:
                # arch file changed (regeneration): restart the daemon on the new seed.
                await self._drop_daemon(existing)
            return await self._start_daemon(arch_file, yamls_dir)

    async def _start_daemon(self, arch_file: str, yamls_dir: str) -> _ReachableDaemon:
        container = self._client().containers.container(self._ap_container())
        exec_obj = await container.exec(
            cmd=[
                "python", "/reachable/reachable.py",
                "--archipelago", arch_file,
                "--yamls", yamls_dir,
                "--daemon",
                "--session",
            ],
            stdin=True,
            stdout=True,
            stderr=True,
            tty=False,
            environment={"AP_WORLDS_DIR": self._config.ap_worlds_dir},
        )
        daemon = _ReachableDaemon(exec_obj.start(detach=False), arch_file)
        try:
            ready_line = await asyncio.wait_for(daemon.read_line(), timeout=_READY_TIMEOUT)
            if not json.loads(ready_line).get("ready"):
                raise RuntimeError(f"reachable daemon not ready: {ready_line[:200]}")
        except BaseException:
            # Not ready, failed, or the caller gave up waiting (a cancelled request, story 17.28):
            # the daemon is never handed out with its ready line still unread - that line would
            # come back as the result of the first request.
            await self._close(daemon)
            raise
        # Only a daemon whose ready line was read serves requests.
        self._daemon = daemon

        log.info("reachable daemon: ready (session, exec in %s)", self._ap_container())
        return daemon

    async def reset_reachable(self, slot: int) -> None:
        """Drop the session daemon (its answers came out of step); the next request starts a fresh one."""
        if self._daemon is not None:
            await self._drop_daemon(self._daemon)

    async def release_idle(self, idle_seconds: float = IDLE_SECONDS) -> bool:
        """Close the session daemon when unused for `idle_seconds` (story 17.28): a sleeping run
        gives its multiworld's memory back. Returns whether it was closed."""
        daemon = self._daemon
        if daemon is None or daemon.lock.locked():
            return False
        if asyncio.get_running_loop().time() - daemon.last_used < idle_seconds:
            return False
        log.info("reachable daemon: idle for %.0fs, closed", idle_seconds)
        await self._drop_daemon(daemon)
        return True

    async def _drop_daemon(self, daemon: _ReachableDaemon) -> None:
        if self._daemon is daemon:
            self._daemon = None
        await self._close(daemon)

    @staticmethod
    async def _close(daemon: _ReachableDaemon) -> None:
        # Closing the exec stream ends the daemon's stdin: its request loop ends and it exits.
        try:
            await daemon.stream.close()
        except Exception:
            pass

    async def aclose(self) -> None:
        """Tear down the reachability daemon and the Docker client (bridge shutdown)."""
        if self._daemon is not None:
            await self._drop_daemon(self._daemon)
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
