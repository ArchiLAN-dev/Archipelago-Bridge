from __future__ import annotations

import asyncio
import json
import logging

from bridge.core.config import Config

log = logging.getLogger("bridge.adapters.reachable")

# The session daemon rebuilds the whole multiworld before it is ready (story 17.28): about a minute
# on a big run. Nobody waits on it any more - requests answer « computing » meanwhile - so the
# limits only catch a daemon that hangs. A slot's first request may build its single-player
# fallback world, hence the request limit.
_READY_TIMEOUT = 300.0
_REQUEST_TIMEOUT = 120.0
# A daemon unused this long is closed, its memory given back; the next request starts another.
IDLE_SECONDS = 1800.0
# A result lists every location of a slot: far above asyncio's 64 KiB default line limit.
_LINE_LIMIT = 64 * 1024 * 1024


class _ReachableDaemon:
    """One connection to the AP container's reachable host, hence one `reachable.py --daemon`.

    Writes one JSON state line and reads one JSON result line, reused across sweeps so the
    apworld/seed stay loaded.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, arch_file: str) -> None:
        self.reader = reader
        self.writer = writer
        self.arch_file = arch_file
        self.lock = asyncio.Lock()
        self.last_used = asyncio.get_running_loop().time()

    async def write_line(self, line: str) -> None:
        self.writer.write((line + "\n").encode())
        await self.writer.drain()

    async def read_line(self) -> str:
        line = await self.reader.readline()
        if not line.endswith(b"\n"):
            raise RuntimeError("reachable daemon stream closed")
        return line.decode().strip()


class ReachableSocketAdapter:
    """Runs AP reachability through the Unix socket the AP server container opens on the session
    volume (story 50.6). The bridge holds no access to Docker: the volume is all it shares with the
    AP container, and generation and server lifecycle belong to the orchestrateur.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        # Story 17.28: one daemon for the whole session - it rebuilds the multiworld once and answers
        # for every slot - instead of one per slot, each rebuilding and holding the same world.
        self._daemon: _ReachableDaemon | None = None
        # One start at a time: a second caller waits for the first one's ready line instead of
        # starting a second daemon.
        self._starting = asyncio.Lock()

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
                await daemon.write_line(state_json)
                line = await asyncio.wait_for(daemon.read_line(), timeout=_REQUEST_TIMEOUT)
            except BaseException:
                # Any I/O hiccup desyncs the request/response stream: drop the daemon so the
                # next request starts a fresh one (e.g. after an AP container relaunch). A
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

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_unix_connection(self._config.reachable_socket, limit=_LINE_LIMIT)

    async def _start_daemon(self, arch_file: str, yamls_dir: str) -> _ReachableDaemon:
        reader, writer = await self._connect()
        daemon = _ReachableDaemon(reader, writer, arch_file)
        try:
            await daemon.write_line(json.dumps({"archipelago": arch_file, "yamls": yamls_dir}))
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

        log.info("reachable daemon: ready (session)")
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
        # Closing the connection makes the host end the daemon's stdin: its request loop ends.
        try:
            daemon.writer.close()
            await daemon.writer.wait_closed()
        except Exception:
            pass

    async def aclose(self) -> None:
        """Tear down the reachability daemon (bridge shutdown)."""
        if self._daemon is not None:
            await self._drop_daemon(self._daemon)
