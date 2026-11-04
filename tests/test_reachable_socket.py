"""Reachability daemon framing: one JSON line per answer, over the AP container's Unix socket."""
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from bridge.adapters.reachable_socket import ReachableSocketAdapter, _ReachableDaemon
from bridge.core.config import Config


def _daemon(data: bytes, *, eof: bool = True) -> _ReachableDaemon:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    if eof:
        reader.feed_eof()
    writer: Any = None
    return _ReachableDaemon(reader, writer, arch_file="x.archipelago")


@pytest.mark.asyncio
async def test_read_line_returns_one_answer_and_keeps_the_next() -> None:
    daemon = _daemon(b'{"counts":1}\n{"next":2}\n')

    assert await daemon.read_line() == '{"counts":1}'
    assert await daemon.read_line() == '{"next":2}'


@pytest.mark.asyncio
async def test_read_line_raises_when_the_stream_closes_before_newline() -> None:
    daemon = _daemon(b"partial")

    with pytest.raises(RuntimeError, match="stream closed"):
        await daemon.read_line()


@pytest.mark.asyncio
async def test_read_line_raises_on_a_closed_stream() -> None:
    with pytest.raises(RuntimeError, match="stream closed"):
        await _daemon(b"").read_line()


class _Writer:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


@pytest.mark.asyncio
async def test_an_idle_daemon_is_closed_and_a_busy_one_is_not() -> None:
    writer: Any = _Writer()
    adapter = ReachableSocketAdapter(Config(session_id="s", internal_token="t"))
    daemon = _ReachableDaemon(asyncio.StreamReader(), writer, arch_file="x.archipelago")
    adapter._daemon = daemon

    async with daemon.lock:
        assert await adapter.release_idle(idle_seconds=0) is False
    assert await adapter.release_idle(idle_seconds=0) is True

    assert writer.closed
    assert adapter._daemon is None
