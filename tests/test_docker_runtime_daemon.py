"""Reachability daemon framing: `_ReachableDaemon.read_line` accumulates multiplexed
exec-stream stdout frames into one newline-terminated line and ignores stderr frames.
The daemon runs `reachable.py --daemon` exec'd inside the running AP server container
(no per-compute container)."""
from __future__ import annotations

import pytest

from bridge.adapters.docker_runtime import _ReachableDaemon


class _FakeStream:
    """Yields (frame_type, data) messages like aiodocker's Stream.read_out (1=stdout, 2=stderr)."""

    def __init__(self, frames: list[tuple[int, bytes] | None]) -> None:
        self._frames = list(frames)

    async def read_out(self) -> tuple[int, bytes] | None:
        return self._frames.pop(0) if self._frames else None


@pytest.mark.asyncio
async def test_read_line_accumulates_stdout_across_frames_and_keeps_remainder() -> None:
    daemon = _ReachableDaemon(
        _FakeStream([(1, b'{"coun'), (1, b'ts":1}\n{"next'), (1, b'":2}\n')]),
        arch_file="x.archipelago",
    )

    assert await daemon.read_line() == '{"counts":1}'
    # A second result already buffered is returned without another read.
    assert await daemon.read_line() == '{"next":2}'


@pytest.mark.asyncio
async def test_read_line_ignores_stderr_frames() -> None:
    daemon = _ReachableDaemon(
        _FakeStream([(2, b"loading apworld...\n"), (1, b'{"ready":true}'), (1, b"\n")]),
        arch_file="x.archipelago",
    )

    assert await daemon.read_line() == '{"ready":true}'


@pytest.mark.asyncio
async def test_read_line_raises_when_stream_closes_before_newline() -> None:
    daemon = _ReachableDaemon(_FakeStream([(1, b"partial"), None]), arch_file="x.archipelago")

    with pytest.raises(RuntimeError, match="stream closed"):
        await daemon.read_line()
