from __future__ import annotations

import asyncio

from bridge.core.shutdown import run_until_server_stops


class _Server:
    def __init__(self) -> None:
        self.should_exit = False


async def _forever(cancelled: list[bool]) -> None:
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        cancelled.append(True)
        raise


async def test_a_stop_signal_ends_the_connection_loop() -> None:
    server = _Server()
    cancelled: list[bool] = []

    async def serve() -> None:
        await asyncio.sleep(0.01)
        server.should_exit = True  # what uvicorn does on SIGTERM

    await asyncio.wait_for(
        run_until_server_stops(_forever(cancelled), asyncio.ensure_future(serve()), server), timeout=1
    )
    assert cancelled == [True]


async def test_a_server_that_stops_by_itself_leaves_the_connection_running() -> None:
    server = _Server()
    finished: list[bool] = []

    async def main() -> None:
        await asyncio.sleep(0.05)
        finished.append(True)

    async def serve() -> None:
        return None  # could not start: no stop was asked

    await asyncio.wait_for(run_until_server_stops(main(), asyncio.ensure_future(serve()), server), timeout=1)
    assert finished == [True]


async def test_the_connection_loop_ending_returns_and_raises_its_error() -> None:
    server = _Server()

    async def serve() -> None:
        await asyncio.Event().wait()

    async def main() -> None:
        raise RuntimeError("boom")

    server_task = asyncio.ensure_future(serve())
    try:
        await run_until_server_stops(main(), server_task, server)
    except RuntimeError as exc:
        assert str(exc) == "boom"
    else:
        raise AssertionError("the error was swallowed")
    finally:
        server_task.cancel()
