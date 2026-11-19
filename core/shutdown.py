"""Stopping the bridge when its container is stopped."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any, Protocol

log = logging.getLogger(__name__)


class _Server(Protocol):
    should_exit: bool


async def run_until_server_stops(
    main: Coroutine[Any, Any, None], server_task: asyncio.Task[None], server: _Server
) -> None:
    """Run ``main`` (the Archipelago connection) until it ends or the REST server stops on a signal.

    uvicorn catches SIGTERM, stops its server, then raises the signal again with the default action. The bridge
    runs as PID 1 in its container, where the kernel ignores a signal left to its default action: the connection
    loop kept running until ``docker stop`` killed it, 10 s later. The REST server stopping on a signal
    (``should_exit``) now ends ``main`` too. A server that stops without being asked (it could not start) leaves
    the connection running, as before.
    """
    main_task = asyncio.ensure_future(main)
    done, _ = await asyncio.wait({main_task, server_task}, return_when=asyncio.FIRST_COMPLETED)
    if main_task in done:
        main_task.result()
        return
    if not server.should_exit:
        await main_task
        return
    log.info("stop requested, closing the Archipelago connection")
    main_task.cancel()
    try:
        await main_task
    except asyncio.CancelledError:
        pass
