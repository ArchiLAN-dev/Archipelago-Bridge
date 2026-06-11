"""Story 9.23 task 4: broadcasting hints must also push them to the API so the
Mercure hints topic (runs/{id}/slots/{n}/hints) is published - otherwise the
frontend hints panel never updates live."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from bridge.core.ap_client import ArchipelagoClient
from bridge.core.config import Config
from bridge.core.state import StateManager


def _client() -> tuple[ArchipelagoClient, StateManager]:
    state = StateManager()
    ap = ArchipelagoClient(Config(session_id="run-1", internal_token="secret"), state, AsyncMock())
    return ap, state


@pytest.mark.asyncio
async def test_broadcast_hints_also_pushes_to_api() -> None:
    ap, state = _client()
    state.ensure_slot(2)

    push = AsyncMock()
    ap._push_hints_to_api = push  # type: ignore[method-assign]

    await ap._broadcast_hints(2)

    push.assert_awaited_once()
    # the same payload is broadcast locally and pushed to the API
    args, _ = push.call_args
    assert args[0] == 2
    assert "hints" in args[1]


@pytest.mark.asyncio
async def test_push_hints_is_noop_without_central_api() -> None:
    # Config without central_api_url/secret -> must return before any HTTP call.
    ap, _ = _client()
    await ap._push_hints_to_api(2, {"hints": []})  # must not raise
