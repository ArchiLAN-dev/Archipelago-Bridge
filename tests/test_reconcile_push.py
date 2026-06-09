"""The apsave reconcile loop must push state when it picks up changes the WS missed."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from bridge.core.loops import _apsave_reconcile_loop
from bridge.core.state import StateManager


async def _run_loop_once(state: StateManager, notify: AsyncMock) -> None:
    task = asyncio.create_task(
        _apsave_reconcile_loop(
            state,
            AsyncMock(),
            "run-1",
            asyncio.Event(),
            runtime=None,
            notify_state_changed=notify,
            interval=0.01,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_reconcile_pushes_when_state_changes() -> None:
    state = StateManager()
    state.ensure_slot(1)

    def fake_merge() -> None:
        state._states[1].checks_done = 3

    state.merge_state_from_save = fake_merge  # type: ignore[method-assign]

    notify = AsyncMock()
    await _run_loop_once(state, notify)

    notify.assert_awaited()


@pytest.mark.asyncio
async def test_reconcile_does_not_push_when_unchanged() -> None:
    state = StateManager()
    state.ensure_slot(1)
    state.merge_state_from_save = lambda: None  # type: ignore[method-assign]

    notify = AsyncMock()
    await _run_loop_once(state, notify)

    notify.assert_not_awaited()
