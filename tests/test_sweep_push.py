"""The reachable sweep must push the `players` topic when it reconciles a slot.

Story 9.23: previously the sweep only pushed `reachable-push` (a different Mercure
topic than the progress grid subscribes to) and left a `pass`, so the grid's
checks/reachable only refreshed on the next WS event. It must now also call
notify_state_changed() (-> players-push) when it picks up a change.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from bridge.core import loops
from bridge.core.loops import _reachable_sweep_loop
from bridge.core.state import StateManager


async def _run_sweep_once(
    state: StateManager,
    notify: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_reachable(slot_id, *_a, **_kw):  # type: ignore[no-untyped-def]
        return ({"counts": {"reachable_now": 2}, "cached": False}, None)

    monkeypatch.setattr(loops, "_compute_reachable", fake_reachable)

    task = asyncio.create_task(
        _reachable_sweep_loop(
            state,
            AsyncMock(),
            "run-1",
            asyncio.Semaphore(1),
            asyncio.Event(),
            runtime=None,
            notify_state_changed=notify,
            initial_delay=0.0,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_sweep_pushes_players_when_a_slot_is_reconciled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StateManager()
    ps = state.ensure_slot(2)
    ps.checks_done = 11  # a slot with progress -> the sweep will (re)compute it

    notify = AsyncMock()
    await _run_sweep_once(state, notify, monkeypatch)

    notify.assert_awaited()


@pytest.mark.asyncio
async def test_sweep_no_push_when_nothing_to_compute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No slots -> nothing to sweep -> no players push.
    state = StateManager()

    notify = AsyncMock()
    await _run_sweep_once(state, notify, monkeypatch)

    notify.assert_not_awaited()
