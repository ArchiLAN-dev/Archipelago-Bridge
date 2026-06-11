"""Story 9.24 task 1: the sweep must mark `last_computed` with the snapshot the
compute actually used (the cache key), not the post-await current state. Otherwise
a check/item arriving DURING the ~seconds compute is marked as computed and the slot
is never re-swept -> reachable_now stuck one batch behind."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from bridge.core import loops, reachable
from bridge.core.loops import _reachable_sweep_loop
from bridge.core.state import StateManager


@pytest.mark.asyncio
async def test_slot_changed_during_compute_is_reswept(monkeypatch: pytest.MonkeyPatch) -> None:
    reachable._reachable_cache.clear()
    state = StateManager()
    ps = state.ensure_slot(2)
    ps.checks_done = 5  # snapshot the first compute will use

    ev = asyncio.Event()
    calls = {"n": 0}

    async def fake_compute(slot_id, st, sem, log, runtime=None):  # type: ignore[no-untyped-def]
        calls["n"] += 1
        sp = st._states[slot_id]
        key = (sp.checks_done, sp.items_received)
        # mirror reachable.py: record the snapshot this compute used
        reachable._reachable_cache[slot_id] = (key, {"counts": {"reachable_now": 1}})
        if calls["n"] == 1:
            sp.checks_done = 8  # an item arrives DURING the first compute
        ev.set()  # let the loop iterate again
        return {"counts": {"reachable_now": 1}, "cached": False}, ""

    monkeypatch.setattr(loops, "_compute_reachable", fake_compute)

    task = asyncio.create_task(
        _reachable_sweep_loop(
            state, AsyncMock(), "run-1",
            asyncio.Semaphore(1), ev,
            notify_state_changed=AsyncMock(),
            initial_delay=0.0,
        )
    )
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Fix: last_computed = snapshot (5,0); state is now (8,0) -> re-swept (>= 2 calls).
    # Bug: last_computed = current (8,0) -> never re-swept -> exactly 1 call.
    assert calls["n"] >= 2
