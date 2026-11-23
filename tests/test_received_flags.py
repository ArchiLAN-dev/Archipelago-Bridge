"""Story 27.14: the AP flags of received items, from the live packets and across a saved state."""
from __future__ import annotations

from bridge.core.domain import PlayerState
from bridge.core.state import StateManager


def test_live_items_record_their_flags_ored_per_item() -> None:
    state = StateManager()
    state.add_item_received(1, 101, 2, 401, 1)
    state.add_item_received(1, 101, 3, 402, 2)
    state.add_item_received(1, 102, 2, 403)

    ps = state.ensure_slot(1)
    assert ps._received_items == [(101, 2, 401), (101, 3, 402), (102, 2, 403)]
    assert ps.received_items_with_flags() == [[101, 2, 401, 3], [101, 3, 402, 3], [102, 2, 403, 0]]


def test_a_saved_state_brings_its_flags_along() -> None:
    state = StateManager()
    saved = PlayerState(slot_index=1)
    saved._received_items = [(101, 2, 401)]
    saved.items_received = 1
    saved.record_received_flags(101, 4)

    state.apply_saved_states({1: saved})

    assert state.ensure_slot(1).received_items_with_flags() == [[101, 2, 401, 4]]
