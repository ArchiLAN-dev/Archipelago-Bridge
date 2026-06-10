"""Story 9.22 — ItemSend tracking uses the structured NetworkItem, not text parsing.

Proves checks are tracked even when the human-readable `data` parts are absent/unparseable
(the case that intermittently failed and left checks to surface only via the apsave).
"""
from __future__ import annotations

from unittest.mock import AsyncMock

from bridge.bridge import ArchipelagoClient, Config, StateManager


def _client() -> tuple[ArchipelagoClient, StateManager]:
    state = StateManager()
    ap = ArchipelagoClient(Config(session_id="run-1", internal_token="secret"), state, AsyncMock())
    return ap, state


def test_self_find_tracked_from_structured_item_without_text_parts() -> None:
    ap, state = _client()
    packet = {
        "cmd": "PrintJSON",
        "type": "ItemSend",
        "receiving": 1,
        "item": {"item": 101, "location": 201, "player": 1, "flags": 0},
        "data": [],  # no parseable text parts → only the structured path can resolve this
    }

    ap._track_item_send(packet)

    ps = state._states.get(1)
    assert ps is not None
    assert ps.checks_done == 1
    assert ps.items_received == 1


def test_cross_player_send_tracked_from_structured_item() -> None:
    ap, state = _client()
    packet = {
        "cmd": "PrintJSON",
        "type": "ItemSend",
        "receiving": 2,
        "item": {"item": 301, "location": 401, "player": 1, "flags": 0},
        "data": [],
    }

    ap._track_item_send(packet)

    assert state._states[1].checks_done == 1  # finder (slot 1) checked location 401
    assert state._states[2].items_received == 1  # receiver (slot 2) got the item


def test_idempotent_repeated_send_does_not_double_count() -> None:
    ap, state = _client()
    packet = {
        "cmd": "PrintJSON",
        "type": "ItemSend",
        "receiving": 1,
        "item": {"item": 101, "location": 201, "player": 1, "flags": 0},
        "data": [],
    }

    ap._track_item_send(packet)
    ap._track_item_send(packet)

    assert state._states[1].checks_done == 1
