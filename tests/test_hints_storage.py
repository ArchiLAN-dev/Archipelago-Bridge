"""Story 9.27: the bridge subscribes to the AP hint data storage
(_read_hints_{team}_{slot}) so hints for ALL slots arrive live, not only the
slot it is connected as. Retrieved/SetReply payloads are ingested, the slot's
hint list is replaced, and a push happens only when the list actually changed."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from bridge.core.ap_client import ArchipelagoClient
from bridge.core.config import Config
from bridge.core.domain import HintInfo
from bridge.core.state import StateManager


def _client() -> tuple[ArchipelagoClient, StateManager]:
    state = StateManager()
    ap = ArchipelagoClient(Config(session_id="run-1", internal_token="secret"), state, AsyncMock())
    return ap, state


def _raw_hint(location: int, item: int = 100, receiving: int = 2, finding: int = 1) -> dict[str, object]:
    return {
        "receiving_player": receiving,
        "finding_player": finding,
        "item": item,
        "location": location,
        "item_flags": 0,
        "found": False,
        "status": 0,
    }


def test_hint_storage_key_roundtrip() -> None:
    ap, _ = _client()
    ap._team = 0
    key = ap._hint_storage_key(2)
    assert key == "_read_hints_0_2"
    assert ap._slot_from_hint_key(key) == 2
    assert ap._slot_from_hint_key("_read_hints_0_x") is None
    assert ap._slot_from_hint_key("_read_race_mode") is None


@pytest.mark.asyncio
async def test_ingest_storage_adds_hints_and_pushes_once() -> None:
    ap, state = _client()
    broadcast = AsyncMock()
    ap._broadcast_hints = broadcast  # type: ignore[method-assign]

    await ap._ingest_hint_storage(2, [_raw_hint(200), _raw_hint(201)])

    assert len(state.get_hints(2)) == 2
    broadcast.assert_awaited_once_with(2)

    # Re-ingesting the identical list must NOT push again (no spurious churn).
    broadcast.reset_mock()
    await ap._ingest_hint_storage(2, [_raw_hint(200), _raw_hint(201)])
    broadcast.assert_not_awaited()

    # A new hint in the list does push.
    await ap._ingest_hint_storage(2, [_raw_hint(200), _raw_hint(201), _raw_hint(202)])
    assert len(state.get_hints(2)) == 3
    broadcast.assert_awaited_once_with(2)


@pytest.mark.asyncio
async def test_ingest_storage_ignores_malformed_payload() -> None:
    ap, state = _client()
    broadcast = AsyncMock()
    ap._broadcast_hints = broadcast  # type: ignore[method-assign]

    await ap._ingest_hint_storage(2, None)  # not a list
    await ap._ingest_hint_storage(2, ["nonsense", 42, {}])  # no valid hint dicts
    await ap._ingest_hint_storage(2, [{"receiving_player": 0, "item": 0, "location": 0}])  # incomplete

    assert state.get_hints(2) == []
    broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_broadcast_resolves_save_derived_id_only_names() -> None:
    """The apsave reconcile overwrites ps._hints with id-only hints (empty names). A live push
    must resolve them, like GET does - otherwise the UI shows 'Item #123'/'Location #456'."""
    ap, state = _client()
    ap._broadcast = AsyncMock()  # type: ignore[method-assign]
    ap._store._slot_games[2] = "TestGame"
    ap._store._slot_games[1] = "TestGame"
    ap._store._item_names["TestGame"] = {100: "Boo Radar"}
    ap._store._location_names["TestGame"] = {200: "Foyer Chest"}
    ap._store._slot_aliases.update({1: "Finder", 2: "Receiver"})

    # Simulate apply_saved_states: a hint with ids but no resolved names.
    state.set_hints(2, [HintInfo(
        receiving_player=2, finding_player=1, location_id=200, item_id=100,
        entrance="", item_flags=0, status=0,
    )])

    await ap._broadcast_hints(2)

    payload = ap._broadcast.call_args.args[1]
    hint = payload["hints"][0]
    assert hint["itemName"] == "Boo Radar"
    assert hint["locationName"] == "Foyer Chest"
    assert hint["receivingPlayerName"] == "Receiver"
