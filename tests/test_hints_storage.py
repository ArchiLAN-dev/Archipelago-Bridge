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
async def test_setreply_announces_new_hints_on_feed_once() -> None:
    """Story 32.12: hints reach the feed through the storage path (the Hint PrintJSON only goes
    to the involved slots). The initial Retrieved snapshot seeds silently; a live SetReply
    announces only never-seen hints, exactly once."""
    ap, _ = _client()
    ap._broadcast_hints = AsyncMock()  # type: ignore[method-assign]
    emit = AsyncMock()
    ap._emit_feed = emit  # type: ignore[method-assign]

    # Initial Retrieved snapshot (announce=False): seeds the seen-set, no feed event.
    await ap._ingest_hint_storage(2, [_raw_hint(200)])
    emit.assert_not_awaited()

    # Live SetReply with one known + one new hint: only the new one is announced.
    await ap._ingest_hint_storage(2, [_raw_hint(200), _raw_hint(201, item=101)], announce=True)
    assert emit.await_count == 1
    event = emit.call_args.args[0]
    assert event["type"] == "hint"
    assert event["item"]["id"] == 101
    assert event["location"]["id"] == 201
    assert event["sender"]["slot"] == 1
    assert event["receiver"]["slot"] == 2

    # Re-delivery of the same list (e.g. found-status flip) announces nothing more.
    await ap._ingest_hint_storage(2, [_raw_hint(200), _raw_hint(201, item=101)], announce=True)
    assert emit.await_count == 1


@pytest.mark.asyncio
async def test_print_json_hint_shares_the_seen_set_with_storage() -> None:
    """A hint involving the bridge's own slot arrives on BOTH channels (PrintJSON + SetReply);
    whichever lands first wins, the other is skipped."""
    ap, _ = _client()
    ap._broadcast_hints = AsyncMock()  # type: ignore[method-assign]
    ap._broadcast_state_changed = AsyncMock()  # type: ignore[method-assign]
    ap._track_hint = AsyncMock()  # type: ignore[method-assign]
    emit = AsyncMock()
    ap._emit_feed = emit  # type: ignore[method-assign]

    # Storage announces first...
    await ap._ingest_hint_storage(2, [_raw_hint(200)], announce=True)
    assert emit.await_count == 1

    # ...then the same hint's PrintJSON arrives: no second feed event.
    await ap._handle_print_json({
        "type": "Hint",
        "receiving": 2,
        "item": {"player": 1, "location": 200, "item": 100, "flags": 0},
        "data": [{"type": "text", "text": "already announced"}],
    })
    assert emit.await_count == 1

    # The reverse order: a PrintJSON for a fresh hint emits and seeds the seen-set...
    await ap._handle_print_json({
        "type": "Hint",
        "receiving": 2,
        "item": {"player": 1, "location": 300, "item": 100, "flags": 0},
        "data": [{"type": "text", "text": "fresh"}],
    })
    assert emit.await_count == 2

    # ...so the follow-up SetReply stays silent.
    await ap._ingest_hint_storage(2, [_raw_hint(200), _raw_hint(300)], announce=True)
    assert emit.await_count == 2


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
