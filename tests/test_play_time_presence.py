"""Presence the play time can trust (story 32.26).

A Part carries no tags, but its text names the kind of client that left; presence restarts from
nobody on every connection to the AP server; the server's own slot status (`_read_client_status`)
stands in for the clients that were already there; and the API heartbeat names the slots held.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from bridge.bridge import ArchipelagoClient, Config, StateManager


def _client() -> ArchipelagoClient:
    return ArchipelagoClient(Config(session_id="run-1", internal_token="t"), StateManager(), AsyncMock())


def _part(text: str, slot: int = 2) -> dict[str, Any]:
    return {"cmd": "PrintJSON", "type": "Part", "team": 0, "slot": slot, "data": [{"text": text}]}


async def _join(ap: ArchipelagoClient, slot: int, tags: list[str]) -> None:
    await ap._handle_print_json(
        {"cmd": "PrintJSON", "type": "Join", "team": 0, "slot": slot, "tags": tags, "data": [{"text": "joined"}]}
    )


# --- Part text ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "tags"),
    [
        ("Zoe (Team #1) has left the game. Client(0.6.7), ['AP', 'DeathLink'].", ["AP", "DeathLink"]),
        ("Zoe (Team #1) has stopped tracking the game. Client(0.6.7), ['Tracker'].", ["Tracker"]),
        ("Zoe (Team #1) has stopped viewing the game. Client(0.6.7), ['TextOnly'].", ["TextOnly"]),
        ("Zoe (Team #1) has stopped hinting the game. Client(0.6.7), ['HintGame'].", ["HintGame"]),
        ("Zoe (Team #1) has left the game. Client(0.6.7), [].", []),
    ],
)
def test_a_part_reads_the_tags_at_the_end_of_its_text(text: str, tags: list[str]) -> None:
    assert ArchipelagoClient._part_tags(_part(text)) == tags


@pytest.mark.parametrize(
    ("text", "tags"),
    [
        ("Zoe (Team #1) has stopped tracking the game.", ["Tracker"]),
        ("Zoe (Team #1) has stopped viewing the game.", ["TextOnly"]),
        ("Zoe (Team #1) has stopped hinting the game.", ["HintGame"]),
        ("Zoe (Team #1) has left the game.", []),
        ("something else entirely", []),
        ("Zoe has left the game. Client(0.6.7), [broken.", []),
    ],
)
def test_without_a_readable_tag_list_the_verb_decides(text: str, tags: list[str]) -> None:
    assert ArchipelagoClient._part_tags(_part(text)) == tags


@pytest.mark.asyncio
async def test_closing_a_tracker_leaves_the_game_client_holding_the_slot() -> None:
    ap = _client()
    await _join(ap, 2, ["AP", "DeathLink"])
    await _join(ap, 2, ["Tracker"])

    await ap._handle_print_json(_part("Zoe (Team #1) has stopped tracking the game. Client(0.6.7), ['Tracker']."))

    assert ap._connected_slots == {2}
    assert ap._state.ensure_slot(2).death_link_live is True

    await ap._handle_print_json(_part("Zoe (Team #1) has left the game. Client(0.6.7), ['AP', 'DeathLink']."))

    assert ap._connected_slots == set()


@pytest.mark.asyncio
async def test_the_leaving_client_takes_its_own_tags_with_it() -> None:
    """Two game clients on one slot: the one that leaves is the one whose tags the Part names."""
    ap = _client()
    await _join(ap, 2, ["AP", "DeathLink"])
    await _join(ap, 2, ["AP"])

    await ap._handle_print_json(_part("Zoe (Team #1) has left the game. Client(0.6.7), ['AP', 'DeathLink']."))

    assert ap._connected_slots == {2}
    assert ap._slot_client_tags[2] == [frozenset({"AP"})]
    assert ap._state.ensure_slot(2).death_link_live is False


# --- reset on (re)connection ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_server_restart_does_not_double_the_count() -> None:
    ap = _client()
    await _join(ap, 2, ["AP", "DeathLink"])

    assert ap._reset_presence() is True
    assert ap._connected_slots == set()
    assert ap._state.ensure_slot(2).death_link_live is None

    # The client comes back after the restart, then leaves: the slot is empty again.
    await _join(ap, 2, ["AP", "DeathLink"])
    await ap._handle_print_json(_part("Zoe (Team #1) has left the game. Client(0.6.7), ['AP', 'DeathLink']."))
    assert ap._connected_slots == set()


def test_resetting_nobody_is_not_a_change() -> None:
    assert _client()._reset_presence() is False


@pytest.mark.asyncio
async def test_a_lost_connection_forgets_who_was_there() -> None:
    ap = _client()
    await _join(ap, 2, ["AP"])
    broadcast = AsyncMock()
    ap._broadcast_state_changed = broadcast  # type: ignore[method-assign]

    await ap._forget_presence_after_disconnect()

    assert ap._connected_slots == set()
    broadcast.assert_awaited_once()


# --- server status resync -------------------------------------------------------------------------


@pytest.mark.parametrize("status", [10, 20])
def test_a_game_status_marks_an_unknown_slot_held_provisionally(status: int) -> None:
    ap = _client()

    assert ap._apply_server_client_status(3, status) is True
    assert ap._connected_slots == {3}
    assert 3 in ap._provisional_slots


@pytest.mark.parametrize("status", [5, 30, None, "x"])
def test_a_tracker_alone_or_a_finished_slot_is_not_held(status: Any) -> None:
    """CLIENT_CONNECTED is what a tracker alone leaves; CLIENT_GOAL stays once everybody left."""
    ap = _client()

    assert ap._apply_server_client_status(3, status) is False
    assert ap._connected_slots == set()


@pytest.mark.asyncio
async def test_status_zero_empties_the_slot_whatever_the_count() -> None:
    ap = _client()
    await _join(ap, 3, ["AP", "DeathLink"])
    await _join(ap, 3, ["AP"])

    assert ap._apply_server_client_status(3, 0) is True
    assert ap._connected_slots == set()
    assert ap._state.ensure_slot(3).death_link_live is None


def test_status_zero_on_an_empty_slot_is_not_a_change() -> None:
    assert _client()._apply_server_client_status(3, 0) is False


def test_the_bridge_own_slot_is_never_resynced() -> None:
    ap = _client()
    ap._my_slot = 1

    assert ap._apply_server_client_status(1, 20) is False
    assert ap._connected_slots == set()


@pytest.mark.asyncio
async def test_a_real_join_replaces_the_provisional_count() -> None:
    ap = _client()
    ap._apply_server_client_status(3, 20)

    await _join(ap, 3, ["AP"])

    assert ap._playing_clients[3] == 1
    assert 3 not in ap._provisional_slots

    await ap._handle_print_json(_part("Zoe (Team #1) has left the game. Client(0.6.7), ['AP'].", slot=3))
    assert ap._connected_slots == set()


@pytest.mark.asyncio
async def test_a_game_part_ends_a_provisional_slot() -> None:
    ap = _client()
    ap._apply_server_client_status(3, 20)

    await ap._handle_print_json(_part("Zoe (Team #1) has left the game. Client(0.6.7), ['AP'].", slot=3))

    assert ap._connected_slots == set()
    assert 3 not in ap._provisional_slots


@pytest.mark.asyncio
async def test_a_tracker_part_keeps_a_provisional_slot() -> None:
    ap = _client()
    ap._apply_server_client_status(3, 20)

    await ap._handle_print_json(
        _part("Zoe (Team #1) has stopped tracking the game. Client(0.6.7), ['Tracker'].", slot=3)
    )

    assert ap._connected_slots == {3}


@pytest.mark.asyncio
async def test_retrieved_and_set_reply_statuses_drive_presence() -> None:
    ap = _client()
    ap._team = 0
    ap._broadcast_state_changed = AsyncMock()  # type: ignore[method-assign]

    await ap._handle_packet({"cmd": "Retrieved", "keys": {"_read_client_status_0_3": 20, "_read_client_status_0_4": 5}})
    assert ap._connected_slots == {3}

    await ap._handle_packet({"cmd": "SetReply", "key": "_read_client_status_0_3", "value": 0})
    assert ap._connected_slots == set()


@pytest.mark.asyncio
async def test_connected_subscribes_to_every_slot_status() -> None:
    ap = _client()
    ws = AsyncMock()
    ap._ws = ws
    ap._load_spoiler = lambda: None  # type: ignore[method-assign]

    await ap._handle_connected({"slot": 1, "team": 0, "players": [], "slot_info": {"2": {"name": "Zoe"}}})

    sent = "".join(str(call.args[0]) for call in ws.send.await_args_list)
    assert '"cmd": "Get"' in sent and "_read_client_status_0_2" in sent
    assert '"cmd": "SetNotify"' in sent


# --- heartbeat ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_heartbeat_names_the_held_slots_by_their_registered_name() -> None:
    ap = _client()
    ap.ws_connected = True
    ap._store._slot_names = {2: "Zoe", 3: "Max"}
    ap._store._slot_aliases = {2: "Zozo", 3: "Max"}
    await _join(ap, 3, ["AP"])
    await _join(ap, 2, ["AP"])
    await _join(ap, 4, ["AP"])  # no registered name known: left out

    assert ap.heartbeat_presence() == [{"slot": 2, "name": "Zoe"}, {"slot": 3, "name": "Max"}]


@pytest.mark.asyncio
async def test_the_heartbeat_names_nobody_while_the_server_is_out_of_reach() -> None:
    ap = _client()
    ap._store._slot_names = {2: "Zoe"}
    await _join(ap, 2, ["AP"])
    ap.ws_connected = False

    assert ap.heartbeat_presence() == []
