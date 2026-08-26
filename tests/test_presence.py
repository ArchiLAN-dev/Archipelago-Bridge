"""Who is actually holding a slot (story 16.18).

Presence was seeded from ``Connected.players``, which is the whole multiworld roster
(``MultiServer.get_players_package``) and not the connected clients: every slot read as connected
from the moment the bridge attached. It only ever looked right because nothing contradicted it.

It becomes visible with an imported seed, where there is no observer slot to spare and the bridge
sits on a real player's - that player would look permanently connected. Join/Part is the only
per-client signal Archipelago sends, and it carries the client's tags, which is what lets the bridge
tell a game client from itself or from a tracker.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from bridge.bridge import ArchipelagoClient, Config, StateManager


def _client() -> ArchipelagoClient:
    return ArchipelagoClient(Config(session_id="run-1", internal_token="t"), StateManager(), AsyncMock())


def test_the_roster_alone_does_not_make_a_slot_connected() -> None:
    ap = _client()

    ap._store.handle_connected({"players": [{"slot": 1, "alias": "Alice"}, {"slot": 2, "alias": "Bob"}]})

    assert ap._connected_slots == set()


def test_a_game_client_joining_marks_its_slot() -> None:
    ap = _client()

    assert ap._client_joined(2, ["AP", "DeathLink"]) is True
    assert ap._connected_slots == {2}

    assert ap._client_left(2, ["AP", "DeathLink"]) is True
    assert ap._connected_slots == set()


def test_the_bridge_does_not_occupy_the_slot_it_attaches_to() -> None:
    """The bridge connects TextOnly. On an imported seed that is a real player's slot."""
    ap = _client()

    assert ap._client_joined(1, ["TextOnly"]) is False
    assert ap._connected_slots == set()


def test_a_tracker_beside_a_game_does_not_double_the_slot() -> None:
    ap = _client()

    ap._client_joined(3, ["AP"])
    ap._client_joined(3, ["Tracker"])
    assert ap._connected_slots == {3}

    # Closing the tracker must not empty the slot the player is still in.
    assert ap._client_left(3, ["Tracker"]) is False
    assert ap._connected_slots == {3}

    assert ap._client_left(3, ["AP"]) is True
    assert ap._connected_slots == set()


def test_two_players_on_one_slot_both_have_to_leave(
) -> None:
    """A co-op world is two game clients on one Archipelago slot (story 16.17)."""
    ap = _client()

    assert ap._client_joined(4, ["AP"]) is True
    assert ap._client_joined(4, ["AP"]) is False  # already occupied - no state change to broadcast
    assert ap._connected_slots == {4}

    assert ap._client_left(4, ["AP"]) is False
    assert ap._connected_slots == {4}
    assert ap._client_left(4, ["AP"]) is True
    assert ap._connected_slots == set()


def test_a_part_without_a_join_cannot_drive_the_count_negative() -> None:
    ap = _client()

    ap._client_left(5, ["AP"])
    ap._client_joined(5, ["AP"])

    assert ap._connected_slots == {5}


@pytest.mark.parametrize("tags", [["TextOnly"], ["Tracker"], ["HintGame"], ["AP", "Tracker"]])
def test_watching_tags_never_count_as_playing(tags: list[str]) -> None:
    ap = _client()

    ap._client_joined(6, tags)

    assert ap._connected_slots == set()


def test_slot_zero_is_ignored() -> None:
    """Slot 0 is Archipelago's server-side pseudo player, never a seat someone takes."""
    ap = _client()

    assert ap._client_joined(0, ["AP"]) is False
    assert ap._connected_slots == set()
