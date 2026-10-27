"""Who plays with DeathLink (story 9.59).

Live, from the tags each client announces: Join carries them, TagsChanged follows a client that turns
DeathLink on or off mid-game (ConnectUpdate), Part carries none. And the setting the seed was
generated with, from the spoiler header, for a player who is not connected.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from bridge.bridge import ArchipelagoClient, Config, StateManager


def _client() -> ArchipelagoClient:
    return ArchipelagoClient(Config(session_id="run-1", internal_token="t"), StateManager(), AsyncMock())


async def _print(ap: ArchipelagoClient, msg_type: str, slot: int, tags: list[str] | None = None) -> None:
    packet: dict[str, object] = {"cmd": "PrintJSON", "type": msg_type, "team": 0, "slot": slot, "data": []}
    if tags is not None:
        packet["tags"] = tags
    await ap._handle_print_json(packet)


def _live(ap: ArchipelagoClient, slot: int) -> bool | None:
    return ap._state.ensure_slot(slot).death_link_live


@pytest.mark.asyncio
async def test_nobody_connected_means_unknown() -> None:
    ap = _client()

    assert ap._state.ensure_slot(2).to_dict()["death_link_live"] is None


@pytest.mark.asyncio
async def test_a_client_joining_with_the_tag_plays_with_death_link() -> None:
    ap = _client()
    ap._broadcast_state_changed = AsyncMock()  # type: ignore[method-assign]

    await _print(ap, "Join", 2, ["AP", "DeathLink"])

    assert _live(ap, 2) is True
    ap._broadcast_state_changed.assert_awaited()

    await _print(ap, "Part", 2)

    assert _live(ap, 2) is None


@pytest.mark.asyncio
async def test_a_client_joining_without_the_tag_plays_without() -> None:
    ap = _client()

    await _print(ap, "Join", 3, ["AP"])

    assert _live(ap, 3) is False


@pytest.mark.asyncio
async def test_turning_death_link_on_and_off_mid_game_is_followed() -> None:
    ap = _client()
    await _print(ap, "Join", 2, ["AP"])
    ap._broadcast_state_changed = AsyncMock()  # type: ignore[method-assign]

    await _print(ap, "TagsChanged", 2, ["AP", "DeathLink"])
    assert _live(ap, 2) is True
    ap._broadcast_state_changed.assert_awaited_once()

    await _print(ap, "TagsChanged", 2, ["AP"])
    assert _live(ap, 2) is False


@pytest.mark.asyncio
async def test_unchanged_death_link_does_not_push() -> None:
    ap = _client()
    await _print(ap, "Join", 2, ["AP", "DeathLink"])
    ap._broadcast_state_changed = AsyncMock()  # type: ignore[method-assign]

    await _print(ap, "TagsChanged", 2, ["AP", "DeathLink", "Other"])

    assert _live(ap, 2) is True
    ap._broadcast_state_changed.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_tracker_beside_the_game_does_not_change_it() -> None:
    ap = _client()
    await _print(ap, "Join", 4, ["AP", "DeathLink"])

    await _print(ap, "Join", 4, ["Tracker"])
    await _print(ap, "TagsChanged", 4, ["Tracker", "DeathLink"])

    assert _live(ap, 4) is True


@pytest.mark.asyncio
async def test_two_clients_on_a_slot() -> None:
    """A co-op slot (story 16.17): DeathLink while either client has it."""
    ap = _client()
    await _print(ap, "Join", 5, ["AP"])
    await _print(ap, "Join", 5, ["AP", "DeathLink"])
    assert _live(ap, 5) is True

    # The one that turns it off is the one that differs by DeathLink alone.
    await _print(ap, "TagsChanged", 5, ["AP"])
    assert _live(ap, 5) is False

    await _print(ap, "Part", 5)
    assert _live(ap, 5) is False
    await _print(ap, "Part", 5)
    assert _live(ap, 5) is None


@pytest.mark.asyncio
async def test_tags_changed_from_a_client_the_bridge_never_saw_join_is_ignored() -> None:
    """It joined before the bridge attached: presence never counted it."""
    ap = _client()

    await _print(ap, "TagsChanged", 6, ["AP", "DeathLink"])

    assert _live(ap, 6) is None


SPOILER = """Archipelago Version 0.6.4  -  Seed: 123

Filling Algorithm:               balance
Players:                         3

Player 1: Alice
Game:                            A Link to the Past
Death Link:                      Yes

Player 2: Bob
Game:                            Hollow Knight
DeathLink:                       No

Player 3: Carol
Game:                            Celeste
Death_Link:                      Receive Only

Locations:

Mushroom (Alice): Death Link (Bob)
"""


def test_the_spoiler_gives_each_player_setting() -> None:
    assert ArchipelagoClient.death_link_options(SPOILER) == {1: True, 2: False, 3: True}


def test_a_one_player_seed_belongs_to_slot_1() -> None:
    spoiler = "Archipelago Version 0.6.4  -  Seed: 1\n\nGame:   Celeste\nDeath Link:   Off\n"

    assert ArchipelagoClient.death_link_options(spoiler) == {1: False}


def test_a_world_without_the_option_is_unknown() -> None:
    spoiler = "Player 1: Alice\nGame:  Minecraft\nGoal:  Ender Dragon\n\nPlayer 2: Bob\nDeath Link:  Yes\n"

    assert ArchipelagoClient.death_link_options(spoiler) == {2: True}


def test_loading_the_spoiler_sets_each_slot_setting(tmp_path: Path) -> None:
    with zipfile.ZipFile(tmp_path / "AP_123.zip", "w") as z:
        z.writestr("AP_123_Spoiler.txt", SPOILER)
    state = StateManager()
    ap = ArchipelagoClient(Config(session_id="run-1", internal_token="t", save_dir=str(tmp_path)), state, AsyncMock())

    ap._load_spoiler()

    slots = state.to_api_dict()["slots"]
    assert [slots[k]["death_link_option"] for k in ("1", "2", "3")] == [True, False, True]


# ── Story 32.25: the deaths themselves ────────────────────────────────────────


def test_the_bridge_carries_the_death_link_tag_but_stays_text_only() -> None:
    """The server only bounces a death to clients with the tag; TextOnly keeps the bridge out of presence."""
    assert set(ArchipelagoClient.CONNECT_TAGS) == {"TextOnly", "DeathLink"}
    assert not ArchipelagoClient._is_playing_client(list(ArchipelagoClient.CONNECT_TAGS))


def _roster(ap: ArchipelagoClient) -> None:
    ap._store.handle_connected({
        "players": [{"slot": 1, "alias": "Alice"}, {"slot": 2, "alias": "Bob"}, {"slot": 3, "alias": "Carol"}],
        "slot_info": {"1": {"name": "alice_slot", "game": "Celeste"}, "2": {"name": "Bob", "game": "Hollow Knight"},
                      "3": {"name": "Carol", "game": "Terraria"}},
    })


async def _death(ap: ArchipelagoClient, source: str) -> dict[str, object]:
    ap._emit_feed = AsyncMock()  # type: ignore[method-assign]
    await ap._handle_bounced({"cmd": "Bounced", "tags": ["DeathLink"], "data": {"source": source, "cause": "Spikes"}})
    event: dict[str, object] = ap._emit_feed.await_args.args[0]
    return event


@pytest.mark.asyncio
async def test_a_death_names_its_slot_and_its_probable_victims() -> None:
    ap = _client()
    _roster(ap)
    await _print(ap, "Join", 1, ["AP", "DeathLink"])
    await _print(ap, "Join", 2, ["AP", "DeathLink"])
    await _print(ap, "Join", 3, ["AP"])

    event = await _death(ap, "Alice")

    assert event["type"] == "death_link"
    assert event["source"] == "Alice"
    assert event["cause"] == "Spikes"
    assert event["sender"] == {"slot": 1, "name": "Alice", "game": "Celeste"}
    # Carol plays without DeathLink, Alice is the one who died.
    assert event["victims"] == ["Bob"]


@pytest.mark.asyncio
async def test_a_source_given_as_the_slot_name_is_resolved_too() -> None:
    ap = _client()
    _roster(ap)

    event = await _death(ap, "alice_slot")

    assert event["sender"] == {"slot": 1, "name": "Alice", "game": "Celeste"}
    assert event["victims"] == []


@pytest.mark.asyncio
async def test_an_unknown_source_keeps_the_death_without_a_slot() -> None:
    """The admin's POST /deathlink, or a client naming itself oddly."""
    ap = _client()
    _roster(ap)
    await _print(ap, "Join", 2, ["AP", "DeathLink"])

    event = await _death(ap, "ArchiLAN")

    assert "sender" not in event
    assert event["victims"] == ["Bob"]
