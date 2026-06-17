"""Tests for _build_feed_event - PrintJSON packet → WS feed event."""
from __future__ import annotations

import pytest

from bridge.core.ap_client import DataPackageStore, _build_feed_event


def _store_with_player(slot: int = 1, alias: str = "Alice") -> DataPackageStore:
    store = DataPackageStore()
    store.handle_connected({
        "players": [{"slot": slot, "alias": alias}],
        "slot_info": {},
    })
    return store


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ap_type,expected", [
    ("Chat",        "chat"),
    ("ItemSend",    "item_sent"),
    ("ItemCheat",   "item_sent"),
    ("Hint",        "hint"),
    ("ServerChat",  "system"),
    ("Tutorial",    "system"),
    ("Goal",        "goal"),
    ("Release",     "release"),
    ("Join",        "join"),
    ("Part",        "part"),
    ("Unknown",     "system"),
    ("",            "system"),
])
def test_type_mapping(ap_type: str, expected: str) -> None:
    store = DataPackageStore()
    packet = {"type": ap_type, "data": [{"type": "text", "text": "msg"}]}
    event = _build_feed_event(packet, store)
    assert event["type"] == expected


# ---------------------------------------------------------------------------
# Text assembly
# ---------------------------------------------------------------------------

def test_plain_text_assembled() -> None:
    store = DataPackageStore()
    packet = {
        "type": "Chat",
        "data": [
            {"type": "text", "text": "Hello "},
            {"type": "text", "text": "world"},
        ],
    }
    event = _build_feed_event(packet, store)
    assert event["text"] == "Hello world"


def test_player_id_resolved() -> None:
    store = _store_with_player(slot=2, alias="Bob")
    packet = {
        "type": "Chat",
        "data": [{"type": "player_id", "text": "2"}],
    }
    event = _build_feed_event(packet, store)
    assert event["text"] == "Bob"


def test_player_id_unknown_falls_back() -> None:
    store = DataPackageStore()
    packet = {
        "type": "Chat",
        "data": [{"type": "player_id", "text": "99"}],
    }
    event = _build_feed_event(packet, store)
    assert event["text"] == "Player 99"


def test_item_id_resolved() -> None:
    store = DataPackageStore()
    store.handle_data_package({
        "data": {
            "games": {
                "MyGame": {
                    "item_name_to_id": {"Magic Sword": 500},
                    "location_name_to_id": {},
                }
            }
        }
    })
    store.handle_connected({
        "players": [{"slot": 1, "alias": "P1"}],
        "slot_info": {"1": {"game": "MyGame"}},
    })
    packet = {
        "type": "ItemSend",
        "data": [{"type": "item_id", "text": "500", "player": "1"}],
    }
    event = _build_feed_event(packet, store)
    assert event["text"] == "Magic Sword"


def test_location_id_resolved() -> None:
    store = DataPackageStore()
    store.handle_data_package({
        "data": {
            "games": {
                "MyGame": {
                    "item_name_to_id": {},
                    "location_name_to_id": {"Old Ruins": 300},
                }
            }
        }
    })
    store.handle_connected({
        "players": [{"slot": 1, "alias": "P1"}],
        "slot_info": {"1": {"game": "MyGame"}},
    })
    packet = {
        "type": "ItemSend",
        "data": [{"type": "location_id", "text": "300", "player": "1"}],
    }
    event = _build_feed_event(packet, store)
    assert event["text"] == "Old Ruins"


def test_empty_data_falls_back_to_message_field() -> None:
    store = DataPackageStore()
    packet = {
        "type": "ServerChat",
        "data": [],
        "message": "Server says hi",
    }
    event = _build_feed_event(packet, store)
    assert event["text"] == "Server says hi"


def test_empty_data_falls_back_to_text_field() -> None:
    store = DataPackageStore()
    packet = {
        "type": "ServerChat",
        "data": [],
        "text": "Fallback text",
    }
    event = _build_feed_event(packet, store)
    assert event["text"] == "Fallback text"


def test_non_dict_parts_ignored() -> None:
    store = DataPackageStore()
    packet = {
        "type": "Chat",
        "data": ["not a dict", {"type": "text", "text": "valid"}],
    }
    event = _build_feed_event(packet, store)
    assert event["text"] == "valid"


# ---------------------------------------------------------------------------
# Color and timestamp
# ---------------------------------------------------------------------------

def test_color_preserved() -> None:
    store = DataPackageStore()
    packet = {"type": "Chat", "data": [], "message": "x", "color": "red"}
    event = _build_feed_event(packet, store)
    assert event["color"] == "red"


def test_default_color_white() -> None:
    store = DataPackageStore()
    packet = {"type": "Chat", "data": [], "message": "x"}
    event = _build_feed_event(packet, store)
    assert event["color"] == "white"


def test_timestamp_present_and_iso8601() -> None:
    store = DataPackageStore()
    packet = {"type": "Chat", "data": [], "message": "x"}
    event = _build_feed_event(packet, store)
    assert "timestamp" in event
    assert "T" in event["timestamp"]


# ---------------------------------------------------------------------------
# Structured item origin (story 29.4)
# ---------------------------------------------------------------------------

def _two_world_store() -> DataPackageStore:
    """Sender (slot 1) on Mario 64; receiver (slot 2) on Wind Waker."""
    store = DataPackageStore()
    store.handle_data_package({
        "data": {
            "games": {
                "Mario 64": {
                    "item_name_to_id": {},
                    "location_name_to_id": {"Bowser": 300},
                },
                "Wind Waker": {
                    "item_name_to_id": {"Master Sword": 500},
                    "location_name_to_id": {},
                },
            }
        }
    })
    store.handle_connected({
        "players": [
            {"slot": 1, "alias": "Michel_M"},
            {"slot": 2, "alias": "Pierre"},
        ],
        "slot_info": {
            "1": {"game": "Mario 64"},
            "2": {"game": "Wind Waker"},
        },
    })
    return store


def test_item_sent_attaches_structured_origin() -> None:
    store = _two_world_store()
    # NetworkItem: finder=slot 1 (Mario 64) checked location 300 (Bowser) holding item 500
    # (Master Sword, a slot-2 / Wind Waker item) for the receiving player slot 2.
    packet = {
        "type": "ItemSend",
        "receiving": 2,
        "item": {"player": 1, "location": 300, "item": 500, "flags": 1},
        "data": [{"type": "text", "text": "Michel_M found Master Sword for Pierre"}],
    }
    event = _build_feed_event(packet, store)

    assert event["type"] == "item_sent"
    assert event["item"] == {"id": 500, "name": "Master Sword"}
    assert event["location"] == {"id": 300, "name": "Bowser"}
    assert event["sender"] == {"slot": 1, "name": "Michel_M", "game": "Mario 64"}
    assert event["receiver"] == {"slot": 2, "name": "Pierre", "game": "Wind Waker"}
    # Prose text is preserved for backward compatibility.
    assert event["text"] == "Michel_M found Master Sword for Pierre"


def test_item_sent_without_network_item_omits_origin() -> None:
    store = _two_world_store()
    packet = {
        "type": "ItemSend",
        "data": [{"type": "text", "text": "legacy shape"}],
    }
    event = _build_feed_event(packet, store)

    assert event["type"] == "item_sent"
    assert event["text"] == "legacy shape"
    for key in ("item", "location", "sender", "receiver"):
        assert key not in event


def test_non_item_event_has_no_origin() -> None:
    store = _two_world_store()
    packet = {"type": "Chat", "data": [{"type": "text", "text": "hi"}]}
    event = _build_feed_event(packet, store)

    for key in ("item", "location", "sender", "receiver"):
        assert key not in event
