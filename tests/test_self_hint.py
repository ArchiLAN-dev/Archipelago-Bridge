"""Unit tests for the paid connect-as-slot self-hint (story 9.30)."""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from bridge.core.ap_client import ArchipelagoClient, SelfHintOutcome
from bridge.core.config import Config
from bridge.core.state import StateManager


class _FakeWs:
    """Minimal async-context-manager WebSocket replaying queued inbound frames."""

    def __init__(self, incoming: list[Any]) -> None:
        self._incoming = [json.dumps(frame) for frame in incoming]
        self.sent: list[Any] = []

    async def __aenter__(self) -> "_FakeWs":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def recv(self) -> str:
        if not self._incoming:
            raise AssertionError("test consumed more frames than provided")
        return self._incoming.pop(0)

    async def send(self, data: str) -> None:
        self.sent.append(json.loads(data))


def _client(incoming: list[Any]) -> tuple[ArchipelagoClient, _FakeWs]:
    cfg = Config(  # type: ignore[call-arg]
        session_id="run-1",
        internal_token="test-token",
        ap_server_password="room-pw",
        ap_admin_password="admin-pw",
    )
    ap = ArchipelagoClient(cfg, StateManager(), AsyncMock())
    ap._store._slot_names[2] = "Amelia"
    ap._store._slot_games[2] = "TestGame"
    ws = _FakeWs(incoming)
    return ap, ws


_ROOM_INFO = [{"cmd": "RoomInfo"}]
_CONNECTED = [{"cmd": "Connected", "slot": 2}]


def _hint_frame(receiving: int, finding: int) -> list[dict[str, Any]]:
    return [{"cmd": "PrintJSON", "type": "Hint", "receiving": receiving,
             "item": {"item": 1, "location": 5, "player": finding, "flags": 0}}]


@pytest.mark.asyncio
async def test_run_self_hint_connects_as_slot_and_succeeds() -> None:
    ap, ws = _client([_ROOM_INFO, _CONNECTED, _hint_frame(receiving=2, finding=2)])

    with patch("bridge.core.ap_client.websockets.connect", return_value=ws):
        outcome = await ap.run_self_hint(2, "!hintSword")

    assert outcome == SelfHintOutcome(ok=True)
    # Connect uses the slot's registered name + game and the SERVER password (no admin login).
    connect = ws.sent[0][0]
    assert connect["cmd"] == "Connect"
    assert connect["name"] == "Amelia"
    assert connect["game"] == "TestGame"
    assert connect["password"] == "room-pw"
    assert connect["tags"] == ["TextOnly"]
    # The self-hint command is sent verbatim (no !admin).
    assert ws.sent[1][0] == {"cmd": "Say", "text": "!hintSword"}


@pytest.mark.asyncio
async def test_run_self_hint_rejected_when_server_refuses() -> None:
    reply = [{"cmd": "PrintJSON", "data": [{"text": "You can't afford the hint."}]}]
    ap, ws = _client([_ROOM_INFO, _CONNECTED, reply])

    with patch("bridge.core.ap_client.websockets.connect", return_value=ws):
        outcome = await ap.run_self_hint(2, "!hintSword")

    assert outcome.ok is False
    assert outcome.reason == "rejected"
    assert "afford" in outcome.message


@pytest.mark.asyncio
async def test_run_self_hint_unknown_slot_returns_unknown_slot() -> None:
    ap, ws = _client([])

    with patch("bridge.core.ap_client.websockets.connect", return_value=ws):
        outcome = await ap.run_self_hint(99, "!hintSword")

    assert outcome.ok is False
    assert outcome.reason == "unknown_slot"
    assert ws.sent == []  # never connected


@pytest.mark.asyncio
async def test_run_self_hint_connection_refused() -> None:
    refused = [{"cmd": "ConnectionRefused", "errors": ["InvalidSlot"]}]
    ap, ws = _client([_ROOM_INFO, refused])

    with patch("bridge.core.ap_client.websockets.connect", return_value=ws):
        outcome = await ap.run_self_hint(2, "!hintSword")

    assert outcome.ok is False
    assert outcome.reason == "refused"


@pytest.mark.asyncio
async def test_run_self_hint_captures_authoritative_hint_points() -> None:
    # AP reports points in Connected (pre-hint) and a RoomUpdate (post-hint); the latest wins.
    connected = [{"cmd": "Connected", "slot": 2, "hint_points": 52}]
    room_update = [{"cmd": "RoomUpdate", "hint_points": 39}]
    ap, ws = _client([_ROOM_INFO, connected, room_update, _hint_frame(receiving=2, finding=2)])

    with patch("bridge.core.ap_client.websockets.connect", return_value=ws):
        outcome = await ap.run_self_hint(2, "!hint Sword")

    assert outcome.ok is True
    assert ap._state.ensure_slot(2).hint_points_reported == 39
    assert ap._state.ensure_slot(2).hint_points_available == 39


@pytest.mark.asyncio
async def test_fetch_hint_points_reads_connected() -> None:
    connected = [{"cmd": "Connected", "slot": 2, "hint_points": 52}]
    ap, ws = _client([_ROOM_INFO, connected])

    with patch("bridge.core.ap_client.websockets.connect", return_value=ws):
        pts = await ap.fetch_hint_points(2)

    assert pts == 52
    assert ap._state.ensure_slot(2).hint_points_reported == 52
    # No Say sent - read-only probe.
    assert all(s[0].get("cmd") != "Say" for s in ws.sent)


@pytest.mark.asyncio
async def test_fetch_hint_points_unknown_slot_returns_none() -> None:
    ap, ws = _client([])
    with patch("bridge.core.ap_client.websockets.connect", return_value=ws):
        assert await ap.fetch_hint_points(99) is None
    assert ws.sent == []


@pytest.mark.asyncio
async def test_connect_as_slot_sets_authoritative_hint_cost() -> None:
    # AP's Connected lists the slot's REAL locations (checked + missing = 130); at 10% the
    # authoritative cost is 13 - overriding any inflated spoiler/DataPackage estimate (story 9.32).
    connected = [{
        "cmd": "Connected", "slot": 2, "hint_points": 52,
        "checked_locations": list(range(30)),
        "missing_locations": list(range(30, 130)),
    }]
    ap, ws = _client([_ROOM_INFO, connected])
    ap._state.handle_room_info({"hint_cost": 10, "location_check_points": 1})

    with patch("bridge.core.ap_client.websockets.connect", return_value=ws):
        await ap.fetch_hint_points(2)

    assert ap._state.ensure_slot(2).checks_total == 130
    assert ap._state.ensure_slot(2).hint_cost == 13  # int(10% * 130), not a DataPackage estimate
