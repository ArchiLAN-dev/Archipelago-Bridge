"""Story 52.1: open one location of a slot as though it checked it (`/send_location`, the Passeur)."""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from bridge.core.ap_client import ArchipelagoClient, SendLocationOutcome
from bridge.core.config import Config
from bridge.core.rest import create_app
from bridge.core.state import StateManager

AUTH = {"Authorization": "Bearer test-token"}

# Slot 1 "Bob le Bricoleur" is absent; location 501 of his world holds item 77 for slot 2 "Alice".
ABSENT, PRESENT, LOCATION, ITEM = 1, 2, 501, 77


def _client() -> tuple[ArchipelagoClient, StateManager]:
    cfg = Config(  # type: ignore[call-arg]
        session_id="run-1",
        internal_token="test-token",
        ap_server_password="room-pw",
        ap_admin_password="admin-pw",
    )
    state = StateManager()
    ap = ArchipelagoClient(cfg, state, AsyncMock())
    ap.ws_connected = True
    ap._store._slot_names[ABSENT] = "Bob le Bricoleur"
    ap._store._slot_names[PRESENT] = "Alice"
    ap._placements = {ABSENT: {LOCATION: (ITEM, PRESENT), 502: (78, ABSENT)}}
    return ap, state


def _item_send(finder: int, location: int, item: int, receiver: int) -> dict[str, Any]:
    return {"cmd": "PrintJSON", "type": "ItemSend", "receiving": receiver,
            "item": {"item": item, "location": location, "player": finder, "flags": 1}, "data": []}


def _answer_with(ap: ArchipelagoClient, packet: dict[str, Any]) -> list[str]:
    """Make the server answer the admin command with `packet`; returns the texts the bridge said."""
    said: list[str] = []

    async def fake_admin(command: str) -> None:
        said.append(command)
        await ap._handle_print_json(packet)

    ap.send_admin_command = fake_admin  # type: ignore[method-assign]
    return said


@pytest.mark.asyncio
async def test_sends_by_location_id_with_the_quoted_player_name() -> None:
    ap, state = _client()
    said = _answer_with(ap, _item_send(ABSENT, LOCATION, ITEM, PRESENT))

    outcome = await ap.send_location(ABSENT, LOCATION)

    assert outcome == SendLocationOutcome(status="sent", item_id=ITEM, receiving_slot=PRESENT)
    assert said == ["!admin /send_location 'Bob le Bricoleur' 501"]
    # The ItemSend went through the usual tracking: checked for the absent, received by the present.
    assert LOCATION in state._states[ABSENT]._checked_locations
    assert ap._send_location_waiter is None


@pytest.mark.asyncio
async def test_refuses_before_sending_what_the_server_would_silently_ignore() -> None:
    ap, state = _client()
    said = _answer_with(ap, _item_send(ABSENT, LOCATION, ITEM, PRESENT))

    assert (await ap.send_location(ABSENT, 999)).status == "unknown_location"
    assert (await ap.send_location(PRESENT, LOCATION)).status == "unknown_location"
    assert (await ap.send_location(42, LOCATION)).status == "unknown_slot"

    state.add_location_checks(ABSENT, [LOCATION])
    assert (await ap.send_location(ABSENT, LOCATION)).status == "already_checked"
    assert said == []

    ap.ws_connected = False
    assert (await ap.send_location(ABSENT, 502)).status == "ws_disconnected"


@pytest.mark.asyncio
async def test_an_admin_command_result_is_a_refusal() -> None:
    ap, _ = _client()
    _answer_with(ap, {"cmd": "PrintJSON", "type": "AdminCommandResult",
                      "data": [{"text": "Can't look up location for unknown game. Send by ID instead."}]})

    outcome = await ap.send_location(ABSENT, LOCATION)

    assert outcome.status == "refused"
    assert "unknown game" in outcome.message


@pytest.mark.asyncio
async def test_another_items_send_does_not_settle_it_and_it_times_out() -> None:
    ap, _ = _client()
    _answer_with(ap, _item_send(PRESENT, 9, 1, ABSENT))

    outcome = await ap.send_location(ABSENT, LOCATION, timeout=0.05)

    assert outcome.status == "timeout"
    assert ap._send_location_waiter is None


@pytest.mark.asyncio
async def test_two_calls_run_one_after_the_other() -> None:
    ap, _ = _client()
    order: list[str] = []

    async def fake_admin(command: str) -> None:
        order.append(command)
        await asyncio.sleep(0)
        location = int(command.rsplit(" ", 1)[1])
        item, receiver = ap._placements[ABSENT][location]
        await ap._handle_print_json(_item_send(ABSENT, location, item, receiver))

    ap.send_admin_command = fake_admin  # type: ignore[method-assign]
    first, second = await asyncio.gather(ap.send_location(ABSENT, LOCATION), ap.send_location(ABSENT, 502))

    assert (first.status, second.status) == ("sent", "sent")
    assert second.receiving_slot == ABSENT
    assert len(order) == 2


@pytest.mark.asyncio
async def test_rest_route_requires_the_token_and_maps_the_outcome() -> None:
    ap, state = _client()
    app = create_app(state, ap)
    _answer_with(ap, _item_send(ABSENT, LOCATION, ITEM, PRESENT))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        assert (await c.post(f"/slots/{ABSENT}/locations/{LOCATION}/send")).status_code == 401

        resp = await c.post(f"/slots/{ABSENT}/locations/{LOCATION}/send", headers=AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert (body["slot"], body["locationId"], body["itemId"], body["receivingSlot"]) == (ABSENT, LOCATION, ITEM, PRESENT)
        assert body["receivingPlayerName"] != ""

        again = await c.post(f"/slots/{ABSENT}/locations/{LOCATION}/send", headers=AUTH)
        assert again.status_code == 409
        assert again.json()["code"] == "already_checked"

        missing = await c.post(f"/slots/{ABSENT}/locations/999/send", headers=AUTH)
        assert missing.status_code == 404
        assert missing.json()["code"] == "unknown_location"
