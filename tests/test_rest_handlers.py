"""Behavior tests for extracted REST handlers."""
from __future__ import annotations

import collections
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.routing import APIRoute
from httpx import ASGITransport, AsyncClient

from bridge.bridge import (
    ArchipelagoClient,
    Config,
    HintInfo,
    StateManager,
    create_app,
)
from bridge.core import rest_reachable
from bridge.core.ap_client import SelfHintOutcome


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config(**overrides: object) -> Config:
    defaults: dict[str, object] = {
        "internal_token": "test-token",
    }
    defaults.update(overrides)
    return Config(  # type: ignore[arg-type]
        session_id="run-1",
        **defaults,
    )


def _make_app(config: Config | None = None) -> tuple[object, StateManager, ArchipelagoClient]:
    cfg = config or _config()
    state = StateManager()
    broadcast = AsyncMock()
    ap_client = ArchipelagoClient(cfg, state, broadcast)
    app = create_app(state, ap_client)
    return app, state, ap_client


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health_returns_ok_when_ws_connected() -> None:
    app, _, ap_client = _make_app()
    ap_client.ws_connected = True

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["wsConnected"] is True


@pytest.mark.asyncio
async def test_health_returns_ok_when_ws_disconnected() -> None:
    app, _, ap_client = _make_app()
    ap_client.ws_connected = False

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["wsConnected"] is False


# ---------------------------------------------------------------------------
# post_command
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_post_command_success() -> None:
    app, _, ap_client = _make_app()
    ap_client.ws_connected = True
    ap_client.send_command = AsyncMock()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/commands", json={"command": "/say hello"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True

    ap_client.send_command.assert_awaited_once_with("/say hello")


@pytest.mark.asyncio
async def test_post_command_ws_disconnected_returns_503() -> None:
    app, _, ap_client = _make_app()
    ap_client.ws_connected = False

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/commands", json={"command": "/say hello"})
        assert resp.status_code == 503
        data = resp.json()
        assert data["error"] == "ws_disconnected"


# ---------------------------------------------------------------------------
# request_hint (legacy /hints/{slot}/request)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_request_hint_success_free() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True
    ap_client.send_admin_command = AsyncMock()
    ap_client._broadcast_hints = AsyncMock()
    # Populate store so location name lookup succeeds
    ap_client._store._slot_games[1] = "TestGame"
    ap_client._store._location_names["TestGame"] = {42: "Test Location"}
    ap_client._store._slot_aliases[1] = "TestPlayer"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/hints/1/request",
            json={"locationId": 42, "free": True},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["slot"] == 1
        assert data["locationId"] == 42
        assert data["free"] is True

    # Admin hint by location: the server resolves location → item (story 9.28)
    ap_client.send_admin_command.assert_awaited_once_with("!admin /hint_location TestPlayer Test Location")
    # Free/admin hints spend no player points, so the "indices demandés" counter must not move.
    assert state.ensure_slot(1).hints_used == 0


@pytest.mark.asyncio
async def test_request_hint_missing_location_id_returns_422() -> None:
    app, _, ap_client = _make_app()
    ap_client.ws_connected = True

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/hints/1/request", json={"free": True})
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_request_hint_non_integer_slot_returns_422() -> None:
    app, _, ap_client = _make_app()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/hints/abc/request", json={"locationId": 42})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Paid hints via connect-as-slot (story 9.30)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_request_hint_paid_connects_as_slot() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True
    ap_client.send_admin_command = AsyncMock()
    ap_client.run_self_hint = AsyncMock(return_value=SelfHintOutcome(ok=True))
    ap_client._broadcast_hints = AsyncMock()
    ap_client._store._slot_games[1] = "TestGame"
    ap_client._store._location_names["TestGame"] = {42: "Test Location"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/hints/1/request", json={"locationId": 42, "free": False})
        assert resp.status_code == 200

    # Paid path uses the player self-hint (charged), NOT the admin command.
    ap_client.run_self_hint.assert_awaited_once_with(1, "!hint_location Test Location")
    ap_client.send_admin_command.assert_not_awaited()
    # hints_used is bumped optimistically so "indices demandés" updates live; the budget
    # itself is driven by AP's authoritative hint_points (captured by run_self_hint).
    assert state.ensure_slot(1).hints_used == 1
    ap_client._broadcast_hints.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_request_hint_paid_rejected_returns_409() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True
    ap_client.run_self_hint = AsyncMock(
        return_value=SelfHintOutcome(ok=False, reason="rejected", message="not enough points")
    )
    ap_client._store._slot_games[1] = "TestGame"
    ap_client._store._location_names["TestGame"] = {42: "Test Location"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/hints/1/request", json={"locationId": 42, "free": False})
        assert resp.status_code == 409

    assert state.ensure_slot(1).hints_used == 0  # not charged on failure


@pytest.mark.asyncio
async def test_request_hint_item_free_uses_admin_command() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True
    ap_client.send_admin_command = AsyncMock()
    ap_client.run_self_hint = AsyncMock()
    ap_client._store._slot_aliases[1] = "TestPlayer"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/slots/1/hints/request-item", json={"itemName": "Sword", "free": True})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["itemName"] == "Sword"
        assert data["free"] is True

    ap_client.send_admin_command.assert_awaited_once_with("!admin /hint TestPlayer Sword")
    ap_client.run_self_hint.assert_not_awaited()
    # Free/admin item hints spend no player points: the counter stays put.
    assert state.ensure_slot(1).hints_used == 0


@pytest.mark.asyncio
async def test_request_hint_item_paid_connects_as_slot() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True
    ap_client.send_admin_command = AsyncMock()
    ap_client.run_self_hint = AsyncMock(return_value=SelfHintOutcome(ok=True))
    ap_client._broadcast_hints = AsyncMock()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/slots/1/hints/request-item", json={"itemName": "Boots", "free": False})
        assert resp.status_code == 200

    ap_client.run_self_hint.assert_awaited_once_with(1, "!hint Boots")
    ap_client.send_admin_command.assert_not_awaited()
    # Paid item hint bumps hints_used so "indices demandés" updates live (budget stays
    # driven by AP's authoritative hint_points).
    assert state.ensure_slot(1).hints_used == 1
    ap_client._broadcast_hints.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_request_hint_item_empty_name_returns_422() -> None:
    app, _, ap_client = _make_app()
    ap_client.ws_connected = True

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/slots/1/hints/request-item", json={"itemName": "  ", "free": False})
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_request_hint_item_ws_disconnected_returns_503() -> None:
    app, _, ap_client = _make_app()
    ap_client.ws_connected = False

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/slots/1/hints/request-item", json={"itemName": "Sword", "free": False})
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# get_feed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_feed_empty() -> None:
    app, _, _ = _make_app()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/feed")
        assert resp.status_code == 200
        data = resp.json()
        assert data["events"] == []


@pytest.mark.asyncio
async def test_get_feed_returns_events_newest_last() -> None:
    app, _, ap_client = _make_app()
    ap_client._feed_events = collections.deque(
        [{"type": "chat", "text": f"msg{i}"} for i in range(5)],
        maxlen=200,
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/feed?limit=3")
        assert resp.status_code == 200
        events = resp.json()["events"]
        assert len(events) == 3
        assert events[-1]["text"] == "msg4"


# ---------------------------------------------------------------------------
# get_data_package
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_data_package_index() -> None:
    app, _, ap_client = _make_app()
    ap_client._store._item_names["TestGame"] = {1: "Sword"}
    ap_client._store._location_names["TestGame"] = {100: "Cave"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/data-package")
        assert resp.status_code == 200
        assert "TestGame" in resp.json()["games"]


@pytest.mark.asyncio
async def test_get_data_package_game() -> None:
    app, _, ap_client = _make_app()
    ap_client._store._item_names["TestGame"] = {1: "Sword", 2: "Shield"}
    ap_client._store._location_names["TestGame"] = {100: "Cave"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/data-package/TestGame")
        assert resp.status_code == 200
        data = resp.json()
        assert data["game"] == "TestGame"
        assert data["items"]["1"] == "Sword"
        assert data["locations"]["100"] == "Cave"


@pytest.mark.asyncio
async def test_get_data_package_game_not_found() -> None:
    app, _, _ = _make_app()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/data-package/UnknownGame")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# get_reachable (legacy /reachable/{slot})
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_reachable_success() -> None:
    app, state, ap_client = _make_app()
    ap_client._broadcast_state_changed = AsyncMock()

    mock_result = {"player": "Tester", "counts": {"reachable_now": 5}, "cached": False}

    with patch.object(rest_reachable, "_compute_reachable", new=AsyncMock(return_value=(mock_result, ""))):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/reachable/1")
            assert resp.status_code == 200
            data = resp.json()
            assert data["player"] == "Tester"


@pytest.mark.asyncio
async def test_get_reachable_non_integer_slot_returns_422() -> None:
    app, _, _ = _make_app()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/reachable/abc")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Route parity test
# ---------------------------------------------------------------------------

def test_route_parity() -> None:
    app = create_app(MagicMock(), MagicMock())
    registered: set[tuple[str, str]] = set()
    for r in app.routes:
        if isinstance(r, APIRoute):
            for method in (r.methods or set()):
                if method not in ("HEAD", "OPTIONS"):
                    registered.add((method, r.path))

    required = {
        ("GET", "/health"),
        ("GET", "/room"),
        ("GET", "/slots"),
        ("GET", "/state"),
        ("GET", "/feed"),
        ("GET", "/data-package"),
        ("GET", "/data-package/{game}"),
        ("POST", "/commands"),
        ("POST", "/deathlink"),
        ("GET", "/slots/{slot}/hints"),
        ("POST", "/slots/{slot}/hints/request"),
        ("PATCH", "/slots/{slot}/hints/{location_id}"),
        ("GET", "/slots/{slot}/checks"),
        ("GET", "/slots/{slot}/items"),
        ("GET", "/slots/{slot}/reachable"),
        ("GET", "/slots/{slot}/item-locations"),
        # Legacy routes kept for backward compatibility
        ("GET", "/hints/{slot}"),
        ("POST", "/hints/{slot}/request"),
        ("GET", "/reachable/{slot}"),
        ("GET", "/item-locations/{slot}"),
    }
    assert required.issubset(registered), f"Missing routes: {required - registered}"


# ---------------------------------------------------------------------------
# update_hint_status (PATCH /slots/{slot}/hints/{location_id})
# ---------------------------------------------------------------------------

def _seed_hint(ap_client: ArchipelagoClient, state: StateManager, slot: int, location_id: int, status: int = 0) -> None:
    hint = HintInfo(
        receiving_player=slot,
        finding_player=slot,
        location_id=location_id,
        item_id=9001,
        entrance="",
        item_flags=0,
        status=status,
        item_name="Sword",
        location_name="Cave",
    )
    state.add_hint(slot, hint)


@pytest.mark.asyncio
async def test_update_hint_status_success() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True
    # Sent over a connect-as-slot connection (AP rejects UpdateHint from the main Bridge slot).
    ap_client.update_hint = AsyncMock(return_value=True)
    ap_client._broadcast_hints = AsyncMock()
    _seed_hint(ap_client, state, slot=1, location_id=42)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.patch("/slots/1/hints/42", json={"status": 30})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["slot"] == 1
        assert data["locationId"] == 42

    # (slot, receiving_player, location_id, status) - player is the hint's receiving player.
    ap_client.update_hint.assert_awaited_once_with(1, 1, 42, 30)
    ap_client._broadcast_hints.assert_awaited_once_with(1)

    hints = state.get_hints(1)
    assert hints[0].status == 30


@pytest.mark.asyncio
async def test_update_hint_status_ap_failure_returns_502() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True
    ap_client.update_hint = AsyncMock(return_value=False)
    ap_client._broadcast_hints = AsyncMock()
    _seed_hint(ap_client, state, slot=1, location_id=42, status=0)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.patch("/slots/1/hints/42", json={"status": 30})
        assert resp.status_code == 502

    # Local state must not be updated when AP rejected the change.
    assert state.get_hints(1)[0].status == 0
    ap_client._broadcast_hints.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_hint_status_invalid_status_returns_422() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True
    _seed_hint(ap_client, state, slot=1, location_id=42)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.patch("/slots/1/hints/42", json={"status": 99})
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_update_hint_status_hint_not_found_returns_404() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = True

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.patch("/slots/1/hints/999", json={"status": 30})
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_hint_status_ws_disconnected_returns_503() -> None:
    app, state, ap_client = _make_app()
    ap_client.ws_connected = False
    _seed_hint(ap_client, state, slot=1, location_id=42)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.patch("/slots/1/hints/42", json={"status": 10})
        assert resp.status_code == 503
        data = resp.json()
        assert data["error"] == "ws_disconnected"
