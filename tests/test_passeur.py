"""Story 52.3: the items a slot still waits for in other worlds, and what each one would unblock."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from bridge.bridge import ArchipelagoClient, Config, StateManager, create_app
from bridge.core import reachable, rest_passeur
from bridge.core.reachable import _reachable_cache

AUTH = {"Authorization": "Bearer secret"}


def _make_app() -> tuple[object, StateManager, ArchipelagoClient]:
    config = Config(session_id="run-1", internal_token="secret")
    state = StateManager()
    ap_client = ArchipelagoClient(config, state, AsyncMock())
    ap_client._store.handle_data_package({
        "data": {
            "games": {
                "HK": {"item_name_to_id": {"Claw": 101, "Geo": 102}, "location_name_to_id": {"Fungal": 201}},
                "ALTTP": {"item_name_to_id": {"Sword": 301}, "location_name_to_id": {"Link House": 401, "Pyramid": 402, "Dam": 403}},
            }
        }
    })
    ap_client._store.handle_connected({
        "players": [{"slot": 1, "alias": "Alice"}, {"slot": 2, "alias": "Bob"}],
        "slot_info": {"1": {"game": "HK"}, "2": {"game": "ALTTP"}},
    })
    # Bob's world holds two Claws and a Geo for Alice, and his own Sword.
    ap_client._placements[2] = {401: (101, 1), 402: (102, 1), 403: (301, 2)}
    ap_client._placements[1] = {201: (101, 1)}
    return create_app(state, ap_client), state, ap_client


def _seed_tracker(finder: int) -> None:
    def loc(loc_id: int, flags: int) -> dict[str, Any]:
        return {"id": loc_id, "name": f"#{loc_id}", "item": {"id": 0, "name": "", "flags": flags, "slot": 1}}

    _reachable_cache[finder] = ((0, 0), {
        "reachable_unchecked": [loc(401, 1)],
        "reachable_checked": [],
        "unreachable_unchecked": [loc(402, 0), loc(403, 1)],
        "checked_unreachable": [],
        "spheres": [
            {"index": 2, "locations": [{"id": 401}]},
            {"index": 5, "locations": [{"id": 402}, {"id": 403}]},
        ],
    })


@pytest.mark.asyncio
async def test_items_lists_what_the_slot_waits_for_in_other_worlds() -> None:
    _reachable_cache.clear()
    app, _, _ = _make_app()
    _seed_tracker(2)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/slots/1/passeur/items", headers=AUTH)

    assert resp.status_code == 200
    items = resp.json()["items"]
    assert [(i["findingSlot"], i["locationId"], i["itemName"], i["flags"], i["sphere"]) for i in items] == [
        (2, 401, "Claw", 1, 2),
        (2, 402, "Geo", 0, 5),
    ], "never the slot's own world, never an item for someone else"
    assert items[0]["findingPlayerName"] == "Bob"
    assert items[0]["locationName"] == "Link House"


@pytest.mark.asyncio
async def test_items_leave_out_opened_locations_and_received_items() -> None:
    _reachable_cache.clear()
    app, state, _ = _make_app()
    state.add_location_checks(2, [402])
    state.add_item_received(1, 101, 2, 401)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/slots/1/passeur/items", headers=AUTH)

    assert resp.status_code == 200
    assert resp.json()["items"] == []


@pytest.mark.asyncio
async def test_items_without_a_tracker_result_have_no_sphere() -> None:
    _reachable_cache.clear()
    app, _, _ = _make_app()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/slots/1/passeur/items", headers=AUTH)

    assert [i["sphere"] for i in resp.json()["items"]] == [None, None]


@pytest.mark.asyncio
async def test_routes_need_the_internal_token() -> None:
    app, _, _ = _make_app()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        assert (await c.get("/slots/1/passeur/items")).status_code == 401
        assert (await c.post("/slots/1/passeur/unlocks", json={"candidates": []})).status_code == 401


@pytest.mark.asyncio
async def test_unlocks_asks_the_tracker_with_each_candidate_received(monkeypatch: pytest.MonkeyPatch) -> None:
    app, _, _ = _make_app()
    asked: list[list[tuple[int, int, int]]] = []

    async def fake(slot: int, extra: list[tuple[int, int, int]], *args: Any) -> tuple[dict | None, str]:
        asked.append(extra)
        return {"counts": {"reachable_now": 3 + 10 * len(extra)}}, ""

    monkeypatch.setattr(rest_passeur, "compute_reachable_with", fake)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/slots/1/passeur/unlocks", headers=AUTH, json={"candidates": [{"itemId": 101, "findingSlot": 2, "locationId": 401}]})

    assert resp.status_code == 200
    assert resp.json() == {"slot": 1, "baseline": 3, "unlocks": [{"findingSlot": 2, "locationId": 401, "reachableNow": 13}]}
    assert asked == [[], [(101, 2, 401)]]


@pytest.mark.asyncio
async def test_unlocks_without_tracking_is_a_503(monkeypatch: pytest.MonkeyPatch) -> None:
    app, _, _ = _make_app()

    async def fake(*args: Any) -> tuple[dict | None, str]:
        return None, "no .archipelago file"

    monkeypatch.setattr(rest_passeur, "compute_reachable_with", fake)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/slots/1/passeur/unlocks", headers=AUTH, json={"candidates": []})

    assert resp.status_code == 503
    assert resp.json()["code"] == "tracking_unavailable"


@pytest.mark.asyncio
async def test_unlocks_refuses_too_many_candidates() -> None:
    app, _, _ = _make_app()
    candidates = [{"itemId": 101, "findingSlot": 2, "locationId": 400 + i} for i in range(rest_passeur.MAX_UNLOCK_CANDIDATES + 1)]

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/slots/1/passeur/unlocks", headers=AUTH, json={"candidates": candidates})

    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_a_what_if_adds_the_item_and_never_touches_the_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    (tmp_path / "AP_1.archipelago").write_bytes(b"")
    monkeypatch.setenv("AP_OUTPUT_DIR", str(tmp_path))
    sent: list[dict[str, Any]] = []

    async def fake_run(slot: int, payload: str, *args: Any) -> tuple[dict | None, str]:
        sent.append(json.loads(payload))
        return {"counts": {"reachable_now": 7}}, ""

    monkeypatch.setattr(reachable, "_run_reachable", fake_run)
    _reachable_cache.clear()
    state = StateManager()
    state.add_item_received(1, 101, 2, 401)

    result, err = await reachable.compute_reachable_with(1, [(102, 2, 402)], state, asyncio.Semaphore(1), logging.getLogger("t"))

    assert (result, err) == ({"counts": {"reachable_now": 7}}, "")
    assert sent[0]["received_items"] == [[101, 2, 401], [102, 2, 402]]
    assert _reachable_cache == {}
