"""Hint cost must be hint_cost% of a slot's REAL location count (spoiler placements),
not the full DataPackage size (every location the game defines). See the 915→132 fix."""
from __future__ import annotations

from unittest.mock import AsyncMock

from bridge.core.ap_client import ArchipelagoClient
from bridge.core.config import Config
from bridge.core.state import StateManager


def _client() -> tuple[ArchipelagoClient, StateManager]:
    state = StateManager()
    ap = ArchipelagoClient(Config(session_id="run-1", internal_token="secret"), state, AsyncMock())
    return ap, state


def test_hint_cost_uses_spoiler_placements_not_datapackage() -> None:
    ap, state = _client()
    # 10% hint cost from RoomInfo.
    state.handle_room_info({"hint_cost": 10, "location_check_points": 1})
    ap._store._slot_games[2] = "LuigisMansion"
    # DataPackage knows every location the game defines (915) ...
    ap._store._location_names["LuigisMansion"] = {i: f"Loc {i}" for i in range(915)}
    # ... but this seed places only 132 locations in slot 2's world.
    ap._placements[2] = {i: (i, 2) for i in range(132)}

    ap._apply_location_totals()

    assert state.get_all()[2].checks_total == 132
    assert state.get_all()[2].hint_cost == 13  # int(10% * 132), not 91


def test_hint_cost_falls_back_to_datapackage_before_spoiler() -> None:
    ap, state = _client()
    state.handle_room_info({"hint_cost": 10, "location_check_points": 1})
    ap._store._slot_games[2] = "LuigisMansion"
    ap._store._location_names["LuigisMansion"] = {i: f"Loc {i}" for i in range(915)}
    # No placements loaded yet → fallback to DataPackage size.

    ap._apply_location_totals()

    assert state.get_all()[2].checks_total == 915
    assert state.get_all()[2].hint_cost == 91
