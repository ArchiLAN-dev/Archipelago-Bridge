from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from .ap_client import ArchipelagoClient
from .deps import get_ap_client, get_bridge_state, get_runtime, get_semaphore
from .reachable import _reachable_cache, start_reachable
from .schemas import ItemLocationResponse, ItemLocationsResponse
from .state import StateManager

log = logging.getLogger("bridge.rest_reachable")

_CHECK_STATUS: dict[str, str] = {
    "reachable_unchecked":   "reachable",
    "reachable_checked":     "checked",
    "unreachable_unchecked": "blocked",
    "checked_unreachable":   "checked",
}

router = APIRouter(tags=["Reachable"])

# How long a reachability request waits before answering 202 « computing » (story 17.28): a warm
# daemon answers within it, a daemon starting on a big multiworld does not hold the request.
REACHABLE_GRACE_SECONDS = 3.0


@router.get("/slots/{slot}/reachable", response_model=None)
@router.get("/reachable/{slot}", include_in_schema=False, response_model=None)
async def get_reachable(
    slot: int,
    state: StateManager = Depends(get_bridge_state),
    ap_client: ArchipelagoClient = Depends(get_ap_client),
    semaphore: asyncio.Semaphore = Depends(get_semaphore),
    runtime: Any = Depends(get_runtime),
) -> dict[str, Any] | JSONResponse:
    state.merge_state_from_save()
    # Story 17.28: a warm daemon answers within the grace; past it (a daemon starting on a big
    # multiworld), the request does not wait - the result goes to the page through the push.
    task = start_reachable(slot, state, semaphore, log, runtime)
    try:
        result, err_msg = await asyncio.wait_for(asyncio.shield(task), timeout=REACHABLE_GRACE_SECONDS)
    except asyncio.TimeoutError:
        previous = _reachable_cache.get(slot)
        ps = state._states.get(slot)
        payload = None
        if previous is not None:
            payload = {**previous[1], "cached": True}
            if ps is not None and ps.slot_name:
                payload["player"] = ps.slot_name
        return JSONResponse({"computing": True, "previous": payload}, status_code=202)

    if result is None:
        status = 504 if "timed out" in err_msg else 500
        raise HTTPException(status_code=status, detail=err_msg)

    ps = state._states.get(slot)
    if ps is not None:
        # Bridge slot_name (from AP Connected packet) is authoritative over the
        # reachability subprocess name (which may be the YAML file name, not the player alias).
        if ps.slot_name:
            result["player"] = ps.slot_name
        new_reachable = result.get("counts", {}).get("reachable_now", 0)
        if ps.reachable_now != new_reachable:
            ps.reachable_now = new_reachable
            await ap_client._broadcast_state_changed()

    return result


@router.get("/slots/{slot}/item-locations", response_model=ItemLocationsResponse)
@router.get("/item-locations/{slot}", response_model=ItemLocationsResponse, include_in_schema=False)
async def get_item_locations(
    slot: int,
    state: StateManager = Depends(get_bridge_state),
    ap_client: ArchipelagoClient = Depends(get_ap_client),
    semaphore: asyncio.Semaphore = Depends(get_semaphore),
    runtime: Any = Depends(get_runtime),
) -> ItemLocationsResponse:
    state.merge_state_from_save()

    # Story 17.28: never waits. The slots not computed yet are started (shared with the sweep);
    # their checks show up in the next answer, once in the cache.
    for s_missing in [s for s in list(state._states.keys()) if s not in _reachable_cache]:
        start_reachable(s_missing, state, semaphore, log, runtime)

    locations: list[ItemLocationResponse] = []
    for sender_slot, (_, result) in _reachable_cache.items():
        sender_name = ap_client._store.resolve_player(sender_slot)
        for list_name, check_status in _CHECK_STATUS.items():
            for check in result.get(list_name, []):
                item = check.get("item")
                if not item or item.get("slot") != slot:
                    continue
                locations.append(ItemLocationResponse(
                    itemId=item["id"],
                    itemName=item["name"],
                    locationId=check["id"],
                    locationName=check["name"],
                    findingPlayer=sender_slot,
                    findingPlayerName=sender_name,
                    checkStatus=check_status,
                ))

    return ItemLocationsResponse(slot=slot, locations=locations)
