"""Story 52.3: what the Passeur needs to pick a chest that unblocks a player.

`GET /slots/{slot}/passeur/items` lists the items still owed to a slot that sit in another world's unopened
location, with their classification and their sphere. `POST /slots/{slot}/passeur/unlocks` asks the exact
tracker how many locations each of them would make reachable. The site picks the candidates; the bridge only
reports.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .ap_client import ArchipelagoClient
from .deps import get_ap_client, get_bridge_state, get_runtime, get_semaphore, require_auth
from .reachable import _reachable_cache, compute_reachable_with
from .state import StateManager

log = logging.getLogger("bridge.rest_passeur")

router = APIRouter(tags=["Passeur"], dependencies=[Depends(require_auth)])

# Every location of a world sits in exactly one of these lists of its tracker result.
_RESULT_LISTS = ("reachable_unchecked", "reachable_checked", "unreachable_unchecked", "checked_unreachable")

# A what-if is a full tracker run: the site asks for the few best candidates, not the whole spoiler.
MAX_UNLOCK_CANDIDATES = 10


class PasseurItem(BaseModel):
    locationId: int
    locationName: str
    findingSlot: int
    findingPlayerName: str
    itemId: int
    itemName: str
    flags: int
    sphere: int | None


class PasseurItemsResponse(BaseModel):
    slot: int
    items: list[PasseurItem]


class UnlockCandidate(BaseModel):
    itemId: int
    findingSlot: int
    locationId: int


class UnlocksRequest(BaseModel):
    candidates: list[UnlockCandidate] = Field(max_length=MAX_UNLOCK_CANDIDATES)


class Unlock(BaseModel):
    findingSlot: int
    locationId: int
    reachableNow: int


class UnlocksResponse(BaseModel):
    slot: int
    baseline: int
    unlocks: list[Unlock]


def _tracked_world(finder: int) -> tuple[dict[int, int], dict[int, int]]:
    """The flags and the sphere of each location of a world, from its last tracker result (empty if none)."""
    cached = _reachable_cache.get(finder)
    if cached is None:
        return {}, {}
    result = cached[1]
    flags: dict[int, int] = {}
    for list_name in _RESULT_LISTS:
        for check in result.get(list_name, []):
            item = check.get("item") or {}
            flags[int(check.get("id", 0))] = int(item.get("flags", 0))
    spheres: dict[int, int] = {}
    for sphere in result.get("spheres", []):
        for loc in sphere.get("locations", []):
            spheres[int(loc.get("id", 0))] = int(sphere.get("index", 0))
    return flags, spheres


@router.get("/slots/{slot}/passeur/items", response_model=PasseurItemsResponse)
async def get_passeur_items(
    slot: int,
    state: StateManager = Depends(get_bridge_state),
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> PasseurItemsResponse:
    """The slot's items not received yet, in another world's location nobody opened."""
    state.merge_state_from_save()
    ps = state._states.get(slot)
    received: set[tuple[int, int, int]] = set(ps._received_items) if ps else set()

    items: list[PasseurItem] = []
    for finder, loc_map in ap_client._placements.items():
        if finder == slot:
            continue
        finder_state = state._states.get(finder)
        opened: set[int] = finder_state._checked_locations if finder_state else set()
        flags, spheres = _tracked_world(finder)
        for location_id, (item_id, receiver) in loc_map.items():
            if receiver != slot or location_id in opened or (item_id, finder, location_id) in received:
                continue
            items.append(PasseurItem(
                locationId=location_id,
                locationName=ap_client._store.resolve_location(location_id, finder),
                findingSlot=finder,
                findingPlayerName=ap_client._store.resolve_player(finder),
                itemId=item_id,
                itemName=ap_client._store.resolve_item(item_id, slot),
                flags=flags.get(location_id, ap_client._store.resolve_item_flags(item_id)),
                sphere=spheres.get(location_id),
            ))

    items.sort(key=lambda i: (i.findingSlot, i.locationId))
    return PasseurItemsResponse(slot=slot, items=items)


@router.post("/slots/{slot}/passeur/unlocks", response_model=UnlocksResponse)
async def post_passeur_unlocks(
    slot: int,
    body: UnlocksRequest,
    state: StateManager = Depends(get_bridge_state),
    semaphore: asyncio.Semaphore = Depends(get_semaphore),
    runtime: Any = Depends(get_runtime),
) -> UnlocksResponse:
    """How many of the slot's locations are reachable with each candidate received, and without any."""
    state.merge_state_from_save()

    async def reachable_with(extra: list[tuple[int, int, int]]) -> int:
        result, err = await compute_reachable_with(slot, extra, state, semaphore, log, runtime)
        if result is None:
            raise HTTPException(status_code=503, detail={"code": "tracking_unavailable", "message": err})
        return int(result.get("counts", {}).get("reachable_now", 0))

    baseline = await reachable_with([])
    unlocks: list[Unlock] = []
    for candidate in body.candidates:
        reachable = await reachable_with([(candidate.itemId, candidate.findingSlot, candidate.locationId)])
        unlocks.append(Unlock(findingSlot=candidate.findingSlot, locationId=candidate.locationId, reachableNow=reachable))

    return UnlocksResponse(slot=slot, baseline=baseline, unlocks=unlocks)
