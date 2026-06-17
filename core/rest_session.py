from __future__ import annotations

import logging
from datetime import datetime
from datetime import timezone as _timezone
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query

from .ap_client import ArchipelagoClient
from .deps import get_ap_client, require_auth
from .reachable import _reachable_cache
from .schemas import (
    CommandRequest,
    DeathLinkRequest,
    HealthResponse,
    LocationPlacementResponse,
    OkResponse,
    SlotDetailResponse,
    SphereResponse,
    SpheresResponse,
)

log = logging.getLogger("bridge.rest_session")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(tags=["Session"])


# ---------------------------------------------------------------------------
# Route handlers - public
# ---------------------------------------------------------------------------

@router.get("/health", response_model=HealthResponse)
async def health(
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> HealthResponse:
    return HealthResponse(
        status="ok",
        wsConnected=ap_client.ws_connected,
        sessionId=ap_client._config.session_id,
    )


@router.get("/room")
async def get_room(
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> dict[str, Any]:
    return ap_client.get_room_dict()


@router.get("/slots")
async def get_slots(
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> dict[str, Any]:
    return {"slots": ap_client.get_slots_summary()}


@router.get("/state")
async def get_state(
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> dict[str, Any]:
    # Enriched with per-slot game + type so clients can filter out the TextOnly "Bridge" observer.
    return ap_client.get_players_state()


@router.post("/commands", response_model=OkResponse)
async def post_command(
    body: CommandRequest,
    ap_client: ArchipelagoClient = Depends(get_ap_client),
    x_ap_admin_password: str = Header(default=""),
) -> OkResponse:
    if not ap_client.ws_connected:
        raise HTTPException(status_code=503, detail="ws_disconnected")
    if x_ap_admin_password:
        await ap_client.send_command(f"!admin login {x_ap_admin_password}")
    await ap_client.send_command(body.command)
    return OkResponse()


@router.get("/feed")
async def get_feed(
    ap_client: ArchipelagoClient = Depends(get_ap_client),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    return {"events": ap_client.get_feed(limit)}


@router.get("/data-package")
async def get_data_package_index(
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> dict[str, Any]:
    return {"games": ap_client.list_data_package_games()}


@router.get("/data-package/{game}")
async def get_data_package_game(
    game: str,
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> dict[str, Any]:
    result = ap_client.get_data_package(game)
    if result is None:
        raise HTTPException(status_code=404, detail=f"game '{game}' not in data package")
    return result


@router.get("/slots/{slot}", response_model=SlotDetailResponse)
async def get_slot_detail(
    slot: int,
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> SlotDetailResponse:
    detail = ap_client.get_slot_detail(slot)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"slot {slot} not found")
    return SlotDetailResponse(**detail)


@router.get("/spheres", response_model=SpheresResponse, dependencies=[Depends(require_auth)])
async def get_spheres(
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> SpheresResponse:
    if not _reachable_cache:
        return SpheresResponse(cached=False, spheres=[])

    sphere_map: dict[int, list[LocationPlacementResponse]] = {}
    for _slot, (_, result) in _reachable_cache.items():
        for sphere_data in result.get("spheres", []):
            idx = int(sphere_data.get("index", 0))
            if idx not in sphere_map:
                sphere_map[idx] = []
            for loc in sphere_data.get("locations", []):
                item = loc.get("item") or {}
                receiver_slot = int(item.get("slot", 0))
                sphere_map[idx].append(LocationPlacementResponse(
                    locationId=int(loc.get("id", 0)),
                    locationName=str(loc.get("name", "")),
                    itemId=int(item.get("id", 0)),
                    itemName=str(item.get("name", "")),
                    receivingSlot=receiver_slot,
                    receivingPlayerName=str(
                        item.get("slot_name")
                        or ap_client._store.resolve_player(receiver_slot)
                    ),
                ))

    return SpheresResponse(
        cached=True,
        spheres=[SphereResponse(index=i, locations=locs) for i, locs in sorted(sphere_map.items())],
    )


# ---------------------------------------------------------------------------
# Route handlers - authenticated
# ---------------------------------------------------------------------------

@router.post("/deathlink", response_model=OkResponse, dependencies=[Depends(require_auth)])
async def post_deathlink(
    body: DeathLinkRequest | None = None,
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> OkResponse:
    if not ap_client.ws_connected:
        raise HTTPException(status_code=503, detail="ws_disconnected")
    source = body.source if body else ""
    cause = body.cause if body else None
    data: dict[str, Any] = {"source": source, "time": datetime.now(_timezone.utc).timestamp()}
    if cause:
        data["cause"] = cause
    await ap_client.send_packet({"cmd": "Bounce", "tags": ["DeathLink"], "data": data})
    return OkResponse()
