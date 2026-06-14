from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from .ap_client import ArchipelagoClient, SelfHintOutcome
from .deps import get_ap_client, get_bridge_state
from .domain import HintInfo
from .schemas import (
    HintItemOkResponse,
    HintItemRequest,
    HintItemResponse,
    HintOkResponse,
    HintRequest,
    HintStatusUpdateRequest,
    HintsResponse,
)
from .state import StateManager

log = logging.getLogger("bridge.rest_hints")

router = APIRouter(tags=["Hints"])

# Paid-hint failure reason → HTTP status (story 9.30).
_SELF_HINT_STATUS: dict[str, int] = {
    "unknown_slot": 422,  # slot's name/game not known to the bridge
    "rejected": 409,      # server refused: not enough points / unknown item-location
    "refused": 502,       # connection refused as that slot
    "timeout": 502,       # no authoritative reply
    "ws_error": 503,      # transport failure
}


def _raise_for_self_hint(outcome: SelfHintOutcome) -> None:
    """Map a failed paid self-hint to an HTTPException; no-op on success."""
    if outcome.ok:
        return
    status = _SELF_HINT_STATUS.get(outcome.reason, 502)
    raise HTTPException(status_code=status, detail=outcome.message or outcome.reason)


@router.get("/slots/{slot}/hints", response_model=HintsResponse)
@router.get("/hints/{slot}", response_model=HintsResponse, include_in_schema=False)
async def get_hints(
    slot: int,
    state: StateManager = Depends(get_bridge_state),
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> HintsResponse:
    state.merge_state_from_save()
    ap_client.resolve_slot_hint_names(slot)
    # Query AP for the slot's authoritative hint points (it only reports them to the connected
    # slot, so we connect as it). Best-effort: on failure the prior/estimated value is kept.
    if ap_client.ws_connected:
        await ap_client.fetch_hint_points(slot)
    ps = state._states.get(slot)
    hints = state.get_hints(slot)
    return HintsResponse(
        slot=slot,
        hints=[HintItemResponse.from_hint(h) for h in hints],
        hintsUsed=ps.hints_used if ps else 0,
        hintPointsAvailable=ps.hint_points_available if ps else 0,
        hintCost=ps.hint_cost if ps else 10,
    )


@router.post("/slots/{slot}/hints/request", response_model=HintOkResponse)
@router.post("/hints/{slot}/request", response_model=HintOkResponse, include_in_schema=False)
async def request_hint(
    slot: int,
    body: HintRequest,
    state: StateManager = Depends(get_bridge_state),
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> HintOkResponse:
    """Request a hint for a location in a slot's world.

    The bridge connects as a single slot, so a non-admin location hint only
    resolves against the connected slot. We issue the AP **admin** command
    `!admin /hint_location <player> <location>` so the *server* resolves the
    location to its item (authoritative) for any slot - unlike keying off the
    spoiler placement, which proved unreliable (hinted the wrong item). The
    created Hint flows back to the UI via the data-storage path (story 9.27).
    See story 9.28.
    """
    if not ap_client.ws_connected:
        raise HTTPException(status_code=503, detail="ws_disconnected")

    # Resolve location ID → name via the DataPackage store
    game = ap_client._store._slot_games.get(slot, "")
    location_name = ap_client._store._location_names.get(game, {}).get(body.locationId)
    if not location_name:
        raise HTTPException(
            status_code=422,
            detail=f"unknown location {body.locationId} for slot {slot} (game={game!r})",
        )

    if body.free:
        # Free/admin path (story 9.28): the bridge issues an admin command on its own
        # connection; the server resolves the location → item and creates a priority hint.
        # Admin hints spend no player points, so hints_used is not bumped here.
        player_name = ap_client._store.resolve_player(slot)
        try:
            await ap_client.send_admin_command(f"!admin /hint_location {player_name} {location_name}")
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    else:
        # Paid path (story 9.30): connect AS the slot and send the self-hint so the AP server
        # charges *that slot's* points, exactly as the player typing !hint_location would.
        # run_self_hint captures AP's authoritative hint_points (which drives the budget), so we
        # can safely bump hints_used to keep "indices demandés" live without affecting points.
        outcome = await ap_client.run_self_hint(slot, f"!hint_location {location_name}")
        _raise_for_self_hint(outcome)
        state.record_paid_hint(slot)
        await ap_client._broadcast_hints(slot)

    # The created hint arrives via the data-storage path (story 9.27); we do not add it
    # optimistically here (the spoiler placement is unreliable for the location→item mapping).

    log.info("hint requested: slot=%d locationId=%d location=%r free=%s",
             slot, body.locationId, location_name, body.free)
    return HintOkResponse(ok=True, slot=slot, locationId=body.locationId, free=body.free)


@router.post("/slots/{slot}/hints/request-item", response_model=HintItemOkResponse)
async def request_hint_item(
    slot: int,
    body: HintItemRequest,
    state: StateManager = Depends(get_bridge_state),
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> HintItemOkResponse:
    """Request a hint for an item a slot has not yet received (story 9.30).

    Free → admin command ``!admin /hint <player> <item>`` (story 9.28/9.29, no points spent).
    Paid → connect AS the slot and send ``!hint <item>`` so the server charges that slot's
    points. The created Hint flows back to the UI via the data-storage path (story 9.27).
    """
    if not ap_client.ws_connected:
        raise HTTPException(status_code=503, detail="ws_disconnected")

    item_name = body.itemName.strip()

    if body.free:
        player_name = ap_client._store.resolve_player(slot)
        try:
            await ap_client.send_admin_command(f"!admin /hint {player_name} {item_name}")
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    else:
        outcome = await ap_client.run_self_hint(slot, f"!hint {item_name}")
        _raise_for_self_hint(outcome)
        # run_self_hint captured AP's authoritative hint_points (which drives the budget), so
        # bumping hints_used keeps "indices demandés" live without affecting points.
        state.record_paid_hint(slot)
        await ap_client._broadcast_hints(slot)

    log.info("item hint requested: slot=%d item=%r free=%s", slot, item_name, body.free)
    return HintItemOkResponse(ok=True, slot=slot, itemName=item_name, free=body.free)


@router.patch("/slots/{slot}/hints/{location_id}", response_model=HintOkResponse)
async def update_hint_status(
    slot: int,
    location_id: int,
    body: HintStatusUpdateRequest,
    state: StateManager = Depends(get_bridge_state),
    ap_client: ArchipelagoClient = Depends(get_ap_client),
) -> HintOkResponse:
    """Update the priority/status of an existing hint on the AP server."""
    if not ap_client.ws_connected:
        raise HTTPException(status_code=503, detail="ws_disconnected")

    hints = state.get_hints(slot)
    hint = next((h for h in hints if h.location_id == location_id), None)
    if hint is None:
        raise HTTPException(
            status_code=404,
            detail=f"no hint for slot={slot} location_id={location_id}",
        )

    updated = HintInfo(
        receiving_player=hint.receiving_player,
        finding_player=hint.finding_player,
        location_id=hint.location_id,
        item_id=hint.item_id,
        entrance=hint.entrance,
        item_flags=hint.item_flags,
        status=body.status,
        receiving_player_name=hint.receiving_player_name,
        finding_player_name=hint.finding_player_name,
        item_name=hint.item_name,
        location_name=hint.location_name,
    )
    state.add_hint(slot, updated)

    await ap_client.send_packet({
        "cmd": "UpdateHint",
        "player": slot,
        "location": location_id,
        "status": body.status,
    })
    await ap_client._broadcast_hints(slot)

    log.info("hint status updated: slot=%d locationId=%d status=%d", slot, location_id, body.status)
    return HintOkResponse(ok=True, slot=slot, locationId=location_id, free=False)
