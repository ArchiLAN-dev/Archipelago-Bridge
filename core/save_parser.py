from __future__ import annotations

import builtins
import collections
import glob
import io
import logging
import os
import pickle
import zlib
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, NamedTuple

from .domain import HintInfo, PlayerState

_SAFE_BUILTINS = frozenset({"set", "frozenset"})


class _NetworkItem(NamedTuple):
    item: int
    location: int
    player: int
    flags: int = 0


class _Hint(NamedTuple):
    receiving_player: int
    finding_player: int
    location: int
    item: int
    found: bool
    entrance: str = ""
    item_flags: int = 0
    status: int = 0


class _NetworkSlot(NamedTuple):
    name: str
    game: str
    type: int
    group_members: Sequence[int] = ()


# NetUtils lives in the AP image, not in this one: what a save holds of it, field for field. The
# enums pickle as a call on their value, and only that value is ever read here.
_NET_UTILS: dict[str, Any] = {
    "NetworkItem": _NetworkItem,
    "Hint": _Hint,
    "NetworkSlot": _NetworkSlot,
    "ClientStatus": int,
    "HintStatus": int,
    "SlotType": int,
}


def _save_slot_map(mapping: dict[Any, Any]) -> dict[int, Any]:
    """Normalize AP save keys: (team, slot[, remote_items]) tuples to a slot int (team 0 only).

    `received_items` uses 3-element keys (team, slot, remote_items). AP appends every item a
    slot receives to the `remote_items=True` list, and only the items coming from *other*
    players to the `False` one (MultiServer.send_items_to), so `True` is a superset of
    `False`, never a disjoint half. Concatenating the two counted every item twice, which
    inflated count-based logic in the reachability pass (`state.has(item, player, n)`: a
    single Progressive Bow read as two, unlocking fire/ice arrows) and doubled the reported
    "items received". Keep the `True` list; fall back to `False` when it is missing.
    """
    result: dict[int, Any] = {}
    for key, val in mapping.items():
        if isinstance(key, int):
            result[key] = val
            continue
        if not (isinstance(key, tuple) and len(key) >= 2 and key[0] == 0):
            continue
        slot = int(key[1])
        if len(key) >= 3:
            # (team, slot, remote_items): the remote_items=True list wins, whatever the order.
            if key[2] or slot not in result:
                result[slot] = val
            continue
        existing = result.get(slot)
        if isinstance(existing, (set, frozenset)) and isinstance(val, (set, frozenset)):
            result[slot] = existing | val
        else:
            result[slot] = val
    return result


def _extract_hints(raw_hints: set) -> list[HintInfo]:
    """Convert a set of AP Hint namedtuples to a list of HintInfo."""
    result = []
    for h in raw_hints:
        try:
            status_raw = getattr(h, "status", 0)
            # HintStatus may be an enum (AP source loaded) or a plain int
            status_val = status_raw.value if hasattr(status_raw, "value") else int(status_raw)
            # Older AP versions have no status field but have found=True/False → map to 40
            if status_val == 0 and getattr(h, "found", False):
                status_val = 40
            result.append(HintInfo(
                receiving_player=int(getattr(h, "receiving_player", 0)),
                finding_player=int(getattr(h, "finding_player", 0)),
                location_id=int(getattr(h, "location", 0)),
                item_id=int(getattr(h, "item", 0)),
                entrance=str(getattr(h, "entrance", "")),
                item_flags=int(getattr(h, "item_flags", 0)),
                status=status_val,
            ))
        except Exception:
            pass
    return result


class _SaveUnpickler(pickle.Unpickler):
    """Only what an Archipelago save holds: the allowlist of ``Utils.restricted_loads``.

    The save directory also receives imported archives, so an ``.apsave`` can come from a member,
    and plain pickle runs code while loading.
    """

    def find_class(self, module: str, name: str) -> Any:  # noqa: ANN401 - pickle's own signature
        if module == "builtins" and name in _SAFE_BUILTINS:
            return getattr(builtins, name)
        if module == "collections" and name == "Counter":
            return collections.Counter
        if module == "NetUtils" and name in _NET_UTILS:
            return _NET_UTILS[name]
        raise pickle.UnpicklingError(f"forbidden global in save: {module}.{name}")


def load_save_state(save_dir: str) -> dict[int, PlayerState]:
    """Read latest .apsave file and return initial PlayerState per slot index."""
    log = logging.getLogger(__name__)
    try:
        files = glob.glob(f"{save_dir}/*.apsave")
        if not files:
            return {}
        latest = max(files, key=os.path.getmtime)
        with open(latest, "rb") as fh:
            raw = fh.read()
        data: dict[str, Any] = _SaveUnpickler(io.BytesIO(zlib.decompress(raw))).load()

        log.info("save keys: %s", list(data.keys()))
        raw_cgs = data.get("client_game_state", {})
        log.info("client_game_state raw (first 10): %s", dict(list(raw_cgs.items())[:10]))

        location_checks = _save_slot_map(data.get("location_checks", {}))
        client_game_state = _save_slot_map(raw_cgs)
        received_items = _save_slot_map(data.get("received_items", {}))
        hints_map = _save_slot_map(data.get("hints", {}))
        hints_used_map = _save_slot_map(data.get("hints_used", {}))

        # NOTE: game_options.hint_cost is a PERCENTAGE (e.g. 10), not a point cost.
        # The actual point cost per hint is provided by the AP server in the RoomInfo
        # WebSocket packet and applied by StateManager.handle_room_info().
        # We only read location_check_points here (it IS already in points).
        opts = data.get("game_options", {})
        location_check_points = int(opts.get("location_check_points", 1))

        log.info("client_game_state after slot_map: %s", client_game_state)

        all_slots = set(location_checks) | set(client_game_state) | set(received_items)
        states: dict[int, PlayerState] = {}
        for slot in all_slots:
            checked: set[int] = set(location_checks.get(slot, set()))
            ps = PlayerState(slot_index=slot)
            ps._checked_locations = checked
            ps.checks_done = len(checked)
            ps.client_status = int(client_game_state.get(slot, 0))
            if ps.client_status == 30:
                ps.goal_reached_at = datetime.now(timezone.utc).isoformat()
            raw_items = received_items.get(slot, [])
            ps._received_items = [
                (
                    int(ni.item if hasattr(ni, "item") else ni[0]),
                    int(ni.player if hasattr(ni, "player") else (ni[2] if len(ni) > 2 else 0)),
                    int(ni.location if hasattr(ni, "location") else (ni[1] if len(ni) > 1 else 0)),
                )
                for ni in raw_items
            ]
            ps.items_received = len(ps._received_items)
            ps.hints_used = int(hints_used_map.get(slot, 0))
            ps.hint_points_per_check = location_check_points
            # ps.hint_cost intentionally left at default 0 - updated by handle_room_info()
            ps._hints = _extract_hints(hints_map.get(slot, set()))
            states[slot] = ps

        log.info(
            "save state loaded: %d slot(s) - statuses: %s",
            len(states),
            {s: p.client_status for s, p in states.items()},
        )
        return states
    except Exception as exc:
        log.warning("failed to load save state: %s", exc)
        return {}
