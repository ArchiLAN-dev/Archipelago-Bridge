from __future__ import annotations

import asyncio
import collections
import glob as _glob
import json
import logging
import re
import uuid
import zipfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
import websockets

from .config import Config
from .domain import HintInfo
from .state import StateManager

_WS_RETRY_DELAYS = [1, 2, 4, 8, 16, 30]

# Maps AP PrintJSON types to the spec's FeedEventType names
_PRINT_TYPE_MAP: dict[str, str] = {
    "Hint": "hint",
    "ItemSend": "item_sent",
    "ItemCheat": "item_sent",
    "Chat": "chat",
    "ServerChat": "system",
    "Tutorial": "system",
    "TagsChanged": "system",
    "Goal": "goal",
    "Release": "release",
    "Collect": "collect",
    "Forfeit": "forfeit",
    "CounterMeasure": "system",
    "Countdown": "countdown",
    "Join": "join",
    "Part": "part",
}

_CLIENT_STATUS_NAMES: dict[int, str] = {
    0: "idle",
    5: "idle",
    10: "idle",
    20: "playing",
    30: "goal_reached",
    40: "done",
}

_SLOT_TYPE_NAMES: dict[int, str] = {
    0: "spectator",
    1: "player",
    2: "group",
}

# AP permission bitmask → named string
_PERMISSION_NAMES: dict[int, str] = {
    0: "disabled",
    1: "goal",
    2: "enabled",
    3: "enabled",
    4: "auto",
    6: "auto",
    7: "auto_enabled",
}

BroadcastFn = Callable[[str, dict[str, Any]], Awaitable[None]]

# Substrings the AP server uses when a self-hint is rejected (insufficient points, unknown
# item/location, …). Matched case-insensitively against the command's text reply so a paid
# hint that the server refused is reported as a failure rather than silently "succeeding".
_HINT_FAILURE_MARKERS: tuple[str, ...] = (
    "not enough",
    "can't afford",
    "cannot afford",
    "could not find",
    "couldn't find",
    "no such",
    "unknown",
    "is not a",
    "did you mean",
)


@dataclass(frozen=True)
class SelfHintOutcome:
    """Result of a paid connect-as-slot self-hint (story 9.30).

    ``reason`` is empty on success; otherwise one of ``unknown_slot`` / ``refused`` /
    ``rejected`` / ``timeout`` / ``ws_error`` so the REST layer can map it to an HTTP code.
    """

    ok: bool
    reason: str = ""
    message: str = ""


class _SlotConnectRefused(Exception):
    """The AP server refused a connect-as-slot attempt (wrong name/password/game)."""


class DataPackageStore:
    """Maps (game, item_id / location_id) → name, and slot_id → player alias / game / type."""

    def __init__(self) -> None:
        self._item_names: dict[str, dict[int, str]] = {}
        self._location_names: dict[str, dict[int, str]] = {}
        self._slot_games: dict[int, str] = {}
        self._slot_aliases: dict[int, str] = {}
        # Registered slot name (from slot_info) - distinct from the display alias.
        # Connecting AS a slot (paid self-hint, story 9.30) requires this name, not the alias.
        self._slot_names: dict[int, str] = {}
        self._slot_types: dict[int, str] = {}
        self._item_flags: dict[int, int] = {}

    def record_item_flags(self, item_id: int, flags: int) -> None:
        self._item_flags[item_id] = flags

    def resolve_item_flags(self, item_id: int) -> int:
        return self._item_flags.get(item_id, 0)

    def handle_connected(self, packet: dict[str, Any]) -> None:
        for p in packet.get("players", []):
            slot = int(p.get("slot", 0))
            alias = str(p.get("alias", p.get("name", f"Player {slot}")))
            self._slot_aliases[slot] = alias
        for slot_str, info in packet.get("slot_info", {}).items():
            if isinstance(info, dict):
                slot = int(slot_str)
                self._slot_games[slot] = str(info.get("game", ""))
                self._slot_names[slot] = str(info.get("name", ""))
                raw_type = info.get("type", 1)
                self._slot_types[slot] = _SLOT_TYPE_NAMES.get(int(raw_type), "player")

    def handle_data_package(self, packet: dict[str, Any]) -> None:
        games: dict[str, Any] = packet.get("data", {}).get("games", {})
        for game, gdata in games.items():
            if not isinstance(gdata, dict):
                continue
            self._item_names[game] = {v: k for k, v in gdata.get("item_name_to_id", {}).items()}
            self._location_names[game] = {v: k for k, v in gdata.get("location_name_to_id", {}).items()}

    def resolve_player(self, slot: int) -> str:
        return self._slot_aliases.get(slot, f"Player {slot}")

    def slot_name(self, slot: int) -> str:
        """The slot's registered name (used to Connect as that slot), not its display alias."""
        return self._slot_names.get(slot, "")

    def slot_game(self, slot: int) -> str:
        """The world/game a slot is playing, or '' if unknown."""
        return self._slot_games.get(slot, "")

    def slot_type(self, slot: int) -> str:
        """The slot's AP type (player / spectator / group), or 'player' if unknown."""
        return self._slot_types.get(slot, "player")

    def resolve_item(self, item_id: int, player_slot: int) -> str:
        game = self._slot_games.get(player_slot, "")
        return self._item_names.get(game, {}).get(item_id, f"Item #{item_id}")

    def resolve_location(self, loc_id: int, player_slot: int) -> str:
        game = self._slot_games.get(player_slot, "")
        return self._location_names.get(game, {}).get(loc_id, f"Location #{loc_id}")

    def slot_by_alias(self, name: str) -> int:
        for slot, alias in self._slot_aliases.items():
            if alias == name:
                return slot
        return 0

    def item_id_by_name(self, game: str, name: str) -> int | None:
        for id_, n in self._item_names.get(game, {}).items():
            if n == name:
                return id_
        return None

    def location_id_by_name(self, game: str, name: str) -> int | None:
        for id_, n in self._location_names.get(game, {}).items():
            if n == name:
                return id_
        return None

    def resolve_hint_names(self, hint: HintInfo) -> HintInfo:
        return HintInfo(
            receiving_player=hint.receiving_player,
            finding_player=hint.finding_player,
            location_id=hint.location_id,
            item_id=hint.item_id,
            entrance=hint.entrance,
            item_flags=hint.item_flags,
            status=hint.status,
            receiving_player_name=self.resolve_player(hint.receiving_player),
            finding_player_name=self.resolve_player(hint.finding_player),
            item_name=self.resolve_item(hint.item_id, hint.receiving_player),
            location_name=self.resolve_location(hint.location_id, hint.finding_player),
        )


def _build_feed_event(packet: dict[str, Any], store: DataPackageStore) -> dict[str, Any]:
    parts: list[str] = []
    for part in packet.get("data", []):
        if not isinstance(part, dict):
            continue
        part_type = part.get("type", "text")
        raw = str(part.get("text", ""))
        try:
            if part_type == "player_id":
                parts.append(store.resolve_player(int(raw)))
            elif part_type == "item_id":
                parts.append(store.resolve_item(int(raw), int(part.get("player", 0) or 0)))
            elif part_type == "location_id":
                parts.append(store.resolve_location(int(raw), int(part.get("player", 0) or 0)))
            else:
                parts.append(raw)
        except (ValueError, TypeError):
            parts.append(raw)

    text = "".join(parts)
    if not text:
        text = str(packet.get("message", "") or packet.get("text", ""))

    msg_type = _PRINT_TYPE_MAP.get(packet.get("type", ""), "system")
    event: dict[str, Any] = {
        "type": msg_type,
        "text": text,
        "color": packet.get("color", "white"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Item events: attach structured origin (item / origin check / sender world / receiver) alongside
    # the prose `text`, so consumers can render "item - check - world (sender)" and filter per slot
    # without parsing the human-readable string. Additive: omitted when the origin can't be resolved.
    if msg_type == "item_sent":
        origin = _build_item_origin(packet, store)
        if origin is not None:
            event.update(origin)

    return event


def _build_item_origin(packet: dict[str, Any], store: DataPackageStore) -> dict[str, Any] | None:
    """Structured origin for an ItemSend, from the authoritative NetworkItem (item/location/player)
    + top-level `receiving`. Mirrors `_track_item_send`'s fast path. Returns None when the packet
    lacks a structured NetworkItem or the finder/location can't be resolved (fallback shapes keep
    only the prose `text`). Item name resolves in the receiver's game; the origin check in the
    sender's (finder's) game."""
    net_item = packet.get("item")
    if not isinstance(net_item, dict):
        return None
    sender = int(net_item.get("player", 0) or 0)
    loc_id = int(net_item.get("location", 0) or 0)
    item_id = int(net_item.get("item", 0) or 0)
    if not sender or loc_id <= 0:
        return None
    receiver = int(packet.get("receiving", sender) or sender)
    return {
        "item": {"id": item_id, "name": store.resolve_item(item_id, receiver)},
        "location": {"id": loc_id, "name": store.resolve_location(loc_id, sender)},
        "sender": {"slot": sender, "name": store.resolve_player(sender), "game": store.slot_game(sender)},
        "receiver": {"slot": receiver, "name": store.resolve_player(receiver), "game": store.slot_game(receiver)},
    }


class ArchipelagoClient:
    def __init__(
        self,
        config: Config,
        state: StateManager,
        broadcast: BroadcastFn,
        recompute_event: asyncio.Event | None = None,
    ) -> None:
        self._config = config
        self._state = state
        self._broadcast = broadcast
        self._recompute_event: asyncio.Event = recompute_event if recompute_event is not None else asyncio.Event()
        self._store = DataPackageStore()
        self._my_slot: int = 0
        self._team: int = 0
        self._ws: Any = None
        self.ws_connected: bool = False
        self._log = logging.getLogger(__name__)

        # Room state (populated from AP RoomInfo / RoomUpdate)
        self._room_forfeit_mode: str = "disabled"
        self._room_release_mode: str = "disabled"
        self._room_collect_mode: str = "disabled"
        self._death_link_active: bool = False
        self._race_mode: bool = False

        # Spoiler-derived placements: finder_slot → {location_id → (item_id, receiver_slot)}
        self._placements: dict[int, dict[int, tuple[int, int]]] = {}

        # Per-slot connection tracking (populated from Join/Part PrintJSON)
        self._connected_slots: set[int] = set()

        # Seed metadata
        self._seed_name: str = ""

        # Circular buffer of the last 200 feed events (PrintJSON + DeathLink bounces)
        self._feed_events: collections.deque[dict[str, Any]] = collections.deque(maxlen=200)

    # ------------------------------------------------------------------
    # Read-only views for WsServer snapshot
    # ------------------------------------------------------------------

    def get_room_dict(self) -> dict[str, Any]:
        return {
            "sessionId": self._config.session_id,
            "seedName": self._seed_name or None,
            "slotCount": len(self._state.get_all()),
            "hintCostPercent": self._state._hint_cost_pct,
            "locationCheckPoints": self._state._location_check_points,
            "forfeitMode": self._room_forfeit_mode,
            "releaseMode": self._room_release_mode,
            "collectMode": self._room_collect_mode,
            "deathLinkActive": self._death_link_active,
            "raceMode": self._race_mode,
            "wsConnected": self.ws_connected,
        }

    def get_feed(self, limit: int = 50) -> list[dict[str, Any]]:
        limit = min(max(1, limit), 200)
        events = list(self._feed_events)
        return events[-limit:]

    def get_data_package(self, game: str) -> dict[str, Any] | None:
        items = self._store._item_names.get(game)
        locations = self._store._location_names.get(game)
        if items is None and locations is None:
            return None
        return {
            "game": game,
            "items": {str(k): v for k, v in (items or {}).items()},
            "locations": {str(k): v for k, v in (locations or {}).items()},
        }

    def list_data_package_games(self) -> list[str]:
        return sorted(
            set(self._store._item_names) | set(self._store._location_names)
        )

    def get_players_state(self) -> dict[str, Any]:
        """`to_api_dict` enriched with per-slot `game` and `slot_type`, so consumers can tell real
        players from the injected TextOnly "Bridge" observer slot (game "Archipelago"). Refreshes from
        the apsave first, like the legacy GET /state did inline."""
        self._state.merge_state_from_save()
        data = self._state.to_api_dict()
        slots = data.get("slots", {})
        if isinstance(slots, dict):
            for key, slot in slots.items():
                if isinstance(slot, dict):
                    sid = int(key)
                    slot["game"] = self._store.slot_game(sid)
                    slot["slot_type"] = self._store.slot_type(sid)
        return data

    def get_slots_summary(self) -> list[dict[str, Any]]:
        result = []
        for slot_id, ps in sorted(self._state.get_all().items()):
            result.append({
                "slot": slot_id,
                "name": ps.slot_name,
                "game": self._store._slot_games.get(slot_id, ""),
                "type": self._store._slot_types.get(slot_id, "player"),
                "status": _CLIENT_STATUS_NAMES.get(ps.client_status, "idle"),
                "connected": slot_id in self._connected_slots,
                "checksDone": ps.checks_done,
                "checksTotal": ps.checks_total,
                "itemsReceived": ps.items_received,
                "goalReachedAt": ps.goal_reached_at,
                "reachableNow": ps.reachable_now,
            })
        return result

    def get_slot_detail(self, slot: int) -> dict[str, Any] | None:
        ps = self._state._states.get(slot)
        if ps is None:
            return None
        return {
            "slot": slot,
            "name": ps.slot_name,
            "game": self._store._slot_games.get(slot, ""),
            "type": self._store._slot_types.get(slot, "player"),
            "status": _CLIENT_STATUS_NAMES.get(ps.client_status, "idle"),
            "connected": slot in self._connected_slots,
            "checksDone": ps.checks_done,
            "checksTotal": ps.checks_total,
            "itemsReceived": ps.items_received,
            "goalReachedAt": ps.goal_reached_at,
            "reachableNow": ps.reachable_now,
            "budget": ps.hint_points_available,
        }

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    async def send_command(self, command: str) -> None:
        if self._ws is not None and self.ws_connected:
            await self._ws.send(json.dumps([{"cmd": "Say", "text": command}]))

    async def send_admin_command(self, command: str) -> None:
        """Send an admin command, authenticating first if an admin password is configured."""
        if self._ws is None or not self.ws_connected:
            return
        if self._config.ap_admin_password:
            await self._ws.send(json.dumps([{"cmd": "Say", "text": f"!admin login {self._config.ap_admin_password}"}]))
        await self._ws.send(json.dumps([{"cmd": "Say", "text": command}]))

    async def send_packet(self, packet: dict[str, Any]) -> None:
        if self._ws is None or not self.ws_connected:
            raise RuntimeError("WebSocket not connected")
        await self._ws.send(json.dumps([packet]))

    # ------------------------------------------------------------------
    # Paid self-hint via an ephemeral connection AS the target slot (story 9.30)
    # ------------------------------------------------------------------

    async def run_self_hint(self, slot: int, command: str, *, timeout: float = 15.0) -> SelfHintOutcome:
        """Open a throwaway connection AS ``slot`` and run a paid self-hint, charging that slot.

        The main bridge connection is the TextOnly "Bridge" slot, so a self-hint there resolves
        against (and would charge) the Bridge slot. To charge slot N - exactly as if the player
        typed ``!hint <item>`` / ``!hint_location <location>`` in their own client - we open a
        second WebSocket, ``Connect`` as slot N using the **server (room) password** (never the
        admin password, never ``!admin login``), send the single ``Say``, read the authoritative
        server reply (a ``Hint`` PrintJSON = success, a text reply = rejection), then close. AP
        allows multiple connections per slot, so this does not disturb the player's real client.
        The created hint reaches the UI live via the data-storage path (story 9.27).
        """
        if not self._store.slot_name(slot) or not self._store._slot_games.get(slot, ""):
            return SelfHintOutcome(ok=False, reason="unknown_slot",
                                   message=f"slot {slot} has no known name/game")

        try:
            async with websockets.connect(self._config.ap_ws_url) as ws:
                try:
                    connected = await self._connect_as_slot(ws, slot, timeout)
                except _SlotConnectRefused as exc:
                    return SelfHintOutcome(ok=False, reason="refused", message=str(exc))
                # AP reports the slot's authoritative hint points in Connected (and RoomUpdate
                # after the hint is paid) - capture it instead of estimating locally.
                self._store_hint_points(slot, connected)
                await ws.send(json.dumps([{"cmd": "Say", "text": command}]))
                return await self._await_self_hint_reply(ws, slot, timeout)
        except asyncio.TimeoutError:
            return SelfHintOutcome(ok=False, reason="timeout", message="no server reply")
        except (OSError, websockets.WebSocketException) as exc:
            return SelfHintOutcome(ok=False, reason="ws_error", message=str(exc))

    async def fetch_hint_points(self, slot: int, *, timeout: float = 8.0) -> int | None:
        """Connect AS ``slot`` and read AP's authoritative hint points (Connected.hint_points).

        AP only sends hint_points for the connected slot, and the bridge's main connection is the
        Bridge slot - so the only way to know another slot's real points is to connect as it. Used
        by GET /hints so the panel always shows AP's value, never a local estimate. Best-effort:
        returns None (leaving any prior value) on failure rather than raising into the request."""
        if not self._store.slot_name(slot) or not self._store._slot_games.get(slot, ""):
            return None
        try:
            async with websockets.connect(self._config.ap_ws_url) as ws:
                connected = await self._connect_as_slot(ws, slot, timeout)
                return self._store_hint_points(slot, connected)
        except (asyncio.TimeoutError, _SlotConnectRefused, OSError, websockets.WebSocketException):
            return None

    async def _connect_as_slot(self, ws: Any, slot: int, timeout: float) -> dict[str, Any]:
        """Drain RoomInfo, Connect AS ``slot`` (server password, TextOnly), return Connected.

        Raises ``_SlotConnectRefused`` on ConnectionRefused, ``asyncio.TimeoutError`` on timeout."""
        await asyncio.wait_for(ws.recv(), timeout=timeout)  # RoomInfo
        await ws.send(json.dumps([{
            "cmd": "Connect",
            "name": self._store.slot_name(slot),
            "game": self._store._slot_games.get(slot, ""),
            "password": self._config.ap_server_password,
            "uuid": str(uuid.uuid4()),
            "version": {"major": 0, "minor": 6, "build": 7, "class": "Version"},
            "tags": ["TextOnly"],
            "items_handling": 0,
            "slot_data": False,
        }]))
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            for packet in json.loads(raw):
                if not isinstance(packet, dict):
                    continue
                if packet.get("cmd") == "Connected":
                    self._apply_authoritative_locations(slot, packet)
                    return packet
                if packet.get("cmd") == "ConnectionRefused":
                    raise _SlotConnectRefused(f"ConnectionRefused: {packet.get('errors', [])}")

    def _apply_authoritative_locations(self, slot: int, connected: dict[str, Any]) -> None:
        """Set a slot's checks total + hint cost from AP's authoritative location set.

        AP's Connected packet for the slot lists its real locations (checked + missing) - the exact
        set the server prices hints against (hint_cost% x locations). Using it overrides the
        spoiler/DataPackage estimate from `_slot_location_total`, which inflates the count (and thus
        the displayed pts/hint) when the spoiler placements weren't resolved. Mirrors what
        `_handle_connected` already does for the bridge's own slot (story 9.32)."""
        checked = connected.get("checked_locations")
        missing = connected.get("missing_locations")
        if not isinstance(checked, list) or not isinstance(missing, list):
            return
        total = len(checked) + len(missing)
        if total > 0:
            self._state.set_checks_total(slot, total)
            self._state.apply_hint_cost_for_slot(slot, total)

    def _store_hint_points(self, slot: int, packet: dict[str, Any]) -> int | None:
        """Store AP's hint_points for a slot (from Connected/RoomUpdate) as the authoritative value."""
        pts = packet.get("hint_points")
        if isinstance(pts, int):
            self._state.ensure_slot(slot).hint_points_reported = pts
            return pts
        return None

    async def _await_self_hint_reply(self, ws: Any, slot: int, timeout: float) -> SelfHintOutcome:
        """Read packets until the hint is confirmed (Hint PrintJSON) or refused (text reply).

        Also captures the post-hint hint_points from any RoomUpdate seen before the resolution."""
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return SelfHintOutcome(ok=False, reason="timeout", message="no hint confirmation")
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            for packet in json.loads(raw):
                if not isinstance(packet, dict):
                    continue
                if packet.get("cmd") == "RoomUpdate":
                    self._store_hint_points(slot, packet)
                    continue
                if packet.get("cmd") != "PrintJSON":
                    continue
                if packet.get("type") == "Hint" and self._hint_involves_slot(packet, slot):
                    return SelfHintOutcome(ok=True)
                text = self._print_json_text(packet)
                if text and any(m in text.lower() for m in _HINT_FAILURE_MARKERS):
                    return SelfHintOutcome(ok=False, reason="rejected", message=text)

    @staticmethod
    def _hint_involves_slot(packet: dict[str, Any], slot: int) -> bool:
        net_item = packet.get("item", {})
        finding = int(net_item.get("player", 0)) if isinstance(net_item, dict) else 0
        return slot in (int(packet.get("receiving", 0)), finding)

    @staticmethod
    def _print_json_text(packet: dict[str, Any]) -> str:
        parts = [str(p.get("text", "")) for p in packet.get("data", []) if isinstance(p, dict)]
        return "".join(parts)

    # ------------------------------------------------------------------
    # Connection loop
    # ------------------------------------------------------------------

    async def _connect_and_run(self) -> None:
        async with websockets.connect(self._config.ap_ws_url) as ws:
            self._ws = ws
            self.ws_connected = True
            self._log.info("connected to archipelago ws at %s", self._config.ap_ws_url)

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
                first_packets: list[dict[str, Any]] = json.loads(raw)
            except Exception as exc:
                self._log.warning("failed to receive RoomInfo: %s", exc)
                return

            games_in_session: list[str] = []
            for packet in first_packets:
                if packet.get("cmd") == "RoomInfo":
                    games_in_session = packet.get("games", [])
                    self._seed_name = str(packet.get("seed_name", "") or "")
                    self._log.info("RoomInfo received - games: %s", games_in_session)
                    self._state.handle_room_info(packet)
                    self._handle_room_info_permissions(packet)
                    await self._broadcast_state_changed()

            if games_in_session:
                await ws.send(json.dumps([{"cmd": "GetDataPackage", "games": games_in_session}]))

            first_slot = self._config.slot_names[0] if self._config.slot_names else {}
            # "Bridge" slot is injected by the orchestrateur into every session's yamls,
            # giving the bridge a TextOnly observer slot in the generated multiworld.
            connect_name = first_slot.get("name", "Bridge")
            connect_game = first_slot.get("game", "Archipelago")

            connect_packet = {
                "cmd": "Connect",
                "name": connect_name,
                "game": connect_game,
                "password": self._config.ap_server_password,
                "uuid": str(uuid.uuid4()),
                "version": {"major": 0, "minor": 6, "build": 7, "class": "Version"},
                "tags": ["TextOnly"],
                "items_handling": 0,
                "slot_data": False,
            }
            await ws.send(json.dumps([connect_packet]))

            async for raw in ws:
                try:
                    packets: list[dict[str, Any]] = json.loads(raw)
                except Exception as exc:
                    self._log.warning("packet parse error: %s", exc)
                    continue
                for packet in packets:
                    try:
                        await self._handle_packet(packet)
                    except Exception as exc:
                        self._log.warning("packet handling error (%s): %s", packet.get("cmd", "?"), exc)

    def _handle_room_info_permissions(self, packet: dict[str, Any]) -> None:
        perms = packet.get("permissions")
        if not isinstance(perms, dict):
            return
        forfeit_raw = perms.get("forfeit", 0)
        remaining_raw = perms.get("remaining", 0)
        release_raw = perms.get("release", remaining_raw)
        collect_raw = perms.get("collect", 0)
        self._room_forfeit_mode = _PERMISSION_NAMES.get(int(forfeit_raw or 0), "disabled")
        self._room_release_mode = _PERMISSION_NAMES.get(int(release_raw or 0), "disabled")
        self._room_collect_mode = _PERMISSION_NAMES.get(int(collect_raw or 0), "disabled")

    async def _handle_packet(self, packet: dict[str, Any]) -> None:
        cmd: str = packet.get("cmd", "")
        self._log.debug("packet received: %s", cmd)

        if cmd == "ConnectionRefused":
            errors = packet.get("errors", [])
            self._log.error("connection refused by archipelago server: %s", errors)
            raise RuntimeError(f"ConnectionRefused: {errors}")

        elif cmd == "RoomInfo":
            self._seed_name = str(packet.get("seed_name", "") or "")
            self._log.info("RoomInfo received (reconnect)")
            self._state.handle_room_info(packet)
            self._handle_room_info_permissions(packet)
            await self._broadcast_state_changed()

        elif cmd == "InvalidPacket":
            self._log.warning(
                "AP server reported invalid packet: cmd=%s text=%s",
                packet.get("original_cmd", "?"),
                packet.get("text", ""),
            )

        elif cmd == "RoomUpdate":
            self._handle_room_update(packet)
            await self._broadcast_room_updated()

        elif cmd == "DataPackage":
            self._store.handle_data_package(packet)
            games_loaded = list(packet.get("data", {}).get("games", {}).keys())
            self._log.info("DataPackage received - games: %s", games_loaded)
            self._resolve_all_hint_names()
            self._apply_location_totals()

        elif cmd == "Connected":
            await self._handle_connected(packet)

        elif cmd == "PrintJSON":
            await self._handle_print_json(packet)

        elif cmd == "StatusUpdate":
            slot = int(packet.get("slot", 0))
            status = int(packet.get("status", 0))
            self._log.info("StatusUpdate: slot=%d status=%d", slot, status)
            self._state.handle_status_update(packet)
            await self._broadcast_state_changed()

        elif cmd == "ReceivedItems":
            await self._handle_received_items(packet)

        elif cmd == "LocationChecks":
            self._state.handle_location_checks(packet)
            self._recompute_event.set()
            await self._broadcast_state_changed()

        elif cmd == "Retrieved":
            keys: dict[str, Any] = packet.get("keys", {})
            if "_read_race_mode" in keys:
                self._race_mode = bool(keys["_read_race_mode"])
                self._log.info("race_mode: %s", self._race_mode)
            for key, value in keys.items():
                hint_slot = self._slot_from_hint_key(key)
                if hint_slot is not None:
                    await self._ingest_hint_storage(hint_slot, value)

        elif cmd == "SetReply":
            hint_slot = self._slot_from_hint_key(str(packet.get("key", "")))
            if hint_slot is not None:
                await self._ingest_hint_storage(hint_slot, packet.get("value"))

        elif cmd == "Bounced":
            await self._handle_bounced(packet)

    def _handle_room_update(self, packet: dict[str, Any]) -> None:
        # RoomUpdate is a partial RoomInfo; only update fields that are present
        if "permissions" in packet:
            self._handle_room_info_permissions(packet)
        if "hint_cost" in packet or "location_check_points" in packet:
            self._state.handle_room_info(packet)

    async def _handle_connected(self, packet: dict[str, Any]) -> None:
        players: list[dict[str, Any]] = packet.get("players", [])
        slot_info: dict[str, Any] = packet.get("slot_info", {})
        self._my_slot = int(packet.get("slot", 0))
        self._team = int(packet.get("team", 0))
        self._log.info("Connected received - slot=%d players=%d", self._my_slot, len(players))
        self._store.handle_connected(packet)


        for slot_str, info in slot_info.items():
            if isinstance(info, dict):
                self._state.set_slot_name(int(slot_str), info.get("name", ""))
        for p in players:
            slot = int(p.get("slot", 0))
            name = p.get("alias", p.get("name", ""))
            if slot:
                self._state.set_slot_name(slot, name)
                self._connected_slots.add(slot)

        checked_locs = [int(loc) for loc in packet.get("checked_locations", [])]
        missing_locs = packet.get("missing_locations", [])
        if self._my_slot:
            total = len(checked_locs) + len(missing_locs)
            if total > 0:
                self._state.set_checks_total(self._my_slot, total)
                self._state.apply_hint_cost_for_slot(self._my_slot, total)
            if checked_locs:
                self._state.add_location_checks(self._my_slot, checked_locs)

        self._apply_location_totals()

        for slot_id, ps in self._state.get_all().items():
            if ps.client_status == 0 and ps.checks_done > 0:
                self._state.update_client_status(slot_id, 20)

        if not self._placements:
            self._load_spoiler()
        # Spoiler placements give each slot's real location count; recompute totals so
        # checks_total and the hint cost no longer use the inflated DataPackage size.
        self._apply_location_totals()

        # Request race_mode from AP data storage (not included in RoomInfo).
        # Subscribe to every slot's hint storage so hints for ALL slots arrive live -
        # the Hint PrintJSON is only sent to the slots involved, and the bridge connects
        # as a single slot, so it would otherwise miss hints between other slots (story 9.27).
        if self._ws is not None:
            hint_keys = [self._hint_storage_key(int(s)) for s in slot_info]
            await self._ws.send(json.dumps([{"cmd": "Get", "keys": ["_read_race_mode", *hint_keys]}]))
            if hint_keys:
                await self._ws.send(json.dumps([{"cmd": "SetNotify", "keys": hint_keys}]))

        await self._broadcast_state_changed()

    async def _handle_print_json(self, packet: dict[str, Any]) -> None:
        msg_type = packet.get("type", "")
        state_changed = False

        if msg_type in ("ItemSend", "ItemCheat"):
            self._track_item_send(packet)
            state_changed = True
        elif msg_type == "Goal":
            goal_slot = self._track_goal_returning_slot(packet)
            state_changed = True
            if goal_slot:
                await self._notify_goal(goal_slot)
        elif msg_type == "Hint":
            await self._track_hint(packet)
            state_changed = True
        elif msg_type == "Join":
            slot = int(packet.get("slot", 0))
            if slot:
                self._connected_slots.add(slot)
                state_changed = True
        elif msg_type == "Part":
            slot = int(packet.get("slot", 0))
            if slot:
                self._connected_slots.discard(slot)
                state_changed = True

        event = _build_feed_event(packet, self._store)
        await self._emit_feed(event)

        if state_changed:
            await self._broadcast_state_changed()

    async def _handle_received_items(self, packet: dict[str, Any]) -> None:
        items: list[Any] = packet.get("items", [])
        if self._my_slot:
            if packet.get("index", -1) == 0:
                ps = self._state.ensure_slot(self._my_slot)
                ps._received_items = [
                    (int(it.get("item", 0)), int(it.get("player", 0)), int(it.get("location", 0)))
                    for it in items if isinstance(it, dict)
                ]
                ps.items_received = len(ps._received_items)
                self._recompute_event.set()
            elif items:
                for it in items:
                    if isinstance(it, dict):
                        self._state.add_item_received(
                            self._my_slot,
                            int(it.get("item", 0)),
                            int(it.get("player", 0)),
                            int(it.get("location", 0)),
                        )
                self._recompute_event.set()
        for it in items:
            if isinstance(it, dict):
                item_id = int(it.get("item", 0))
                flags = int(it.get("flags", 0))
                if item_id and flags:
                    self._store.record_item_flags(item_id, flags)
        if items:
            await self._broadcast_state_changed()

    async def _handle_bounced(self, packet: dict[str, Any]) -> None:
        tags: list[str] = packet.get("tags", [])
        data: dict[str, Any] = packet.get("data", {})
        if "DeathLink" not in tags:
            return

        source = str(data.get("source", ""))
        cause = data.get("cause")
        self._death_link_active = True

        death_event: dict[str, Any] = {
            "type": "death_link",
            "source": source,
            "cause": cause if isinstance(cause, str) else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self._emit_feed(death_event)

    async def _emit_feed(self, event: dict[str, Any]) -> None:
        """Record one feed event, broadcast it to local WS clients, and push it to Symfony so it can
        publish to the Mercure topic runs/{id}/feed (mirrors the players/hints/reachable push model)."""
        self._feed_events.append(event)
        await self._broadcast("feed", {
            "sessionId": self._config.session_id,
            "event": event,
        })
        await self._push_feed_to_api(event)

    async def _push_feed_to_api(self, event: dict[str, Any]) -> None:
        """Push a single feed event to Symfony so it publishes the Mercure topic runs/{id}/feed the
        frontend (EventFeed + OBS overlays) subscribes to. Without this the feed only ever reaches
        local WS clients and the GET /feed snapshot - never the live Mercure stream."""
        url = self._config.central_api_url
        secret = self._config.central_api_secret
        if not url or not secret:
            return
        endpoint = (
            f"{url.rstrip('/')}"
            f"/api/v1/internal/sessions/{self._config.session_id}/feed-push"
        )
        headers = {"X-Internal-Secret": secret}
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.post(endpoint, json=event, headers=headers)
                if resp.status_code not in (200, 204):
                    self._log.warning("feed push: unexpected status %d", resp.status_code)
        except Exception as exc:
            self._log.warning("feed push error: %s", exc)

    # ------------------------------------------------------------------
    # State tracking helpers
    # ------------------------------------------------------------------

    def _slot_from_part(self, part: dict[str, Any]) -> int:
        try:
            if part.get("type") == "player_id":
                return int(str(part.get("text", "0")))
            if part.get("type") == "player_name":
                return self._store.slot_by_alias(str(part.get("text", "")))
        except (ValueError, TypeError):
            pass
        return 0

    def _track_item_send(self, packet: dict[str, Any]) -> None:
        # Fast path: the ItemSend packet carries a structured NetworkItem
        # (item/location/player/flags) + top-level "receiving". These are
        # authoritative and game-agnostic, unlike parsing the human-readable
        # "data" parts (which intermittently fails to resolve the slot for some
        # games/message shapes, leaving checks to surface only via the apsave).
        net_item = packet.get("item")
        if isinstance(net_item, dict):
            sender = int(net_item.get("player", 0) or 0)  # the finder
            loc_id = int(net_item.get("location", 0) or 0)
            item_id = int(net_item.get("item", 0) or 0)
            if sender and loc_id > 0:
                receiver = int(packet.get("receiving", sender) or sender)
                self._apply_item_send(sender, loc_id, item_id, receiver)
                return

        # Fallback: parse the human-readable parts (e.g. ItemCheat, unusual shapes).
        data = [p for p in packet.get("data", []) if isinstance(p, dict)]

        sender = 0
        for part in data:
            sender = self._slot_from_part(part)
            if sender:
                break
        if not sender:
            return

        found_item_id = 0
        found_loc_id = 0
        found_receiver = 0

        for part in data:
            part_type = part.get("type", "")
            try:
                raw_val = int(str(part.get("text", "0")))
            except (ValueError, TypeError):
                continue
            if part_type == "location_id" and raw_val:
                found_loc_id = raw_val
            elif part_type == "item_id" and raw_val:
                found_item_id = raw_val
                found_receiver = int(part.get("player", 0) or sender)

        self._apply_item_send(sender, found_loc_id, found_item_id, found_receiver)

    def _apply_item_send(self, sender: int, loc_id: int, item_id: int, receiver: int) -> None:
        if loc_id:
            self._state.add_location_checks(sender, [loc_id])
            ps = self._state.ensure_slot(sender)
            if ps.client_status < 20:
                self._state.update_client_status(sender, 20)

        if item_id and receiver:
            self._state.add_item_received(receiver, item_id, sender, loc_id)

        if loc_id or (item_id and receiver):
            self._recompute_event.set()

    def _track_goal(self, packet: dict[str, Any]) -> None:
        self._track_goal_returning_slot(packet)

    def _track_goal_returning_slot(self, packet: dict[str, Any]) -> int:
        """Update goal state and return the slot that reached the goal (0 if unknown)."""
        top_slot = int(packet.get("slot", 0))
        if top_slot:
            self._state.update_client_status(top_slot, 30)
            return top_slot
        for part in packet.get("data", []):
            if not isinstance(part, dict):
                continue
            slot = self._slot_from_part(part)
            if slot:
                self._state.update_client_status(slot, 30)
                return slot
        return 0

    async def _notify_goal(self, slot_id: int) -> None:
        """Notify Symfony that a slot reached its goal. Symfony handles dispatch."""
        url = self._config.central_api_url
        secret = self._config.central_api_secret
        if not url or not secret:
            return

        ps = self._state._states.get(slot_id)
        if ps is None or ps.goal_reached_at is None:
            return

        endpoint = (
            f"{url.rstrip('/')}"
            f"/api/v1/internal/sessions/{self._config.session_id}/slot-goal"
        )
        headers = {"X-Internal-Secret": secret}
        payload = {
            "slotId": slot_id,
            "checksTotal": ps.checks_done,
            "itemsTotal": ps.items_received,
            "goalReachedAt": ps.goal_reached_at,
        }

        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.post(endpoint, json=payload, headers=headers)
                if resp.status_code not in (200, 204):
                    self._log.warning(
                        "slot-goal callback: unexpected status %d (session=%s slot=%d)",
                        resp.status_code,
                        self._config.session_id,
                        slot_id,
                    )
                else:
                    self._log.info(
                        "slot-goal callback: notified goal for slot %d session %s",
                        slot_id,
                        self._config.session_id,
                    )
        except Exception as exc:
            self._log.warning("slot-goal callback error (slot=%d): %s", slot_id, exc)

    def _resolve_all_hint_names(self) -> None:
        for slot_id, ps in self._state._states.items():
            if not ps._hints:
                continue
            ps._hints = [self._store.resolve_hint_names(h) for h in ps._hints]
            self._log.info("resolved names for %d hint(s) on slot %d", len(ps._hints), slot_id)

    def resolve_slot_hint_names(self, slot_id: int) -> None:
        ps = self._state._states.get(slot_id)
        if not ps or not ps._hints:
            return
        ps._hints = [self._store.resolve_hint_names(h) for h in ps._hints]

    # ------------------------------------------------------------------
    # Spoiler-based placement lookup
    # ------------------------------------------------------------------

    def _load_spoiler(self) -> None:
        """Parse AP spoiler files from all zips in the output dir to build placement map."""
        output_dir = self._config.save_dir
        zips = _glob.glob(f"{output_dir}/*.zip")
        if not zips:
            self._log.warning("spoiler: no zip in %s", output_dir)
            return

        pattern = re.compile(r"^(.+) \((.+)\): (.+) \((.+)\)$")
        placements: dict[int, dict[int, tuple[int, int]]] = {}
        total_count = 0

        for zip_path in zips:
            try:
                with zipfile.ZipFile(zip_path) as z:
                    spoiler_names = [n for n in z.namelist() if n.endswith("_Spoiler.txt")]
                    if not spoiler_names:
                        continue
                    text = z.read(spoiler_names[0]).decode("utf-8", errors="replace")
            except Exception as exc:
                self._log.warning("spoiler: failed to read %s: %s", zip_path, exc)
                continue

            count = 0
            for line in text.splitlines():
                m = pattern.match(line.strip())
                if not m:
                    continue
                loc_name, finder_name, item_name, receiver_name = m.groups()
                finder_slot = self._store.slot_by_alias(finder_name)
                receiver_slot = self._store.slot_by_alias(receiver_name)
                if not finder_slot or not receiver_slot:
                    continue
                finder_game = self._store._slot_games.get(finder_slot, "")
                receiver_game = self._store._slot_games.get(receiver_slot, "")
                location_id = self._store.location_id_by_name(finder_game, loc_name)
                item_id = self._store.item_id_by_name(receiver_game, item_name)
                if location_id is None or item_id is None:
                    continue
                placements.setdefault(finder_slot, {})[location_id] = (item_id, receiver_slot)
                count += 1
            if count:
                self._log.info("spoiler: loaded %d placements from %s", count, zip_path)
                total_count += count

        self._placements = placements
        if not total_count:
            self._log.warning("spoiler: no placements resolved (player name mismatch?)")

    def get_placement(self, finder_slot: int, location_id: int) -> tuple[int, int] | None:
        """Return (item_id, receiver_slot) for a location, or None if unknown."""
        return self._placements.get(finder_slot, {}).get(location_id)

    def _slot_location_total(self, slot_id: int, game: str) -> int:
        """A slot's real location count: spoiler placements when loaded (authoritative - this is
        the set AP uses), else the full DataPackage size as a fallback. The DataPackage lists
        *every* location the game defines, which is far larger than the locations actually in this
        seed, so using it inflates both checks_total and the hint cost (hint_cost% × locations)."""
        placed = len(self._placements.get(slot_id, {}))
        if placed > 0:
            return placed
        return len(self._store._location_names.get(game, {}))

    def _apply_location_totals(self) -> None:
        """Set checks_total + hint cost for every non-connected slot from its real location count.

        Re-runnable: called once on Connect (DataPackage size) and again after the spoiler loads,
        at which point the placement count replaces the inflated DataPackage size."""
        for slot_id, game in self._store._slot_games.items():
            if slot_id == self._my_slot:
                continue
            total = self._slot_location_total(slot_id, game)
            if total > 0:
                self._state.set_checks_total(slot_id, total)
                self._state.apply_hint_cost_for_slot(slot_id, total)

    async def _track_hint(self, packet: dict[str, Any]) -> None:
        receiving_player = int(packet.get("receiving", 0))
        net_item = packet.get("item", {})
        if not isinstance(net_item, dict):
            return

        item_id = int(net_item.get("item", 0))
        location_id = int(net_item.get("location", 0))
        finding_player = int(net_item.get("player", 0))
        item_flags = int(net_item.get("flags", 0))

        if not (item_id and location_id and receiving_player):
            return

        if item_flags:
            self._store.record_item_flags(item_id, item_flags)

        status_raw = packet.get("status", None)
        if status_raw is not None:
            status = int(status_raw)
        elif packet.get("found", False):
            status = 40
        else:
            status = 0

        hint = HintInfo(
            receiving_player=receiving_player,
            finding_player=finding_player,
            location_id=location_id,
            item_id=item_id,
            entrance=str(packet.get("entrance", "")),
            item_flags=item_flags,
            status=status,
            receiving_player_name=self._store.resolve_player(receiving_player),
            finding_player_name=self._store.resolve_player(finding_player),
            item_name=self._store.resolve_item(item_id, receiving_player),
            location_name=self._store.resolve_location(location_id, finding_player),
        )
        changed = self._state.add_hint(receiving_player, hint)
        if changed:
            await self._broadcast_hints(receiving_player)

    # ------------------------------------------------------------------
    # Hint data storage (live hints for ALL slots - story 9.27)
    # ------------------------------------------------------------------

    def _hint_storage_key(self, slot: int) -> str:
        return f"_read_hints_{self._team}_{slot}"

    def _slot_from_hint_key(self, key: str) -> int | None:
        prefix = f"_read_hints_{self._team}_"
        if not key.startswith(prefix):
            return None
        try:
            return int(key[len(prefix):])
        except ValueError:
            return None

    def _hint_from_storage(self, raw: dict[str, Any]) -> HintInfo | None:
        """Build a HintInfo from a serialized AP data-storage Hint (flat fields)."""
        receiving_player = int(raw.get("receiving_player", 0))
        finding_player = int(raw.get("finding_player", 0))
        item_id = int(raw.get("item", 0))
        location_id = int(raw.get("location", 0))
        if not (item_id and location_id and receiving_player):
            return None
        item_flags = int(raw.get("item_flags", 0))
        if item_flags:
            self._store.record_item_flags(item_id, item_flags)
        status_raw = raw.get("status", None)
        if status_raw is not None:
            status = int(status_raw)
        elif raw.get("found", False):
            status = 40
        else:
            status = 0
        return HintInfo(
            receiving_player=receiving_player,
            finding_player=finding_player,
            location_id=location_id,
            item_id=item_id,
            entrance=str(raw.get("entrance", "")),
            item_flags=item_flags,
            status=status,
            receiving_player_name=self._store.resolve_player(receiving_player),
            finding_player_name=self._store.resolve_player(finding_player),
            item_name=self._store.resolve_item(item_id, receiving_player),
            location_name=self._store.resolve_location(location_id, finding_player),
        )

    async def _ingest_hint_storage(self, slot: int, raw_value: Any) -> None:
        """Replace a slot's hints from a data-storage payload and push when changed."""
        if not isinstance(raw_value, list):
            return
        hints: list[HintInfo] = []
        for raw in raw_value:
            if isinstance(raw, dict):
                hint = self._hint_from_storage(raw)
                if hint is not None:
                    hints.append(hint)
        if self._state.set_hints(slot, hints):
            await self._broadcast_hints(slot)

    # ------------------------------------------------------------------
    # Broadcast helpers
    # ------------------------------------------------------------------

    async def notify_state_changed(self) -> None:
        """Public hook: broadcast the current state and push it to the API.

        Used by the apsave reconcile loop so that checks the AP server did not
        broadcast over the WS (e.g. local items) still reach the UI within the
        reconcile interval, instead of waiting for the next WS-driven push.
        """
        await self._broadcast_state_changed()

    async def _broadcast_state_changed(self) -> None:
        await self._broadcast("state_changed", {
            "sessionId": self._config.session_id,
            "slots": self.get_slots_summary(),
        })
        await self._push_state_to_api()

    async def _push_state_to_api(self) -> None:
        """Push state to Symfony so it can publish to Mercure topic runs/{id}/players.

        Uses to_api_dict() format (dict keyed by slot, snake_case fields) which
        matches what GET /players returns and what the frontend SSE handler expects.
        """
        url = self._config.central_api_url
        secret = self._config.central_api_secret
        if not url or not secret:
            return
        endpoint = (
            f"{url.rstrip('/')}"
            f"/api/v1/internal/sessions/{self._config.session_id}/players-push"
        )
        headers = {"X-Internal-Secret": secret}
        payload = self._state.to_api_dict()
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.post(endpoint, json=payload, headers=headers)
                if resp.status_code not in (200, 204):
                    self._log.warning("players push: unexpected status %d", resp.status_code)
        except Exception as exc:
            self._log.warning("players push error: %s", exc)

    async def _broadcast_room_updated(self) -> None:
        await self._broadcast("room_updated", {
            "sessionId": self._config.session_id,
            "room": self.get_room_dict(),
        })

    async def _broadcast_hints(self, slot: int) -> None:
        # Resolve names before pushing: the apsave reconcile (apply_saved_states) overwrites
        # ps._hints with save-derived hints that carry ids only (empty item/location names), so a
        # live push would otherwise show "Item #123"/"Location #456". The GET path already calls
        # this; doing it here is the single choke point covering every push.
        self.resolve_slot_hint_names(slot)
        hints = self._state.get_hints(slot)
        ps = self._state._states.get(slot)
        payload: dict[str, Any] = {
            "sessionId": self._config.session_id,
            "slot": slot,
            "hints": [
                {
                    "receivingPlayer": h.receiving_player,
                    "receivingPlayerName": h.receiving_player_name,
                    "findingPlayer": h.finding_player,
                    "findingPlayerName": h.finding_player_name,
                    "locationId": h.location_id,
                    "locationName": h.location_name,
                    "itemId": h.item_id,
                    "itemName": h.item_name,
                    "itemFlags": h.item_flags,
                    "entrance": h.entrance,
                    "found": h.found,
                    "status": h.status,
                    "statusName": h.status_name,
                }
                for h in hints
            ],
            "hintsUsed": ps.hints_used if ps else 0,
            "hintPointsAvailable": ps.hint_points_available if ps else 0,
            "hintCost": ps.hint_cost if ps else 0,
        }
        # Local WS broadcast + push to Symfony so it publishes the hints Mercure topic
        # (runs/{id}/slots/{n}/hints) the frontend subscribes to - otherwise hints only
        # ever update on a manual GET /hints (story 9.23 task 4).
        await self._broadcast("hints_changed", payload)
        await self._push_hints_to_api(slot, payload)

    async def _push_hints_to_api(self, slot: int, payload: dict[str, Any]) -> None:
        """Push a slot's hints to Symfony for the Mercure topic runs/{id}/slots/{n}/hints."""
        url = self._config.central_api_url
        secret = self._config.central_api_secret
        if not url or not secret:
            return
        endpoint = (
            f"{url.rstrip('/')}"
            f"/api/v1/internal/sessions/{self._config.session_id}/slots/{slot}/hints-push"
        )
        headers = {"X-Internal-Secret": secret}
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.post(endpoint, json=payload, headers=headers)
                if resp.status_code not in (200, 204):
                    self._log.warning("hints push: unexpected status %d for slot %d", resp.status_code, slot)
        except Exception as exc:
            self._log.warning("hints push error slot %d: %s", slot, exc)

    # ------------------------------------------------------------------
    # Reconnect loop
    # ------------------------------------------------------------------

    async def run_with_reconnect(self) -> None:
        retry_idx = 0
        while True:
            try:
                await self._connect_and_run()
                self.ws_connected = False
                self._ws = None
                retry_idx = 0
                self._log.info("ws connection closed cleanly, reconnecting in 5s")
                await asyncio.sleep(5)
            except Exception as exc:
                self.ws_connected = False
                self._ws = None
                delay = _WS_RETRY_DELAYS[min(retry_idx, len(_WS_RETRY_DELAYS) - 1)]
                self._log.warning("ws disconnected (%s), reconnecting in %ds", exc, delay)
                retry_idx += 1
                await asyncio.sleep(delay)
