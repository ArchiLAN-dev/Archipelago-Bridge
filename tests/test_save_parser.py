"""Tests for .apsave parsing (AC #8, #13)."""
from __future__ import annotations

import os
import pickle
import zlib
from pathlib import Path

import pytest

from bridge.bridge import load_save_state


def _write_apsave(tmp_path: Path, data: dict) -> str:
    compressed = zlib.compress(pickle.dumps(data))
    p = tmp_path / "session.apsave"
    p.write_bytes(compressed)
    return str(tmp_path)


def test_empty_save_dir_returns_empty(tmp_path: Path) -> None:
    result = load_save_state(str(tmp_path))
    assert result == {}


def test_missing_save_dir_returns_empty() -> None:
    result = load_save_state("/does/not/exist")
    assert result == {}


def test_corrupt_file_returns_empty(tmp_path: Path) -> None:
    (tmp_path / "bad.apsave").write_bytes(b"not valid")
    result = load_save_state(str(tmp_path))
    assert result == {}


def test_basic_save_state_parsed(tmp_path: Path) -> None:
    data = {
        "location_checks": {1: {100, 101, 102}, 2: {200}},
        "client_game_state": {1: 20, 2: 30},
    }
    save_dir = _write_apsave(tmp_path, data)
    result = load_save_state(save_dir)

    assert 1 in result
    assert result[1].checks_done == 3
    assert result[1].client_status == 20

    assert 2 in result
    assert result[2].checks_done == 1
    assert result[2].client_status == 30


def test_goal_status_preserved(tmp_path: Path) -> None:
    data = {
        "location_checks": {1: set(range(47))},
        "client_game_state": {1: 30},  # GOAL
    }
    save_dir = _write_apsave(tmp_path, data)
    result = load_save_state(save_dir)

    assert result[1].checks_done == 47
    assert result[1].client_status == 30


def test_slot_with_no_location_checks(tmp_path: Path) -> None:
    data = {
        "location_checks": {},
        "client_game_state": {1: 5},
    }
    save_dir = _write_apsave(tmp_path, data)
    result = load_save_state(save_dir)

    assert result[1].checks_done == 0
    assert result[1].client_status == 5


def test_slot_with_no_game_state(tmp_path: Path) -> None:
    data = {
        "location_checks": {3: {1, 2, 3}},
        "client_game_state": {},
    }
    save_dir = _write_apsave(tmp_path, data)
    result = load_save_state(save_dir)

    assert result[3].checks_done == 3
    assert result[3].client_status == 0  # default UNKNOWN


@pytest.mark.parametrize("slots,expected_counts", [
    ({1: set(range(10)), 2: set(range(5))}, {1: 10, 2: 5}),
    ({0: set(), 1: set(range(47))}, {0: 0, 1: 47}),
])
def test_multiple_slots(tmp_path: Path, slots: dict, expected_counts: dict) -> None:
    data = {"location_checks": slots, "client_game_state": {}}
    save_dir = _write_apsave(tmp_path, data)
    result = load_save_state(save_dir)

    for slot, expected in expected_counts.items():
        assert result[slot].checks_done == expected


def test_latest_file_selected_by_mtime(tmp_path: Path) -> None:
    # Write an old file with wrong data
    old_data = {"location_checks": {1: {999}}, "client_game_state": {1: 5}}
    old = tmp_path / "old.apsave"
    old.write_bytes(zlib.compress(pickle.dumps(old_data)))
    # Make old file older
    old_mtime = old.stat().st_mtime - 100
    os.utime(old, (old_mtime, old_mtime))

    # Write a new file with the correct data
    new_data = {"location_checks": {1: {1, 2}}, "client_game_state": {1: 20}}
    new = tmp_path / "new.apsave"
    new.write_bytes(zlib.compress(pickle.dumps(new_data)))

    result = load_save_state(str(tmp_path))
    assert result[1].checks_done == 2
    assert result[1].client_status == 20
# ---------------------------------------------------------------------------
# received_items slot keys: (team, slot, remote_items)
#
# MultiServer.send_items_to appends every item a slot receives to the remote_items=True list
# and only the items sent by *other* players to the False one, so True is a superset of False.
# _save_slot_map used to concatenate both, counting every received item twice. That inflated
# count-based logic in the reachability pass (state.has(item, player, n)): on The Wind Waker a
# single Progressive Bow read as two, which satisfies has_fire_arrows / has_ice_arrows and
# reported six unreachable checks as accessible.
# ---------------------------------------------------------------------------

def test_received_items_are_not_doubled(tmp_path: Path) -> None:
    """Both remote_items lists describe the same slot: keep one, never concatenate."""
    items = [(500, 100, 2), (501, 101, 3), (502, 102, 4)]
    data = {
        "location_checks": {(0, 6): {100}},
        "received_items": {(0, 6, False): list(items), (0, 6, True): list(items)},
    }
    result = load_save_state(_write_apsave(tmp_path, data))

    assert result[6].items_received == 3
    assert result[6]._received_items == [(500, 2, 100), (501, 3, 101), (502, 4, 102)]


@pytest.mark.parametrize("reversed_keys", [False, True])
def test_remote_true_list_wins_over_the_partial_one(tmp_path: Path, reversed_keys: bool) -> None:
    """`True` holds every item, `False` only those sent by other players."""
    from_others = [(500, 100, 2)]
    everything = [(500, 100, 2), (600, 200, 6)]
    keys = [((0, 6, True), everything), ((0, 6, False), from_others)]
    if not reversed_keys:
        keys.reverse()
    data = {"location_checks": {(0, 6): {100}}, "received_items": dict(keys)}

    result = load_save_state(_write_apsave(tmp_path, data))

    assert result[6].items_received == 2
    assert result[6]._received_items == [(500, 2, 100), (600, 6, 200)]


def test_received_items_fall_back_to_the_partial_list(tmp_path: Path) -> None:
    data = {
        "location_checks": {(0, 6): {100}},
        "received_items": {(0, 6, False): [(500, 100, 2)]},
    }
    result = load_save_state(_write_apsave(tmp_path, data))

    assert result[6].items_received == 1


def test_other_teams_are_ignored(tmp_path: Path) -> None:
    data = {
        "location_checks": {(0, 6): {100}, (1, 6): {999}},
        "received_items": {(0, 6, True): [(500, 100, 2)], (1, 6, True): [(999, 900, 2)]},
    }
    result = load_save_state(_write_apsave(tmp_path, data))

    assert result[6].checks_done == 1
    assert result[6].items_received == 1


class _Exploit:
    """Pickles into a call of ``pathlib.Path.touch``: what a forged save would run on load."""

    def __init__(self, marker: Path) -> None:
        self.marker = marker

    def __reduce__(self) -> tuple[object, tuple[Path]]:
        return (Path.touch, (self.marker,))


def test_a_forged_save_runs_nothing(tmp_path: Path) -> None:
    marker = tmp_path / "pwned"
    save_dir = _write_apsave(tmp_path, {"location_checks": _Exploit(marker)})

    assert load_save_state(save_dir) == {}
    assert not marker.exists()


def test_sets_pickled_as_a_global_still_load(tmp_path: Path) -> None:
    # Protocol 3 has no set opcode: the set goes through find_class, unlike the default protocol.
    (tmp_path / "old.apsave").write_bytes(zlib.compress(pickle.dumps({"location_checks": {1: {100}}}, protocol=3)))

    assert load_save_state(str(tmp_path))[1].checks_done == 1


class _FakeGlobal:
    """Pickles as a reference to `module.name`, called with `args`: how AP's own types land in a save."""

    def __init__(self, module: str, name: str, args: tuple[object, ...]) -> None:
        self.module, self.name, self.args = module, name, args

    def dump(self, protocol: int = 4) -> bytes:
        return b"".join([
            b"c", self.module.encode(), b"\n", self.name.encode(), b"\n",
            pickle.dumps(self.args, protocol=0)[:-1], b"R",
        ])


def _save_with(tmp_path: Path, *, item: _FakeGlobal, hint: _FakeGlobal, status: _FakeGlobal) -> str:
    """A save holding AP's NetUtils types, which do not exist in this image."""
    stream = b"".join([
        b"(dp0\n",
        b"S'received_items'\n(dp1\nI1\n(l", item.dump(), b"as",
        b"sS'client_game_state'\n(dp2\nI1\n", status.dump(), b"s",
        b"sS'hints'\n(dp3\nI1\n(l", hint.dump(), b"as",
        b"s.",
    ])
    (tmp_path / "ap.apsave").write_bytes(zlib.compress(stream))
    return str(tmp_path)


def test_a_save_reads_without_the_ap_sources(tmp_path: Path) -> None:
    save_dir = _save_with(
        tmp_path,
        item=_FakeGlobal("NetUtils", "NetworkItem", (500, 100, 2, 1)),
        hint=_FakeGlobal("NetUtils", "Hint", (1, 2, 81000, 123456, False, "", 1, 30)),
        status=_FakeGlobal("NetUtils", "ClientStatus", (30,)),
    )

    state = load_save_state(save_dir)[1]

    assert state._received_items == [(500, 2, 100)]
    assert state.client_status == 30
    assert state._hints[0].location_id == 81000
    assert state._hints[0].status == 30


def test_an_unknown_net_utils_name_is_refused(tmp_path: Path) -> None:
    save_dir = _save_with(
        tmp_path,
        item=_FakeGlobal("NetUtils", "encode", ("x",)),
        hint=_FakeGlobal("NetUtils", "Hint", (1, 2, 3, 4, False)),
        status=_FakeGlobal("NetUtils", "ClientStatus", (0,)),
    )

    assert load_save_state(save_dir) == {}
