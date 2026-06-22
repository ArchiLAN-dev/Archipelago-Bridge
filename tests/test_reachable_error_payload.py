"""Regression: reachable.py emits {"error": "..."} (exit 0, valid JSON) when a single
per-request compute fails - e.g. in --daemon mode. _compute_reachable must surface that as a
failure (None, error), NOT return it as a successful reachability result, and must NOT cache it
(a cached error would stick until the slot state changes). Pairs with the archipelago-side
protocol_io fix that keeps apworld prints off the protocol stdout in the first place."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import pytest

from bridge.core import reachable
from bridge.core.reachable import _compute_reachable
from bridge.core.state import StateManager


class _StubRuntime:
    """Runtime stub whose reachability daemon returns a fixed JSON line."""

    def __init__(self, payload: str) -> None:
        self._payload = payload
        self.calls = 0

    async def run_reachable(self, *, slot: int, arch_file: str, yamls_dir: str, state_json: str) -> str:
        self.calls += 1
        return self._payload


def _make_state_with_archipelago(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> StateManager:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "AP_seed.archipelago").write_bytes(b"x")  # _compute_reachable needs one to exist
    monkeypatch.setenv("AP_OUTPUT_DIR", str(output_dir))
    state = StateManager()
    state.ensure_slot(2)
    return state


@pytest.mark.asyncio
async def test_error_payload_is_surfaced_and_not_cached(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reachable._reachable_cache.clear()
    state = _make_state_with_archipelago(monkeypatch, tmp_path)
    runtime = _StubRuntime(json.dumps({"error": "No world found to handle game Simpsons Hit and Run"}))

    result, err = await _compute_reachable(
        2, state, asyncio.Semaphore(1), logging.getLogger("test"), runtime=runtime
    )

    assert result is None, "an {'error': ...} payload must not be returned as a valid result"
    assert err == "No world found to handle game Simpsons Hit and Run"
    assert 2 not in reachable._reachable_cache, "an error payload must not be cached"


@pytest.mark.asyncio
async def test_valid_result_is_returned_and_cached(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reachable._reachable_cache.clear()
    state = _make_state_with_archipelago(monkeypatch, tmp_path)
    payload = {"game": "X", "counts": {"reachable_now": 4}}
    runtime = _StubRuntime(json.dumps(payload))

    result, err = await _compute_reachable(
        2, state, asyncio.Semaphore(1), logging.getLogger("test"), runtime=runtime
    )

    assert err == ""
    assert result == payload
    assert 2 in reachable._reachable_cache, "a valid result should be cached"