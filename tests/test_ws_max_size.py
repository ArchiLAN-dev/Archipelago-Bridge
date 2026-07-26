"""Regression tests for the AP websocket frame-size limit.

The websockets library defaults to ``max_size=`` 1 MiB, but an AP DataPackage frame grows
with the session's games: a 16-custom-world session exceeded it, so every bridge connect
died with close 1009 ("message too big" - frame exceeds limit of 1048576 bytes) right after
joining the room, and the session never got a working bridge. Every ``websockets.connect``
in the AP client must therefore pass an explicit, generous ``max_size``.
"""
from __future__ import annotations

import ast
import pathlib

from bridge.core.ap_client import _WS_MAX_SIZE

_AP_CLIENT_PATH = pathlib.Path(__file__).resolve().parents[1] / "core" / "ap_client.py"


def _websocket_connect_calls() -> list[ast.Call]:
    tree = ast.parse(_AP_CLIENT_PATH.read_text(encoding="utf-8"))
    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "connect"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "websockets"
        ):
            calls.append(node)
    return calls


def test_every_ws_connect_passes_max_size() -> None:
    calls = _websocket_connect_calls()
    assert calls, "no websockets.connect call found in ap_client.py - test needs updating"
    for call in calls:
        keywords = {kw.arg for kw in call.keywords}
        assert "max_size" in keywords, (
            f"websockets.connect at line {call.lineno} relies on the 1 MiB default max_size; "
            "a large-session DataPackage frame will kill the connection with close 1009"
        )


def test_max_size_is_generous() -> None:
    # 1 MiB (the library default) was exceeded by a real 16-game session; anything under a
    # few MiB just moves the cliff. 64 MiB keeps headroom while still bounding memory.
    assert _WS_MAX_SIZE >= 16 * 2**20
