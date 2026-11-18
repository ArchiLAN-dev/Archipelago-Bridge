"""Story 9.56: what the console may send over the bridge's permanent connection."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from bridge.bridge import ArchipelagoClient, Config, StateManager
from bridge.core.commands import slot_acting_command


@pytest.mark.parametrize("text", ["!hint Master Sword", " !HINT x", "!hint_location Cave", "!release", "!collect", "!getitem Bomb", "!alias Bob", "!ready"])
def test_commands_that_act_on_the_connected_slot_are_named(text: str) -> None:
    assert slot_acting_command(text) is not None


@pytest.mark.parametrize("text", ["!admin /hint Alice Sword", "!admin login secret", "!players", "!status", "!countdown 10", "hello everyone", "!", ""])
def test_admin_commands_chat_and_read_only_commands_are_safe(text: str) -> None:
    assert slot_acting_command(text) is None


@pytest.mark.asyncio
async def test_an_empty_roster_tracks_nothing_instead_of_guessing_a_slot(caplog: pytest.LogCaptureFixture) -> None:
    # Story 9.56: there is no "Bridge" slot to fall back on any more.
    client = ArchipelagoClient(Config(session_id="run-1", internal_token="secret"), StateManager(), AsyncMock())
    client._connect_and_run = AsyncMock()  # type: ignore[method-assign]

    await client.run_with_reconnect()

    client._connect_and_run.assert_not_awaited()
    assert "SLOT_NAMES is empty" in caplog.text
