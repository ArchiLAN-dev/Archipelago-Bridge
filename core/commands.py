"""Commands the admin console may send over the bridge's permanent connection (story 9.56).

Since story 9.56 the bridge attaches to the first real slot of the multiworld instead of an injected
observer. A client command sent over that connection acts as that player: `!hint` would spend their hint
points, `!release` would release their world. The console therefore keeps `!admin ...` commands, plain chat
and the read-only client commands, and refuses the ones that act on the connected slot. Per-slot actions
(paid hints, hint status) go through a temporary connection as the target slot (stories 9.30, 9.32).
"""
from __future__ import annotations

# Client commands that act on the connected slot (Archipelago's ClientMessageProcessor).
_SLOT_ACTING_COMMANDS = frozenset({"hint", "hint_location", "release", "collect", "getitem", "alias", "ready"})


def slot_acting_command(text: str) -> str | None:
    """The name of a client command that would act on the connected slot, or None when the text is safe."""
    stripped = text.strip()
    if not stripped.startswith("!"):
        return None
    name = stripped[1:].split(maxsplit=1)[0].lower() if len(stripped) > 1 else ""
    return name if name in _SLOT_ACTING_COMMANDS else None
