"""Error reporting to Bugsink (Sentry protocol), story 33.32.

Off unless ``SENTRY_DSN`` is set: a local run or a test sends nothing. Errors only (no performance traces), no
personal data: the SDK's default PII is off and request headers and bodies are dropped before sending.
"""
from __future__ import annotations

import os
import re
from typing import Any

import sentry_sdk

_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def _scrub(event: dict[str, Any], _hint: dict[str, Any]) -> dict[str, Any] | None:
    request = event.get("request")
    if isinstance(request, dict):
        request.pop("headers", None)
        request.pop("cookies", None)
        request.pop("data", None)
    for entry in (event.get("exception") or {}).get("values") or []:
        if isinstance(entry, dict) and isinstance(entry.get("value"), str):
            entry["value"] = _EMAIL.sub("[email]", entry["value"])
    logentry = event.get("logentry")
    if isinstance(logentry, dict) and isinstance(logentry.get("message"), str):
        logentry["message"] = _EMAIL.sub("[email]", logentry["message"])
    return event


def init_error_reporting(session_id: str, environ: dict[str, str] | None = None) -> bool:
    """Starts the SDK when a DSN is configured; returns whether it did."""
    env = os.environ if environ is None else environ
    dsn = env.get("SENTRY_DSN", "").strip()
    if dsn == "":
        return False
    sentry_sdk.init(
        dsn=dsn,
        environment=env.get("SENTRY_ENVIRONMENT", "production"),
        release=env.get("SENTRY_RELEASE") or None,
        send_default_pii=False,
        traces_sample_rate=0.0,
        before_send=_scrub,
    )
    sentry_sdk.set_tag("session_id", session_id)
    return True
