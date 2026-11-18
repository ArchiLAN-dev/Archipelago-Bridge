from __future__ import annotations

from typing import Any
from unittest.mock import patch

from bridge.core import error_reporting
from bridge.core.error_reporting import _scrub, init_error_reporting


def test_without_a_dsn_nothing_starts() -> None:
    with patch.object(error_reporting.sentry_sdk, "init") as init:
        assert init_error_reporting("s1", {}) is False
        assert init_error_reporting("s1", {"SENTRY_DSN": "  "}) is False
    init.assert_not_called()


def test_with_a_dsn_errors_only_and_no_personal_data() -> None:
    with patch.object(error_reporting.sentry_sdk, "init") as init, patch.object(
        error_reporting.sentry_sdk, "set_tag"
    ) as set_tag:
        assert init_error_reporting("s1", {"SENTRY_DSN": "https://k@errors.example.org/3", "SENTRY_RELEASE": "0.18.0"})
    kwargs = init.call_args.kwargs
    assert kwargs["send_default_pii"] is False
    assert kwargs["traces_sample_rate"] == 0.0
    assert kwargs["release"] == "0.18.0"
    set_tag.assert_called_once_with("session_id", "s1")


def test_scrub_drops_request_details_and_masks_emails() -> None:
    event: dict[str, Any] = {
        "request": {"url": "/slots/1", "headers": {"X-Internal-Secret": "s"}, "cookies": "c", "data": "{}"},
        "exception": {"values": [{"type": "ValueError", "value": "no user alice@example.org"}]},
        "logentry": {"message": "push failed for bob@example.org"},
    }
    scrubbed = _scrub(event, {})
    assert scrubbed is not None
    assert scrubbed["request"] == {"url": "/slots/1"}
    assert scrubbed["exception"]["values"][0]["value"] == "no user [email]"
    assert scrubbed["logentry"]["message"] == "push failed for [email]"
