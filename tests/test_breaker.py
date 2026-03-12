from __future__ import annotations

import pendulum
import pytest

from daemon import breaker
from daemon.state import DaemonState


def _state(**overrides: object) -> DaemonState:
    values: dict[str, object] = {
        "month_key": "2026-03",
        "month_used": 0,
        "month_limit": 100,
        "consecutive_failures": 0,
        "breaker_open_until_utc": None,
        "last_attempt_utc": None,
        "last_success_utc": None,
        "last_http_status": None,
        "etag": None,
        "last_modified": None,
        "last_download_sha256": None,
        "last_output_path": None,
        "last_archive_path": None,
        "last_archive_metadata_path": None,
        "last_archive_downloaded_at_utc": None,
        "last_gtfs_entity_count": None,
        "api_cost_per_poll": 1,
        "cooldown_seconds": 60,
    }
    values.update(overrides)
    return DaemonState(**values)


@pytest.mark.unit
def test_is_open_true_when_cooldown_in_future(monkeypatch: pytest.MonkeyPatch) -> None:
    now = pendulum.datetime(2026, 3, 6, 12, 0, 0, tz="UTC")
    monkeypatch.setattr(breaker.pendulum, "now", lambda _: now)
    state = _state(breaker_open_until_utc=now.add(seconds=10).to_iso8601_string())

    assert breaker.is_open(state) is True


@pytest.mark.unit
def test_is_open_false_when_cooldown_expired(monkeypatch: pytest.MonkeyPatch) -> None:
    now = pendulum.datetime(2026, 3, 6, 12, 0, 0, tz="UTC")
    monkeypatch.setattr(breaker.pendulum, "now", lambda _: now)
    state = _state(breaker_open_until_utc=now.subtract(seconds=1).to_iso8601_string())

    assert breaker.is_open(state) is False


@pytest.mark.unit
def test_register_success_resets_failure_state() -> None:
    state = _state(consecutive_failures=3, breaker_open_until_utc="2026-03-06T12:01:00Z")

    updated = breaker.register_success(state)

    assert updated.consecutive_failures == 0
    assert updated.breaker_open_until_utc is None


@pytest.mark.unit
def test_register_outage_failure_opens_breaker_at_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = pendulum.datetime(2026, 3, 6, 12, 0, 0, tz="UTC")
    monkeypatch.setattr(breaker.pendulum, "now", lambda _: now)
    state = _state(consecutive_failures=1)

    updated = breaker.register_outage_failure(state, threshold=2, cooldown_seconds=30)

    assert updated.consecutive_failures == 2
    assert updated.breaker_open_until_utc == "2026-03-06T12:00:30Z"


@pytest.mark.unit
def test_register_outage_failure_below_threshold_does_not_open() -> None:
    state = _state(consecutive_failures=0)

    updated = breaker.register_outage_failure(state, threshold=2, cooldown_seconds=30)

    assert updated.consecutive_failures == 1
    assert updated.breaker_open_until_utc is None
