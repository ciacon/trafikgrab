from __future__ import annotations

import pytest

from daemon import quota
from daemon.state import DaemonState


def _state(**overrides: object) -> DaemonState:
    values: dict[str, object] = {
        "month_key": "2026-03",
        "month_used": 0,
        "month_limit": 10,
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
def test_refresh_month_if_needed_rolls_over(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(quota, "month_key_utc", lambda: "2026-04")
    state = _state(month_key="2026-03", month_used=9, month_limit=99)

    updated = quota.refresh_month_if_needed(state, configured_limit=55)

    assert updated.month_key == "2026-04"
    assert updated.month_used == 0
    assert updated.month_limit == 55


@pytest.mark.unit
def test_refresh_month_if_needed_no_change(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(quota, "month_key_utc", lambda: "2026-03")
    state = _state(month_key="2026-03", month_used=4, month_limit=10)

    updated = quota.refresh_month_if_needed(state, configured_limit=77)

    assert updated.month_key == "2026-03"
    assert updated.month_used == 4
    assert updated.month_limit == 10


@pytest.mark.unit
def test_can_spend_reflects_monthly_budget() -> None:
    allowed = _state(month_used=9, month_limit=10, api_cost_per_poll=1)
    blocked = _state(month_used=10, month_limit=10, api_cost_per_poll=1)

    assert quota.can_spend(allowed) is True
    assert quota.can_spend(blocked) is False


@pytest.mark.unit
def test_consume_increments_by_api_cost() -> None:
    state = _state(month_used=2, api_cost_per_poll=3)

    updated = quota.consume(state)

    assert updated.month_used == 5
