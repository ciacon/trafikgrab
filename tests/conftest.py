from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from google.transit import gtfs_realtime_pb2

from daemon.config import AppConfig
from daemon.state import DaemonState


def _build_gtfs_payload(entity_count: int) -> bytes:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    for idx in range(entity_count):
        entity = msg.entity.add()
        entity.id = str(idx)
    return msg.SerializeToString()


@pytest.fixture
def gtfs_payload_factory() -> Callable[[int], bytes]:
    return _build_gtfs_payload


@pytest.fixture
def make_state() -> Callable[..., DaemonState]:
    def _make_state(**overrides: Any) -> DaemonState:
        values: dict[str, Any] = {
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

    return _make_state


@pytest.fixture
def make_config(tmp_path: Path) -> Callable[..., AppConfig]:
    def _make_config(**overrides: Any) -> AppConfig:
        values: dict[str, Any] = {
            "base_url": "https://example.test/feed.pb",
            "output_path": tmp_path / "latest.pb",
            "archive_root": tmp_path / "archive",
            "region": "stockholm",
            "scheduler_timezone": "Europe/Stockholm",
            "scheduler_db_path": tmp_path / "scheduler.sqlite3",
            "state_db_path": tmp_path / "state.sqlite3",
            "state_table": "app_state",
            "user_agent": "trafikgrab-tests/1.0",
            "request_timeout_seconds": 5.0,
            "retry_count": 0,
            "retry_backoff_seconds": 0.01,
            "normal_interval_seconds": 30,
            "night_interval_seconds": 600,
            "peak_interval_seconds": 2,
            "peak_am_start": "07:00",
            "peak_am_end": "09:00",
            "peak_pm_start": "16:00",
            "peak_pm_end": "18:00",
            "monthly_limit": 100,
            "api_cost_per_poll": 1,
            "breaker_failure_threshold": 2,
            "breaker_cooldown_seconds": 120,
            "breaker_probe_interval_seconds": 60,
            "poller_misfire_grace_seconds": 5,
            "lock_file": tmp_path / "trafikgrab.lock",
        }
        values.update(overrides)
        return AppConfig(**values)

    return _make_config
