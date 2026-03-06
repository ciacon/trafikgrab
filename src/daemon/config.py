"""Configuration loading and validation.

Sources are merged in order:
1. Hardcoded defaults
2. Optional TOML file values
3. Environment overrides via TRAFIKGRAB_* variables

This keeps the daemon easy to run locally while still deployment-friendly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

import tomllib


@dataclass(slots=True)
class AppConfig:
    """Validated runtime settings for the daemon."""

    base_url: str
    output_path: Path
    archive_root: Path
    region: str
    scheduler_timezone: str
    scheduler_db_path: Path
    state_db_path: Path
    state_table: str
    user_agent: str
    request_timeout_seconds: float
    retry_count: int
    retry_backoff_seconds: float
    normal_interval_seconds: int
    night_interval_seconds: int
    peak_interval_seconds: int
    peak_am_start: str
    peak_am_end: str
    peak_pm_start: str
    peak_pm_end: str
    monthly_limit: int
    api_cost_per_poll: int
    breaker_failure_threshold: int
    breaker_cooldown_seconds: int
    breaker_probe_interval_seconds: int
    poller_misfire_grace_seconds: int
    lock_file: Path

    @property
    def timezone(self) -> ZoneInfo:
        """Return scheduler timezone as ZoneInfo object."""

        return ZoneInfo(self.scheduler_timezone)


DEFAULT_CONFIG = {
    "base_url": "https://example.invalid/data.bin",
    "output_path": "./data/download.bin",
    "archive_root": "./data/archive",
    "region": "unknown",
    "scheduler_timezone": "Europe/Stockholm",
    "scheduler_db_path": "./data/scheduler.sqlite3",
    "state_db_path": "./data/state.sqlite3",
    "state_table": "app_state",
    "user_agent": "trafikgrab/0.0.1",
    "request_timeout_seconds": 20.0,
    "retry_count": 1,
    "retry_backoff_seconds": 1.0,
    "normal_interval_seconds": 30,
    "night_interval_seconds": 600,
    "peak_interval_seconds": 2,
    "peak_am_start": "07:00",
    "peak_am_end": "09:00",
    "peak_pm_start": "16:00",
    "peak_pm_end": "18:00",
    "monthly_limit": 50_000,
    "api_cost_per_poll": 1,
    "breaker_failure_threshold": 5,
    "breaker_cooldown_seconds": 600,
    "breaker_probe_interval_seconds": 300,
    "poller_misfire_grace_seconds": 5,
    "lock_file": "./data/trafikgrab.lock",
}


def _get_toml_config(path: Path) -> dict[str, Any]:
    """Load optional TOML file and support `[trafikgrab]` sub-table."""

    if not path.exists():
        return {}
    with path.open("rb") as fh:
        raw = tomllib.load(fh)
    scoped = raw.get("trafikgrab", raw)
    if isinstance(scoped, dict):
        return cast(dict[str, Any], scoped)
    return {}


def _apply_env(config: dict[str, Any]) -> dict[str, Any]:
    """Apply `TRAFIKGRAB_<KEY>` overrides for each known config key."""

    merged = dict(config)
    for key in DEFAULT_CONFIG:
        env_key = f"TRAFIKGRAB_{key.upper()}"
        if env_key in os.environ:
            merged[key] = os.environ[env_key]
    return merged


def _to_int(value: Any) -> int:
    return int(value)


def _to_float(value: Any) -> float:
    return float(value)


def load_config(path: Path | None = None) -> AppConfig:
    """Build validated AppConfig from defaults + file + env."""

    raw = dict(DEFAULT_CONFIG)
    if path is not None:
        raw.update(_get_toml_config(path))
    raw = _apply_env(raw)

    config = AppConfig(
        base_url=str(raw["base_url"]),
        output_path=Path(str(raw["output_path"])),
        archive_root=Path(str(raw["archive_root"])),
        region=str(raw["region"]),
        scheduler_timezone=str(raw["scheduler_timezone"]),
        scheduler_db_path=Path(str(raw["scheduler_db_path"])),
        state_db_path=Path(str(raw["state_db_path"])),
        state_table=str(raw["state_table"]),
        user_agent=str(raw["user_agent"]),
        request_timeout_seconds=_to_float(raw["request_timeout_seconds"]),
        retry_count=_to_int(raw["retry_count"]),
        retry_backoff_seconds=_to_float(raw["retry_backoff_seconds"]),
        normal_interval_seconds=max(2, _to_int(raw["normal_interval_seconds"])),
        night_interval_seconds=max(2, _to_int(raw["night_interval_seconds"])),
        peak_interval_seconds=max(2, _to_int(raw["peak_interval_seconds"])),
        peak_am_start=str(raw["peak_am_start"]),
        peak_am_end=str(raw["peak_am_end"]),
        peak_pm_start=str(raw["peak_pm_start"]),
        peak_pm_end=str(raw["peak_pm_end"]),
        monthly_limit=max(1, _to_int(raw["monthly_limit"])),
        api_cost_per_poll=max(1, _to_int(raw["api_cost_per_poll"])),
        breaker_failure_threshold=max(1, _to_int(raw["breaker_failure_threshold"])),
        breaker_cooldown_seconds=max(1, _to_int(raw["breaker_cooldown_seconds"])),
        breaker_probe_interval_seconds=max(5, _to_int(raw["breaker_probe_interval_seconds"])),
        poller_misfire_grace_seconds=max(1, _to_int(raw["poller_misfire_grace_seconds"])),
        lock_file=Path(str(raw["lock_file"])),
    )

    # Hard safety invariant: never poll faster than every 2 seconds.
    if config.peak_interval_seconds < 2:
        raise ValueError("peak_interval_seconds must be >= 2")
    return config
