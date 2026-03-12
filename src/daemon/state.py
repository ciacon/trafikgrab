"""SQLite-backed application state.

This store persists daemon business state that APScheduler does not model:
quota counters, breaker timers, HTTP metadata, and latest artifact metadata.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator

import pendulum


@dataclass(slots=True)
class DaemonState:
    """Mutable snapshot of persisted daemon state."""

    month_key: str
    month_used: int
    month_limit: int
    consecutive_failures: int
    breaker_open_until_utc: str | None
    last_attempt_utc: str | None
    last_success_utc: str | None
    last_http_status: int | None
    etag: str | None
    last_modified: str | None
    last_download_sha256: str | None
    last_output_path: str | None
    last_archive_path: str | None
    last_archive_metadata_path: str | None
    last_archive_downloaded_at_utc: str | None
    last_gtfs_entity_count: int | None
    api_cost_per_poll: int
    cooldown_seconds: int


def utc_now_iso() -> str:
    """Current UTC timestamp encoded as ISO-8601 string."""

    return str(pendulum.now("UTC").to_iso8601_string())


def month_key_utc() -> str:
    """Stable monthly key used for budget tracking (YYYY-MM in UTC)."""

    return str(pendulum.now("UTC").format("YYYY-MM"))


class StateStore:
    """Simple key/value state store over SQLite."""

    def __init__(self, db_path: Path, table_name: str = "app_state") -> None:
        self.db_path = db_path
        self.table_name = table_name
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        """Yield transactional SQLite connection and auto-commit on success."""

        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Ensure key/value table exists."""

        with self._conn() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                )
                """
            )

    def _get(self, key: str, default: Any = None) -> Any:
        """Load one value by key, returning default when absent."""

        with self._conn() as conn:
            row = conn.execute(
                f"SELECT v FROM {self.table_name} WHERE k = ?", (key,)
            ).fetchone()
        if row is None:
            return default
        return json.loads(row[0])

    def _set(self, key: str, value: Any) -> None:
        """Upsert one JSON-encoded key/value pair."""

        payload = json.dumps(value)
        with self._conn() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.table_name}(k, v) VALUES(?, ?)
                ON CONFLICT(k) DO UPDATE SET v = excluded.v
                """,
                (key, payload),
            )

    def load(self, month_limit: int, api_cost_per_poll: int, cooldown_seconds: int) -> DaemonState:
        """Load full daemon state with defaults for missing keys."""

        month_key = self._get("month_key", month_key_utc())
        return DaemonState(
            month_key=month_key,
            month_used=int(self._get("month_used", 0)),
            month_limit=int(self._get("month_limit", month_limit)),
            consecutive_failures=int(self._get("consecutive_failures", 0)),
            breaker_open_until_utc=self._get("breaker_open_until_utc", None),
            last_attempt_utc=self._get("last_attempt_utc", None),
            last_success_utc=self._get("last_success_utc", None),
            last_http_status=self._get("last_http_status", None),
            etag=self._get("etag", None),
            last_modified=self._get("last_modified", None),
            last_download_sha256=self._get("last_download_sha256", None),
            last_output_path=self._get("last_output_path", None),
            last_archive_path=self._get("last_archive_path", None),
            last_archive_metadata_path=self._get("last_archive_metadata_path", None),
            last_archive_downloaded_at_utc=self._get("last_archive_downloaded_at_utc", None),
            last_gtfs_entity_count=self._get("last_gtfs_entity_count", None),
            api_cost_per_poll=int(self._get("api_cost_per_poll", api_cost_per_poll)),
            cooldown_seconds=int(self._get("cooldown_seconds", cooldown_seconds)),
        )

    def save(self, state: DaemonState) -> None:
        """Persist all fields from current state snapshot."""

        for key, value in asdict(state).items():
            self._set(key, value)

    def reset_month(self, month_limit: int) -> None:
        """Reset monthly counters; called by monthly cron gearbox job."""

        self._set("month_key", month_key_utc())
        self._set("month_used", 0)
        self._set("month_limit", month_limit)
