from __future__ import annotations

import re
from pathlib import Path

import pytest

from daemon import state as state_module
from daemon.state import StateStore


@pytest.mark.integration
def test_state_store_loads_defaults(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3", "app_state")

    loaded = store.load(month_limit=123, api_cost_per_poll=2, cooldown_seconds=99)

    assert re.fullmatch(r"\d{4}-\d{2}", loaded.month_key) is not None
    assert loaded.month_used == 0
    assert loaded.month_limit == 123
    assert loaded.api_cost_per_poll == 2
    assert loaded.cooldown_seconds == 99


@pytest.mark.integration
def test_state_store_save_and_reload_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    store = StateStore(db_path, "app_state")
    loaded = store.load(month_limit=100, api_cost_per_poll=1, cooldown_seconds=60)
    loaded.month_used = 7
    loaded.last_http_status = 304
    loaded.etag = "etag-1"
    loaded.last_archive_path = "/tmp/archive.pb"
    store.save(loaded)

    reloaded = StateStore(db_path, "app_state").load(
        month_limit=999,
        api_cost_per_poll=9,
        cooldown_seconds=9,
    )

    assert reloaded.month_used == 7
    assert reloaded.last_http_status == 304
    assert reloaded.etag == "etag-1"
    assert reloaded.last_archive_path == "/tmp/archive.pb"
    assert reloaded.month_limit == 100
    assert reloaded.api_cost_per_poll == 1


@pytest.mark.integration
def test_state_store_reset_month(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = StateStore(tmp_path / "state.sqlite3", "app_state")
    monkeypatch.setattr(state_module, "month_key_utc", lambda: "2031-10")

    store.reset_month(month_limit=555)
    loaded = store.load(month_limit=1, api_cost_per_poll=1, cooldown_seconds=1)

    assert loaded.month_key == "2031-10"
    assert loaded.month_used == 0
    assert loaded.month_limit == 555
