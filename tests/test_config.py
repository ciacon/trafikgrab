from __future__ import annotations

import os
from pathlib import Path

import pytest

from daemon.config import load_config


def _clear_trafikgrab_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("TRAFIKGRAB_"):
            monkeypatch.delenv(key, raising=False)


@pytest.mark.unit
def test_load_config_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_trafikgrab_env(monkeypatch)

    config = load_config()

    assert config.base_url == "https://example.invalid/data.bin"
    assert config.scheduler_timezone == "Europe/Stockholm"
    assert config.peak_interval_seconds >= 2
    assert config.breaker_probe_interval_seconds >= 5


@pytest.mark.unit
def test_toml_overrides_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_trafikgrab_env(monkeypatch)
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "[trafikgrab]\n"
        "base_url = 'https://toml.example/feed.pb'\n"
        "monthly_limit = 999\n"
        "normal_interval_seconds = 45\n",
        encoding="utf-8",
    )

    config = load_config(config_file)

    assert config.base_url == "https://toml.example/feed.pb"
    assert config.monthly_limit == 999
    assert config.normal_interval_seconds == 45


@pytest.mark.unit
def test_env_overrides_toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_trafikgrab_env(monkeypatch)
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "[trafikgrab]\n"
        "base_url = 'https://toml.example/feed.pb'\n"
        "monthly_limit = 100\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TRAFIKGRAB_BASE_URL", "https://env.example/feed.pb")
    monkeypatch.setenv("TRAFIKGRAB_MONTHLY_LIMIT", "321")

    config = load_config(config_file)

    assert config.base_url == "https://env.example/feed.pb"
    assert config.monthly_limit == 321


@pytest.mark.unit
def test_numeric_values_are_clamped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_trafikgrab_env(monkeypatch)
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "[trafikgrab]\n"
        "normal_interval_seconds = 0\n"
        "night_interval_seconds = 1\n"
        "peak_interval_seconds = 1\n"
        "monthly_limit = 0\n"
        "api_cost_per_poll = 0\n"
        "breaker_failure_threshold = 0\n"
        "breaker_cooldown_seconds = 0\n"
        "breaker_probe_interval_seconds = 1\n"
        "poller_misfire_grace_seconds = 0\n",
        encoding="utf-8",
    )

    config = load_config(config_file)

    assert config.normal_interval_seconds == 2
    assert config.night_interval_seconds == 2
    assert config.peak_interval_seconds == 2
    assert config.monthly_limit == 1
    assert config.api_cost_per_poll == 1
    assert config.breaker_failure_threshold == 1
    assert config.breaker_cooldown_seconds == 1
    assert config.breaker_probe_interval_seconds == 5
    assert config.poller_misfire_grace_seconds == 1
