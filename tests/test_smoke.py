from __future__ import annotations

import pytest

from daemon.config import load_config


@pytest.mark.unit
def test_load_config_defaults_smoke() -> None:
    config = load_config()

    assert config.base_url
    assert config.peak_interval_seconds >= 2
