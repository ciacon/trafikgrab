from __future__ import annotations

import json
from pathlib import Path

import pendulum
import pytest

from daemon.files import build_archive_path, sanitize_region, write_archive_record


@pytest.mark.unit
def test_sanitize_region_normalizes_text() -> None:
    assert sanitize_region(" Stockholm  City ") == "stockholm-city"
    assert sanitize_region("___") == "unknown"


@pytest.mark.unit
def test_build_archive_path_uses_utc_layout(tmp_path: Path) -> None:
    captured_at = pendulum.datetime(2026, 3, 6, 12, 45, 1, tz="UTC")

    path = build_archive_path(tmp_path, "Stockholm", captured_at)

    assert path == tmp_path / "2026" / "03" / "20260306-124501Z_trafikgrab_stockholm.pb"


@pytest.mark.integration
def test_write_archive_record_persists_payload_sidecar_and_sha(tmp_path: Path) -> None:
    payload = b"payload-data"
    captured_at = pendulum.datetime(2026, 3, 6, 12, 45, 1, tz="UTC")

    payload_path, metadata_path, digest = write_archive_record(
        archive_root=tmp_path,
        region="Stockholm",
        captured_at_utc=captured_at,
        payload=payload,
        metadata={"source_url": "https://example.test/feed.pb", "http_status": 200},
    )

    assert payload_path.exists()
    assert payload_path.read_bytes() == payload
    assert metadata_path.exists()
    assert len(digest) == 64

    sidecar = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert sidecar["archive_path"] == str(payload_path)
    assert sidecar["sha256"] == digest
    assert sidecar["http_status"] == 200


@pytest.mark.integration
def test_write_archive_record_adds_collision_suffix(tmp_path: Path) -> None:
    captured_at = pendulum.datetime(2026, 3, 6, 12, 45, 1, tz="UTC")

    first_payload_path, _, _ = write_archive_record(
        archive_root=tmp_path,
        region="Stockholm",
        captured_at_utc=captured_at,
        payload=b"one",
        metadata={},
    )
    second_payload_path, _, _ = write_archive_record(
        archive_root=tmp_path,
        region="Stockholm",
        captured_at_utc=captured_at,
        payload=b"two",
        metadata={},
    )

    assert first_payload_path.exists()
    assert second_payload_path.exists()
    assert second_payload_path.stem.endswith("_001")
