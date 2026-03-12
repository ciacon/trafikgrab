"""Filesystem helpers for safe artifact writes."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

import pendulum


def atomic_write_bytes(path: Path, data: bytes) -> str:
    """Write bytes atomically and return SHA-256 digest.

    Strategy:
    1. Write to sibling temporary file.
    2. Flush and fsync file contents.
    3. Atomically replace target path.

    This avoids torn writes if the process crashes during update.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(data).hexdigest()
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    return digest


def atomic_write_text(path: Path, content: str) -> None:
    """Write text atomically using UTF-8."""

    atomic_write_bytes(path, content.encode("utf-8"))


def sanitize_region(region: str) -> str:
    """Convert region to filesystem-safe token."""

    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", region.strip())
    cleaned = cleaned.strip("-_")
    if cleaned:
        return cleaned.lower()
    return "unknown"


def build_archive_path(archive_root: Path, region: str, captured_at_utc: pendulum.DateTime) -> Path:
    """Build UTC/Zulu archive path using YYYY/MM/YYYYMMDD-HHMMSSZ format."""

    safe_region = sanitize_region(region)
    year = str(captured_at_utc.format("YYYY"))
    month = str(captured_at_utc.format("MM"))
    stamp = str(captured_at_utc.format("YYYYMMDD-HHmmss"))
    filename = f"{stamp}Z_trafikgrab_{safe_region}.pb"
    return archive_root / year / month / filename


def _next_available_archive_path(base_path: Path) -> Path:
    """Return base path or collision-safe variant if timestamp already exists."""

    if not base_path.exists():
        return base_path

    for idx in range(1, 1000):
        candidate = base_path.with_name(f"{base_path.stem}_{idx:03d}{base_path.suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError("could not allocate unique archive path")


def write_archive_record(
    archive_root: Path,
    region: str,
    captured_at_utc: pendulum.DateTime,
    payload: bytes,
    metadata: dict[str, Any],
) -> tuple[Path, Path, str]:
    """Persist immutable payload + sidecar metadata for replay/auditing.

    Returns:
        (payload_path, metadata_path, sha256)
    """

    base_payload_path = build_archive_path(archive_root, region, captured_at_utc)
    payload_path = _next_available_archive_path(base_payload_path)
    digest = atomic_write_bytes(payload_path, payload)

    metadata_path = payload_path.with_suffix(".json")
    sidecar = dict(metadata)
    sidecar["downloaded_at_utc"] = captured_at_utc.to_iso8601_string()
    sidecar["archive_path"] = str(payload_path)
    sidecar["sha256"] = digest

    atomic_write_text(
        metadata_path,
        json.dumps(sidecar, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n",
    )

    return payload_path, metadata_path, digest
