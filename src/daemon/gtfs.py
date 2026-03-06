"""GTFS-Realtime parsing helpers."""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2


def parse_feed_entity_count(payload: bytes) -> int | None:
    """Parse GTFS-RT payload and return entity count when it looks valid.

    Returns None when payload is not a valid GTFS-RT feed. This function is
    intentionally permissive because some endpoints may return non-GTFS content
    despite successful HTTP status.
    """

    msg = gtfs_realtime_pb2.FeedMessage()
    try:
        msg.ParseFromString(payload)
    except Exception:
        return None

    # A parsed message with no version and no entities is treated as invalid.
    if not msg.header.gtfs_realtime_version and len(msg.entity) == 0:
        return None
    return len(msg.entity)
