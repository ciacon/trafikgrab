from __future__ import annotations

from collections.abc import Callable

import pytest

from daemon.gtfs import parse_feed_entity_count


@pytest.mark.unit
def test_parse_feed_entity_count_valid_payload(
    gtfs_payload_factory: Callable[[int], bytes],
) -> None:
    payload = gtfs_payload_factory(2)

    count = parse_feed_entity_count(payload)

    assert count == 2


@pytest.mark.unit
def test_parse_feed_entity_count_invalid_payload_returns_none() -> None:
    count = parse_feed_entity_count(b"not-a-gtfs-feed")

    assert count is None


@pytest.mark.unit
def test_parse_feed_entity_count_empty_message_returns_none() -> None:
    empty = b""

    count = parse_feed_entity_count(empty)

    assert count is None
