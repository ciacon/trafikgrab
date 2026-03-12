from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from daemon.config import AppConfig
from daemon.http_client import fetch, probe_health


@pytest.mark.asyncio
@pytest.mark.unit
async def test_fetch_returns_200_body_and_headers(
    make_config: Callable[..., AppConfig],
) -> None:
    cfg = make_config(retry_count=0)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["User-Agent"]
        return httpx.Response(
            200,
            content=b"abc",
            headers={"ETag": "etag-1", "Last-Modified": "lm-1"},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        result = await fetch(client, cfg, etag=None, last_modified=None)
    finally:
        await client.aclose()

    assert result.status == 200
    assert result.body == b"abc"
    assert result.etag == "etag-1"
    assert result.last_modified == "lm-1"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_fetch_retries_and_succeeds_after_transient_error(
    make_config: Callable[..., AppConfig],
) -> None:
    cfg = make_config(retry_count=1, retry_backoff_seconds=0)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("temporary", request=request)
        return httpx.Response(304)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        result = await fetch(client, cfg, etag="a", last_modified="b")
    finally:
        await client.aclose()

    assert calls == 2
    assert result.status == 304
    assert result.body is None


@pytest.mark.asyncio
@pytest.mark.unit
async def test_probe_health_true_for_non_5xx(
    make_config: Callable[..., AppConfig],
) -> None:
    cfg = make_config()

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(404)))
    try:
        healthy = await probe_health(client, cfg)
    finally:
        await client.aclose()

    assert healthy is True


@pytest.mark.asyncio
@pytest.mark.unit
async def test_probe_health_false_for_5xx(
    make_config: Callable[..., AppConfig],
) -> None:
    cfg = make_config()

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(503)))
    try:
        healthy = await probe_health(client, cfg)
    finally:
        await client.aclose()

    assert healthy is False
