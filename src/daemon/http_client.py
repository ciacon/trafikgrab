"""Async HTTP client helpers for poll and health probe."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from .config import AppConfig


@dataclass(slots=True)
class FetchResult:
    """Response envelope used by poller decision logic."""

    status: int
    body: bytes | None
    etag: str | None
    last_modified: str | None


def create_client(config: AppConfig) -> httpx.AsyncClient:
    """Build a long-lived async client shared by polling and health probes."""

    timeout = httpx.Timeout(config.request_timeout_seconds)
    return httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": config.user_agent},
    )


async def fetch(
    client: httpx.AsyncClient,
    config: AppConfig,
    etag: str | None,
    last_modified: str | None,
) -> FetchResult:
    """Fetch remote artifact with conditional request headers and retries."""

    headers: dict[str, str] = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    last_exc: Exception | None = None

    for attempt in range(config.retry_count + 1):
        try:
            resp = await client.get(config.base_url, headers=headers)
            body = resp.content if resp.status_code == 200 else None
            return FetchResult(
                status=resp.status_code,
                body=body,
                etag=resp.headers.get("ETag"),
                last_modified=resp.headers.get("Last-Modified"),
            )
        except (httpx.HTTPError, asyncio.TimeoutError) as exc:
            last_exc = exc
            if attempt < config.retry_count:
                await asyncio.sleep(config.retry_backoff_seconds)
            continue

    assert last_exc is not None
    raise last_exc


async def probe_health(client: httpx.AsyncClient, config: AppConfig) -> bool:
    """Cheap health probe used by breaker recovery job."""

    try:
        resp = await client.get(config.base_url)
        return resp.status_code < 500
    except (httpx.HTTPError, asyncio.TimeoutError):
        return False
