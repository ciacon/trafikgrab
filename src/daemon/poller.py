"""Poller and recovery jobs.

`run_poller_job` is the only job that performs remote fetches.
It enforces breaker/quota decisions before I/O and persists all outcomes.
"""

from __future__ import annotations

import asyncio

import pendulum
from loguru import logger

from . import breaker, http_client, quota
from .files import atomic_write_bytes, write_archive_record
from .gtfs import parse_feed_entity_count
from .runtime import get_runtime
from .state import utc_now_iso

# Belt-and-suspenders protection in addition to APScheduler max_instances=1.
_poll_lock = asyncio.Lock()


async def run_poller_job() -> None:
    """Execute one polling cycle with quota and breaker controls."""

    runtime = get_runtime()
    cfg = runtime.config

    async with _poll_lock:
        state = runtime.state.load(
            month_limit=cfg.monthly_limit,
            api_cost_per_poll=cfg.api_cost_per_poll,
            cooldown_seconds=cfg.breaker_cooldown_seconds,
        )

        # Keep monthly counters coherent across restarts/month boundaries.
        state = quota.refresh_month_if_needed(state, cfg.monthly_limit)
        state.last_attempt_utc = utc_now_iso()

        if breaker.is_open(state):
            logger.bind(event="poll_skip_breaker_open").info("poll skipped: breaker open")
            runtime.state.save(state)
            return

        # Quota check is done before request to avoid accidental budget overflow.
        if not quota.can_spend(state):
            runtime.scheduler.pause_job("poller")
            logger.bind(event="poller_paused_quota").warning("poller paused: monthly budget exhausted")
            runtime.state.save(state)
            return

        try:
            result = await http_client.fetch(cfg, state.etag, state.last_modified)
            state.last_http_status = result.status

            if result.status == 200 and result.body is not None:
                captured_at_utc = pendulum.now("UTC")
                archive_path, archive_metadata_path, digest = write_archive_record(
                    archive_root=cfg.archive_root,
                    region=cfg.region,
                    captured_at_utc=captured_at_utc,
                    payload=result.body,
                    metadata={
                        "source_url": cfg.base_url,
                        "region": cfg.region,
                        "http_status": result.status,
                        "etag": result.etag,
                        "last_modified": result.last_modified,
                    },
                )

                atomic_write_bytes(cfg.output_path, result.body)
                state.last_download_sha256 = digest
                state.last_output_path = str(cfg.output_path)
                state.last_archive_path = str(archive_path)
                state.last_archive_metadata_path = str(archive_metadata_path)
                state.last_archive_downloaded_at_utc = str(captured_at_utc.to_iso8601_string())
                state.last_success_utc = utc_now_iso()
                state.etag = result.etag or state.etag
                state.last_modified = result.last_modified or state.last_modified

                # Optional GTFS visibility for debugging feed quality changes.
                entity_count = parse_feed_entity_count(result.body)
                state.last_gtfs_entity_count = entity_count

                quota.consume(state)
                breaker.register_success(state)
                logger.bind(
                    event="poll_success_200",
                    gtfs_entities=entity_count,
                    archive_path=str(archive_path),
                ).info(
                    "poll success: content updated"
                )
            elif result.status == 304:
                quota.consume(state)
                breaker.register_success(state)
                logger.bind(event="poll_success_304").info("poll success: not modified")
            elif 500 <= result.status <= 599:
                breaker.register_outage_failure(
                    state, cfg.breaker_failure_threshold, cfg.breaker_cooldown_seconds
                )
                logger.bind(event="poll_fail_5xx", status=result.status).warning(
                    "poll failure: server outage"
                )
            else:
                # 4xx class is treated as non-outage by default.
                state.consecutive_failures = 0
                logger.bind(event="poll_fail_non_outage", status=result.status).warning(
                    "poll non-outage failure"
                )

        except Exception:
            breaker.register_outage_failure(
                state, cfg.breaker_failure_threshold, cfg.breaker_cooldown_seconds
            )
            logger.bind(event="poll_fail_exception").exception("poll failure: network or timeout")

        # Pause poller when breaker transitions to open.
        if breaker.is_open(state):
            runtime.scheduler.pause_job("poller")
            logger.bind(event="poller_paused_breaker").warning("poller paused: breaker opened")

        runtime.state.save(state)


async def run_recovery_job() -> None:
    """Probe endpoint health and resume paused poller when breaker cooldown ends."""

    runtime = get_runtime()
    cfg = runtime.config
    state = runtime.state.load(
        month_limit=cfg.monthly_limit,
        api_cost_per_poll=cfg.api_cost_per_poll,
        cooldown_seconds=cfg.breaker_cooldown_seconds,
    )

    if not breaker.is_open(state):
        return

    until = pendulum.parse(state.breaker_open_until_utc).in_timezone("UTC") if state.breaker_open_until_utc else None
    if until and until > pendulum.now("UTC"):
        return

    healthy = await http_client.probe_health(cfg)
    if healthy:
        state.breaker_open_until_utc = None
        state.consecutive_failures = 0
        runtime.state.save(state)
        runtime.scheduler.resume_job("poller")
        logger.bind(event="poller_resumed_recovery").info("poller resumed: recovery probe healthy")
