from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pendulum
import pytest

from daemon import poller
from daemon.config import AppConfig
from daemon.http_client import FetchResult
from daemon.runtime import Runtime, set_runtime
from daemon.state import StateStore


class RecordingScheduler:
    def __init__(self) -> None:
        self.paused_jobs: list[str] = []
        self.resumed_jobs: list[str] = []

    def pause_job(self, job_id: str) -> None:
        self.paused_jobs.append(job_id)

    def resume_job(self, job_id: str) -> None:
        self.resumed_jobs.append(job_id)


def _init_runtime(config: AppConfig) -> tuple[Runtime, RecordingScheduler, StateStore]:
    scheduler = RecordingScheduler()
    store = StateStore(config.state_db_path, config.state_table)
    runtime = Runtime(
        config=config,
        scheduler=scheduler,  # type: ignore[arg-type]
        state=store,
        http_client=object(),  # type: ignore[arg-type]
        output_dir=config.output_path.parent,
    )
    set_runtime(runtime)
    return runtime, scheduler, store


def _seed_state(store: StateStore, mutator: Callable[[Any], None], cfg: AppConfig) -> None:
    state = store.load(
        month_limit=cfg.monthly_limit,
        api_cost_per_poll=cfg.api_cost_per_poll,
        cooldown_seconds=cfg.breaker_cooldown_seconds,
    )
    mutator(state)
    store.save(state)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_poller_skips_when_breaker_open(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config()
    _, scheduler, store = _init_runtime(cfg)
    _seed_state(
        store,
        lambda state: setattr(
            state,
            "breaker_open_until_utc",
            pendulum.now("UTC").add(minutes=5).to_iso8601_string(),
        ),
        cfg,
    )
    called = False

    async def _fetch(*args: object, **kwargs: object) -> FetchResult:
        nonlocal called
        called = True
        raise AssertionError("fetch should not be called while breaker is open")

    monkeypatch.setattr(poller.http_client, "fetch", _fetch)

    await poller.run_poller_job()

    saved = store.load(cfg.monthly_limit, cfg.api_cost_per_poll, cfg.breaker_cooldown_seconds)
    assert called is False
    assert saved.last_attempt_utc is not None
    assert scheduler.paused_jobs == []


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_poller_pauses_when_quota_exhausted(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config(monthly_limit=1, api_cost_per_poll=1)
    _, scheduler, store = _init_runtime(cfg)
    _seed_state(
        store,
        lambda state: (setattr(state, "month_limit", 1), setattr(state, "month_used", 1)),
        cfg,
    )

    async def _fetch(*args: object, **kwargs: object) -> FetchResult:
        raise AssertionError("fetch should not run when quota is exhausted")

    monkeypatch.setattr(poller.http_client, "fetch", _fetch)

    await poller.run_poller_job()

    saved = store.load(cfg.monthly_limit, cfg.api_cost_per_poll, cfg.breaker_cooldown_seconds)
    assert scheduler.paused_jobs == ["poller"]
    assert saved.month_used == 1


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_poller_200_persists_output_archive_and_state(
    make_config: Callable[..., AppConfig],
    gtfs_payload_factory: Callable[[int], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config()
    _, scheduler, store = _init_runtime(cfg)
    payload = gtfs_payload_factory(2)

    async def _fetch(*args: object, **kwargs: object) -> FetchResult:
        return FetchResult(status=200, body=payload, etag="etag-200", last_modified="lm-200")

    monkeypatch.setattr(poller.http_client, "fetch", _fetch)

    await poller.run_poller_job()

    saved = store.load(cfg.monthly_limit, cfg.api_cost_per_poll, cfg.breaker_cooldown_seconds)
    assert scheduler.paused_jobs == []
    assert cfg.output_path.exists()
    assert cfg.output_path.read_bytes() == payload
    assert saved.month_used == 1
    assert saved.last_http_status == 200
    assert saved.last_success_utc is not None
    assert saved.last_archive_path is not None
    assert saved.last_archive_metadata_path is not None
    assert saved.last_gtfs_entity_count == 2

    archive_path = Path(saved.last_archive_path)
    metadata_path = Path(saved.last_archive_metadata_path)
    assert archive_path.exists()
    assert metadata_path.exists()

    sidecar = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert sidecar["http_status"] == 200
    assert sidecar["etag"] == "etag-200"
    assert sidecar["last_modified"] == "lm-200"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_poller_304_consumes_quota_and_resets_breaker(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config()
    _, scheduler, store = _init_runtime(cfg)
    _seed_state(
        store,
        lambda state: setattr(state, "consecutive_failures", 4),
        cfg,
    )

    async def _fetch(*args: object, **kwargs: object) -> FetchResult:
        return FetchResult(status=304, body=None, etag=None, last_modified=None)

    monkeypatch.setattr(poller.http_client, "fetch", _fetch)

    await poller.run_poller_job()

    saved = store.load(cfg.monthly_limit, cfg.api_cost_per_poll, cfg.breaker_cooldown_seconds)
    assert scheduler.paused_jobs == []
    assert saved.month_used == 1
    assert saved.consecutive_failures == 0
    assert saved.breaker_open_until_utc is None
    assert not cfg.output_path.exists()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_poller_5xx_opens_breaker_and_pauses_poller(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config(breaker_failure_threshold=1, breaker_cooldown_seconds=60)
    _, scheduler, store = _init_runtime(cfg)

    async def _fetch(*args: object, **kwargs: object) -> FetchResult:
        return FetchResult(status=503, body=None, etag=None, last_modified=None)

    monkeypatch.setattr(poller.http_client, "fetch", _fetch)

    await poller.run_poller_job()

    saved = store.load(cfg.monthly_limit, cfg.api_cost_per_poll, cfg.breaker_cooldown_seconds)
    assert saved.last_http_status == 503
    assert saved.breaker_open_until_utc is not None
    assert scheduler.paused_jobs == ["poller"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_poller_exception_opens_breaker_and_pauses_poller(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config(breaker_failure_threshold=1, breaker_cooldown_seconds=60)
    _, scheduler, store = _init_runtime(cfg)

    async def _fetch(*args: object, **kwargs: object) -> FetchResult:
        raise RuntimeError("boom")

    monkeypatch.setattr(poller.http_client, "fetch", _fetch)

    await poller.run_poller_job()

    saved = store.load(cfg.monthly_limit, cfg.api_cost_per_poll, cfg.breaker_cooldown_seconds)
    assert saved.breaker_open_until_utc is not None
    assert scheduler.paused_jobs == ["poller"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_recovery_noop_when_breaker_not_open(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config()
    _, scheduler, _store = _init_runtime(cfg)
    called = False

    async def _probe(*args: object, **kwargs: object) -> bool:
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(poller.http_client, "probe_health", _probe)

    await poller.run_recovery_job()

    assert called is False
    assert scheduler.resumed_jobs == []


@pytest.mark.asyncio
@pytest.mark.integration
async def test_recovery_noop_before_cooldown_expires(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config()
    _, scheduler, store = _init_runtime(cfg)
    _seed_state(
        store,
        lambda state: setattr(
            state,
            "breaker_open_until_utc",
            pendulum.now("UTC").add(minutes=2).to_iso8601_string(),
        ),
        cfg,
    )
    called = False

    async def _probe(*args: object, **kwargs: object) -> bool:
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(poller.http_client, "probe_health", _probe)

    await poller.run_recovery_job()

    assert called is False
    assert scheduler.resumed_jobs == []


@pytest.mark.asyncio
@pytest.mark.integration
async def test_recovery_healthy_probe_resumes_poller(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config()
    _, scheduler, store = _init_runtime(cfg)
    _seed_state(
        store,
        lambda state: (
            setattr(state, "consecutive_failures", 3),
            setattr(
                state,
                "breaker_open_until_utc",
                pendulum.now("UTC").subtract(minutes=1).to_iso8601_string(),
            ),
        ),
        cfg,
    )

    async def _probe(*args: object, **kwargs: object) -> bool:
        return True

    monkeypatch.setattr(poller.http_client, "probe_health", _probe)

    await poller.run_recovery_job()

    saved = store.load(cfg.monthly_limit, cfg.api_cost_per_poll, cfg.breaker_cooldown_seconds)
    assert scheduler.resumed_jobs == ["poller"]
    assert saved.consecutive_failures == 0
    assert saved.breaker_open_until_utc is None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_recovery_unhealthy_probe_does_not_resume(
    make_config: Callable[..., AppConfig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = make_config()
    _, scheduler, store = _init_runtime(cfg)
    open_until = pendulum.now("UTC").subtract(minutes=1).to_iso8601_string()
    _seed_state(
        store,
        lambda state: (
            setattr(state, "consecutive_failures", 3),
            setattr(state, "breaker_open_until_utc", open_until),
        ),
        cfg,
    )

    async def _probe(*args: object, **kwargs: object) -> bool:
        return False

    monkeypatch.setattr(poller.http_client, "probe_health", _probe)

    await poller.run_recovery_job()

    saved = store.load(cfg.monthly_limit, cfg.api_cost_per_poll, cfg.breaker_cooldown_seconds)
    assert scheduler.resumed_jobs == []
    assert saved.consecutive_failures == 3
    assert saved.breaker_open_until_utc == open_until
