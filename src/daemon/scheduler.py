"""Scheduler construction and job registration.

Design rules implemented here:
- Exactly one real polling job (`poller`) uses an interval trigger.
- Cron "gearbox" jobs only mutate `poller` interval via `reschedule_job`.
- Pause/resume is used for breaker and budget control.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from .config import AppConfig
from .runtime import get_runtime


def _hhmm(value: str) -> tuple[int, int]:
    """Parse HH:MM string into integer hour/minute."""

    hour_str, minute_str = value.split(":", 1)
    return int(hour_str), int(minute_str)


def create_scheduler(config: AppConfig) -> AsyncIOScheduler:
    """Create AsyncIOScheduler with persistent SQLite job store."""

    config.scheduler_db_path.parent.mkdir(parents=True, exist_ok=True)
    return AsyncIOScheduler(
        timezone=ZoneInfo(config.scheduler_timezone),
        jobstores={"default": SQLAlchemyJobStore(url=f"sqlite:///{config.scheduler_db_path}")},
    )


def _reschedule_poller(interval_seconds: int, jitter: int | None = None) -> None:
    """Replace poller interval trigger while preserving single job identity."""

    runtime = get_runtime()
    safe_interval = max(2, interval_seconds)
    runtime.scheduler.reschedule_job(
        "poller",
        trigger=IntervalTrigger(
            seconds=safe_interval,
            start_date=datetime.now(tz=runtime.config.timezone),
            timezone=runtime.config.timezone,
            jitter=jitter,
        ),
    )
    logger.bind(event="poller_rescheduled", interval_seconds=safe_interval).info(
        "poller interval changed"
    )


def set_mode_night() -> None:
    """Gearbox hook: switch poller to low-frequency night mode."""

    runtime = get_runtime()
    _reschedule_poller(runtime.config.night_interval_seconds, jitter=2)


def set_mode_normal() -> None:
    """Gearbox hook: switch poller to normal daytime frequency."""

    runtime = get_runtime()
    _reschedule_poller(runtime.config.normal_interval_seconds, jitter=1)


def set_mode_peak() -> None:
    """Gearbox hook: switch poller to strict peak frequency (no jitter)."""

    runtime = get_runtime()
    _reschedule_poller(runtime.config.peak_interval_seconds, jitter=None)


def monthly_reset() -> None:
    """Reset monthly budget and resume poller if it was quota-paused."""

    runtime = get_runtime()
    runtime.state.reset_month(runtime.config.monthly_limit)
    runtime.scheduler.resume_job("poller")
    logger.bind(event="quota_monthly_reset").info("monthly quota reset")


def install_jobs(scheduler: AsyncIOScheduler, config: AppConfig) -> None:
    """Create all persistent jobs with stable IDs and replace_existing=True."""

    # The one and only fetch job. All mode transitions mutate this trigger.
    scheduler.add_job(
        "daemon.poller:run_poller_job",
        trigger=IntervalTrigger(
            seconds=config.night_interval_seconds,
            start_date=datetime.now(tz=config.timezone),
            timezone=config.timezone,
            jitter=2,
        ),
        id="poller",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=config.poller_misfire_grace_seconds,
    )

    # Daily base mode transitions.
    scheduler.add_job(
        "daemon.scheduler:set_mode_night",
        trigger=CronTrigger(hour=0, minute=0),
        id="mode_night",
        replace_existing=True,
        coalesce=True,
    )
    scheduler.add_job(
        "daemon.scheduler:set_mode_normal",
        trigger=CronTrigger(hour=6, minute=0),
        id="mode_normal_morning",
        replace_existing=True,
        coalesce=True,
    )

    # Peak window transitions (AM + PM).
    am_h, am_m = _hhmm(config.peak_am_start)
    am_off_h, am_off_m = _hhmm(config.peak_am_end)
    pm_h, pm_m = _hhmm(config.peak_pm_start)
    pm_off_h, pm_off_m = _hhmm(config.peak_pm_end)

    scheduler.add_job(
        "daemon.scheduler:set_mode_peak",
        trigger=CronTrigger(hour=am_h, minute=am_m),
        id="mode_peak_am_on",
        replace_existing=True,
        coalesce=True,
    )
    scheduler.add_job(
        "daemon.scheduler:set_mode_normal",
        trigger=CronTrigger(hour=am_off_h, minute=am_off_m),
        id="mode_peak_am_off",
        replace_existing=True,
        coalesce=True,
    )
    scheduler.add_job(
        "daemon.scheduler:set_mode_peak",
        trigger=CronTrigger(hour=pm_h, minute=pm_m),
        id="mode_peak_pm_on",
        replace_existing=True,
        coalesce=True,
    )
    scheduler.add_job(
        "daemon.scheduler:set_mode_normal",
        trigger=CronTrigger(hour=pm_off_h, minute=pm_off_m),
        id="mode_peak_pm_off",
        replace_existing=True,
        coalesce=True,
    )

    # Monthly quota reset at local midnight on day 1.
    scheduler.add_job(
        "daemon.scheduler:monthly_reset",
        trigger=CronTrigger(day=1, hour=0, minute=0),
        id="monthly_reset",
        replace_existing=True,
        coalesce=True,
    )

    # Slow recovery probe while breaker is open.
    scheduler.add_job(
        "daemon.poller:run_recovery_job",
        trigger=IntervalTrigger(
            seconds=config.breaker_probe_interval_seconds,
            start_date=datetime.now(tz=config.timezone),
            timezone=config.timezone,
        ),
        id="breaker_recovery",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
