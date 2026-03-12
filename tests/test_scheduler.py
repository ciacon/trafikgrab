from __future__ import annotations

from collections.abc import Callable

import pytest
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from daemon import scheduler
from daemon.config import AppConfig
from daemon.runtime import Runtime, set_runtime


class RecordingScheduler:
    def __init__(self) -> None:
        self.reschedules: list[tuple[str, IntervalTrigger]] = []
        self.resumed_jobs: list[str] = []

    def reschedule_job(self, job_id: str, trigger: IntervalTrigger) -> None:
        self.reschedules.append((job_id, trigger))

    def resume_job(self, job_id: str) -> None:
        self.resumed_jobs.append(job_id)


class RecordingState:
    def __init__(self) -> None:
        self.reset_calls: list[int] = []

    def reset_month(self, month_limit: int) -> None:
        self.reset_calls.append(month_limit)


@pytest.mark.integration
def test_install_jobs_registers_expected_ids_and_triggers(
    make_config: Callable[..., AppConfig],
) -> None:
    cfg = make_config()
    real_scheduler = scheduler.create_scheduler(cfg)

    scheduler.install_jobs(real_scheduler, cfg)
    jobs = {job.id: job for job in real_scheduler.get_jobs()}

    expected_ids = {
        "poller",
        "mode_night",
        "mode_normal_morning",
        "mode_peak_am_on",
        "mode_peak_am_off",
        "mode_peak_pm_on",
        "mode_peak_pm_off",
        "monthly_reset",
        "breaker_recovery",
    }
    assert expected_ids.issubset(jobs.keys())
    assert isinstance(jobs["poller"].trigger, IntervalTrigger)
    assert isinstance(jobs["breaker_recovery"].trigger, IntervalTrigger)
    assert isinstance(jobs["mode_night"].trigger, CronTrigger)
    assert isinstance(jobs["monthly_reset"].trigger, CronTrigger)


@pytest.mark.unit
def test_mode_switches_reschedule_single_poller_job(
    make_config: Callable[..., AppConfig],
) -> None:
    cfg = make_config(night_interval_seconds=120, normal_interval_seconds=30, peak_interval_seconds=2)
    recording_scheduler = RecordingScheduler()
    runtime = Runtime(
        config=cfg,
        scheduler=recording_scheduler,  # type: ignore[arg-type]
        state=RecordingState(),  # type: ignore[arg-type]
        http_client=object(),  # type: ignore[arg-type]
        output_dir=cfg.output_path.parent,
    )
    set_runtime(runtime)

    scheduler.set_mode_night()
    scheduler.set_mode_normal()
    scheduler.set_mode_peak()

    assert len(recording_scheduler.reschedules) == 3

    _, night_trigger = recording_scheduler.reschedules[0]
    _, normal_trigger = recording_scheduler.reschedules[1]
    _, peak_trigger = recording_scheduler.reschedules[2]
    assert night_trigger.interval.total_seconds() == 120
    assert night_trigger.jitter == 2
    assert normal_trigger.interval.total_seconds() == 30
    assert normal_trigger.jitter == 1
    assert peak_trigger.interval.total_seconds() == 2
    assert peak_trigger.jitter is None


@pytest.mark.unit
def test_monthly_reset_resets_state_and_resumes_poller(
    make_config: Callable[..., AppConfig],
) -> None:
    cfg = make_config(monthly_limit=4321)
    recording_scheduler = RecordingScheduler()
    recording_state = RecordingState()
    runtime = Runtime(
        config=cfg,
        scheduler=recording_scheduler,  # type: ignore[arg-type]
        state=recording_state,  # type: ignore[arg-type]
        http_client=object(),  # type: ignore[arg-type]
        output_dir=cfg.output_path.parent,
    )
    set_runtime(runtime)

    scheduler.monthly_reset()

    assert recording_state.reset_calls == [4321]
    assert recording_scheduler.resumed_jobs == ["poller"]
