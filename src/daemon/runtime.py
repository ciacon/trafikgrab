"""Process-local runtime container.

APScheduler persists only job definitions. The live application objects
(config, scheduler, state store) are process memory and need a stable access
point for job callables imported by APScheduler.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .config import AppConfig
from .state import StateStore


@dataclass(slots=True)
class Runtime:
    """Live runtime context shared by all scheduled jobs in this process."""

    config: AppConfig
    scheduler: AsyncIOScheduler
    state: StateStore
    http_client: httpx.AsyncClient
    output_dir: Path


_runtime: Runtime | None = None


def set_runtime(runtime: Runtime) -> None:
    """Initialize the module-level runtime.

    Called once at startup before scheduler jobs run.
    """

    global _runtime
    _runtime = runtime


def get_runtime() -> Runtime:
    """Return initialized runtime or fail fast if startup is incomplete."""

    if _runtime is None:
        raise RuntimeError("runtime is not initialized")
    return _runtime
