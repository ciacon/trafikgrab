"""Daemon entrypoint and lifecycle orchestration."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import signal
from pathlib import Path
from typing import TextIO

from dotenv import load_dotenv
from loguru import logger

from .config import load_config
from .http_client import create_client
from .logging import configure_logging
from .runtime import Runtime, set_runtime
from .scheduler import create_scheduler, install_jobs
from .state import StateStore


class SingleInstanceLock:
    """Advisory file lock to prevent two daemon processes on one host."""

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fh: TextIO | None = None

    def acquire(self) -> None:
        """Acquire non-blocking exclusive lock or raise BlockingIOError."""

        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.lock_path.open("w")
        assert self._fh is not None
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def release(self) -> None:
        """Release file lock at shutdown."""

        if self._fh is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None


async def run(config_path: Path | None = None) -> None:
    """Run daemon until SIGINT/SIGTERM."""

    load_dotenv()
    cfg = load_config(config_path)
    configure_logging()

    lock = SingleInstanceLock(cfg.lock_file)
    lock.acquire()

    scheduler = create_scheduler(cfg)
    state = StateStore(cfg.state_db_path, cfg.state_table)
    client = create_client(cfg)
    try:
        runtime = Runtime(
            config=cfg,
            scheduler=scheduler,
            state=state,
            http_client=client,
            output_dir=cfg.output_path.parent,
        )
        set_runtime(runtime)

        install_jobs(scheduler, cfg)
        scheduler.start()
        logger.bind(event="daemon_started").info("daemon started")

        stop_event = asyncio.Event()

        def _stop() -> None:
            stop_event.set()

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, _stop)
        loop.add_signal_handler(signal.SIGINT, _stop)

        await stop_event.wait()
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
        await client.aclose()
        lock.release()
        logger.bind(event="daemon_stopped").info("daemon stopped")


def main() -> None:
    """CLI wrapper used by `python -m daemon.main` and script entrypoint."""

    parser = argparse.ArgumentParser(description="trafikgrab daemon")
    parser.add_argument("--config", type=Path, default=None, help="Path to TOML config")
    args = parser.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
