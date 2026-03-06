"""Structured logging configuration.

Loguru is configured to emit JSON to stdout so systemd/journald and log
aggregators can parse events consistently.
"""

from __future__ import annotations

import sys

from loguru import logger


def configure_logging(level: str = "INFO") -> None:
    """Configure global logger sink for daemon execution."""

    logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        serialize=True,
        backtrace=False,
        diagnose=False,
    )
