"""Circuit breaker state transitions.

The breaker is persisted in UTC in application state and controls when the
poller must be paused after repeated outage-like failures.
"""

from __future__ import annotations

import pendulum

from .state import DaemonState


def parse_utc(value: str) -> pendulum.DateTime:
    """Parse persisted ISO timestamp and normalize to UTC."""

    parsed = pendulum.parse(value)
    if not isinstance(parsed, pendulum.DateTime):
        raise ValueError("expected datetime value")
    return parsed.in_timezone("UTC")


def is_open(state: DaemonState) -> bool:
    """True when breaker cooldown has not expired yet."""

    if not state.breaker_open_until_utc:
        return False
    return bool(parse_utc(state.breaker_open_until_utc) > pendulum.now("UTC"))


def register_success(state: DaemonState) -> DaemonState:
    """Reset outage counters after a healthy poll cycle."""

    state.consecutive_failures = 0
    state.breaker_open_until_utc = None
    return state


def register_outage_failure(state: DaemonState, threshold: int, cooldown_seconds: int) -> DaemonState:
    """Record outage-like failure and open breaker when threshold is reached."""

    state.consecutive_failures += 1
    if state.consecutive_failures >= threshold:
        state.breaker_open_until_utc = (
            pendulum.now("UTC").add(seconds=cooldown_seconds).to_iso8601_string()
        )
    return state
