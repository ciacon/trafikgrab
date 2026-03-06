"""Monthly API quota bookkeeping.

Quota is stored in app state (not APScheduler metadata) so business rules are
restart-safe and independent of trigger internals.
"""

from __future__ import annotations

from .state import DaemonState, month_key_utc


def refresh_month_if_needed(state: DaemonState, configured_limit: int) -> DaemonState:
    """Roll quota counters at UTC month boundary in persisted state."""

    current = month_key_utc()
    if state.month_key != current:
        state.month_key = current
        state.month_used = 0
        state.month_limit = configured_limit
    return state


def can_spend(state: DaemonState) -> bool:
    """Check whether one more poll attempt fits within budget."""

    return (state.month_used + state.api_cost_per_poll) <= state.month_limit


def consume(state: DaemonState) -> DaemonState:
    """Consume one poll attempt worth of API budget."""

    state.month_used += state.api_cost_per_poll
    return state
