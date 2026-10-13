"""Metrics layer (ticket 11): per-run trace persistence + derived tables.

Public entry point: :func:`collect_run`.
"""

from __future__ import annotations

from agent_db_sim.metrics.collect import MetricsConfig, collect_run
from agent_db_sim.metrics.derive import percentile
from agent_db_sim.metrics.trace import STATES, TraceRow, TraceWriter, split_signal

__all__ = [
    "collect_run",
    "MetricsConfig",
    "percentile",
    "STATES",
    "TraceWriter",
    "TraceRow",
    "split_signal",
]
