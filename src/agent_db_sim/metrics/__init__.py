"""Metrics layer (tickets 11/12 + streaming follow-up): per-run trace
persistence, live sample streaming into SQLite, and derived tables.

Entry points:
- :func:`start_collection` — subscribe a live RunCollector to the probe sink
  before the run executes; ``.finalize()`` derives all tables from metrics.db.
- :func:`collect_run` — legacy one-shot path for full-history sink lists.
- :func:`rebuild_samples` — read the ``samples`` table back as Sample objects.
"""

from __future__ import annotations

from agent_db_sim.metrics.collect import (
    MetricsConfig,
    RunCollector,
    collect_run,
    rebuild_samples,
    start_collection,
)
from agent_db_sim.metrics.derive import percentile
from agent_db_sim.metrics.trace import STATES, TraceRow, TraceWriter, split_signal

__all__ = [
    "collect_run",
    "start_collection",
    "RunCollector",
    "rebuild_samples",
    "MetricsConfig",
    "percentile",
    "STATES",
    "TraceWriter",
    "TraceRow",
    "split_signal",
]
