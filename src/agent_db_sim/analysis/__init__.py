"""agent_db_sim.analysis — analytical cross-check of simulation results.

A closed-network queueing reference (the exact Gordon–Newell chain ported
from the user-authored ``core2.js`` oracle at the repo root) evaluated
against completed simulation runs:

- :mod:`.chain` — the ported exact closed-chain math (+ Erlang helpers);
- :mod:`.model` — run config -> station model (means only, config-derived);
- :mod:`.compare` — joins a run's ``metrics.db`` against the model and
  builds the side-by-side comparison table + qualitative verdict.

The analytical side assumes exponential service and uses configured means
only; it validates the simulator's *mean* throughput/latency/utilization
behavior, never its percentiles.
"""

from agent_db_sim.analysis.model import StationModel, build_model
from agent_db_sim.analysis.compare import (
    AnalysisReport,
    analyze_run,
    format_text,
)

__all__ = ["StationModel", "build_model", "AnalysisReport", "analyze_run",
           "format_text"]
