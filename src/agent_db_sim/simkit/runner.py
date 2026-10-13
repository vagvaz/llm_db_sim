"""SimPy runner: lifecycle phase driving + the minimal smoke loop.

``run_model`` drives the two-phase lifecycle in order (desmod-style):
``construct`` on all components -> ``elaborate`` (wiring validated, fail
fast) -> ``start`` (processes launched) -> ``env.run until`` -> ``collect``.
The metrics collector and VCD emission hook onto the returned probe sink
once those layers exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional

import simpy

from agent_db_sim.simkit.component import Component
from agent_db_sim.simkit.probes import InMemorySink, ProbeSink

__all__ = ["ModelResult", "run_model", "LoopResult", "run_minimal"]


@dataclass
class ModelResult:
    """Everything harvestable from one full model run in this ticket."""

    end_time: float
    collected: Dict[str, Any]
    sink: ProbeSink


def run_model(
    top: Component,
    cfg: Any,
    until: float,
    sink: Optional[ProbeSink] = None,
) -> ModelResult:
    """Drive the lifecycle phases of ``top`` through one simulation run."""
    env = simpy.Environment()
    if sink is None:
        sink = InMemorySink(clock=lambda: env.now)
    top.construct(env, cfg)  # phase 1: all components
    top.elaborate()  # phase 2: wiring validation (fail fast)
    top.start()  # phase 3: processes
    env.run(until=until)
    return ModelResult(end_time=env.now, collected=top.collect(), sink=sink)


# ------------------------------------------------------------------------- #
# minimal smoke loop (ticket 06 placeholder; superseded by ticket 07)       #
# ------------------------------------------------------------------------- #


@dataclass(frozen=True)
class LoopResult:
    """Result summary of a minimal smoke loop run."""

    agents: int
    timeouts_per_agent: int
    timeout: float
    until: float
    end_time: float
    total_timeouts: int


def run_minimal(agents: int, n_timeouts: int, timeout: float, until: float) -> LoopResult:
    """Run ``agents`` concurrent processes that time out ``n_timeouts`` times."""
    env = simpy.Environment()
    counts: Dict[str, int] = {}

    def worker(name: str) -> Iterator[Any]:
        for _ in range(n_timeouts):
            yield env.timeout(timeout)
            counts[name] = counts.get(name, 0) + 1

    for i in range(agents):
        env.process(worker(f"agent-{i}"))

    env.run(until=until)

    total = sum(counts.values())
    return LoopResult(
        agents=agents,
        timeouts_per_agent=n_timeouts,
        timeout=timeout,
        until=until,
        end_time=env.now,
        total_timeouts=total,
    )
