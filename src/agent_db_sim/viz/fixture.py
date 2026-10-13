"""Synthetic run fixture: makes the viz stack testable without tickets 08-10.

Builds deterministic closed-loop request samples on :class:`InMemorySink`
(the same contracts the components emit), then materializes a run directory
with ``run.vcd`` + ``metrics.db`` via the one-shot writer and the collector.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

from agent_db_sim.metrics import collect_run
from agent_db_sim.simkit.probes import InMemorySink
from agent_db_sim.viz.vcd import write_vcd

__all__ = ["FixtureRun", "synthesize_samples", "make_fixture_run"]

LLM_SLOTS = 4
DB_SLOTS = 8


@dataclass
class FixtureRun:
    """Materialized run directory handles."""

    run_dir: Path
    vcd: Path
    metrics_db: Path
    sink: InMemorySink = field(repr=False, default=None)  # type: ignore[assignment]


def synthesize_samples(
    agents: int = 3,
    until: float = 50.0,
    seed: int = 7,
) -> InMemorySink:
    """Deterministic closed-loop agent samples into a fresh InMemorySink."""
    rng = random.Random(seed)
    sink = InMemorySink()

    def level(signal: str, value: float, t: float, component: str, **tags: Any) -> None:
        sink.sample(signal, value, t=t, component=component, **tags)

    def edge(signal: str, state: str, t: float, agent: str, rid: str, parent: str) -> None:
        sink.edge(signal, state, t=t, agent_id=agent, request_id=rid, parent_id=parent)

    for i in range(agents):
        agent = f"a-{i}"
        t = 0.2 * i
        k = 0
        lvl_q = {"llm": 0, "db": 0}
        lvl_s = {"llm": 0, "db": 0}
        while t < until:
            k += 1
            rid = f"r{i}-{k}"
            llm_q = 0.5 + round(rng.uniform(0.0, 2.0), 1)      # queued wait
            llm_p = 1.0 + round(rng.uniform(0.0, 3.0), 1)      # processing
            db_q = 0.5 + round(rng.uniform(0.0, 3.0), 1)
            db_p = 1.0 + round(rng.uniform(0.0, 3.0), 1)

            edge("agent.loop", "submitted", t, agent, rid, rid)
            for hop, (q, p), slots in (
                ("llm", (llm_q, llm_p), LLM_SLOTS),
                ("db", (db_q, db_p), DB_SLOTS),
            ):
                sig = f"{hop}.request"
                edge(sig, "submitted", t, agent, rid, rid)
                edge(sig, "queued", t, agent, rid, rid)
                # queue depth rises when the hop is entered
                lvl_q[hop] += 1
                t_q = t + q
                level("queue_depth", lvl_q[hop], t, hop)
                edge(sig, "processing", t_q, agent, rid, rid)
                lvl_q[hop] -= 1
                level("queue_depth", lvl_q[hop], t_q, hop)
                # slot occupancy while processing
                lvl_s[hop] += 1
                level("slot_busy", lvl_s[hop], t_q, hop, slots=slots)
                t_c = t_q + p
                if hop == "llm":  # token counter streamed during inference
                    chunks = 3
                    for n in range(chunks):
                        sink.count(
                            "tokens", 2 + (n % 3), t=t_q + p * (n + 1) / (chunks + 1),
                            component=hop,
                        )
                edge(sig, "completed", t_c, agent, rid, rid)
                lvl_s[hop] -= 1
                level("slot_busy", lvl_s[hop], t_c, hop, slots=slots)
                t = t_c
            edge("agent.loop", "received", t, agent, rid, rid)
            t += round(rng.uniform(0.2, 1.0), 1)  # agent think time
    return sink


def make_fixture_run(
    run_dir: Any,
    agents: int = 3,
    until: float = 50.0,
    seed: int = 7,
    warmup: float = 0.1,
    sample_interval: float = 1.0,
    cfg_extra: Dict[str, Any] | None = None,
) -> FixtureRun:
    """Create run_dir with run.vcd + metrics.db from synthetic samples."""
    run_dir = Path(run_dir)
    sink = synthesize_samples(agents=agents, until=until, seed=seed)
    vcd = write_vcd(run_dir / "run.vcd", sink.samples)
    cfg = {"warmup": warmup, "sample_interval": sample_interval}
    if cfg_extra:
        cfg.update(cfg_extra)
    db = collect_run(run_dir, sink, cfg=cfg)
    return FixtureRun(run_dir=run_dir, vcd=vcd, metrics_db=db, sink=sink)
