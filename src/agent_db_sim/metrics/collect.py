"""Per-run metrics collection entry point.

``collect_run(run_dir, sink, cfg)`` consumes a finished simulation run's
probe sink (whatever samples it holds — no fixed component set is assumed),
persists lifecycle traces + derived tables into ``<run_dir>/metrics.db`` and
returns that path. Wired from the sweep runner / CLI by the orchestrator.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from agent_db_sim.metrics.derive import (
    create_derived,
    derive_bottleneck,
    derive_percentiles,
    derive_utilization,
    saturation_scores,
)
from agent_db_sim.metrics.trace import TraceWriter
from agent_db_sim.simkit.probes import ProbeSink

__all__ = ["collect_run", "MetricsConfig"]

DEFAULT_WARMUP = 0.1
DEFAULT_SAMPLE_INTERVAL = 1.0
DEFAULT_BATCH_SIZE = 2048


class MetricsConfig:
    """Resolved metrics options; accepts OmegaConf mappings or plain dicts.

    Keys are read from ``cfg.metrics.<key>`` first, then ``cfg.<key>``, with
    spec defaults applied last.
    """

    def __init__(
        self,
        warmup: float = DEFAULT_WARMUP,
        sample_interval: float = DEFAULT_SAMPLE_INTERVAL,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.warmup = float(warmup)
        self.sample_interval = float(sample_interval)
        self.batch_size = int(batch_size)

    @classmethod
    def from_any(cls, cfg: Any) -> "MetricsConfig":
        return cls(
            warmup=_cfg_get(cfg, "warmup", DEFAULT_WARMUP) or 0.0,
            sample_interval=_cfg_get(
                cfg, "sample_interval", DEFAULT_SAMPLE_INTERVAL
            ),
            batch_size=_cfg_get(cfg, "batch_size", DEFAULT_BATCH_SIZE),
        )

    def serialized(self) -> Dict[str, Any]:
        return {
            "warmup": self.warmup,
            "sample_interval": self.sample_interval,
            "batch_size": self.batch_size,
        }


def _cfg_get(cfg: Any, key: str, default: Any) -> Any:
    for holder in (cfg, getattr(cfg, "metrics", None)):
        if holder is None:
            continue
        try:
            if key in holder:
                return holder[key]
        except (TypeError, KeyError):
            pass
    return default


def _consume_sink(
    sink: Any, writer: TraceWriter
) -> Tuple[
    Dict[Tuple[str, str], List[Tuple[float, float]]],
    Dict[Tuple[str, str], List[Tuple[float, float]]],
    Dict[Tuple[str, str], float],
]:
    """One pass over the sink's samples: lifecycle rows -> writer, level and
    counter samples -> (level_points, token_points, capacities)."""
    samples = getattr(sink, "samples", None)
    if samples is None:
        raise TypeError(
            f"sink {sink!r} exposes no '.samples' sequence; collect_run "
            "consumes a completed InMemorySink (or compatible shape)"
        )
    level_points: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    token_points: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    capacities: Dict[Tuple[str, str], float] = {}
    for s in samples:
        if writer.add_sample(s):
            continue
        comp = str(s.tags.get("component", ""))
        if s.kind == "counter":
            if "token" in s.signal.lower():
                token_points.setdefault((s.signal, comp), []).append(
                    (float(s.t), float(s.value))
                )
            continue
        if s.kind == "level":
            key = (s.signal, comp)
            level_points.setdefault(key, []).append((float(s.t), float(s.value)))
            for cap_tag in ("capacity", "slots"):
                if cap_tag in s.tags:
                    capacities.setdefault(key, float(s.tags[cap_tag]))
    return level_points, token_points, capacities


def collect_run(run_dir: Any, sink: ProbeSink, cfg: Any = None) -> Path:
    """Persist metrics for one finished run.

    - Livecycle edge samples -> ``trace`` table (batched writes).
    - Derived tables (percentiles / utilization series / attribution) at
      finalize, warm-up fraction excluded.
    - Returns the ``metrics.db`` path.
    """
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    db_path = run_dir / "metrics.db"

    if db_path.exists():
        db_path.unlink()  # rerun-friendly: fresh collection replaces it

    mcfg = MetricsConfig.from_any(cfg)
    writer = TraceWriter(db_path, batch_size=mcfg.batch_size)
    try:
        level_points, token_points, capacities = _consume_sink(sink, writer)
        writer.finalize()

        samples = getattr(sink, "samples", [])
        all_times = [float(s.t) for s in samples] + [
            t for pts in level_points.values() for t, _ in pts
        ]
        if all_times:
            t0 = min(all_times)
            t_end = max(all_times)
        else:
            t0 = t_end = 0.0
        warmup_end = t0 + mcfg.warmup * (t_end - t0)

        conn = writer.conn()
        create_derived(conn)
        conn.execute("DELETE FROM meta")
        conn.execute("INSERT INTO meta VALUES (?, ?)", ("warmup_end", str(warmup_end)))
        conn.execute("INSERT INTO meta VALUES (?, ?)", ("t_end", str(t_end)))
        conn.commit()
        derive_percentiles(conn, warmup_end)
        derive_utilization(
            conn,
            level_points=level_points,
            token_points=token_points,
            capacities=capacities,
            warmup_end=warmup_end,
            t_end=t_end,
            sample_interval=mcfg.sample_interval,
        )
        derive_bottleneck(
            conn,
            warmup_end=warmup_end,
            sat_by_component=saturation_scores(conn),
        )
        conn.commit()
    finally:
        writer.close()
    return db_path
