"""Per-run metrics collection entry point.

Streaming architecture (post tickets 11/12): the sink no longer accumulates
the whole run's samples in memory. :func:`start_collection` attaches a
:class:`RunCollector` as a :meth:`~agent_db_sim.simkit.probes.ProbeSink.subscribe`
consumer **before** the simulation executes; every recorded sample is pushed
into ``<run_dir>/metrics.db`` live through the batched :class:`TraceWriter`
(both the raw ``samples`` table and the lifecycle ``trace`` table), with
buffers flushing at N-events batch boundaries and at
``sample_interval`` tick boundaries. Batches are flushed synchronously on
the emitting path — a failing flush propagates through
``ProbeSink.record`` and fails the run loudly (no silent sample loss).

After the run, :meth:`RunCollector.finalize` flushes the tail, builds the
indexes, and computes every derived table (percentiles / utilization series
/ attribution) from ``metrics.db`` itself — finished-request history lives
only in SQLite.

Two entry points:

- :func:`start_collection` — streamed runs (pipeline wiring): subscribe
  before the sim, finalize after.
- :func:`collect_run` — unchanged callers' contract: consumes a completed
  sink's sample list (legacy full-history sinks, viz fixture) through the
  same writer + derivation path.

:func:`rebuild_samples` reads the ``samples`` table back as ``Sample``
objects (e.g. the one-shot VCD writer after finalize).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent_db_sim.metrics.derive import (
    create_derived,
    derive_bottleneck,
    derive_percentiles,
    derive_utilization,
    saturation_scores,
)
from agent_db_sim.metrics.trace import TraceWriter
from agent_db_sim.simkit.probes import ProbeSink, Sample

__all__ = ["collect_run", "MetricsConfig", "RunCollector", "start_collection", "rebuild_samples"]

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


# --------------------------------------------------------------------------- #
# streaming collector (the run's live consumer)                                #
# --------------------------------------------------------------------------- #


class RunCollector:
    """Live metrics consumer for one run, appended into ``<run_dir>/metrics.db``.

    Registered on the probe sink with
    :meth:`~agent_db_sim.simkit.probes.InMemorySink.subscribe`; every
    :meth:`push` routes through the batched :class:`TraceWriter`, which
    auto-flushes at ``batch_size`` rows and at ``sample_interval`` tick
    boundaries. Memory is O(batch buffers) + whichever retention policy the
    sink itself uses (:class:`~agent_db_sim.simkit.probes.InMemorySink`
    bounded mode keeps only the active-request index).
    """

    def __init__(self, run_dir: Any, cfg: Any = None) -> None:
        run_dir = Path(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = run_dir / "metrics.db"
        if self.db_path.exists():
            self.db_path.unlink()  # rerun-friendly: fresh collection replaces it

        self._mcfg = MetricsConfig.from_any(cfg)
        self.writer = TraceWriter(self.db_path, batch_size=self._mcfg.batch_size)
        self._last_flush_t: Optional[float] = None
        self._sink: Optional[ProbeSink] = None

    # -- streaming ingestion ------------------------------------------------ #
    def push(self, sample: Sample) -> None:
        """Consume one live sample (the subscribed callback). Simulated-time
        tick boundaries force a flush in addition to the batch cap; a flush
        error propagates to the emitter (loud run failure)."""
        self.writer.add_sample(sample)
        t = float(sample.t)
        if (
            self._last_flush_t is None
            or t - self._last_flush_t >= self._mcfg.sample_interval
        ):
            self._last_flush_t = t
            self.writer.flush()

    # alias: the subscriber registration name
    on_sample = push

    # -- finalize / derivation ---------------------------------------------- #
    def finalize(self) -> Path:
        """Flush tail + indexes, then derive all tables from ``metrics.db``.

        Returns the db path. Detaches from the sink first (post-close pushes
        are a caller bug, not a silent no-op). Raises on derivation errors
        (nothing swept).
        """
        self._detach()
        self.writer.finalize()
        try:
            self._derive()
        except BaseException:
            self.abort()
            raise
        self.writer.close()
        return self.db_path

    def _derive(self) -> None:
        conn = self.writer.conn()
        row = conn.execute("SELECT MIN(t), MAX(t) FROM samples").fetchone()
        t0 = float(row[0]) if row[0] is not None else 0.0
        t_end = float(row[1]) if row[1] is not None else 0.0
        warmup_end = t0 + self._mcfg.warmup * (t_end - t0)

        create_derived(conn)
        conn.execute("DELETE FROM meta")
        conn.execute("INSERT INTO meta VALUES (?, ?)", ("warmup_end", str(warmup_end)))
        conn.execute("INSERT INTO meta VALUES (?, ?)", ("t_end", str(t_end)))
        conn.commit()
        derive_percentiles(conn, warmup_end)
        derive_utilization(
            conn,
            warmup_end=warmup_end,
            t_end=t_end,
            sample_interval=self._mcfg.sample_interval,
        )
        derive_bottleneck(
            conn,
            warmup_end=warmup_end,
            sat_by_component=saturation_scores(conn),
        )
        conn.commit()

    def abort(self) -> None:
        """Close the writer without finalizing (error path; loud, no report)."""
        self._detach()
        try:
            self.writer.close()
        except Exception:  # noqa: BLE001 - close must never mask the real error
            pass

    def _detach(self) -> None:
        """Unsubscribe from the bound sink (finalize/abort only)."""
        if self._sink is not None:
            self._sink.unsubscribe(self.push)
            self._sink = None


def start_collection(run_dir: Any, sink: ProbeSink, cfg: Any = None) -> RunCollector:
    """Attach a live :class:`RunCollector` to ``sink`` before the sim runs.

    Call before the model executes; afterwards call
    :meth:`RunCollector.finalize` (returns the ``metrics.db`` path; it also
    detaches the collector from the sink).
    """
    collector = RunCollector(run_dir, cfg)
    collector._sink = sink
    sink.subscribe(collector.push)
    return collector


def collect_run(run_dir: Any, sink: Any, cfg: Any = None) -> Path:
    """Persist metrics for one finished run — legacy (replay) entry point.

    Accepts a sink exposing its full sample list as ``.samples`` (the
    default full-history :class:`InMemorySink`, viz fixture) and streams
    that list through the identical writer + derivation path used for live
    runs. Sinks exposing no ``.samples`` raise :class:`TypeError`.
    """
    samples = getattr(sink, "samples", None)
    if samples is None:
        raise TypeError(
            f"sink {sink!r} exposes no '.samples' sequence; collect_run "
            "consumes a completed full-history InMemorySink (live runs use "
            "start_collection(...).finalize() instead)"
        )
    collector = RunCollector(run_dir, cfg)
    try:
        for s in samples:
            collector.push(s)
        return collector.finalize()
    except BaseException:
        collector.abort()
        raise


def rebuild_samples(db_path: Any) -> List[Sample]:
    """Read the ``samples`` table back as probe :class:`Sample` objects, in
    insertion (emission) order. Used post-``finalize`` by artifacts that
    still want the full list (e.g. the one-shot VCD writer)."""
    conn = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    try:
        out: List[Sample] = []
        for t, signal, kind, value, tags in conn.execute(
            "SELECT t, signal, kind, value, tags FROM samples ORDER BY id"
        ):
            out.append(
                Sample(
                    t=float(t),
                    signal=str(signal),
                    kind=str(kind),
                    value=json.loads(value),
                    tags=json.loads(tags),
                )
            )
        return out
    finally:
        conn.close()
