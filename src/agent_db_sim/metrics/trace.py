"""Trace writer: probe-sink samples -> per-run SQLite ``metrics.db`` rows.

Consumer side of the metrics contract (ticket 04): components emit request
lifecycle events as ``edge`` samples named ``{component}.{stage}`` carrying
``agent_id`` / ``request_id`` / optional ``parent_id`` / ``state`` tags with
the canonical states ``(submitted | queued | processing | completed |
received)``. Rows land in two tables:

- ``samples`` — every recorded sample (edges, levels, counters) streamed
  live as one row each; the derived utilization layer reads this raw log.
- ``trace`` — the request-lifecycle edges only.

Rows are accumulated in bounded per-table buffers and flushed to ``sqlite3``
in batches (or at caller-chosen tick boundaries via :meth:`TraceWriter.flush`);
indexes are built only at :meth:`TraceWriter.finalize` so long runs stay
append-friendly and memory stays O(batch_size) — a flush failure raises out
of the write path (loud run failure, no silent sample loss).
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from agent_db_sim.simkit.probes import Sample

__all__ = [
    "STATES",
    "TraceRow",
    "split_signal",
    "sample_to_trace_row",
    "TRACE_DDL",
    "SAMPLES_DDL",
    "TraceWriter",
]

# Canonical lifecycle states (ticket 04).
STATES: Tuple[str, ...] = ("submitted", "queued", "processing", "completed", "received")


def split_signal(signal: str) -> Tuple[str, str]:
    """Split ``{component}.{stage}`` into (component, stage).

    The stage is always the last dotted segment; everything before it is the
    (possibly dotted, hierarchical) component path. A signal with no dot is
    treated as its own stage under the empty component name (the sink tag
    ``component=`` wins later if present).
    """
    if "." not in signal:
        return "", signal
    component, _, stage = signal.rpartition(".")
    return component, stage


@dataclass(frozen=True)
class TraceRow:
    """One lifecycle transition, matching the ``trace`` table columns."""

    agent_id: str
    request_id: str
    parent_id: Optional[str]
    component: str
    stage: str
    state: str
    t: float
    attrs: str  # json blob with the complete sample tags

    def table_tuple(self) -> Tuple[Any, ...]:
        return (
            self.agent_id,
            self.request_id,
            self.parent_id,
            self.component,
            self.stage,
            self.state,
            self.t,
            self.attrs,
        )

    def state_key(self) -> Tuple[str, str, str]:
        """Grouping key for the stage state machine: component, stage, request."""
        return (self.component, self.stage, self.request_id)


def sample_to_trace_row(sample: Sample) -> Optional[TraceRow]:
    """Convert one probe sample into a trace row, or ``None`` if it is not a
    request-lifecycle event.

    A lifecycle event is an ``edge`` sample whose tags carry ``agent_id`` and
    ``request_id``. The state is taken from the ``state`` tag and falls back
    to the sample value for emitters that put it in ``value``. Unknown states
    pass through as text (no fixed component set is assumed); the derived
    layer only matches canonical :data:`STATES`.
    """
    if sample.kind != "edge":
        return None
    tags: Dict[str, Any] = dict(sample.tags)
    agent_id = tags.get("agent_id")
    request_id = tags.get("request_id")
    if agent_id is None or request_id is None:
        return None
    state = tags.get("state", sample.value)
    if state is None:
        return None
    component, stage = split_signal(sample.signal)
    attrs = json.dumps(tags, default=str, sort_keys=True)
    return TraceRow(
        agent_id=str(agent_id),
        request_id=str(request_id),
        parent_id=str(tags["parent_id"]) if "parent_id" in tags else None,
        component=component,
        stage=stage,
        state=str(state),
        t=float(sample.t),
        attrs=attrs,
    )


TRACE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS trace (
        id INTEGER PRIMARY KEY,
        agent_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        parent_id TEXT,
        component TEXT NOT NULL,
        stage TEXT NOT NULL,
        state TEXT NOT NULL,
        t REAL NOT NULL,
        attrs TEXT NOT NULL
    )
    """,
]

# Per-run raw sample log (streamed live, one row per recorded probe sample):
# lifecycle edges (duplicated in ``trace``), level reads and counter deltas.
# ``value``/``tags`` are JSON-encoded; numbers round-trip exactly.
SAMPLES_DDL = [
    """
    CREATE TABLE IF NOT EXISTS samples (
        id INTEGER PRIMARY KEY,
        t REAL NOT NULL,
        signal TEXT NOT NULL,
        kind TEXT NOT NULL,
        value TEXT NOT NULL,
        tags TEXT NOT NULL
    )
    """,
]

_INSERT = (
    "INSERT INTO trace (agent_id, request_id, parent_id, component, stage, "
    "state, t, attrs) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)

_SAMPLE_INSERT = (
    "INSERT INTO samples (t, signal, kind, value, tags) VALUES (?, ?, ?, ?, ?)"
)


def _enc(value: Any) -> str:
    return json.dumps(value, default=str, sort_keys=True)


def sample_row_tuple(sample: Sample) -> Tuple[Any, ...]:
    """One ``samples``-table row for any probe sample."""
    return (
        float(sample.t),
        sample.signal,
        sample.kind,
        _enc(sample.value),
        _enc(sample.tags),
    )


class TraceWriter:
    """Batched appender of probe samples into ``<run-dir>/metrics.db``.

    Every sample lands in the raw ``samples`` table (live row log); lifecycle
    edges additionally become ``trace`` rows. Both tables buffer up to
    ``batch_size`` rows in memory and flush with one ``executemany`` each;
    a flush failure raises out of the writer (and therefore out of the
    emitting component — the run fails loudly). Indexes are only created in
    :meth:`finalize`.
    """

    def __init__(self, db_path: Any, batch_size: int = 2048) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        self.batch_size = int(batch_size)
        self._buf: List[TraceRow] = []
        self._sample_buf: List[Tuple[Any, ...]] = []
        self.n_rows = 0
        self.n_sample_rows = 0
        self._conn = sqlite3.connect(str(db_path))
        for ddl in TRACE_DDL + SAMPLES_DDL:
            self._conn.execute(ddl)
        self._conn.commit()

    # ------------------------------------------------------------------ #
    # ingestion                                                           #
    # ------------------------------------------------------------------ #
    def add_sample(self, sample: Sample) -> bool:
        """Route one probe sample: always into ``samples``, plus ``trace``
        when it is a lifecycle event (returns True in the latter case)."""
        self._sample_buf.append(sample_row_tuple(sample))
        if len(self._sample_buf) >= self.batch_size:
            self._flush_samples()
        row = sample_to_trace_row(sample)
        if row is None:
            return False
        self.add_row(row)
        return True

    def add_rows(self, rows: Iterable[TraceRow]) -> None:
        """Bulk-append directly constructed rows (fixture/testing use)."""
        for row in rows:
            self.add_row(row)

    def add_row(self, row: TraceRow) -> None:
        self._buf.append(row)
        if len(self._buf) >= self.batch_size:
            self._flush()

    @property
    def buffer_len(self) -> int:
        """Trace rows currently held in memory (always ``< batch_size``)."""
        return len(self._buf)

    @property
    def sample_buffer_len(self) -> int:
        """Raw sample rows currently held in memory."""
        return len(self._sample_buf)

    # ------------------------------------------------------------------ #
    # persistence                                                         #
    # ------------------------------------------------------------------ #
    def flush(self) -> None:
        """Push both pending buffers to disk now (batch/tick boundary)."""
        self._flush()
        self._flush_samples()

    def _flush(self) -> None:
        if not self._buf:
            return
        self._conn.executemany(_INSERT, [r.table_tuple() for r in self._buf])
        self.n_rows += len(self._buf)
        self._buf.clear()
        self._conn.commit()

    def _flush_samples(self) -> None:
        if not self._sample_buf:
            return
        self._conn.executemany(_SAMPLE_INSERT, self._sample_buf)
        self.n_sample_rows += len(self._sample_buf)
        self._sample_buf.clear()
        self._conn.commit()

    def finalize(self) -> None:
        """Flush the tail and build the query indexes (post-append, once)."""
        self.flush()
        for cols in (
            ("agent_id", "request_id"),
            ("component", "stage"),
            ("parent_id",),
            ("t",),
        ):
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS trace_%s_idx ON trace (%s)"
                % ("_".join(cols), ", ".join(cols))
            )
        for cols in (("t",), ("signal",)):
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS samples_%s_idx ON samples (%s)"
                % ("_".join(cols), ", ".join(cols))
            )
        self._conn.commit()

    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()
