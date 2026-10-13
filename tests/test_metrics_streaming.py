"""Streaming metrics contract (post tickets 11/12):

- all samples (lifecycle edges AND level/counter) stream live into
  metrics.db through the subscribed batched TraceWriter;
- the bounded InMemorySink retains only the active-request index — memory
  grows with in-flight concurrency, not total requests;
- a failing batch flush fails the run loudly (no silent sample loss);
- the streamed derive results match the legacy replay path exactly
  (golden comparison on the viz fixture);
- collect_run derives from SQLite when the sink keeps no history.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from agent_db_sim.metrics import (
    RunCollector,
    collect_run,
    rebuild_samples,
    start_collection,
)
from agent_db_sim.simkit.probes import InMemorySink, Sample

# --------------------------------------------------------------------- #
# helpers                                                                #
# --------------------------------------------------------------------- #

EDGES_PER_REQUEST = 7  # agent.loop + {agent}.loop submitted + llm sub/queued/proc/completed + received


def emit_request(
    sink: InMemorySink,
    t0: float,
    agent: str = "a",
    rid: str = "r",
    *,
    terminal: str = "received",
) -> float:
    """One fully-instrumented lifecycle request; returns the end time."""
    sink.edge("agent.loop", "submitted", t=t0, agent_id=agent, request_id=rid)
    sink.edge(f"{agent}.loop", "submitted", t=t0, agent_id=agent, request_id=rid)
    sink.edge("llm.request", "submitted", t=t0, agent_id=agent, request_id=rid, parent_id=rid)
    sink.edge("llm.request", "queued", t=t0 + 1.0, agent_id=agent, request_id=rid, parent_id=rid)
    sink.edge("llm.request", "processing", t=t0 + 2.0, agent_id=agent, request_id=rid, parent_id=rid)
    t_end = t0 + 4.0
    sink.edge("llm.request", "completed", t=t_end, agent_id=agent, request_id=rid, parent_id=rid)
    if terminal == "received":
        end_state = "received"
        end_t = t0 + 5.0
    else:  # endpoint-only request: nobody observes past completion
        end_state = None
        end_t = t_end
    if end_state is not None:
        sink.edge(f"{agent}.loop", end_state, t=end_t, agent_id=agent, request_id=rid)
    return end_t


def read_table(db_path: Path, table: str, order: str) -> list[tuple]:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(f"SELECT * FROM {table} ORDER BY {order}").fetchall()
    finally:
        conn.close()


# --------------------------------------------------------------------- #
# live streaming                                                         #
# --------------------------------------------------------------------- #


def test_streaming_writes_live_rows_during_run(tmp_path: Path) -> None:
    sink = InMemorySink(bounded=True)
    collector = start_collection(
        tmp_path / "run", sink, cfg={"warmup": 0.0, "batch_size": 2048}
    )
    # emit a couple of lifecycle edges and non-lifecycle probes live
    emit_request(sink, 0.0, "a0", "r0")
    sink.sample("slot_busy", 1, t=6.0, component="llm", capacity=2)
    sink.count("tokens", 3.0, t=7.0, component="llm")
    dbg = tmp_path / "run" / "metrics.db"

    # mid-run (before finalize): rows are already on disk (flushed at the
    # batch cap and at simulated-time tick boundaries)
    conn = sqlite3.connect(dbg)
    assert conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0] == 9
    assert conn.execute("SELECT COUNT(*) FROM trace").fetchone()[0] == 7
    conn.close()

    db_path = collector.finalize()
    assert db_path == dbg
    conn = sqlite3.connect(db_path)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    # derived tables exist alongside trace + samples, and meta is present
    assert {"trace", "samples", "latency_percentiles", "utilization_series",
            "utilization_stats", "token_rate_series", "bottleneck", "meta"} <= tables
    conn.close()


def test_tick_boundary_forces_flush(tmp_path: Path) -> None:
    sink = InMemorySink(bounded=True)
    collector = start_collection(
        tmp_path / "run", sink, cfg={"sample_interval": 1.0, "batch_size": 4096}
    )
    for t in (0.0, 0.25, 0.5, 0.75):
        sink.sample("slot_busy", t, t=t, component="x")
    assert collector.writer.sample_buffer_len == 3  # 0.25..0.75 buffered
    sink.sample("slot_busy", 1.0, t=1.0, component="x")  # tick boundary
    assert collector.writer.sample_buffer_len == 0  # flushed at the tick
    assert collector.writer.n_sample_rows == 5
    collector.abort()


def test_flush_failure_fails_the_run_loudly(tmp_path: Path) -> None:
    cfg: dict[str, Any] = {"batch_size": 1}
    sink = InMemorySink(bounded=True)
    collector = start_collection(tmp_path / "run", sink, cfg=cfg)
    sink.sample("slot_busy", 1, t=0.0, component="x")  # healthy first flush
    # sabotage the backend: the next batch flush is guaranteed broken
    collector.writer._conn.close()
    # the failure must escape the emitting path (sink record -> subscriber)
    with pytest.raises(sqlite3.ProgrammingError):
        sink.sample("slot_busy", 2, t=1.0, component="x")
    with pytest.raises(sqlite3.ProgrammingError):
        collector.push(Sample(t=2.0, signal="slot_busy", kind="level", value=3))
    collector.abort()


def test_collect_run_rejects_sink_without_samples(tmp_path: Path) -> None:
    class Bare:
        pass

    with pytest.raises(TypeError):
        collect_run(tmp_path / "run", Bare())


# --------------------------------------------------------------------- #
# bounded sink memory                                                    #
# --------------------------------------------------------------------- #


def test_bounded_sink_retention_follows_concurrency_not_totals() -> None:
    retained_by_run: dict[int, list[int]] = {}
    peaks: dict[int, int] = {}
    for n in (20, 200):
        sink = InMemorySink(bounded=True)
        retained: list[int] = []
        for i in range(n):
            emit_request(sink, float(i), "a", f"r{i}")
            retained.append(len(sink.samples) + len(sink.active_requests))
        retained_by_run[n] = retained
        peaks[n] = max(retained)
        # nothing is held back once every request finished
        assert len(sink.samples) == 0
        assert len(sink.active_requests) == 0
        # counters are fixed-size per-signal totals, not per-request history
        assert len(sink.counters) == 0
    # the retained sample count never grows with the number of requests
    assert peaks[20] == peaks[200] <= EDGES_PER_REQUEST
    assert all(c <= EDGES_PER_REQUEST for c in retained_by_run[200])


def test_bounded_sink_retains_interleaved_in_flight_only() -> None:
    sink = InMemorySink(bounded=True)
    t = 0.0
    # 3 requests in flight interleaved, one hop-advancement per sweep;
    # the terminal 'completed' sweep evicts each request's samples
    for sweep in range(4):
        for i in range(3):
            sink.edge("llm.request", ["submitted", "queued", "processing",
                                      "completed"][sweep],
                      t=t + 0.1 * i, agent_id=f"a{i}", request_id=f"r{i}")
            # the active index never holds more than the in-flight requests
            assert len(sink.active_requests) <= 3
            # every retained sample belongs to an active request
            for s in sink.samples:
                assert s.tags["request_id"] in sink.active_requests
            t += 1.0
    # each request finishes its agent-side observation afterwards
    for i in range(3):
        emit_request(sink, t, f"a{i}", f"r{i}")
    assert len(sink.samples) == 0
    assert len(sink.active_requests) == 0

    # a request observed once but never terminated stays in the index
    sink.edge("llm.request", "submitted", t=t + 1.0,
              agent_id="a9", request_id="pressed")
    assert len(sink.active_requests) == 1
    assert len(sink.samples) <= EDGES_PER_REQUEST
    assert sink.samples[0].tags["request_id"] == "pressed"


def test_bounded_sink_evicts_endpoint_only_requests_at_completed() -> None:
    sink = InMemorySink(bounded=True)
    emit_request(sink, 0.0, "a", "r1", terminal="completed")
    assert len(sink.samples) == 0
    assert len(sink.active_requests) == 0


def test_bounded_sink_levels_land_in_latest_cache() -> None:
    sink = InMemorySink(bounded=True)
    for i, t in enumerate(range(50)):
        sink.sample("slot_busy", i, t=float(t), component="llm", capacity=2)
        sink.count("tokens", 1.0, t=float(t), component="llm")
    assert len(sink.samples) == 0  # un-attributed samples are not appended
    latest = sink.latest
    assert len(latest) == 2  # fixed per distinct (signal, component)
    assert latest[("slot_busy", "llm")].value == 49  # latest read, not a list
    assert sink.counters["tokens"] == 50.0  # total, fixed size
    assert len(sink.active_requests) == 0


def test_unbounded_sink_keeps_full_history() -> None:
    sink = InMemorySink()  # legacy default: nothing changes for fixtures
    for i in range(10):
        emit_request(sink, float(i), "a", f"r{i}")
    assert len(sink.samples) == 10 * EDGES_PER_REQUEST


# --------------------------------------------------------------------- #
# golden comparison: streamed run == legacy replay of the same stream     #
# --------------------------------------------------------------------- #

GOLDEN_TABLES = [
    ("trace", "id"),
    ("samples", "id"),
    ("latency_percentiles", "source, kind"),
    ("utilization_series", "signal, component, t"),
    ("utilization_stats", "signal, component"),
    ("token_rate_series", "component, t"),
    ("bottleneck", "component, stage"),
    ("meta", "key"),
]


def test_streamed_run_matches_legacy_replay_golden(tmp_path: Path) -> None:
    from agent_db_sim.viz.fixture import synthesize_samples

    cfg = {"warmup": 0.1, "sample_interval": 1.0}

    # legacy path: full-history sink, list replayed through collect_run
    legacy_sink = synthesize_samples(agents=3, until=30.0, seed=7)
    legacy_db = collect_run(tmp_path / "legacy", legacy_sink, cfg=cfg)

    # streamed path: the same emission sequence pushed live into a bounded
    # sink whose subscribed collector writes metrics.db as it goes
    live_sink = InMemorySink(bounded=True)
    collector = start_collection(tmp_path / "live", live_sink, cfg=cfg)
    for s in legacy_sink.samples:  # simulate the run's emission order
        live_sink.record(s)
    live_db = collector.finalize()

    # bounded sink: nothing beyond the trailing window is retained after
    # every fixture request reached its terminal state
    assert len(live_sink.samples) == 0
    assert len(live_sink.active_requests) == 0
    assert len(live_sink.latest) >= 1  # level latest-values cache only

    for table, order in GOLDEN_TABLES:
        legacy_rows = read_table(legacy_db, table, order)
        live_rows = read_table(live_db, table, order)
        assert len(legacy_rows) == len(live_rows) > 0, table
        assert legacy_rows == live_rows, table


def test_rebuild_samples_roundtrips_the_stream(tmp_path: Path) -> None:
    from agent_db_sim.viz.fixture import synthesize_samples

    legacy_sink = synthesize_samples(agents=2, until=15.0, seed=7)
    db = collect_run(tmp_path / "run", legacy_sink, cfg={"warmup": 0.0})
    rebuilt = rebuild_samples(db)
    assert len(rebuilt) == len(legacy_sink.samples)
    for orig, back in zip(legacy_sink.samples, rebuilt):
        assert (back.t, back.signal, back.kind, back.value) == (
            orig.t, orig.signal, orig.kind, orig.value)
        assert back.tags == orig.tags


def test_streaming_collector_replaces_db_rerun_safe(tmp_path: Path) -> None:
    sink = InMemorySink(bounded=True)
    collector = start_collection(tmp_path / "run", sink, cfg={"warmup": 0.0})
    emit_request(sink, 0.0)
    collector.finalize()

    # a second full collection on the same run dir must not accumulate
    collector2 = start_collection(tmp_path / "run", sink, cfg={"warmup": 0.0})
    emit_request(sink, 10.0)
    collector2.finalize()
    conn = sqlite3.connect(tmp_path / "run" / "metrics.db")
    assert conn.execute("SELECT COUNT(*) FROM trace").fetchone()[0] == EDGES_PER_REQUEST
    assert conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0] == EDGES_PER_REQUEST
    conn.close()


def test_run_collector_is_a_run_collector(tmp_path: Path) -> None:
    assert isinstance(
        start_collection(tmp_path / "run", InMemorySink(), cfg=None), RunCollector
    )
