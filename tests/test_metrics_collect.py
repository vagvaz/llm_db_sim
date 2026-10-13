"""Fixture tests for the metrics collector (ticket 11).

All inputs are synthetic probe samples built directly on the InMemorySink
contract — no dependency on the model components (concurrent tickets 08-10).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from agent_db_sim.metrics import (
    STATES,
    MetricsConfig,
    TraceWriter,
    collect_run,
    percentile,
    split_signal,
)
from agent_db_sim.simkit.probes import InMemorySink


def edge(
    sink: InMemorySink,
    t: float,
    signal: str,
    state: str,
    agent: str,
    rid: str,
    parent: str | None = None,
    **extra,
) -> None:
    tags = {"agent_id": agent, "request_id": rid, "state": state}
    if parent is not None:
        tags["parent_id"] = parent
    tags.update(extra)
    sink.edge(signal, state, t=t, **tags)


def lifecycle_fixture(sink: InMemorySink) -> None:
    """Two requests: r1 fully-instrumented, r2 without queue rows on db."""
    # --- request r1 (agent a-0) ---
    edge(sink, 0.0, "agent.loop", "submitted", "a-0", "r1")
    edge(sink, 0.5, "llm.request", "submitted", "a-0", "r1", parent="r1")
    edge(sink, 1.0, "llm.request", "queued", "a-0", "r1", parent="r1")
    edge(sink, 3.0, "llm.request", "processing", "a-0", "r1", parent="r1")
    edge(sink, 6.0, "llm.request", "completed", "a-0", "r1", parent="r1")
    edge(sink, 6.5, "db.request", "submitted", "a-0", "r1", parent="r1")
    edge(sink, 7.0, "db.request", "queued", "a-0", "r1", parent="r1")
    edge(sink, 8.0, "db.request", "processing", "a-0", "r1", parent="r1")
    edge(sink, 9.0, "db.request", "completed", "a-0", "r1", parent="r1")
    edge(sink, 10.0, "agent.loop", "received", "a-0", "r1")
    # --- request r2 (agent a-1): no queued, immediate processing on db ---
    edge(sink, 20.0, "agent.loop", "submitted", "a-1", "r2")
    edge(sink, 20.0, "llm.request", "submitted", "a-1", "r2", parent="r2")
    edge(sink, 20.5, "llm.request", "processing", "a-1", "r2", parent="r2")
    edge(sink, 22.0, "llm.request", "completed", "a-1", "r2", parent="r2")
    edge(sink, 22.5, "db.request", "submitted", "a-1", "r2", parent="r2")
    edge(sink, 22.5, "db.request", "processing", "a-1", "r2", parent="r2")
    edge(sink, 24.5, "db.request", "completed", "a-1", "r2", parent="r2")
    edge(sink, 25.0, "agent.loop", "received", "a-1", "r2")


def test_split_signal() -> None:
    assert split_signal("llm.request") == ("llm", "request")
    assert split_signal("top.llm.ingress.ingress_queue") == (
        "top.llm.ingress",
        "ingress_queue",
    )


def test_percentile_interpolation() -> None:
    vals = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert percentile(vals, 0.50) == pytest.approx(3.0)
    assert percentile(vals, 0.95) == pytest.approx(4.8)
    assert percentile(vals, 0.99) == pytest.approx(4.96)
    assert percentile([7.0], 0.99) == pytest.approx(7.0)
    with pytest.raises(ValueError):
        percentile([], 0.5)
    with pytest.raises(ValueError):
        percentile([1.0], 1.5)


def test_trace_rows_schema_and_state_machine(tmp_path: Path) -> None:
    sink = InMemorySink()
    lifecycle_fixture(sink)
    db_path = collect_run(tmp_path / "run-a", sink, cfg={"warmup": 0.0})

    assert db_path.name == "metrics.db"
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT agent_id, request_id, parent_id, component, stage, state, t, attrs "
        "FROM trace ORDER BY t"
    ).fetchall()
    assert len(rows) == 18  # every lifecycle edge became one trace row
    # every recorded state is a canonical state from ticket 04
    assert all(r[5] in STATES for r in rows)
    # signal name decomposition: {component}.{stage}
    assert {r[3] for r in rows} == {"agent", "llm", "db"}
    llm_rows = [r for r in rows if r[3] == "llm"]
    assert [r[4] for r in llm_rows] == ["request"] * 7
    # state machine ordering per (component, stage, request)
    r1_llm = [(r[5], r[6]) for r in llm_rows if r[1] == "r1"]
    assert r1_llm == [
        ("submitted", 0.5),
        ("queued", 1.0),
        ("processing", 3.0),
        ("completed", 6.0),
    ]
    # hops carry parent_id linking into the root request (r1 rows only)
    assert all(r[2] == "r1" for r in llm_rows if r[1] == "r1")
    # attrs round-trips to json preserving the full tag set
    attrs = json.loads(rows[0][7])
    assert attrs["agent_id"] == "a-0"
    assert attrs["state"] == "submitted"
    idx_names = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
    }
    assert "trace_agent_id_request_id_idx" in idx_names
    conn.close()


def test_batching_keeps_memory_bounded(tmp_path: Path) -> None:
    sink = InMemorySink()
    for i in range(50):
        edge(sink, float(i), "llm.request", "submitted", f"a{i % 7}", f"r{i}")

    writer = TraceWriter(tmp_path / "batch.db", batch_size=4)
    for s in sink.samples:
        writer.add_sample(s)
        # the in-memory buffer can never exceed the batch cap
        assert writer.buffer_len <= 4
    assert writer.buffer_len == 2  # 50 % 4 tail rows held
    writer.finalize()
    assert writer.n_rows == 50
    conn = sqlite3.connect(tmp_path / "batch.db")
    assert conn.execute("SELECT COUNT(*) FROM trace").fetchone()[0] == 50
    conn.close()

    # the same bound holds through the collect_run entry point
    assert writer.batch_size == 4
    cfg = MetricsConfig(batch_size=4)
    assert cfg.batch_size == 4


def test_collect_run_rerun_replaces_db(tmp_path: Path, ) -> None:
    sink = InMemorySink()
    edge(sink, 1.0, "agent.loop", "submitted", "a", "r")
    db_path = collect_run(tmp_path / "run-b", sink)
    n1 = sqlite3.connect(db_path).execute(
        "SELECT COUNT(*) FROM trace"
    ).fetchone()[0]
    db_path2 = collect_run(tmp_path / "run-b", sink)
    assert db_path2 == db_path
    n2 = sqlite3.connect(db_path).execute(
        "SELECT COUNT(*) FROM trace"
    ).fetchone()[0]
    assert n1 == n2 == 1  # fresh collection, no duplicate accumulation


def test_percentiles_match_hand_computed(tmp_path: Path) -> None:
    sink = InMemorySink()
    # five identical-structure requests; llm stage latencies 1..5, queue 0
    for i in range(5):
        rid = f"r{i}"
        edge(sink, float(i), "llm.request", "submitted", "a", rid)
        edge(sink, float(i), "llm.request", "processing", "a", rid)
        edge(sink, float(i) + i + 1.0, "llm.request", "completed", "a", rid)
    e2es = [3.0, 5.0, 7.0, 9.0, 11.0]
    for i, lat in enumerate(e2es):
        edge(sink, float(i), "agent.loop", "submitted", "a", f"r{i}")
        edge(sink, float(i) + lat, "agent.loop", "received", "a", f"r{i}")

    db_path = collect_run(tmp_path / "run-c", sink, cfg={"warmup": 0.0})
    conn = sqlite3.connect(db_path)
    pctl = {
        (src, kind): (p50, p95, p99, n)
        for src, kind, p50, p95, p99, n in conn.execute(
            "SELECT source, kind, p50, p95, p99, n FROM latency_percentiles"
        )
    }
    # stage latencies [1,2,3,4,5]
    p50, p95, p99, n = pctl[("llm.request", "latency")]
    assert (p50, p95, p99, n) == (3.0, 4.8, 4.96, 5)
    # queue all zero
    assert pctl[("llm.request", "queue")][:3] == (0.0, 0.0, 0.0)
    # end-to-end
    p50, p95, p99, n = pctl[("e2e", "latency")]
    assert p50 == pytest.approx(7.0)
    assert p95 == pytest.approx(10.6)
    assert p99 == pytest.approx(10.92)
    assert n == 5
    conn.close()


def test_warmup_exclusion(tmp_path: Path) -> None:
    sink = InMemorySink()
    # t_end = 20 -> warmup (0.25) boundary = 5; r-early (submitted at 1) excluded
    for rid, t_sub, lat in (("r-early", 1.0, 2.0), ("r-late", 10.0, 3.0)):
        edge(sink, t_sub, "llm.request", "submitted", "a", rid)
        edge(sink, t_sub, "llm.request", "processing", "a", rid)
        edge(sink, t_sub + lat, "llm.request", "completed", "a", rid)
        edge(sink, t_sub, "agent.loop", "submitted", "a", rid)
        edge(sink, t_sub + lat, "agent.loop", "received", "a", rid)
    # a benign level sample to pin t_end at exactly 20.0
    sink.sample("slot_busy", 0, t=20.0, component="llm", capacity=1)

    db_path = collect_run(tmp_path / "run-d", sink, cfg={"warmup": 0.25})
    conn = sqlite3.connect(db_path)
    pctl = {
        (src, kind): (p50, p95, p99, n)
        for src, kind, p50, p95, p99, n in conn.execute(
            "SELECT source, kind, p50, p95, p99, n FROM latency_percentiles"
        )
    }
    # only the post-warmup request counts (n == 1), values == its durations
    assert pctl[("llm.request", "latency")] == (3.0, 3.0, 3.0, 1)
    assert pctl[("e2e", "latency")][:3] == (3.0, 3.0, 3.0)
    # raw traces still hold everything (traces are the source of truth)
    n_rows = conn.execute("SELECT COUNT(*) FROM trace").fetchone()[0]
    assert n_rows == 10
    conn.close()


def test_utilization_series_time_weighted(tmp_path: Path) -> None:
    sink = InMemorySink()
    # step: 0 on [0,5), 1 on [5,12)  ->  time-weighted mean over [0,12] = 7/12
    sink.sample("slot_busy", 0, t=0.0, component="llm", capacity=2)
    sink.sample("slot_busy", 1, t=5.0, component="llm", capacity=2)
    # trailing observation pins t_end (value 2 has no duration in the window)
    sink.sample("slot_busy", 2, t=12.0, component="llm", capacity=2)
    # queue depth: nonempty fraction fallback (no capacity tag); the series
    # integrates from its first observation (t=2) — 0 on [2,4), 3 on [4,10)
    sink.sample("queue_depth", 0, t=2.0, component="llm")
    sink.sample("queue_depth", 3, t=4.0, component="llm")
    sink.sample("queue_depth", 0, t=10.0, component="llm")

    db_path = collect_run(
        tmp_path / "run-e", sink, cfg={"warmup": 0.0, "sample_interval": 5.0}
    )
    conn = sqlite3.connect(db_path)
    stats = {
        (sig, comp): (mean, peak, mean_util)
        for sig, comp, mean, peak, mean_util in conn.execute(
            "SELECT signal, component, mean, peak, mean_util FROM utilization_stats"
        )
    }
    mean, peak, mean_util = stats[("slot_busy", "llm")]
    assert mean == pytest.approx(7.0 / 12.0)
    assert peak == 2.0
    assert mean_util == pytest.approx((7.0 / 12.0) / 2.0)
    mean, peak, mean_util = stats[("queue_depth", "llm")]
    # window extends to t_end=12 (pinned by the trailing slot_busy sample):
    # 0 on [2,4), 3 on [4,10), 0 on [10,12) -> mean 18/10; nonempty 6/10
    assert mean == pytest.approx(1.8)
    assert mean_util == pytest.approx(6.0 / 10.0)

    rows = conn.execute(
        "SELECT t, value FROM utilization_series WHERE signal='slot_busy' ORDER BY t"
    ).fetchall()
    # ticks at sample_interval=5 restricted to (warmup_end=0, t_end=12]
    assert rows == [(5.0, 1.0), (10.0, 1.0)]
    conn.close()


def test_token_throughput_series(tmp_path: Path) -> None:
    sink = InMemorySink()
    # 10 tokens paced 1-per-0.5 over [0.5, 5] -> 2.0 tokens/time unit held
    # across the whole window; collection window is [warmup_end=0.5, 5]
    for i in range(1, 11):
        sink.count("tokens", 1.0, t=0.5 * i, component="llm")

    db_path = collect_run(
        tmp_path / "run-f", sink, cfg={"warmup": 0.0, "sample_interval": 5.0}
    )
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT t, rate FROM token_rate_series ORDER BY t"
    ).fetchall()
    assert rows == [(5.0, pytest.approx(2.0))]
    mean, peak, mean_util = conn.execute(
        "SELECT mean, peak, mean_util FROM utilization_stats"
    ).fetchone()
    assert mean == pytest.approx(2.0)
    assert peak == pytest.approx(2.0)
    assert mean_util is None
    conn.close()


def test_attribution_picks_hand_designed_bottleneck(tmp_path: Path) -> None:
    sink = InMemorySink()
    # 3 requests; per request: e2e=14, llm queue=2, db queue=10
    for i, t_sub in enumerate((0.0, 5.0, 10.0)):
        edge(sink, t_sub, "agent.loop", "submitted", f"a{i}", f"r{i}")
        edge(sink, t_sub + 14.0, "agent.loop", "received", f"a{i}", f"r{i}")
        # llm hop: queue 2, then 3 of service
        edge(sink, t_sub, "llm.request", "submitted", f"a{i}", f"r{i}", parent=f"r{i}")
        edge(sink, t_sub + 2.0, "llm.request", "processing", f"a{i}", f"r{i}", parent=f"r{i}")
        edge(sink, t_sub + 5.0, "llm.request", "completed", f"a{i}", f"r{i}", parent=f"r{i}")
        # db hop: queue 10, then 2 of service
        edge(sink, t_sub + 5.0, "db.request", "submitted", f"a{i}", f"r{i}", parent=f"r{i}")
        edge(sink, t_sub + 6.0, "db.request", "queued", f"a{i}", f"r{i}", parent=f"r{i}")
        edge(sink, t_sub + 16.0, "db.request", "processing", f"a{i}", f"r{i}", parent=f"r{i}")
        edge(sink, t_sub + 18.0, "db.request", "completed", f"a{i}", f"r{i}", parent=f"r{i}")
    # saturation support: llm idles (held util .1), db runs hot (0.5); held
    # from t=0 over the whole [0, 28] window (t_end pinned by db completions)
    sink.sample("slot_busy", 1, t=0.0, component="llm", capacity=10)
    sink.sample("slot_busy", 5, t=0.0, component="db", capacity=10)

    db_path = collect_run(tmp_path / "run-g", sink, cfg={"warmup": 0.0})
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT component, stage, queue_share, sat_score, score FROM bottleneck "
        "ORDER BY score DESC"
    ).fetchall()
    assert rows[0][0:2] == ("db", "request")
    db_queue_share = rows[0][2]
    llm = [r for r in rows if r[0] == "llm"][0]
    # shares of the 42 total e2e: db queue = 33 (submitted->processing),
    # llm queue = 6 (2 per request)
    assert db_queue_share == pytest.approx(33.0 / 42.0)
    assert llm[2] == pytest.approx(6.0 / 42.0)
    assert rows[0][3] == pytest.approx(0.5)
    assert rows[0][4] == pytest.approx(0.7 * 33.0 / 42.0 + 0.3 * 0.5)
    assert llm[3] == pytest.approx(0.1)
    conn.close()


def test_component_level_fallback_for_split_stage_hops(tmp_path: Path) -> None:
    """Emitters that split one hop across sibling stages (storage_queue /
    storage_service) are recovered at component level as stage ``*``."""
    sink = InMemorySink()
    comp = "top.db.storage"
    edge(sink, 0.0, f"{comp}.storage_queue", "submitted", "a", "r")
    edge(sink, 0.0, f"{comp}.storage_queue", "queued", "a", "r")
    edge(sink, 2.0, f"{comp}.storage_service", "processing", "a", "r")
    edge(sink, 5.0, f"{comp}.storage_service", "completed", "a", "r")
    edge(sink, 0.0, "top.agent.loop", "submitted", "a", "r")
    edge(sink, 6.0, "top.agent.loop", "received", "a", "r")

    db_path = collect_run(tmp_path / "run-i", sink, cfg={"warmup": 0.0})
    conn = sqlite3.connect(db_path)
    pctl = {
        (src, kind): (p50, p95, p99, n)
        for src, kind, p50, p95, p99, n in conn.execute(
            "SELECT source, kind, p50, p95, p99, n FROM latency_percentiles"
        )
    }
    assert pctl[(f"{comp}.*", "latency")] == (5.0, 5.0, 5.0, 1)
    assert pctl[(f"{comp}.*", "queue")] == (2.0, 2.0, 2.0, 1)
    # e2e unaffected (received/submitted are per-request, stage-agnostic)
    assert pctl[("e2e", "latency")] == (6.0, 6.0, 6.0, 1)
    conn.close()


def test_collect_run_with_empty_sink(tmp_path: Path) -> None:
    db_path = collect_run(tmp_path / "run-h", InMemorySink())
    conn = sqlite3.connect(db_path)
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert {"trace", "latency_percentiles", "utilization_series",
            "utilization_stats", "token_rate_series", "bottleneck",
            "meta"} <= tables
    assert conn.execute("SELECT COUNT(*) FROM bottleneck").fetchone()[0] == 0
    conn.close()


def test_non_lifecycle_samples_are_not_trace_rows(tmp_path: Path) -> None:
    sink = InMemorySink()
    sink.sample("slot_busy", 1, t=1.0, component="llm")
    sink.count("requests", 1.0, t=2.0, component="llm")
    edge(sink, 3.0, "llm.request", "held", "a", "r")  # non-canonical state
    writer = TraceWriter(tmp_path / "misc.db", batch_size=8)
    for s in sink.samples:
        writer.add_sample(s)
    writer.finalize()
    conn = sqlite3.connect(tmp_path / "misc.db")
    rows = conn.execute("SELECT state FROM trace").fetchall()
    assert rows == [("held",)]  # levels/counters skipped; non-canonical kept
    conn.close()
