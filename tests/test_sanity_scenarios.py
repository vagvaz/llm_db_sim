"""Sanity scenarios: a few tiny 1-of-everything simulations whose latency
math a human can verify on paper (post-stage-5 bounded task).

Every expectation here is HAND-DERIVED from the scenario config values —
the derivation formulas are written down in
``scenarios/sanity/README.md`` and re-implemented below from the loaded
configs (so the tests cannot silently drift from the YAMLs):

- **sanity_single** — 1 agent, 1 LLM (1 slot), centralized DB (1 compute
  slot, 1 colocated ssd storage). No queueing at all; the run is a
  perfectly periodic arithmetic system with period tau = L + D.
- **sanity_queue** — 2 agents, LLM with 1 slot. Hand-derived FIFO
  serialization: agent-1's first inference waits exactly L; afterwards
  the two agents ping-pong with every later inference queueing L - D.
- **sanity_parallel** — 2 agents, LLM with 2 slots. Everything overlaps
  in time, nothing queues, and e2e stays equal to the single-request
  latency.
- **sanity_disaggregated** — 1 agent, disaggregated DB (1 compute, 1
  storage, one shared segment). Adds the net_req / net_resp hops with
  exactly propagation + volume/bandwidth times; centralized runs have
  no net stages.

Assertions compare against ``1e-9`` (the model's deterministic fp noise
over ~30 loop iterations is < 1e-12).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest
from omegaconf import OmegaConf
from typer.testing import CliRunner

from agent_db_sim.cli import app
from agent_db_sim.components.db.backend import STORAGE_BACKEND_REGISTRY
from agent_db_sim.metrics.derive import percentile
from agent_db_sim.pipeline import execute_run

REPO = Path(__file__).resolve().parents[1]
SCENARIO_DIR = REPO / "scenarios" / "sanity"

TOL = 1e-9

# trace component paths (verified once against the live model; the names
# are the ticket-10/13 structural contracts)
LLM_COMP = "llm"
COMPUTE_COMP = "top.db.topology.compute-0"
COMPUTE_STORAGE = "top.db.topology.compute-0.storage"
STORAGE_NODE = "top.db.topology.storage-0"
SEGMENT = "top.db.topology.segment"

runner = CliRunner()

# --------------------------------------------------------------------------- #
# hand model: latency math recomputed from the loaded scenario config         #
# --------------------------------------------------------------------------- #


def _prof(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return cfg["agents"]["query_profiles"][0]


def _llm_latency(cfg: Dict[str, Any]) -> float:
    """L = input/input_rate + output/output_rate (one solo slot hold)."""
    llm = cfg["llm"]
    return (
        cfg["agents"]["input_tokens"] / llm["input_tokens_per_s"]
        + cfg["agents"]["output_tokens"] / llm["output_tokens_per_s"]
    )


def _storage_hop(cfg: Dict[str, Any]) -> Tuple[str, float, float]:
    """(storage component, service time, queue time) for the sole query.

    backend.py lone-op spec: base_latency first (recorded inside the
    storage_queue stage), then the pacing window max(per_gb*vol, vol/bw,
    1/iops) — with a lone op that window is met exactly.
    """
    preset = STORAGE_BACKEND_REGISTRY[str(cfg["db"]["storage"]["backend"])]
    overrides = dict(cfg["db"]["storage"].get("overrides") or {})
    base = float(overrides.get("base_latency", preset.base_latency))
    per_gb = float(overrides.get("per_gb_time", preset.per_gb_time))
    bw = float(overrides.get("bandwidth_ceiling", preset.bandwidth_ceiling))
    iops = float(overrides.get("iops_ceiling", preset.iops_ceiling))
    volume = _prof(cfg)["volume"]
    needed = max(per_gb * volume, volume / bw, 1.0 / iops)
    comp = COMPUTE_STORAGE if cfg["db"]["topology"] == "centralized" else STORAGE_NODE
    return comp, base + needed, base


def _net(sf: Any, cfg: Dict[str, Any]) -> Dict[str, float]:
    net = cfg["db"]["network"]
    volume = _prof(cfg)["volume"]
    size = float(net["request_size"])
    lat = float(net["latency"])
    return {
        "net_req": lat + size / float(net["bandwidth"]),
        "net_resp": lat + volume / float(net["bandwidth"]),
        "latency": lat,
    }


def _chain(cfg: Dict[str, Any]) -> List[Tuple[str, Tuple[str, ...], float, float]]:
    """The per-request DB hop chain: (component, stages, latency, queue).

    queue is the *stage-machine* queue = earliest processing − earliest
    submitted/queued t (derive.py's state-machine rule). Only the storage
    hop has nonzero queue time: the backend charges base_latency before
    the paced service starts.
    """
    topo = str(cfg["db"]["topology"])
    complexity = _prof(cfg)["complexity"]
    issue_ratio = float(cfg["db"]["compute"]["issue_ratio"])
    storage_comp, storage_latency, storage_queue = _storage_hop(cfg)

    hops = [
        (COMPUTE_COMP, ("compute_issue",), complexity * issue_ratio, 0.0),
        (storage_comp, ("storage_queue", "storage_service"), storage_latency, storage_queue),
        (COMPUTE_COMP, ("compute_finalize",), complexity * (1.0 - issue_ratio), 0.0),
    ]
    if topo == "disaggregated":
        times = _net(None, cfg)
        wired = [
            (COMPUTE_COMP, ("compute_issue",), hops[0][2], 0.0),
            # the net stage machine books propagation as its processing
            # onset — the trace-machine "queue" is the propagation walk
            # (the pool's own queueing time is 0 with a lone agent)
            (SEGMENT, ("net_req",), times["net_req"], times["latency"]),
            (STORAGE_NODE, ("storage_queue", "storage_service"), storage_latency, storage_queue),
            (SEGMENT, ("net_resp",), times["net_resp"], times["latency"]),
            (COMPUTE_COMP, ("compute_finalize",), hops[2][2], 0.0),
        ]
        return wired
    return hops


def _D(cfg: Dict[str, Any]) -> float:
    """DB latency = sum of the chain hop latencies (slot held throughout)."""
    return sum(h[2] for h in _chain(cfg))


def _model(cfg: Dict[str, Any]) -> Tuple[float, float, float]:
    """(L, D, tau) — the loop period is exactly L + D (no queueing)."""
    L = _llm_latency(cfg)
    D = _D(cfg)
    return L, D, L + D


# --------------------------------------------------------------------------- #
# expected per-request series (the paper timeline, re-derived)                #
# --------------------------------------------------------------------------- #


def _rid(agent: str, n: int) -> str:
    return f"{agent}#{n}"


def _grid_series(cfg: Dict[str, Any], agent: str) -> List[Dict[str, Any]]:
    """Lockstep closed loop with no queueing: submission #k at (k-1)*tau.

    Inference #n (n odd) is submitted at s_k and received at s_k + L; its
    db request #n+1 is submitted then and received D later.
    """
    L, D, tau = _model(cfg)
    until = float(cfg["sim"]["until"])
    out: List[Dict[str, Any]] = []
    k = 0
    sub = 0.0
    while sub <= until + TOL:
        k += 1
        inf_recv = sub + L if sub + L <= until + TOL else None
        inf_svc = L  # solo slot: queue 0, service L
        db_sub = inf_recv
        db_recv = sub + tau if db_sub is not None and sub + tau <= until + TOL else None
        out.append(
            {
                "agent": agent,
                "k": k,
                "inf_rid": _rid(agent, 2 * k - 1),
                "db_rid": _rid(agent, 2 * k),
                "inf_sub": sub,
                "inf_recv": inf_recv,
                "inf_svc": inf_svc,
                "inf_q": 0.0,
                "db_sub": db_sub,
                "db_recv": db_recv,
            }
        )
        sub = k * tau
    return out


def _pingpong_series(cfg: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """sanity_queue paper timeline (see scenarios/sanity/sanity_queue.yaml):

    LLM services alternate a0/a1 in 1.0-wide blocks: a0's inferences hold
    [0,1), [2,3), ...; a1's hold [1,2), [3,4), ... Every later submission
    (both agents) waits in the slot queue for the next a0-block start.
    """
    L, D, _ = _model(cfg)
    until = float(cfg["sim"]["until"])
    a0: List[Dict[str, Any]] = []
    a1: List[Dict[str, Any]] = []

    def agent_block(agent: str, first_service_start: float, first_wait: float):
        rows: List[Dict[str, Any]] = []
        k = 1
        start = first_service_start  # service start of submission #k
        while True:
            if k == 1:
                sub, wait = 0.0, first_wait
            else:
                sub = start - (L - D)  # next submission lands at (start - L) + D
                wait = start - sub
            if sub > until + TOL:
                break
            svc = L
            recv = start + L
            db_sub = recv if recv <= until + TOL else None
            db_recv = (
                recv + D if db_sub is not None and recv + D <= until + TOL else None
            )
            rows.append(
                {
                    "agent": agent,
                    "k": k,
                    "inf_rid": _rid(agent, 2 * k - 1),
                    "db_rid": _rid(agent, 2 * k),
                    "inf_sub": sub,
                    "inf_recv": recv if recv <= until + TOL else None,
                    "inf_svc": svc,
                    "inf_q": wait,
                    "db_sub": db_sub,
                    "db_recv": db_recv,
                }
            )
            if start + L > until + TOL:
                break
            k += 1
            start += 2.0 * L
        return rows

    a0 = agent_block("top.agents.agent-0", 0.0, 0.0)
    a1 = agent_block("top.agents.agent-1", L, L)
    return a0, a1


# --------------------------------------------------------------------------- #
# trace loading + assertion helpers                                           #
# --------------------------------------------------------------------------- #

# trace[rid][(component, stage)][state] = t (first occurrence wins)
_TRACE_ROW = Tuple[str, str, str, str, str, float]  # rid, comp, stage, state, t


def load_trace(db_path: Path) -> Dict[str, Dict[Tuple[str, str], Dict[str, float]]]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT request_id, component, stage, state, t FROM trace"
        ).fetchall()
    finally:
        conn.close()
    trace: Dict[str, Dict[Tuple[str, str], Dict[str, float]]] = {}
    for rid, comp, stage, state, t in rows:
        trace.setdefault(rid, {}).setdefault((comp, stage), {}).setdefault(
            str(state), float(t)
        )
    return trace


def _hop(tr, rid: str, comp: str, stages: Sequence[str]) -> Optional[Tuple[float, float, float]]:
    """(latency, queue, service) of one logical hop.

    latency = latest completed − earliest submitted/queued t; queue =
    earliest processing − start (0.0 absent); service = latest completed
    − earliest processing. This mirrors derive.py's state-machine rule."""
    tr_rid = tr.get(rid)
    if tr_rid is None:
        return None
    starts, procs, ends = [], [], []
    for (c, s), sm in tr_rid.items():
        if c != comp or s not in stages:
            continue
        starts += [t for st, t in sm.items() if st in ("submitted", "queued")]
        procs.append(sm["processing"]) if "processing" in sm else None
        ends.append(sm["completed"]) if "completed" in sm else None
    if not ends:
        return None
    if not starts:
        if not procs:
            return None
        starts = [min(procs)]
    start = min(starts)
    end = max(ends)
    proc = min(procs) if procs else None
    queue = (proc - start) if proc is not None else 0.0
    service = (end - proc) if proc is not None else end - start
    return end - start, queue, service


def _assert_hop(tr, rid, comp, stages, latency, queue=TOL, what=""):
    got = _hop(tr, rid, comp, stages)
    assert got is not None, f"{what}: no complete machine for {rid} {comp}/{stages}"
    lat, q, _svc = got
    assert abs(lat - latency) <= TOL, f"{what}: {rid} {comp}/{stages} latency {lat} != {latency}"
    assert abs(q - queue) <= TOL, f"{what}: {rid} {comp}/{stages} queue {q} != {queue}"


def _received_or_absent(tr, rid, recv_t, what=""):
    agent = rid.rsplit("#", 1)[0]
    stage = "inference" if int(rid.rsplit("#", 1)[1]) % 2 == 1 else "db"
    states = tr[rid].get((agent, stage), {})
    if recv_t is None:
        assert "received" not in states, f"{what}: {rid} unexpectedly received"
        assert "submitted" in states, f"{what}: {rid} never submitted"
    else:
        got = states["received"]
        assert abs(got - recv_t) <= TOL, f"{what}: {rid} received {got} != {recv_t}"


def _submit_submitted(tr, rid, sub_t, agent, stage, what=""):
    states = tr[rid][(agent, stage)]
    assert abs(states["submitted"] - sub_t) <= TOL, f"{what}: {rid} submitted != {sub_t}"


# --------------------------------------------------------------------------- #
# run + artifact plumbing                                                     #
# --------------------------------------------------------------------------- #

_ARTIFACTS = ("config.yaml", "metrics.db", "run.vcd", "report.html", "summary.json", "plotly.min.js")


def run_scenario(name: str, run_dir: Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    cfg = OmegaConf.to_container(OmegaConf.load(SCENARIO_DIR / name), resolve=True)
    assert isinstance(cfg, dict)
    execute_run(OmegaConf.create(cfg), run_dir)
    trace = load_trace(run_dir / "metrics.db")
    return cfg, trace  # type: ignore[return-value]


def assert_artifacts(run_dir: Path) -> None:
    names = sorted(p.name for p in run_dir.iterdir())
    for name in _ARTIFACTS:
        assert name in names, f"missing artifact {name} (have {names})"
        assert (run_dir / name).stat().st_size > 0, f"{name} is empty"


def _percentiles(db_path: Path) -> Dict[Tuple[str, str], Dict[str, float]]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        out: Dict[Tuple[str, str], Dict[str, float]] = {}
        for source, kind, p50, p95, p99, n in conn.execute(
            "SELECT source, kind, p50, p95, p99, n FROM latency_percentiles"
        ):
            out[(source, kind)] = {"p50": p50, "p95": p95, "p99": p99, "n": n}
        return out
    finally:
        conn.close()


def _summary(db_path: Path) -> Dict[str, Any]:
    return json.loads((db_path.parent / "summary.json").read_text())


def check_percentiles(
    db_path: Path,
    expected: Dict[Tuple[str, str], List[float]],
    what: str,
) -> None:
    """Derived percentiles of the hand multisets (all values present).

    metrics.warmup: 0.0 in every sanity scenario, so every completed
    request is included — the derived percentiles must equal the
    linear-interpolation percentiles of the paper multisets exactly.
    """
    got = _percentiles(db_path)
    for key, values in expected.items():
        assert key in got, f"{what}: no derived row {key} (have {sorted(got)})"
        row = got[key]
        exp = {
            "p50": percentile(values, 0.50),
            "p95": percentile(values, 0.95),
            "p99": percentile(values, 0.99),
            "n": len(values),
        }
        for stat in ("p50", "p95", "p99"):
            assert abs(row[stat] - exp[stat]) <= TOL, (
                f"{what}: {key} {stat} {row[stat]} != {exp[stat]}"
            )
        assert row["n"] == exp["n"], f"{what}: {key} n {row['n']} != {exp['n']}"


def _e2e_values(series_union: List[Dict[str, Any]]) -> List[float]:
    out: List[float] = []
    for row in series_union:
        if row["inf_recv"] is not None:
            out.append(row["inf_recv"] - row["inf_sub"])
        if row["db_recv"] is not None:
            out.append(row["db_recv"] - row["db_sub"])
    return out


def _llm_records(cfg) -> Tuple[float, float]:
    """(L, total token cap): sanity LLMs serve in one solo slot hold."""
    return _llm_latency(cfg), 1.0


def _llm_records(cfg) -> Tuple[float, float]:  # noqa: ARG001 - kept for symmetry
    """(L, total token cap): sanity LLMs serve one request at a time."""
    return _llm_latency(cfg), 1.0


def _counts_from_trace(trace, agent: str) -> Dict[str, int]:
    counts = {"inference_submitted": 0, "inference_received": 0, "db_submitted": 0, "db_received": 0}
    for rid, stages in trace.items():
        if not rid.startswith(agent + "#"):
            continue
        for stage, key in (("inference", "inference"), ("db", "db")):
            sm = stages.get((agent, stage))
            if sm is None:
                continue
            if "submitted" in sm:
                counts[f"{key}_submitted"] += 1
            if "received" in sm:
                counts[f"{key}_received"] += 1
    return counts


# --------------------------------------------------------------------------- #
# 1) sanity_single                                                            #
# --------------------------------------------------------------------------- #


def test_sanity_single(tmp_path: Path) -> None:
    cfg, trace = run_scenario("sanity_single.yaml", tmp_path / "single")
    assert_artifacts(tmp_path / "single")
    agent = "top.agents.agent-0"
    L, D, tau = _model(cfg)
    # paper math (also in the YAML header):
    assert L == pytest.approx(1.0, abs=1e-12)
    assert D == pytest.approx(0.6001, abs=1e-12)
    assert tau == pytest.approx(1.6001, abs=1e-12)

    series = _grid_series(cfg, agent)
    chain = _chain(cfg)
    inf_recv = sum(1 for r in series if r["inf_recv"] is not None)  # type: ignore
    db_sent = sum(1 for r in series if r["db_sub"] is not None)
    db_recv = sum(1 for r in series if r["db_recv"] is not None)
    assert inf_recv == 32 and db_sent == 32 and db_recv == 31
    db_path = tmp_path / "single" / "metrics.db"
    summary = _summary(db_path)
    assert summary["inference_completed"] == inf_recv == 32
    assert summary["db_completed"] == db_recv == 31
    assert summary["agents"] == 1

    counts = _counts_from_trace(trace, agent)
    assert counts == {
        "inference_submitted": 32,
        "inference_received": 32,
        "db_submitted": 32,
        "db_received": 31,
    }

    # every request: agent timeline, LLM stage, full DB chain, e2e == chain
    for row in series:
        what = f"single {row['inf_rid']}"
        _submit_submitted(trace, row["inf_rid"], row["inf_sub"], agent, "inference", what)
        _received_or_absent(trace, row["inf_rid"], row["inf_recv"], what)
        if row["db_sub"] is not None:
            _submit_submitted(trace, row["db_rid"], row["db_sub"], agent, "db", f"single {row['db_rid']}")
            _received_or_absent(trace, row["db_rid"], row["db_recv"], f"single {row['db_rid']}")
        # LLM stage: submitted/queued at the agent's submit, processing at service
        # start (= submit time, no queue), completed exactly L later.
        got = _hop(trace, row["inf_rid"], LLM_COMP, ("submitted", "queued", "processing", "completed"))
        assert got is not None, f"{what}: no complete LLM machine"
        lat, q, svc = got
        assert abs(q - row["inf_q"]) <= TOL
        assert abs(lat - L) <= TOL
        if row["db_recv"] is not None:
            for comp, stages, latency, queue in chain:
                _assert_hop(
                    trace, row["db_rid"], comp, stages, latency, queue,
                    what=f"single {row['db_rid']} hop",
                )
            db_total = _hop(trace, row["db_rid"], COMPUTE_COMP, ("compute_issue", "compute_finalize"))
            assert db_total is not None
            assert abs(db_total[0] - D) <= TOL, f"db chain total {db_total[0]} != {D}"

    # no internal network stages on centralized
    net_rows = [
        (comp, stage)
        for rid, stages in trace.items()
        for comp, stage in stages
        if stage in ("net_req", "net_resp")
    ]
    assert net_rows == []

    # derived tables = percentiles of the paper multisets
    check_percentiles(
        db_path,
        {
            ("e2e", "latency"): _e2e_values(series),
        },
        what="single e2e",
    )
    # llm.*: all completed services are exactly L; queue all 0
    check_percentiles(
        db_path,
        {
            (LLM_COMP + ".*", "latency"): [L] * inf_recv,
            (LLM_COMP + ".*", "queue"): [0.0] * inf_recv,
            (COMPUTE_COMP + ".compute_issue", "latency"): [chain[0][2]] * db_sent,
            (COMPUTE_COMP + ".compute_issue", "queue"): [0.0] * db_sent,
            (COMPUTE_STORAGE + ".*", "latency"): [chain[1][2]] * db_sent,
            (COMPUTE_STORAGE + ".*", "queue"): [chain[1][3]] * db_sent,
        },
        what="single stages",
    )

    # attribution is trivially sane: no queueing anywhere except the
    # storage backend's base_latency charge
    e2e_total = sum(_e2e_values(series))
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        bottlenecks = conn.execute(
            "SELECT component, stage, queue_delay, queue_share, sat_score, score FROM bottleneck ORDER BY score DESC"
        ).fetchall()
    finally:
        conn.close()
    rows = {(c, s): (qd, qs, sat, sc) for c, s, qd, qs, sat, sc in bottlenecks}
    assert set(rows) == {
        (LLM_COMP, "*"),
        (COMPUTE_COMP, "compute_issue"),
        (COMPUTE_STORAGE, "*"),
    }, sorted(rows)
    assert rows[(LLM_COMP, "*")][0] <= TOL
    assert rows[(COMPUTE_COMP, "compute_issue")][0] <= TOL
    # the storage hop's "queue" is exactly the backend base_latency charge
    assert rows[(COMPUTE_STORAGE, "*")][0] == pytest.approx(db_sent * chain[1][3], abs=1e-9)
    assert rows[(LLM_COMP, "*")][1] <= TOL
    # winner row: hand queue_delay/share and the hybrid score 0.7*q + 0.3*sat
    # (the supporting saturation comes from the ingress occupancy probes)
    winner = summary["bottleneck_winner"]
    assert winner["component"] == COMPUTE_STORAGE
    assert winner["stage"] == "*"
    qd, qs, sat, sc = rows[(COMPUTE_STORAGE, "*")]
    assert qs == pytest.approx(qd / e2e_total, abs=1e-9)
    assert sc == pytest.approx(0.7 * qs + 0.3 * sat, abs=1e-9)
    assert sc == winner["score"]


def test_sanity_single_deterministic(tmp_path: Path) -> None:
    """Same seed -> bit-identical trace (seed pinned everywhere)."""
    first = tmp_path / "a"
    second = tmp_path / "b"
    _, trace_a = run_scenario("sanity_single.yaml", first)
    _, trace_b = run_scenario("sanity_single.yaml", second)
    rows_a = sorted((rid, c, s, st, t) for rid, stages in trace_a.items() for (c, s), sm in stages.items() for st, t in sm.items())
    rows_b = sorted((rid, c, s, st, t) for rid, stages in trace_b.items() for (c, s), sm in stages.items() for st, t in sm.items())
    assert rows_a == rows_b


# --------------------------------------------------------------------------- #
# 2) sanity_queue                                                             #
# --------------------------------------------------------------------------- #


def test_sanity_queue(tmp_path: Path) -> None:
    cfg, trace = run_scenario("sanity_queue.yaml", tmp_path / "queue")
    assert_artifacts(tmp_path / "queue")
    a0, a1 = "top.agents.agent-0", "top.agents.agent-1"
    L, D, tau = _model(cfg)
    assert L == pytest.approx(1.0, abs=1e-12)
    assert D == pytest.approx(0.6001, abs=1e-12)

    series0, series1 = _pingpong_series(cfg)
    for series, agent in ((series0, a0), (series1, a1)):
        counts = _counts_from_trace(trace, agent)
        exp = {
            "inference_submitted": sum(1 for r in series if True),
            "inference_received": sum(1 for r in series if r["inf_recv"] is not None),
            "db_submitted": sum(1 for r in series if r["db_sub"] is not None),
            "db_received": sum(1 for r in series if r["db_recv"] is not None),
        }
        assert counts == exp, (agent, counts, exp)

    # hand-serialized orders and wait/service splits
    for series, agent, first_wait in ((series0, a0, 0.0), (series1, a1, L)):
        for row in series:
            what = f"queue {row['inf_rid']}"
            _submit_submitted(trace, row["inf_rid"], row["inf_sub"], agent, "inference", what)
            _received_or_absent(trace, row["inf_rid"], row["inf_recv"], what)
            llm_states = trace[row["inf_rid"]].get((LLM_COMP, "completed"), {})
            if row["inf_recv"] is None:
                # submitted inside the horizon but served past it: admitted and
                # queued, never completed
                assert "completed" not in llm_states and "processing" not in llm_states
                continue
            got = _hop(trace, row["inf_rid"], LLM_COMP, ("submitted", "queued", "processing", "completed"))
            assert got is not None, f"{what}: no complete LLM machine"
            lat, q, svc = got
            # queue_time > 0 exactly where the paper math puts it
            assert abs(q - row["inf_q"]) <= TOL, f"{what}: queue {q} != {row['inf_q']}"
            # service stays L (only the wait grows)
            assert abs(svc - L) <= TOL, f"{what}: llm service {svc} != {L}"
            assert abs(lat - (L + row["inf_q"])) <= TOL, f"{what}: llm latency {lat}"
            # agent #2's first request: e2e == its service + measured wait
            e2e = row["inf_recv"] - row["inf_sub"]
            assert abs(e2e - (L + row["inf_q"])) <= TOL, f"{what}: e2e {e2e}"
            if row["db_recv"] is not None:
                dwhat = f"queue {row['db_rid']}"
                _submit_submitted(trace, row["db_rid"], row["db_sub"], agent, "db", dwhat)
                _received_or_absent(trace, row["db_rid"], row["db_recv"], dwhat)
                for comp, stages, latency, queue in _chain(cfg):
                    _assert_hop(trace, row["db_rid"], comp, stages, latency, queue, what=dwhat)

    # the ping-pong: hand-tabulated first rows (per YAML header)
    assert series0[0]["inf_q"] == 0.0
    assert series0[1]["inf_q"] == pytest.approx(L - D)          # 0.3999
    assert series1[0]["inf_q"] == pytest.approx(1.0)            # full L
    assert series1[0]["inf_recv"] == pytest.approx(2.0)         # service + wait
    assert series0[1]["inf_sub"] == pytest.approx(L + D)        # 1.6001
    assert series1[1]["inf_sub"] == pytest.approx(2 * L + D)    # 2.6001

    # derived percentiles = paper multisets (warmup 0)
    db_path = tmp_path / "queue" / "metrics.db"
    summary = _summary(db_path)
    all_series = series0 + series1
    llm_queues: List[float] = []
    llm_lat = [(L + r["inf_q"]) for r in all_series if r["inf_recv"] is not None]
    llm_queues = [r["inf_q"] for r in all_series if r["inf_recv"] is not None]
    check_percentiles(
        db_path,
        {
            ("e2e", "latency"): _e2e_values(all_series),
            (LLM_COMP + ".*", "latency"): llm_lat,
            (LLM_COMP + ".*", "queue"): llm_queues,
        },
        what="queue",
    )

    # attribution: the queue lives at the LLM and only there (plus the
    # backend base_latency charge); check the hand sums
    qsum_llm = sum(q for q in llm_queues)
    e2e_total = sum(_e2e_values(all_series))
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = {
            (c, s): (qd, qs)
            for c, s, qd, qs, sat, sc in conn.execute(
                "SELECT component, stage, queue_delay, queue_share, sat_score, score FROM bottleneck"
            )
        }
    finally:
        conn.close()
    assert rows[(LLM_COMP, "*")][0] == pytest.approx(qsum_llm, abs=1e-6)
    assert rows[(LLM_COMP, "*")][1] == pytest.approx(qsum_llm / e2e_total, abs=1e-6)
    # the winner is the LLM (all the real queueing lives there)
    assert summary["bottleneck_winner"]["component"] == LLM_COMP
    # dbs never queue: storage rows carry only the backend base charge
    db_sent = sum(1 for r in all_series if r["db_sub"] is not None)
    assert rows[(COMPUTE_STORAGE, "*")][0] == pytest.approx(db_sent * 0.0001, abs=1e-9)


def test_sanity_queue_cli(tmp_path: Path) -> None:
    # brief sanity: the CLI path produces the same headline numbers by hand
    out = tmp_path / "runs"
    result = runner.invoke(app, ["run", str(SCENARIO_DIR / "sanity_queue.yaml"), "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert "run dir:" in result.output
    assert "bottleneck:" in result.output
    assert "e2e p50/p95/p99" in result.output


# --------------------------------------------------------------------------- #
# 3) sanity_parallel                                                          #
# --------------------------------------------------------------------------- #


def test_sanity_parallel(tmp_path: Path) -> None:
    cfg, trace = run_scenario("sanity_parallel.yaml", tmp_path / "parallel")
    assert_artifacts(tmp_path / "parallel")
    a0, a1 = "top.agents.agent-0", "top.agents.agent-1"
    L, D, tau = _model(cfg)
    # the ceiling-override keeps the storage hop contention-free
    assert D == pytest.approx(0.7201, abs=1e-12)
    assert tau == pytest.approx(1.7201, abs=1e-12)

    series0 = _grid_series(cfg, a0)
    series1 = _grid_series(cfg, a1)
    for series, agent in ((series0, a0), (series1, a1)):
        counts = _counts_from_trace(trace, agent)
        exp = {
            "inference_submitted": len(series),
            "inference_received": sum(1 for r in series if r["inf_recv"] is not None),
            "db_submitted": sum(1 for r in series if r["db_sub"] is not None),
            "db_received": sum(1 for r in series if r["db_recv"] is not None),
        }
        assert counts == exp, (agent, counts, exp)

    all_series = series0 + series1
    for series, agent in ((series0, a0), (series1, a1)):
        for row in series:
            what = f"parallel {row['inf_rid']}"
            _submit_submitted(trace, row["inf_rid"], row["inf_sub"], agent, "inference", what)
            _received_or_absent(trace, row["inf_rid"], row["inf_recv"], what)
            got = _hop(trace, row["inf_rid"], LLM_COMP, ("submitted", "queued", "processing", "completed"))
            assert got is not None, f"{what}: no complete LLM machine"
            lat, q, svc = got
            # NO queueing anywhere: LLM queue_time == 0 for every request
            assert q <= TOL, f"{what}: unexpected llm queue {q}"
            assert abs(lat - L) <= TOL, f"{what}: llm latency {lat} != {L}"
            if row["db_recv"] is not None:
                dwhat = f"parallel {row['db_rid']}"
                _submit_submitted(trace, row["db_rid"], row["db_sub"], agent, "db", dwhat)
                _received_or_absent(trace, row["db_rid"], row["db_recv"], dwhat)
                for comp, stages, latency, queue in _chain(cfg):
                    _assert_hop(trace, row["db_rid"], comp, stages, latency, queue, what=dwhat)

    # the two agents are in perfect lockstep: for every request index the
    # two lifecycles overlap in time (they share the exact same span)
    for r0, r1 in zip(series0, series1):
        assert r0["inf_sub"] == pytest.approx(r1["inf_sub"], abs=1e-9)
        if r0["inf_recv"] is not None and r1["inf_recv"] is not None:
            s0, e0 = r0["inf_sub"], r0["inf_recv"]
            s1, e1 = r1["inf_sub"], r1["inf_recv"]
            overlap = min(e0, e1) - max(s0, s1)
            assert overlap > TOL, f"inference requests do not overlap (k={r0['k']})"

    # e2e == what a lone request takes (no queue, no contention stretch)
    db_path = tmp_path / "parallel" / "metrics.db"
    summary = _summary(db_path)
    check_percentiles(db_path, {("e2e", "latency"): _e2e_values(all_series)}, what="parallel e2e")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = {
            (c, s): qd for c, s, qd, qs, sat, sc in conn.execute(
                "SELECT component, stage, queue_delay, queue_share, sat_score, score FROM bottleneck"
            )
        }
    finally:
        conn.close()
    assert rows[(LLM_COMP, "*")] <= TOL
    assert rows[(COMPUTE_COMP, "compute_issue")] <= TOL
    # winner: the storage row (supporting saturation saturates); its queue
    # is exactly the backend base-latency charge × completed storage ops
    winner = summary["bottleneck_winner"]
    n_storage = sum(1 for r in all_series if r["db_recv"] is not None)
    expected_qd = n_storage * 0.0001
    e2e_total = sum(_e2e_values(all_series))
    assert rows[(COMPUTE_STORAGE, "*")] == pytest.approx(expected_qd, abs=1e-9)
    assert winner["component"] == COMPUTE_STORAGE
    assert winner["stage"] == "*"
    assert summary["bottleneck"][0]["queue_delay"] == pytest.approx(expected_qd, abs=1e-9)
    assert summary["bottleneck"][0]["queue_share"] == pytest.approx(
        expected_qd / e2e_total, abs=1e-9
    )


# --------------------------------------------------------------------------- #
# 4) sanity_disaggregated                                                     #
# --------------------------------------------------------------------------- #


def test_sanity_disaggregated(tmp_path: Path) -> None:
    cfg, trace = run_scenario("sanity_disaggregated.yaml", tmp_path / "disagg")
    assert_artifacts(tmp_path / "disagg")
    agent = "top.agents.agent-0"
    L, D, tau = _model(cfg)
    times = _net(None, cfg)
    # paper math (YAML header)
    assert times["net_req"] == pytest.approx(0.0205, abs=1e-12)
    assert times["net_resp"] == pytest.approx(0.024, abs=1e-12)
    assert D == pytest.approx(0.6446, abs=1e-12)
    assert tau == pytest.approx(1.6446, abs=1e-12)

    series = _grid_series(cfg, agent)
    counts = _counts_from_trace(trace, agent)
    db_recv = sum(1 for r in series if r["db_recv"] is not None)
    assert counts["inference_submitted"] == 32
    assert counts["inference_received"] == 31
    assert counts["db_submitted"] == 31
    assert counts["db_received"] == 31

    chain = _chain(cfg)
    for row in series:
        what = f"disagg {row['inf_rid']}"
        _submit_submitted(trace, row["inf_rid"], row["inf_sub"], agent, "inference", what)
        _received_or_absent(trace, row["inf_rid"], row["inf_recv"], what)
        if row["inf_recv"] is not None:
            got = _hop(trace, row["inf_rid"], LLM_COMP, ("submitted", "queued", "processing", "completed"))
            assert got is not None, f"{what}: no complete LLM machine"
            lat, q, svc = got
            assert q <= TOL and abs(lat - L) <= TOL
        if row["db_recv"] is not None:
            dwhat = f"disagg {row['db_rid']}"
            _submit_submitted(trace, row["db_rid"], row["db_sub"], agent, "db", dwhat)
            _received_or_absent(trace, row["db_rid"], row["db_recv"], dwhat)
            for comp, stages, latency, queue in chain:
                _assert_hop(trace, row["db_rid"], comp, stages, latency, queue, what=dwhat)

    # the hop chain must contain exactly the net legs with the transfer
    # times volume/network_bandwidth (StageTiming meta carries volume)
    assert [h[:2] for h in chain] == [
        (COMPUTE_COMP, ("compute_issue",)),
        (SEGMENT, ("net_req",)),
        (STORAGE_NODE, ("storage_queue", "storage_service")),
        (SEGMENT, ("net_resp",)),
        (COMPUTE_COMP, ("compute_finalize",)),
    ]

    db_path = tmp_path / "disagg" / "metrics.db"
    check_percentiles(db_path, {("e2e", "latency"): _e2e_values(series)}, what="disagg e2e")
    # every completion e2e db == exact chain sum
    e2e_db = [r["db_recv"] - r["db_sub"] for r in series if r["db_recv"] is not None]
    assert all(abs(v - D) <= TOL for v in e2e_db)

    # attribution: the segment's staged "level" signal is its saturation
    # proxy; net latency is booked as stage queue_delay by the derivation
    e2e_total = sum(_e2e_values(series))
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = {
            (c, s): (qd, qs) for c, s, qd, qs, sat, sc in conn.execute(
                "SELECT component, stage, queue_delay, queue_share, sat_score, score FROM bottleneck"
            )
        }
    finally:
        conn.close()
    net_req_rows = [(c, s) for c, s in rows if s == "net_req"]
    assert net_req_rows == [(SEGMENT, "net_req")]
    seg_queue = sum(times["latency"] for _ in range(db_recv))
    assert rows[(SEGMENT, "net_req")][0] == pytest.approx(seg_queue, abs=1e-6)
    assert rows[(SEGMENT, "net_req")][1] == pytest.approx(seg_queue / e2e_total, abs=1e-6)
    assert rows[(LLM_COMP, "*")][0] <= TOL


def test_disagg_network_stages_absent_centralized(tmp_path: Path) -> None:
    """Centralized twin of the disagg config: no net_req/net_resp rows at all."""
    cfg, trace = run_scenario("sanity_single.yaml", tmp_path / "centralized-twin")
    for rid, stages in trace.items():
        for comp, stage in stages:
            assert stage not in ("net_req", "net_resp"), (rid, comp, stage)


# --------------------------------------------------------------------------- #
# 5) CLI: each scenario is runnable with sane output                          #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "name",
    ["sanity_single.yaml", "sanity_queue.yaml", "sanity_parallel.yaml", "sanity_disaggregated.yaml"],
)
def test_cli_runs_sanity_scenarios(tmp_path: Path, name: str) -> None:
    out = tmp_path / "runs"
    result = runner.invoke(app, ["run", str(SCENARIO_DIR / name), "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert "run dir:" in result.output
    assert "e2e p50/p95/p99" in result.output
    assert "bottleneck:" in result.output
    run_dirs = [d for d in out.iterdir() if d.is_dir()]
    assert len(run_dirs) == 1
    for artifact in _ARTIFACTS:
        assert (run_dirs[0] / artifact).is_file(), artifact
