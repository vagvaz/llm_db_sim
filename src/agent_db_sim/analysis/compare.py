"""Simulated-vs-analytical cross-check for one run directory.

Reads (read-only) the run's ``metrics.db`` + ``config.yaml`` snapshot and
joins them against the config-only analytical model of
:mod:`agent_db_sim.analysis.model`:

- simulated side: per-kind e2e hop means + throughput (from the trace with
  the unbiased ``(n-1)/(t_last - t_first)`` completion-rate estimator),
  per-request bundle means (the db residence tiles into issue / middle /
  finalize spans exactly), slot utilization (from ``utilization_stats``
  and config slot counts), and the attribution winner (``bottleneck``
  table);
- analytical side: the exact closed-chain solve (exponential service,
  means only) over the decomposed station model;
- a joined table with a relative-error column and a qualitative verdict
  (bottleneck match + mean band), plus regime/assumption notes.

Nothing here writes to the run directory or the metrics package — the
``metrics``/``pipeline`` layers are consumed read-only.
"""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from omegaconf import OmegaConf

from agent_db_sim.analysis.model import StationModel, build_model

__all__ = ["ComparisonRow", "StationRow", "AnalysisReport", "analyze_run",
           "format_text"]


# --------------------------------------------------------------------------- #
# data structures                                                             #
# --------------------------------------------------------------------------- #

@dataclass
class ComparisonRow:
    """One joined simulated-vs-analytical metric."""

    metric: str
    sim: Optional[float]
    model: float
    sim_unit: str = "tu"     # tu = sim time units; req/s; fraction

    @property
    def rel_err(self) -> Optional[float]:
        if self.sim is None or not self.model:
            return None
        return (self.sim - self.model) / self.model

    def as_cells(self) -> List[str]:
        rel = self.rel_err
        return [self.metric, _num(self.sim), _num(self.model),
                ("—" if rel is None else f"{rel * 100:+.1f}%")]


@dataclass
class StationRow:
    """One analytical station with its simulated utilization counterpart."""

    name: str
    tau: float
    c: int
    scv: float
    L: float
    Lq: float
    R: float
    rho: float
    sim_util: Optional[float] = None
    sim_queue_mean: Optional[float] = None


@dataclass
class AnalysisReport:
    """Pure-data result of one run's analytical cross-check."""

    run_dir: Path
    n_agents: int
    topology: str
    regime: str                      # 'floor' | 'token-cap'
    throughput_sim: Optional[float]
    throughput_model: float
    rows: List[ComparisonRow] = field(default_factory=list)
    stations: List[StationRow] = field(default_factory=list)
    verdict_bottleneck: str = "—"
    verdict_band: str = "—"
    verdict: str = "—"
    notes: List[str] = field(default_factory=list)
    stages_table: List[List[str]] = field(default_factory=list)
    sim_bottleneck: Optional[Tuple[str, str, float]] = None
    bottleneck_matched: bool = False


# --------------------------------------------------------------------------- #
# small helpers                                                               #
# --------------------------------------------------------------------------- #

def _num(v: Optional[float]) -> str:
    if v is None:
        return "—"
    if isinstance(v, float) and (math.isinf(v) or math.isnan(v)):
        return "—"
    return f"{v:.4g}"


def _load_config(run_dir: Path) -> Any:
    path = run_dir / "config.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"{path}: no run config snapshot")
    return OmegaConf.load(path)


def _connect(run_dir: Path) -> sqlite3.Connection:
    path = run_dir / "metrics.db"
    if not path.is_file():
        raise FileNotFoundError(f"{path}: no run metrics.db")
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


@dataclass
class SimStats:
    """Simulated means extracted from one run's metrics.db."""

    #: per-kind mean e2e hop latency (agent submitted -> agent received)
    hop_mean: Dict[str, float] = field(default_factory=dict)
    #: per-kind unbiased throughput (cycles per tu)
    x: Dict[str, Optional[float]] = field(default_factory=dict)
    #: llm mean queue wait (llm submitted -> slot acquired)
    llm_wait: Optional[float] = None
    #: mean db ingress wait (agent db submitted -> first compute processing)
    db_wait: Optional[float] = None
    #: per-request db bundles: issue / middle / finalize span means
    db_issue: Optional[float] = None
    db_middle: Optional[float] = None
    db_finalize: Optional[float] = None
    #: time-weighted slot utilization means from utilization_stats
    llm_busy_mean: Optional[float] = None
    db_busy_sum: float = 0.0
    db_busy_comps: int = 0
    winner: Optional[Tuple[str, str, float]] = None
    n_hops: Dict[str, int] = field(default_factory=dict)


_DB_BUNDLES = {
    "compute_issue": "issue",
    "compute_finalize": "finalize",
    "net_req": "middle",
    "net_resp": "middle",
    # net_conn overlaps the net legs when connection pools are configured;
    # excluded so the bundle spans stay additive to the db residence
}


def _collect_sim(run_dir: Path) -> SimStats:
    """One pass over the run's trace + utilization tables -> SimStats."""
    conn = _connect(run_dir)
    try:
        meta = {
            str(k): float(v)
            for k, v in conn.execute("SELECT key, value FROM meta")
        }
        warmup_end = float(meta.get("warmup_end", 0.0) or 0.0)

        events: Dict[Tuple[str, str], List[Tuple[str, str, str, float]]] = defaultdict(list)
        for agent, rid, comp, stage, state, t in conn.execute(
            "SELECT agent_id, request_id, component, stage, state, t FROM trace"
        ):
            events[(str(agent), str(rid))].append(
                (str(comp), str(stage), str(state), float(t))
            )

        stats = SimStats()
        hop_sum: Dict[str, List[float]] = defaultdict(list)
        received: Dict[str, List[float]] = defaultdict(list)
        llm_lat: List[float] = []
        llm_wait: List[float] = []
        bundle_sum: Dict[str, float] = defaultdict(float)
        bundle_n: Dict[str, int] = defaultdict(int)
        db_wait_sum = 0.0
        db_wait_n = 0

        for evs in events.values():
            sub = next((e for e in evs
                        if ".agent-" in e[0] and e[2] == "submitted"), None)
            rec = next((e for e in evs
                        if ".agent-" in e[0] and e[2] == "received"), None)
            if sub is None or rec is None:
                continue
            kind = sub[1]  # 'inference' | 'db'
            # throughput: the completion-time stream (received >= warmup),
            # independent of the request's submission time (the warmup gate
            # on submissions would truncate the stream left of the boundary)
            if rec[3] >= warmup_end:
                received[kind].append(rec[3])
            # hop mean: request must have been submitted post-warmup
            if sub[3] < warmup_end:
                continue
            hop_sum[kind].append(rec[3] - sub[3])
            if kind == "inference":
                llm_evs = [e for e in evs if e[0] == "llm" or e[0].startswith("llm.")]
                if llm_evs:
                    t0 = min(e[3] for e in llm_evs if e[2] == "submitted")
                    tp = min((e[3] for e in llm_evs if e[2] == "processing"),
                             default=t0)
                    tc = max((e[3] for e in llm_evs if e[2] == "completed"),
                             default=t0)
                    llm_lat.append(tc - t0)
                    llm_wait.append(tp - t0)
                continue
            # db request: bundle spans tile the db residence exactly
            buckets: Dict[str, List[float]] = defaultdict(list)
            proc_first: Optional[float] = None
            for comp, stage, state, t in evs:
                if not comp.startswith("top.db"):
                    continue
                bucket = _DB_BUNDLES.get(stage)
                if bucket is not None:
                    buckets[bucket].append(t)
                if state == "processing" and proc_first is None \
                        and stage.startswith("compute"):
                    proc_first = t
            if proc_first is not None:
                db_wait_sum += proc_first - sub[3]
                db_wait_n += 1
            for bucket, ts in buckets.items():
                if not ts:
                    continue
                span = max(ts) - min(ts)
                if bucket == "issue" and proc_first is not None:
                    # the issue bundle span starts at the agent's submit; the
                    # ingress/slot wait is its own metric row, so compare
                    # issue SERVICE only (completed - slot acquired)
                    completed = max(t for t in ts)
                    span = max(0.0, completed - proc_first)
                bundle_sum[bucket] += span
                bundle_n[bucket] += 1

        for kind, values in hop_sum.items():
            if values:
                stats.hop_mean[kind] = sum(values) / len(values)
                stats.n_hops[kind] = len(values)
        for kind, times in received.items():
            if len(times) >= 2 and max(times) > min(times):
                stats.x[kind] = (len(times) - 1) / (max(times) - min(times))
        stats.n_hops = dict(stats.n_hops)
        if llm_lat:
            stats.hop_mean.setdefault("llm_machine",
                                      sum(llm_lat) / len(llm_lat))
        if llm_wait:
            stats.llm_wait = sum(llm_wait) / len(llm_wait)
        if db_wait_n:
            stats.db_wait = db_wait_sum / db_wait_n
        if bundle_n.get("issue"):
            stats.db_issue = bundle_sum["issue"] / bundle_n["issue"]
        if bundle_n.get("middle"):
            stats.db_middle = bundle_sum["middle"] / bundle_n["middle"]
        if bundle_n.get("finalize"):
            stats.db_finalize = bundle_sum["finalize"] / bundle_n["finalize"]

        for comp, mean in conn.execute(
            "SELECT component, mean FROM utilization_stats "
            "WHERE signal = 'slot_busy' AND mean IS NOT NULL"
        ):
            comp = str(comp)
            if comp.startswith("llm"):
                stats.llm_busy_mean = (
                    mean if stats.llm_busy_mean is None
                    else max(stats.llm_busy_mean, mean)
                )
            elif _is_db_compute_comp(comp):
                stats.db_busy_sum += float(mean)
                stats.db_busy_comps += 1

        row = conn.execute(
            "SELECT component, stage, score FROM bottleneck "
            "ORDER BY score DESC LIMIT 1"
        ).fetchone()
        if row is not None:
            stats.winner = (str(row[0]), str(row[1]), float(row[2]))
        return stats
    finally:
        conn.close()


def _is_db_compute_comp(component: str) -> bool:
    """Is this utilization probe component a db entry-pool node?"""
    if component.startswith("top.db") or component in ("llm", "llm"):
        pass
    parts = component.split(".")
    leaf = parts[-1] if parts else component
    if "storage" in leaf or "segment" in leaf or leaf.startswith("link"):
        return False
    for part in parts:
        if part.startswith("compute-") or part.startswith("node-"):
            return True
    return False


def _slots_from_cfg(cfg: Any) -> int:
    """db.compute.slots from the run config snapshot."""
    value = OmegaConf.select(cfg, "db.compute.slots")
    return max(1, int(float(1 if value is None else value)))


# --------------------------------------------------------------------------- #
# the analysis                                                                #
# --------------------------------------------------------------------------- #

def analyze_run(run_dir: Any) -> AnalysisReport:
    """Cross-check one run directory: metrics.db + config vs the chain model."""
    run_dir = Path(run_dir)
    cfg = _load_config(run_dir)
    model: StationModel = build_model(cfg)
    sim = _collect_sim(run_dir)

    llm_slots = model.stations[0].c
    llm_util = (
        sim.llm_busy_mean / llm_slots
        if (sim.llm_busy_mean is not None and llm_slots > 0) else None
    )
    db_util: Optional[float] = None
    if sim.db_busy_sum > 0 and sim.db_busy_comps:
        per_node = _slots_from_cfg(cfg)
        db_util = sim.db_busy_sum / (sim.db_busy_comps * per_node)

    x_inf = sim.x.get("inference")
    cycle_sim = model.n_agents / x_inf if (x_inf and x_inf > 0) else None

    rows: List[ComparisonRow] = [
        ComparisonRow("throughput (cycles/s)", x_inf, model.x, "req/s"),
        ComparisonRow("cycle (N/X)", cycle_sim, model.e2e_mean, "tu"),
        ComparisonRow("inference hop (R_llm)", sim.hop_mean.get("inference"),
                      model.llm.tau + model.llm.Wq, "tu"),
        ComparisonRow("db hop (R_db)", sim.hop_mean.get("db"),
                      model.db.tau + model.db.Wq, "tu"),
    ]
    if sim.llm_wait is not None:
        rows.append(ComparisonRow("llm queue wait", sim.llm_wait, model.llm.Wq, "tu"))
    if sim.db_wait is not None:
        rows.append(ComparisonRow("db wait (ingress)", sim.db_wait, model.db.Wq, "tu"))
    rows.append(ComparisonRow("util llm slots", llm_util, model.llm.rho, "fraction"))
    rows.append(ComparisonRow("util db slots", db_util, model.db.rho, "fraction"))
    if sim.db_issue is not None:
        rows.append(ComparisonRow("db issue (service)", sim.db_issue,
                                  model.tau_issue, "tu"))
    if sim.db_middle is not None:
        rows.append(ComparisonRow("db middle (net+storage)", sim.db_middle,
                                  model.tau_middle, "tu"))
    if sim.db_finalize is not None:
        rows.append(ComparisonRow("db finalize (service)", sim.db_finalize,
                                  model.tau_finalize, "tu"))

    sim_llm_wait = sim.llm_wait
    sim_db_wait = sim.db_wait
    stations: List[StationRow] = []
    for res in model.unique_stations():
        stations.append(StationRow(
            name=res.name, tau=res.tau, c=res.c, scv=res.scv,
            L=res.L, Lq=res.Lq, R=res.R, rho=res.rho,
            sim_util=llm_util if res.name == "llm" else (
                db_util if res.name == "db" else None
            ),
            sim_queue_mean=sim_llm_wait if res.name == "llm" else sim_db_wait,
        ))

    stages_table = [["db decomposition", "kind", "mean (tu)"]]
    for st in model.stages:
        stages_table.append([st.name, st.kind, _num(st.tau)])

    # --- verdict ----------------------------------------------------------- #
    sim_family = _component_family(sim.winner[0]) if sim.winner else None
    model_family = model.bottleneck.name if model.bottleneck else None
    matched = sim_family is not None and sim_family == model_family
    verdict_basis: Optional[str] = None
    queue_free = model.total_queue_length() < 1e-9
    if queue_free:
        # no station queues: queue-share attribution is idle on the sim side
        # (it degenerates to saturation proxies over fixed base charges); the
        # qualitative call falls back to the utilization argmax on both sides
        sim_utils = [("llm", llm_util or 0.0), ("db", db_util or 0.0)]
        sim_family = max(sim_utils, key=lambda kv: kv[1])[0] if sim_utils else None
        model_rho = model.bottleneck.rho if model.bottleneck else 0.0
        matched = sim_family is not None and sim_family == model_family
        verdict_basis = "no-queue regime (utilization argmax both sides)"
        del model_rho
    elif model.cap_bound:
        # token-cap regime: the analytical bottleneck is the aggregate token
        # pipe inside the LLM endpoint; the sim's queue-share attribution is
        # blind there (saturation lives on the token rate). Check the sim's
        # peak token rate against the configured cap instead.
        conn2 = _connect(run_dir)
        try:
            rates = [
                float(mean)
                for (mean,) in conn2.execute(
                    "SELECT mean FROM utilization_stats "
                    "WHERE signal LIKE '%token%.rate' AND mean IS NOT NULL"
                )
            ]
        finally:
            conn2.close()
        cap = model.token_cap_rate or 0.0
        rate = sum(rates)  # input + output counter streams
        token_saturated = cap > 0 and rate >= 0.85 * cap
        matched = token_saturated
        model_family = "llm token cap"
        sim_family = "llm token rate" if token_saturated else \
            f"llm token rate ({_num(rate)}/{_num(cap)} tok/s)"
        verdict_basis = "token-cap regime (token rate vs configured cap)"
    cycle_err = next(
        (abs(r.rel_err) for r in rows
         if r.metric == "cycle (N/X)" and r.rel_err is not None),
        None,
    )
    if not cycle_err:
        band = "unknown (no simulated throughput)"
    elif cycle_err <= 0.05:
        band = "tight (cycle within 5%)"
    elif cycle_err <= 0.15:
        band = "ok (cycle within 15%)"
    else:
        band = f"off (cycle {cycle_err:.0%} off)"
    verdict_line = (
        "verdict: bottleneck "
        + ("MATCH " if matched else "MISMATCH ")
        + (f"(sim={sim_family} / model={model_family})"
           if (sim_family or model_family) else "")
        + f" | means {band}"
        + (f" | {verdict_basis}" if verdict_basis else "")
    )

    notes = list(model.notes) + [
        "simulated percentiles have no analytical counterpart "
        "(the closed-chain model is means-only).",
        "sim means exclude warmup; analytical side is config-only "
        "(configured distribution means, never trace data).",
    ]
    if x_inf is None:
        notes.append("no post-warmup inference completions: the analytical "
                     "throughput has no simulated counterpart.")
    if not matched and model.total_queue_length() < 1e-9:  # type: ignore[attr-defined]
        notes.append(
            "queue-free regime: attribution degenerates to saturation "
            "proxies on both sides — MATCH is not expected when no "
            "station queues."
        )

    return AnalysisReport(
        run_dir=run_dir,
        n_agents=model.n_agents,
        topology=_topology(cfg),
        regime="token-cap" if model.cap_bound else "floor",
        throughput_sim=x_inf,
        throughput_model=model.x,
        rows=rows,
        stations=stations,
        verdict_bottleneck=(
            f"sim={sim_family or '—'} vs model={model_family or '—'}"
        ),
        verdict_band=band,
        verdict=verdict_line,
        notes=notes,
        stages_table=stages_table,
        sim_bottleneck=sim.winner,
        bottleneck_matched=matched,
    )


def _component_family(component: str) -> str:
    """glossary-family mapping for the verdict: 'llm' | 'db'."""
    return "db" if (component.startswith("top.db")
                    or component.split(".")[-1].startswith(("compute-", "node-"))
                    ) else "llm"


def _topology(cfg: Any) -> str:
    value = OmegaConf.select(cfg, "db.topology")
    return str(value) if value is not None else "—"


def format_text(report: AnalysisReport) -> str:
    """Plain-text rendering for the CLI ``analyze`` output."""
    out: List[str] = []
    out.append(
        f"analytical cross-check: {report.run_dir} "
        f"(N={report.n_agents} agents, topology={report.topology}, "
        f"regime={report.regime})"
    )
    out.append("")
    out.append("side-by-side (rel err = (sim - model) / model):")
    head = ["metric", "simulated", "analytical", "rel err"]
    widths = [max(len(h), *(len(r.as_cells()[i]) for r in report.rows))
              for i, h in enumerate(head)]
    out.append("  " + "  ".join(h.ljust(w) for h, w in zip(head, widths)))
    out.append("  " + "  ".join("-" * w for w in widths))
    for row in report.rows:
        out.append("  " + "  ".join(
            c.ljust(w) for c, w in zip(row.as_cells(), widths)))
    out.append("")
    out.append("analytical stations (exact closed chain, means only):")
    shead = ["station", "tau", "c", "scv", "L", "Lq", "R", "rho",
             "sim util", "sim wait"]
    srows = [
        [s.name, _num(s.tau), str(s.c), _num(s.scv), _num(s.L),
         _num(s.Lq), _num(s.R), _num(s.rho), _num(s.sim_util),
         _num(s.sim_queue_mean)]
        for s in report.stations
    ]
    widths = [max(len(h), *(len(r[i]) for r in srows)) for i, h in enumerate(shead)]
    out.append("  " + "  ".join(h.ljust(w) for h, w in zip(shead, widths)))
    out.append("  " + "  ".join("-" * w for w in widths))
    for r in srows:
        out.append("  " + "  ".join(c.ljust(w) for c, w in zip(r, widths)))
    out.append("")
    out.append("db decomposition (analytical, config-only means):")
    for row in report.stages_table[1:]:
        out.append("  " + f"{row[0]:<28} {row[1]:<8} {row[2]:>10}")
    out.append("")
    if report.sim_bottleneck is not None:
        comp, stage, score = report.sim_bottleneck
        out.append(
            f"simulated attribution winner: {comp}.{stage} "
            f"(score={score:.3f}) — {report.verdict_bottleneck}"
        )
    out.append(report.verdict)
    out.append("")
    for note in report.notes:
        out.append(f"note: {note}")
    return "\n".join(out)
