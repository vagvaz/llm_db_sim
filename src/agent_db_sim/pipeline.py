"""Run pipeline (ticket 13): one scenario -> one complete run directory.

``execute_run`` drives the pipeline every ``agent-db-sim run``/``sweep`` run
goes through:

1. config snapshot -> ``<run-dir>/config.yaml``
2. build the model (registry ``build_model``: AgentClient + LLMEndpoint +
   DBEndpoint wired per the ticket-03 connect graph) and simulate until
   ``sim.until``; the shared sink is a **bounded** :class:`InMemorySink`
   (active-request index only) subscribed to a streaming ``RunCollector``
3. metrics: samples stream live into ``<run-dir>/metrics.db`` during the
   run (trace + samples tables, batched); ``collector.finalize()`` then
   computes the derived tables (percentiles / utilization / bottleneck
   attribution) from SQLite — finished-request history lives only on disk
4. viz: the ``samples`` table -> ``rebuild_samples`` -> ``write_vcd`` ->
   ``<run-dir>/run.vcd``
5. reports: the standard report rendered from ``metrics.db`` ->
   ``<run-dir>/report.html`` (standalone, vendored plotly)
6. a compact ``summary.json`` with the destination's analytical numbers:
   e2e latency percentiles, per-hop percentiles, utilization overview and
   the ranked bottleneck attribution.

``enumerate_sweep`` runs the same pipeline for every factor combination of
the scenario's ``sweep:`` section and writes a ``sweep_summary.json`` plus a
``sweep_summary.html`` comparing the runs (which component bottlenecks first
per topology, and what scale-out changes).
"""

from __future__ import annotations

import itertools
import json
import multiprocessing
import shutil
import sqlite3
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from omegaconf import OmegaConf

from agent_db_sim.metrics.collect import (
    rebuild_samples,
    start_collection,
)
from agent_db_sim.simkit.errors import SweepError
from agent_db_sim.simkit.probes import InMemorySink
from agent_db_sim.simkit.registry import build_model
from agent_db_sim.simkit.runner import run_model
from agent_db_sim.simkit.sweep import (
    RunResult,
    _sanitize,
    apply_factors,
    read_sweep,
    run_id_for,
    validate_jobs,
)
from agent_db_sim.viz.report import render_report, static_dir
from agent_db_sim.viz.vcd import write_vcd

__all__ = ["execute_run", "summarize_run", "enumerate_sweep"]


# --------------------------------------------------------------------------- #
# single run                                                                   #
# --------------------------------------------------------------------------- #

def sim_until(cfg: Any, default: float = 100.0) -> float:
    value = OmegaConf.select(cfg, "sim.until")
    return float(default if value is None else value)


def execute_run(cfg: Any, run_dir: Any) -> Dict[str, Any]:
    """Drive one full simulation run and materialize the run directory."""
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    OmegaConf.save(cfg, run_dir / "config.yaml")

    # Bounded sink: only the active-request index stays in memory; EVERY
    # sample (lifecycle edges + level/counter) streams into metrics.db live
    # through the subscribed batched TraceWriter (flushes at batch-size and
    # sample_interval tick boundaries; a failed flush fails the run loudly).
    # Emitters pass explicit simulated times, so the sink clock is unused.
    sink = InMemorySink(bounded=True)
    collector = start_collection(run_dir, sink, cfg)  # opens metrics.db first
    try:
        top = build_model(cfg, sink=sink)
        result = run_model(top, cfg, until=sim_until(cfg), sink=sink)
    except BaseException:
        collector.abort()  # loud failure: close without deducing a report
        raise

    db_path = collector.finalize()  # tail flush + indexes + derived-from-SQLite
    # Finished-request history now lives only in metrics.db; rebuild the
    # full sample sequence from the samples table for the one-shot VCD
    # writer (run.vcd is materialized, not retained in memory).
    vcd_path = write_vcd(run_dir / "run.vcd", rebuild_samples(db_path))
    report_path = _render_standard_report(run_dir)

    summary = summarize_run(run_dir, collected=result.collected)
    summary["artifacts"] = {
        "metrics.db": db_path.name,
        "run.vcd": vcd_path.name,
        "report.html": report_path.name,
        "end_time": result.end_time,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    return summary


def _render_standard_report(run_dir: Path) -> Path:
    html = render_report(run_dir, name="standard", standalone=True)
    out_path = run_dir / "report.html"
    out_path.write_text(html)
    plotly = run_dir / "plotly.min.js"
    if not plotly.exists():
        shutil.copyfile(static_dir / "plotly.min.js", plotly)
    return out_path


# --------------------------------------------------------------------------- #
# per-run summary numbers (read back from metrics.db)                          #
# --------------------------------------------------------------------------- #

def summarize_run(run_dir: Any, collected: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The destination's analytical numbers for one run, from its metrics.db.

    - e2e latency percentiles (agent-observed, warmup excluded)
    - per-hop latency percentiles for every recorded hop
    - utilization overview (top saturation rows)
    - ranked hybrid bottleneck attribution (top 5)
    """
    conn = sqlite3.connect(f"file:{Path(run_dir) / 'metrics.db'}?mode=ro", uri=True)
    try:
        percentiles: List[Dict[str, Any]] = []
        for src, kind, p50, p95, p99, n in conn.execute(
            "SELECT source, kind, p50, p95, p99, n FROM latency_percentiles "
            "ORDER BY kind, source"
        ):
            percentiles.append(
                {
                    "source": str(src),
                    "kind": str(kind),
                    "p50": p50,
                    "p95": p95,
                    "p99": p99,
                    "n": n,
                }
            )

        bottleneck: List[Dict[str, Any]] = []
        for comp, stage, q, share, sat, score in conn.execute(
            "SELECT component, stage, queue_delay, queue_share, sat_score, score "
            "FROM bottleneck ORDER BY score DESC LIMIT 5"
        ):
            bottleneck.append(
                {
                    "component": str(comp),
                    "stage": str(stage),
                    "queue_delay": q,
                    "queue_share": share,
                    "sat_score": sat,
                    "score": score,
                }
            )

        utilization = [
            {
                "signal": str(sig),
                "component": str(comp),
                "mean": mean,
                "peak": peak,
                "mean_util": mean_util,
            }
            for sig, comp, mean, peak, mean_util in conn.execute(
                "SELECT signal, component, mean, peak, mean_util "
                "FROM utilization_stats "
                "WHERE mean_util IS NOT NULL ORDER BY mean_util DESC LIMIT 8"
            )
        ]

        counts: Dict[str, Any] = {}
        if collected is not None:
            agents = collected.get("agents")
            if isinstance(agents, dict) and agents:
                counts["agents"] = len(agents)
                counts["inference_completed"] = sum(
                    int(v.get("inference_received", 0)) for v in agents.values()
                )
                counts["db_completed"] = sum(
                    int(v.get("db_received", 0)) for v in agents.values()
                )
        config = {}
        config_path = Path(run_dir) / "config.yaml"
        if config_path.exists():
            config = OmegaConf.to_container(OmegaConf.load(config_path), resolve=False)  # type: ignore[assignment]
    finally:
        conn.close()

    return _condense(
        {
            "percentiles": percentiles,
            "bottleneck": bottleneck,
            "utilization_top": utilization,
            "counts": counts,
            "config": _config_digest(config),
        }
    )


def _config_digest(config: Dict[str, Any]) -> Dict[str, Any]:
    db_cfg = config.get("db", {}) if isinstance(config, dict) else {}
    agents_cfg = config.get("agents", {}) if isinstance(config, dict) else {}
    llm_cfg = config.get("llm", {}) if isinstance(config, dict) else {}
    compute_nodes = db_cfg.get("compute_nodes")
    coordinators = db_cfg.get("coordinators")
    return {
        "db.topology": db_cfg.get("topology"),
        "db.compute_nodes": compute_nodes,
        "db.storage_nodes": db_cfg.get("storage_nodes"),
        "db.coordinators": coordinators,
        "agents.count": agents_cfg.get("count"),
        "llm.slots": llm_cfg.get("slots"),
    }


def _condense(summary: Dict[str, Any]) -> Dict[str, Any]:
    """Pull the headline numbers up to the top level of the summary."""
    e2e = next(
        (row for row in summary["percentiles"] if row["source"] == "e2e"),
        None,
    )
    bottleneck = summary["bottleneck"]
    digest = summary.pop("config", {})
    counts = summary.pop("counts", {})
    out: Dict[str, Any] = {
        **digest,
        **counts,
        "e2e_percentiles": (
            {k: e2e[k] for k in ("p50", "p95", "p99", "n")}
            if e2e is not None
            else None
        ),
        "bottleneck_winner": (
            {k: bottleneck[0][k] for k in ("component", "stage", "score", "queue_share")}
            if bottleneck
            else None
        ),
        **summary,
    }
    return out


# --------------------------------------------------------------------------- #
# factorial sweep                                                              #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class _RunSpec:
    """One enumerated run: what a worker needs to execute it in isolation."""

    index: int
    run_id: str
    directory: Path
    run_cfg: Any
    values: Dict[str, Any]


def _run_one(spec: _RunSpec) -> Dict[str, Any]:
    """Worker entry point: execute one run's full pipeline in this process.

    Every child process starts fresh (spawn context), so the model is built
    from scratch with the run's own config: the RNG streams derive from
    ``sim.seed`` + component paths (hash-seed independent) and never touch
    process-global state, therefore each run's summary is deterministic and
    independent of execution order / parallelism. Never raises: failures come
    back as ``ok=False`` records with the error message captured.
    """
    started = time.monotonic()
    record: Dict[str, Any] = {
        "index": spec.index,
        "run_id": spec.run_id,
        "values": spec.values,
        "directory": str(spec.directory),
    }
    try:
        summary = execute_run(spec.run_cfg, spec.directory)
        record["ok"] = True
        record["summary"] = summary
    except BaseException as exc:  # noqa: BLE001 - failure is data, not a crash
        record["ok"] = False
        record["error"] = str(exc)
        record["error_type"] = type(exc).__name__
        # the config snapshot is written before execution, so the sweep
        # comparison still knows which factors the failed run carried
        config_path = spec.directory / "config.yaml"
        if config_path.exists():
            record["config"] = _config_digest(
                OmegaConf.to_container(  # type: ignore[arg-type]
                    OmegaConf.load(config_path), resolve=False
                )
            )
    record["duration"] = time.monotonic() - started
    return record


def _progress(record: Dict[str, Any], done: int, total: int) -> None:
    """Per-run completion line, printed as child processes finish.

    Completion order is nondeterministic under ``--jobs N``; the final
    ``sweep_summary`` echo (and the file itself) stay in enumeration order.
    """
    tag = f"[{done}/{total}] {record['run_id']}"
    if record["ok"]:
        line = f"{tag} done in {record['duration']:.1f}s"
    else:
        line = f"{tag} FAILED in {record['duration']:.1f}s: {record['error']}"
    print(line, flush=True)


def _summary_from_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Aggregate one worker record into a sweep-summary run entry."""
    if record["ok"]:
        out = dict(record["summary"])
        out.update(
            {"run_id": record["run_id"], "values": dict(record["values"])}
        )
        out["failed"] = False
        return out
    # failed run: keep the run's identity + factor digest, and neutral
    # placeholders for the analytical tables so summary consumers can rely
    # on the same keys across entries
    digest = record.get("config") or {}
    return {
        **digest,
        "run_id": record["run_id"],
        "values": record["values"],
        "percentiles": [],
        "bottleneck": [],
        "utilization_top": [],
        "e2e_percentiles": None,
        "bottleneck_winner": None,
        "failed": True,
        "error": record["error"],
        "error_type": record["error_type"],
    }


def enumerate_sweep(
    cfg: Any,
    out_root: Any = "runs",
    jobs: int = 1,
    fail_fast: bool = False,
) -> Tuple[List[RunResult], Dict[str, Any]]:
    """Enumerate the scenario's sweep factors and run the full pipeline once
    per combination, sharing the run-dir layout of simkit's sweep runner
    (``<out_root>/<sweep-name>/<run-id>/``).

    ``jobs`` selects the bounded-core execution model: ``1`` (default) runs
    every combination sequentially in this process — byte-identical to the
    pre-parallel behavior — while ``N >= 2`` executes the runs in a
    ``ProcessPoolExecutor`` of
    ``max_workers = min(N, len(runs))`` spawn-context children. Each child
    returns one run record; the parent aggregates in enumeration order, so
    ``sweep_summary.json``/``.html`` are identical to the sequential output
    (golden-tested). A failed child marks its run failed in the summary
    (error message captured) and does not abort the sweep, unless
    ``fail_fast`` stops the remaining queued runs.
    """
    jobs = validate_jobs(jobs)
    name, factors = read_sweep(cfg)
    sweep_dir = Path(out_root) / _sanitize(name)
    sweep_dir.mkdir(parents=True, exist_ok=True)

    specs: List[_RunSpec] = []
    for index, combo in enumerate(itertools.product(*factors.values())):
        assignments = dict(zip(factors.keys(), combo))
        run_cfg = apply_factors(cfg, assignments)
        run_id = run_id_for(index, assignments)
        specs.append(
            _RunSpec(
                index=index,
                run_id=run_id,
                directory=sweep_dir / run_id,
                run_cfg=run_cfg,
                values=assignments,
            )
        )

    total = len(specs)
    records, aborted = _execute_runs(specs, jobs=jobs, fail_fast=fail_fast)

    results: List[RunResult] = []
    runs: List[Dict[str, Any]] = []
    # aggregate in enumeration order regardless of completion order
    for record in sorted(records, key=lambda rec: rec["index"]):
        results.append(
            RunResult(
                index=record["index"],
                run_id=record["run_id"],
                directory=Path(record["directory"]),
                values=record["values"],
            )
        )
        runs.append(_summary_from_record(record))

    comparison = _compare_runs(runs)
    sweep_summary: Dict[str, Any] = {
        "name": name,
        "out_root": str(Path(out_root)),
        "n_runs": len(results),
        "jobs": jobs,
        "factors": factors,
        "runs": runs,
        "comparison": comparison,
    }
    if aborted:
        n_done = len(records)
        sweep_summary["aborted"] = True
        sweep_summary["skipped"] = max(0, total - n_done)
    (sweep_dir / "sweep_summary.json").write_text(
        json.dumps(sweep_summary, indent=2, sort_keys=True, default=str) + "\n"
    )
    (sweep_dir / "sweep_summary.html").write_text(_render_sweep_html(sweep_summary))
    return results, sweep_summary


def _fresh_run_dir(directory: Path) -> Path:
    """Create a run's private directory (must not already exist)."""
    try:
        directory.mkdir(parents=True)
    except FileExistsError as exc:
        raise SweepError(
            f"run directory already exists: {directory} "
            f"(remove it or change out_root to re-run)"
        ) from exc
    return directory


def _execute_runs(
    specs: List[_RunSpec],
    jobs: int,
    fail_fast: bool,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Execute the enumerated run specs; returns ``(records, aborted)``.

    ``jobs == 1`` stays fully sequential and in-process. ``jobs > 1`` routes
    through a bounded ``ProcessPoolExecutor`` (spawn context,
    ``max_workers = min(jobs, len(specs))``): children are submitted lazily so
    each run dir is created just-in-time and ``fail_fast`` leaves the
    not-yet-submitted runs (and their directories) untouched. Every future is
    consumed in the executor's completion order, but ``records`` is returned
    unordered — the caller aggregates in enumeration order.
    """
    total = len(specs)
    records: List[Dict[str, Any]] = []
    done = 0

    def accept(record: Dict[str, Any]) -> None:
        nonlocal done
        done += 1
        _progress(record, done, total)
        records.append(record)

    if jobs == 1 or total <= 1:
        # sequential fast path: identical to the historical behavior, plus
        # per-run progress lines and fail-fast handling
        for spec in specs:
            _fresh_run_dir(spec.directory)
            record = _run_one(spec)
            accept(record)
            if not record["ok"] and fail_fast:
                return records, True
        return records, False

    # ---- parallel path (bounded-core workers) ------------------------------
    max_workers = min(jobs, total)
    submit_cursor = 0
    pending: Dict[Future, _RunSpec] = {}
    aborted = False

    executor = ProcessPoolExecutor(
        max_workers=max_workers,
        mp_context=multiprocessing.get_context("spawn"),
    )
    try:

        def submit_next() -> None:
            """Keep a free worker slot busy (dir created just-in-time)."""
            nonlocal submit_cursor
            if aborted or submit_cursor >= total or len(pending) >= max_workers:
                return
            spec = specs[submit_cursor]
            submit_cursor += 1
            _fresh_run_dir(spec.directory)
            pending[executor.submit(_run_one, spec)] = spec

        def refill() -> None:
            for _ in range(max_workers):
                submit_next()

        refill()  # seed the pool with the first min(jobs, total) runs
        while pending:
            finished, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in finished:
                spec = pending.pop(future)
                if future.cancelled():
                    # drained queued run (cancelled right after fail-fast):
                    # it never executed, so it is not reported
                    continue
                try:
                    record = future.result()
                except BaseException as exc:  # broken pool / pickling error
                    record = {
                        "index": spec.index,
                        "run_id": spec.run_id,
                        "values": spec.values,
                        "directory": str(spec.directory),
                        "ok": False,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "duration": 0.0,
                    }
                accept(record)
                if not record["ok"] and fail_fast:
                    aborted = True
                    for live in pending:
                        live.cancel()

            # each completed run frees a slot: refill up to the cap
            refill()
    finally:
        executor.shutdown(wait=True)

    return records, aborted


def _compare_runs(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Destination-question cross-run digest: e2e + bottleneck winner per run,
    and the grouped scale-out deltas."""
    by_run = [
        {
            "values": run["values"],
            "e2e_p95": (run.get("e2e_percentiles") or {}).get("p95"),
            "bottleneck": run.get("bottleneck_winner"),
        }
        for run in runs
    ]
    scale_groups: Dict[str, List[Dict[str, Any]]] = {}
    for run in runs:
        topology = run.get("db.topology")
        scale_groups.setdefault(str(topology), []).append(
            {
                "compute_nodes": run.get("db.compute_nodes"),
                "agents": run.get("agents.count"),
                "e2e_p95": (run.get("e2e_percentiles") or {}).get("p95"),
                "bottleneck": (run.get("bottleneck_winner") or {}).get("component"),
                "queue_share": (run.get("bottleneck_winner") or {}).get("queue_share"),
            }
        )
    return {"by_run": by_run, "scale_by_topology": scale_groups}


_SWEEP_TEMPLATE = """<!doctype html>
<html><head><meta charset="utf-8"><title>sweep: {{ name }}</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 2rem; }
 table { border-collapse: collapse; margin: 1rem 0; }
 th, td { border: 1px solid #bbb; padding: 4px 10px; text-align: left; }
 th { background: #eee; }
 code { background: #f4f4f4; padding: 0 4px; }
</style></head><body>
<h1>Sweep: {{ name }}</h1>
<p>{{ n_runs }} runs &mdash; factors: {{ factors }}</p>
<h2>Per-run comparison</h2>
<table>
<tr><th>run</th><th>factors</th><th>e2e p50</th><th>e2e p95</th><th>e2e p99</th>
    <th>bottleneck winner</th><th>score</th><th>queue share</th></tr>
{% for run in runs %}
<tr>
 <td><code>{{ run.run_id }}</code></td>
 <td><code>{{ run.values }}</code></td>
 <td>{{ '%.3f'|format(run.e2e_percentiles.p50) if run.e2e_percentiles else '-' }}</td>
 <td>{{ '%.3f'|format(run.e2e_percentiles.p95) if run.e2e_percentiles else '-' }}</td>
 <td>{{ '%.3f'|format(run.e2e_percentiles.p99) if run.e2e_percentiles else '-' }}</td>
 <td>{% if run.bottleneck_winner %}<code>{{ run.bottleneck_winner.component }}.{{ run.bottleneck_winner.stage }}</code>{% else %}-{% endif %}</td>
 <td>{{ '%.3f'|format(run.bottleneck_winner.score) if run.bottleneck_winner else '-' }}</td>
 <td>{{ '%.3f'|format(run.bottleneck_winner.queue_share) if run.bottleneck_winner else '-' }}</td>
</tr>
{% endfor %}
</table>
</body></html>
"""


def _render_sweep_html(summary: Dict[str, Any]) -> str:
    from jinja2 import Environment

    env = Environment(autoescape=True)
    template = env.from_string(_SWEEP_TEMPLATE)
    return template.render(**summary)
