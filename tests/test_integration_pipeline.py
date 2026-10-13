"""Integration tests (ticket 13): full-model wiring + the run pipeline.

Covers the build_model wiring (agent <-> llm <-> db over the connect graph),
one closed-loop model run, the complete per-run artifact set produced by
``execute_run`` and the sweep-level artifacts of ``enumerate_sweep``.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from omegaconf import OmegaConf
from typer.testing import CliRunner

from agent_db_sim.cli import app
from agent_db_sim.metrics.collect import collect_run
from agent_db_sim.pipeline import execute_run, enumerate_sweep, summarize_run
from agent_db_sim.simkit.errors import ComponentError, SweepError
from agent_db_sim.simkit.probes import InMemorySink
from agent_db_sim.simkit.registry import build_model
from agent_db_sim.simkit.runner import run_model
from agent_db_sim.simkit.sweep import validate_jobs
from agent_db_sim.viz.vcd import parse_vcd_bytes

runner = CliRunner()


def scenario_cfg(**over) -> Any:
    cfg: dict = {
        "sim": {"until": 20.0, "seed": 5},
        "agents": {
            "count": 2,
            "input_tokens": 64,
            "output_tokens": 32,
            "query_profiles": [
                {"name": "point", "complexity": 0.2, "volume": 0.05, "weight": 0.8},
                {"name": "scan", "complexity": 0.5, "volume": 1.5, "weight": 0.2},
            ],
        },
        "llm": {
            "slots": 4,
            "input_tokens_per_s": 2000,
            "output_tokens_per_s": 1000,
            "ingress": "fifo",
        },
        "db": {
            "topology": "disaggregated",
            "compute_nodes": 2,
            "storage_nodes": 2,
            "coordinators": 1,
            "compute": {"slots": 2, "ingress": "fifo", "issue_ratio": 0.2},
            "storage": {"backend": "nvme"},
            "network": {
                "latency": 0.01,
                "bandwidth": 500.0,
                "capacity": 8.0,
                "request_size": 0.01,
                "coordinator_overhead": 1.2,
            },
        },
    }
    cfg.update(over)
    return OmegaConf.create(cfg)


def test_build_model_wires_the_full_model() -> None:
    cfg = scenario_cfg()
    sink = InMemorySink()
    top = build_model(cfg, sink=sink)
    names = [c.name for c in top._children]
    assert "agents" in names and "llm" in names and "queries_bridge" in names
    links = [(lnk.src.name, lnk.src_port, lnk.dst.name, lnk.dst_port) for lnk in top.connect_graph.links]
    assert ("agents", "inference_requests", "llm", "requests") in links
    assert ("llm", "responses", "agents", "inference_responses") in links
    assert ("agents", "db_queries", "queries_bridge", "requests") in links
    assert ("queries_bridge", "queries", "db", "queries") in links
    assert ("db", "responses", "agents", "db_responses") in links
    assert top.probe_sink is sink


def test_full_model_runs_closed_loop() -> None:
    cfg = scenario_cfg()
    sink = InMemorySink()
    top = build_model(cfg, sink=sink)
    result = run_model(top, cfg, until=20.0, sink=sink)

    agents = result.collected["agents"]
    assert len(agents) == 2
    for counters in agents.values():
        # the closed loop ran: several complete round trips per agent; only
        # the final iteration may sit unfinished at the sim horizon
        assert counters["db_received"] >= 10
        assert counters["inference_received"] == counters["inference_submitted"] - (
            1 if counters["inference_submitted"] > counters["inference_received"] else 0
        )
        assert counters["db_received"] >= counters["db_submitted"] - 1
        assert counters["inference_received"] == counters["db_submitted"]
    # the llm actually served everything the agents submitted
    served = result.collected["llm"]["served"]
    assert served == sum(c["inference_submitted"] for c in agents.values())

    # lifecycle edges exist for all three component families on one sink
    edge_agents = [s for s in sink.samples if s.signal.endswith("agent-0.inference")]
    edge_llm = [s for s in sink.samples if s.signal == "llm.completed"]
    edge_db = [
        s for s in sink.samples if s.signal.endswith("compute_issue") and s.kind == "edge"
    ]
    assert edge_agents and edge_llm and edge_db
    assert edge_llm[0].tags["agent_id"].endswith("agent-0")


def test_agents_without_llm_section_fails_fast() -> None:
    cfg = scenario_cfg()
    del cfg["llm"]
    top = build_model(cfg)
    with pytest.raises(ComponentError, match="input_tokens_per_s"):
        run_model(top, cfg, until=1.0)


def test_execute_run_produces_complete_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run-a"
    execute_run(scenario_cfg(sim={"until": 30.0, "seed": 5}), run_dir)

    for name in ("config.yaml", "metrics.db", "run.vcd", "report.html", "summary.json"):
        assert (run_dir / name).is_file(), name
    assert (run_dir / "plotly.min.js").is_file()

    # -- metrics.db: derived tables populated -----------------------------
    conn = sqlite3.connect(f"file:{run_dir / 'metrics.db'}?mode=ro", uri=True)
    try:
        e2e = conn.execute(
            "SELECT p50, p95, p99, n FROM latency_percentiles WHERE source='e2e'"
        ).fetchone()
        assert e2e is not None and e2e[3] > 0 and 0.0 < e2e[0] < e2e[1] <= e2e[2]
        hops = conn.execute(
            "SELECT count(*), sum(n) FROM latency_percentiles WHERE kind='latency'"
        ).fetchone()
        assert hops[0] >= 4 and hops[1] > 0
        bottlenecks = conn.execute(
            "SELECT component, stage, score, queue_share FROM bottleneck "
            "ORDER BY score DESC"
        ).fetchall()
        assert bottlenecks and bottlenecks[0][2] > 0
        util = conn.execute("SELECT count(*) FROM utilization_stats").fetchone()[0]
        assert util > 0
        trace_n = conn.execute("SELECT count(*) FROM trace").fetchone()[0]
        assert trace_n > 100
        meta = dict(conn.execute("SELECT key, value FROM meta"))
        assert 0.0 < float(meta["warmup_end"]) < float(meta["t_end"])
    finally:
        conn.close()

    # -- run.vcd: parseable, non-trivial signal catalog -------------------
    data = parse_vcd_bytes((run_dir / "run.vcd").read_bytes(), include_tail=True)
    catalog = data.catalog()
    assert len(catalog) >= 15  # agents + llm + db stages + counters/levels
    assert data.t_max > 5000  # quantized ns quanta across the sim horizon

    # -- report.html: rendered standard report -----------------------------
    html = (run_dir / "report.html").read_text()
    assert "plotly.min.js" in html and "fig-" in html

    # -- summary.json: the destination's numbers ---------------------------
    loaded = json.loads((run_dir / "summary.json").read_text())
    assert loaded["agents"] == 2
    assert loaded["e2e_percentiles"]["n"] > 0
    assert loaded["bottleneck_winner"]["score"] >= 0
    assert loaded["db.topology"] == "disaggregated"
    assert loaded["db_completed"] > 0
    # summarize_run reproduces the headline numbers on demand
    again = summarize_run(run_dir)
    assert again["e2e_percentiles"]["n"] == loaded["e2e_percentiles"]["n"]
    assert again["bottleneck_winner"] == loaded["bottleneck_winner"]


def test_execute_run_scenario_file_coverage(tmp_path: Path) -> None:
    """The shipped acceptance scenario executes through the pipeline."""
    scenario = (
        Path(__file__).resolve().parents[1] / "scenarios" / "topology_compare.yaml"
    )
    cfg = OmegaConf.load(scenario)
    cfg.sim.until = 30.0  # keep it a fast smoke; the real acceptance is a sweep
    summary = execute_run(cfg, tmp_path / "acceptance-smoke")
    assert summary["db.topology"] == "centralized"
    assert summary["e2e_percentiles"]["n"] > 0


def test_execute_run_workflow_compare_scenario_coverage(tmp_path: Path) -> None:
    """The workflow-template demo scenario runs through the full pipeline
    (fast small-count smoke) and traces slice per workflow template."""
    scenario = (
        Path(__file__).resolve().parents[1] / "scenarios" / "workflow_compare.yaml"
    )
    cfg = OmegaConf.load(scenario)
    cfg.sim.until = 30.0  # fast smoke variant of the until=200.0 demo
    cfg.agents.count = 6
    summary = execute_run(cfg, tmp_path / "workflow-smoke")
    assert summary["agents"] == 6
    assert summary["e2e_percentiles"]["n"] > 0
    assert {k: v for k, v in summary.items() if k.startswith("agents")}

    run_dir = tmp_path / "workflow-smoke"
    conn = sqlite3.connect(f"file:{run_dir / 'metrics.db'}?mode=ro", uri=True)
    try:
        # agent-side rows carry both attrs; endpoint-side rows (llm/db) emit
        # only agent/request ids, so slice on the workflow attr being present
        rows = conn.execute(
            "SELECT json_extract(attrs, '$.workflow_template'), "
            "json_extract(attrs, '$.workflow_step') FROM trace "
            "WHERE json_extract(attrs, '$.workflow_template') IS NOT NULL "
            "GROUP BY 1, 2 ORDER BY 1, 2"
        ).fetchall()
        templates = {r[0] for r in rows}
        assert templates == {"simple_qa", "multi_hop", "rag_batch"}
        # every template's agent-side step indices run 0..len(steps)-1
        assert max(r[1] for r in rows if r[0] == "simple_qa") == 1
        assert max(r[1] for r in rows if r[0] == "multi_hop") == 3
        assert max(r[1] for r in rows if r[0] == "rag_batch") == 4
    finally:
        conn.close()


def test_enumerate_sweep_artifacts_and_summary(tmp_path: Path) -> None:
    cfg = scenario_cfg()
    cfg.sweep = OmegaConf.create(
        {"name": "int-sweep", "factors": {"db.topology": ["centralized", "disaggregated"]}}
    )
    results, summary = enumerate_sweep(cfg, out_root=tmp_path)

    assert len(results) == 2
    sweep_dir = tmp_path / "int-sweep"
    assert (sweep_dir / "sweep_summary.json").is_file()
    assert (sweep_dir / "sweep_summary.html").is_file()
    for run in results:
        for name in ("config.yaml", "metrics.db", "run.vcd", "report.html", "summary.json"):
            assert (run.directory / name).is_file(), (run.run_id, name)

    loaded = json.loads((sweep_dir / "sweep_summary.json").read_text())
    assert loaded["n_runs"] == 2 and len(loaded["runs"]) == 2
    assert {r["values"]["db.topology"] for r in loaded["runs"]} == {
        "centralized",
        "disaggregated",
    }
    for run in loaded["runs"]:
        assert run["e2e_percentiles"]["n"] > 0
        assert run["bottleneck_winner"] is not None
    # sweep summary html lists every run with an e2e + bottleneck line
    html = (sweep_dir / "sweep_summary.html").read_text()
    assert html.count("<tr>") >= len(results) + 1
    # collect_run on one of the sample sinks again is rerun-safe
    rerun = collect_run(results[0].directory, InMemorySink(), cfg)
    assert Path(rerun).exists()


def test_run_command_produces_a_run_dir(tmp_path: Path) -> None:
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(OmegaConf.to_yaml(scenario_cfg()))
    out = tmp_path / "out"

    result = runner.invoke(app, ["run", str(scenario), "--out", str(out)])

    assert result.exit_code == 0, result.output
    assert "run dir:" in result.output
    assert "bottleneck:" in result.output
    run_dirs = [d for d in out.iterdir() if d.is_dir()]
    assert len(run_dirs) == 1
    assert (run_dirs[0] / "metrics.db").is_file()
    assert (run_dirs[0] / "run.vcd").is_file()
    assert (run_dirs[0] / "report.html").is_file()

    # second run into the same out root gets a fresh dir (no clobber)
    result = runner.invoke(app, ["run", str(scenario), "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert len([d for d in out.iterdir() if d.is_dir()]) == 2


def test_sweep_command_produces_comparable_runs(tmp_path: Path) -> None:
    scenario = tmp_path / "scenario.yaml"
    text = OmegaConf.to_yaml(scenario_cfg())
    text += (
        "\nsweep:\n  name: cli-int-sweep\n  factors:\n"
        "    db.topology: [centralized, disaggregated]\n"
        "    agents.count: [1, 2]\n"
    )
    scenario.write_text(text)
    out = tmp_path / "out"

    result = runner.invoke(app, ["sweep", str(scenario), "--out", str(out)])

    assert result.exit_code == 0, result.output
    assert "e2e p95=" in result.output
    sweep_dir = out / "cli-int-sweep"
    run_dirs = sorted(d for d in sweep_dir.iterdir() if d.is_dir())
    assert len(run_dirs) == 4
    for d in run_dirs:
        assert (d / "metrics.db").is_file()
        assert (d / "run.vcd").is_file()
        assert (d / "report.html").is_file()
    assert (sweep_dir / "sweep_summary.json").is_file()
    assert (sweep_dir / "sweep_summary.html").is_file()


def test_run_smoke_scenario_keeps_placeholder_loop() -> None:
    scenarios = Path(__file__).resolve().parents[1] / "scenarios"
    result = runner.invoke(app, ["run", str(scenarios / "smoke.yaml")])
    assert result.exit_code == 0, result.output
    assert "smoke:" in result.output and "timeouts" in result.output


# --------------------------------------------------------------------------- #
# bounded-core parallel sweeps (stage 6 lane C)                                #
# --------------------------------------------------------------------------- #

def parallel_sweep_cfg(**over) -> Any:
    """Tiny 4-run factorial sweep (2 factors x 2 values x 2 agents)."""
    cfg = scenario_cfg(**over)
    cfg.sweep = OmegaConf.create(
        {
            "name": "par-sweep",
            "factors": {
                "db.topology": ["centralized", "disaggregated"],
                "agents.count": [1, 2],
            },
        }
    )
    return cfg


def test_sweep_parallel_summary_is_golden_identical_to_sequential(
    tmp_path: Path,
) -> None:
    """Golden test: 2-worker sweep summary == sequential sweep summary.

    Also checks enumeration-order run ids and that per-run e2e metrics are
    identical (per-run seeds derive from ``sim.seed`` deterministically, so
    parallelism never changes a run's results).
    """
    cfg = parallel_sweep_cfg()
    results_seq, summary_seq = enumerate_sweep(cfg, out_root=tmp_path / "seq")
    results_par, summary_par = enumerate_sweep(cfg, out_root=tmp_path / "par", jobs=2)

    assert len(results_seq) == len(results_par) == 4
    assert [r.run_id for r in results_par] == [r.run_id for r in results_seq]
    assert [r.values for r in results_par] == [r.values for r in results_seq]
    # run dirs mirror each other under <tmp>/seq/ and <tmp>/par/
    assert [r.directory.name for r in results_par] == [
        r.directory.name for r in results_seq
    ]

    # golden: sweep summaries identical (only out_root + jobs differ on purpose)
    par = dict(summary_par)
    seq = dict(summary_seq)
    del par["out_root"], seq["out_root"], par["jobs"], seq["jobs"]
    assert par == seq

    # seed independence: per-run e2e metrics match run-for-run
    for run_seq, run_par in zip(seq["runs"], par["runs"]):
        assert run_seq["run_id"] == run_par["run_id"]
        assert run_seq["e2e_percentiles"] == run_par["e2e_percentiles"]
        assert run_seq["bottleneck_winner"] == run_par["bottleneck_winner"]
        assert run_seq["agents"] == run_par["agents"]

    # the parallel artifacts recall the same per-run summaries from disk
    for run in par["runs"]:
        loaded = json.loads(
            ((tmp_path / "par" / "par-sweep") / run["run_id"] / "summary.json")
            .read_text()
        )
        assert loaded["e2e_percentiles"] == run["e2e_percentiles"]


def test_sweep_failed_run_is_captured_not_fatal(tmp_path: Path) -> None:
    """A deliberately broken run config fails that run only: its entry is
    marked failed (error captured), the other runs complete."""
    cfg = scenario_cfg()
    cfg.sweep = OmegaConf.create(
        {
            "name": "fail-sweep",
            "factors": {"sim.until": [-1.0, 20.0]},  # simpy rejects until <= 0
        }
    )
    results, summary = enumerate_sweep(cfg, out_root=tmp_path)

    assert len(results) == 2
    runs = summary["runs"]
    assert runs[0]["failed"] is True
    assert runs[0]["run_id"] == results[0].run_id
    assert runs[0]["error"], "failure message must be captured"
    assert runs[0]["e2e_percentiles"] is None
    assert runs[0]["bottleneck_winner"] is None
    # the second run succeeded and the sweep still produced full artifacts
    assert runs[1]["failed"] is False
    assert runs[1]["e2e_percentiles"]["n"] > 0
    assert runs[1]["bottleneck_winner"] is not None
    # failed run kept its config snapshot; sweep artifacts complete
    assert (results[0].directory / "config.yaml").is_file()
    assert (tmp_path / "fail-sweep" / "sweep_summary.json").is_file()
    assert (tmp_path / "fail-sweep" / "sweep_summary.html").is_file()
    # the failure shows up in the cross-run comparison as placeholder values
    by_run = {b["values"]["sim.until"]: b for b in summary["comparison"]["by_run"]}
    assert by_run[-1.0]["e2e_p95"] is None
    assert by_run[20.0]["e2e_p95"] is not None


def test_sweep_fail_fast_stops_queued_runs(tmp_path: Path) -> None:
    """``fail_fast`` aborts remaining queued runs after the first failure."""
    cfg = scenario_cfg()
    cfg.sweep = OmegaConf.create(
        {
            "name": "aborted-sweep",
            "factors": {"sim.until": [-1.0, 20.0, 5.0]},  # run 0 fails
        }
    )
    results, summary = enumerate_sweep(cfg, out_root=tmp_path, fail_fast=True)

    assert len(results) == 1  # only the failed run executed
    assert summary["aborted"] is True
    assert summary["skipped"] == 2
    assert len(summary["runs"]) == 1
    assert summary["runs"][0]["failed"] is True
    sweep_dir = tmp_path / "aborted-sweep"
    run_dirs = sorted(d.name for d in sweep_dir.iterdir() if d.is_dir())
    assert run_dirs == [results[0].run_id]  # queued run dirs were never created
    assert (sweep_dir / "sweep_summary.json").is_file()


def test_sweep_jobs_validation_rejects_invalid_counts(tmp_path: Path) -> None:
    """``--jobs 0``/negative (and junk values) fail with a clear error."""
    cfg = parallel_sweep_cfg()
    for bad in (0, -1, -7):
        with pytest.raises(SweepError, match="integer >= 1"):
            enumerate_sweep(cfg, out_root=tmp_path, jobs=bad)
    with pytest.raises(SweepError, match="integer >= 1"):
        validate_jobs("not-a-number")
    # jobs=1 and bigger-than-runs counts are both fine (cap: workers=runs)
    for _jobs in (1, max(1, 2**10)):
        _, summary = enumerate_sweep(cfg, out_root=tmp_path / f"jobs-{_jobs}", jobs=_jobs)
        assert summary["n_runs"] == 4


def test_sweep_cli_runs_parallel_and_reports_progress(tmp_path: Path) -> None:
    """``sweep --jobs 2``: progress lines as children finish, deterministic
    (enumeration-ordered) final per-run summary e2e lines."""
    scenario = tmp_path / "scenario.yaml"
    text = OmegaConf.to_yaml(parallel_sweep_cfg())
    scenario.write_text(text)
    out = tmp_path / "out"

    result = runner.invoke(app, ["sweep", str(scenario), "--out", str(out), "--jobs", "2"])

    assert result.exit_code == 0, result.output
    assert "[1/4]" in result.output and "done in" in result.output  # progress
    assert result.output.count("e2e p95=") == 4  # per-run echo, all 4 runs
    assert "workers=2" in result.output
    sweeps = sorted(out.glob("*/sweep_summary.json"))
    assert len(sweeps) == 1
    loaded = json.loads(sweeps[0].read_text())
    assert loaded["jobs"] == 2 and loaded["n_runs"] == 4
    for entry in loaded["runs"]:
        assert entry["failed"] is False


def test_sweep_cli_max_jobs_config_key_sets_workers(tmp_path: Path) -> None:
    """``sweep.max_jobs`` in the scenario drives the worker cap without --jobs."""
    scenario = tmp_path / "scenario.yaml"
    text = OmegaConf.to_yaml(parallel_sweep_cfg())
    text += "\n  max_jobs: 2\n"
    scenario.write_text(text)
    out = tmp_path / "out"

    result = runner.invoke(app, ["sweep", str(scenario), "--out", str(out)])

    assert result.exit_code == 0, result.output
    assert "workers=2" in result.output
    sweeps = sorted(out.glob("*/sweep_summary.json"))
    assert json.loads(sweeps[0].read_text())["jobs"] == 2


def test_sweep_cli_broken_run_fails_with_nonzero_exit(tmp_path: Path) -> None:
    """A failed run: red FAILED line, sweep completes, exit code 1."""
    scenario = tmp_path / "scenario.yaml"
    cfg = scenario_cfg()
    cfg.sweep = OmegaConf.create(
        {"name": "cli-fail", "factors": {"sim.until": [-1.0, 20.0]}}
    )
    scenario.write_text(OmegaConf.to_yaml(cfg))
    out = tmp_path / "out"

    result = runner.invoke(app, ["sweep", str(scenario), "--out", str(out)])

    assert result.exit_code == 1, result.output
    assert "FAILED" in result.output
    assert "1 failed run(s)" in result.output
    # the healthy run still produced its artifacts
    sweep_dir = out / "cli-fail"
    assert (sweep_dir / "sweep_summary.json").is_file()
    run_dirs = sorted(d for d in sweep_dir.iterdir() if d.is_dir())
    assert len(run_dirs) == 2


def test_sweep_cli_jobs_zero_fails_cleanly(tmp_path: Path) -> None:
    """``--jobs 0`` is rejected with a clear error (exit code 2)."""
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(OmegaConf.to_yaml(parallel_sweep_cfg()))

    result = runner.invoke(app, ["sweep", str(scenario), "--out", str(tmp_path), "--jobs", "0"])

    assert result.exit_code == 2, result.output
    assert "jobs" in result.output.lower()
