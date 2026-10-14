"""Analytical cross-check end-to-end: run the real pipeline, then analyze.

The sanity scenarios are the hand-derived ground truths
(``scenarios/sanity/README.md``): queue-free systems should cross-check to
near-exact agreement (<1% on throughput/cycle; the utilizations within a
percent), the queued scenario shows the exponential-service assumption
error (documented band), and the grounded 70b run demonstrates the
token-cap regime where queue-based attribution is blind. Also covers the
``agent-db-sim analyze`` CLI and the ``analytical_compare`` viz block.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from omegaconf import OmegaConf
from typer.testing import CliRunner

from agent_db_sim.analysis.compare import analyze_run
from agent_db_sim.cli import app as cli_app
from agent_db_sim.pipeline import execute_run

REPO = Path(__file__).resolve().parents[1]
SCENARIOS = REPO / "scenarios"

runner = CliRunner()


def _run(tmp_path: Path, scenario_rel: str, name: str):
    run_dir = tmp_path / name
    cfg = OmegaConf.load(str(SCENARIOS / scenario_rel))
    execute_run(cfg, run_dir)
    return run_dir


@pytest.fixture(scope="module")
def sanity_runs(tmp_path_factory: pytest.TempPathFactory) -> dict:
    base = tmp_path_factory.mktemp("anityz")
    out = {}
    for rel, name in (
        ("sanity/sanity_single.yaml", "single"),
        ("sanity/sanity_parallel.yaml", "parallel"),
        ("sanity/sanity_queue.yaml", "queue"),
        ("sanity/sanity_disaggregated.yaml", "disagg"),
    ):
        out[name] = _run(base, rel, name)
    return out


# --------------------------------------------------------------------------- #
# queue-free sanity runs: near-exact hand math                                 #
# --------------------------------------------------------------------------- #

def test_sanity_single_cross_check_exact(sanity_runs: dict) -> None:
    report = analyze_run(sanity_runs["single"])
    assert report.regime == "floor"
    assert report.bottleneck_matched
    # tau = 1.6001 exactly; the throughput/cycle are within the estimator
    cycle = next(r for r in report.rows if r.metric == "cycle (N/X)")
    assert abs(cycle.rel_err) < 0.01  # < 1%
    hop_llm = next(r for r in report.rows if r.metric.startswith("inference hop"))
    assert abs(hop_llm.rel_err) < 0.01
    db_row = next(r for r in report.rows if r.metric.startswith("db hop"))
    assert abs(db_row.rel_err) < 0.01
    # per-stage decomposition matches the hand multisets
    issue = next(r for r in report.rows if r.metric.startswith("db issue"))
    assert abs(issue.rel_err) < 0.01
    fin = next(r for r in report.rows if r.metric.startswith("db finalize"))
    assert abs(fin.rel_err) < 0.01


def test_sanity_parallel_cross_check(sanity_runs: dict) -> None:
    report = analyze_run(sanity_runs["parallel"])
    cycle = next(r for r in report.rows if r.metric == "cycle (N/X)")
    # hand math: tau = 1.7201 exactly, no queueing anywhere
    assert abs(cycle.rel_err) < 0.03
    assert report.bottleneck_matched


def test_sanity_disaggregated_cross_check(sanity_runs: dict) -> None:
    report = analyze_run(sanity_runs["disagg"])
    assert report.topology == "disaggregated"
    cycle = next(r for r in report.rows if r.metric == "cycle (N/X)")
    assert abs(cycle.rel_err) < 0.01
    middle = next(r for r in report.rows if r.metric.startswith("db middle"))
    # net legs are exactly computable on paper: 0.0205 + 0.2001 + 0.024
    assert abs(middle.rel_err) < 0.01
    assert abs(middle.model - 0.2446) < 1e-9


# --------------------------------------------------------------------------- #
# queued sanity run: the exponential assumption shows, in band                 #
# --------------------------------------------------------------------------- #

def test_sanity_queue_cross_check_in_band(sanity_runs: dict) -> None:
    report = analyze_run(sanity_runs["queue"])
    assert report.regime == "floor"
    cycle = next(r for r in report.rows if r.metric == "cycle (N/X)")
    # deterministic closed ping-pong (period 2L=2.0) vs exponential chain:
    # the band is documented as assumption error, bounded here
    assert abs(cycle.rel_err) < 0.2
    # the LLM is identified as the bottleneck on both sides
    assert report.bottleneck_matched
    assert report.bottleneck_matched is True
    llm_wait = next(r for r in report.rows if r.metric.startswith("llm queue"))
    assert llm_wait.sim > 0 and llm_wait.model > 0


# --------------------------------------------------------------------------- #
# CLI + viz block                                                              #
# --------------------------------------------------------------------------- #

def test_cli_analyze_sanity(sanity_runs: dict, capsys) -> None:
    result = runner.invoke(cli_app, ["analyze", str(sanity_runs["single"])])
    assert result.exit_code == 0, result.output
    assert "analytical cross-check" in result.output
    assert "verdict: bottleneck" in result.output
    assert "no analytical counterpart" in result.output


def test_cli_analyze_missing_metrics_errors(tmp_path: Path) -> None:
    result = runner.invoke(cli_app, ["analyze", str(tmp_path)])
    assert result.exit_code == 2


def test_viz_block_analytical_compare(sanity_runs: dict) -> None:
    from agent_db_sim.viz.blocks import compose_run_report

    html = compose_run_report(
        [{"type": "analytical_compare",
          "title": "Analytical cross-check"}],
        sanity_runs["single"], name="probe",
    )
    assert "Analytical cross-check" in html
    assert "rel err" in html
    assert "verdict" in html or "sequencer" in html  # verdict table line


def test_viz_block_fails_cleanly_on_placeholder(tmp_path: Path) -> None:
    from agent_db_sim.viz.blocks import compose_run_report

    run = tmp_path / "empty"
    run.mkdir()
    with pytest.raises(ValueError, match="analytical_compare"):
        compose_run_report([{"type": "analytical_compare"}], run, name="probe")
