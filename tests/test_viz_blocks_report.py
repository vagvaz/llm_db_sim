"""Stage-6 lane B: block-composable reports + sweep/multi-run analysis.

The composable layer is validated against synthetic fixture data only:

- a block list drives report content (adding a block adds its section;
  removing removes it);
- every block type renders from fixture data (run blocks from a single
  fixture run's metrics.db, sweep blocks from a synthetic 2-run sweep);
- the sweep_compare template renders over a sweep root and the new
  ``/sweeps`` server routes + ``render --sweep`` CLI mode work;
- the auto-indexed ``standard`` report stays golden: identical sections.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from agent_db_sim.cli import app as cli_app
from agent_db_sim.viz.blocks import (
    compose_run_report,
    compose_sweep_report,
    load_blocks,
)
from agent_db_sim.viz.fixture import make_fixture_run, make_fixture_sweep
from agent_db_sim.viz.report import (
    list_templates,
    render_report,
    render_sweep_report,
)
from agent_db_sim.viz.server import create_app

runner = CliRunner()
TEMPLATES = Path("src/agent_db_sim/viz/templates")


@pytest.fixture(scope="module")
def fixture_run(tmp_path_factory: pytest.TempPathFactory):
    run_dir = tmp_path_factory.mktemp("run") / "fixture-run"
    return make_fixture_run(run_dir, agents=2, until=30.0)


@pytest.fixture(scope="module")
def sweep(tmp_path_factory: pytest.TempPathFactory) -> Path:
    sweep_dir = tmp_path_factory.mktemp("sweep") / "fixture-sweep"
    return make_fixture_sweep(sweep_dir)


# --------------------------------------------------------------------- #
# standard report: blocks-driven, identical content (golden-ish)         #
# --------------------------------------------------------------------- #

EXPECTED_SECTIONS = [
    "Run metadata",
    "Latency percentiles (per stage)",
    "Utilization (slots / token rate)",
    "Bottleneck attribution (hybrid: queue share 70% + saturation 30%)",
]


def test_standard_report_golden_sections(fixture_run) -> None:
    html = render_report(fixture_run.run_dir, name="standard", standalone=True)
    headings = _headings(html)
    assert headings == EXPECTED_SECTIONS
    for div in ("fig-latency-percentiles", "fig-utilization",
                "fig-token-rate", "fig-bottleneck"):
        assert div in html
    assert 'src="plotly.min.js"' in html  # standalone asset reference
    # one h2 per section (inline blocks do not add headings)
    assert html.count("<h2>") == len(EXPECTED_SECTIONS)


def test_standard_report_same_numbers_and_rows(fixture_run) -> None:
    import sqlite3

    html = render_report(fixture_run.run_dir, name="standard")
    conn = sqlite3.connect(f"file:{fixture_run.metrics_db}?mode=ro", uri=True)
    e2e = conn.execute(
        "SELECT p50, p95 FROM latency_percentiles WHERE source='e2e' AND kind='latency'"
    ).fetchone()
    n_percentile_rows = conn.execute(
        "SELECT COUNT(*) FROM latency_percentiles"
    ).fetchone()[0]
    n_util_rows = conn.execute("SELECT COUNT(*) FROM utilization_stats").fetchone()[0]
    conn.close()
    for value in e2e:
        assert ("%.4g" % float(value)) in html
    # both tables keep every db row ("<tr><td>" is a data row)
    assert html.count("<tr><td>") >= n_percentile_rows + n_util_rows


def test_list_templates_includes_blocks_mode(fixture_run) -> None:
    names = {entry["name"]: entry for entry in list_templates()}
    assert names["standard"]["mode"] == "blocks"
    assert names["sweep_compare"]["mode"] == "blocks"
    assert names["standard"]["file"] == "standard.blocks.yaml"
    # both flavors coexist
    modular = {e["name"] for e in list_templates()}
    assert "standard" in modular


def _headings(html: str) -> list[str]:
    import re

    return [m.strip() for m in re.findall(r"<h2>(.*?)</h2>", html)]


# --------------------------------------------------------------------- #
# the blocks file drives report content                                  #
# --------------------------------------------------------------------- #

@pytest.fixture()
def probe_template():
    path = TEMPLATES / "laneb_probe.blocks.yaml"
    if path.exists():  # name must be free between test stages
        path.unlink()
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


def test_blocks_file_drives_add_and_remove(fixture_run, probe_template) -> None:
    # 1. minimal: metadata + markdown only
    probe_template.write_text(yaml.safe_dump([
        {"type": "metadata", "title": "Mini metadata"},
        {"type": "markdown", "title": "Notes", "text": "hello **block world**"},
    ]))
    html = render_report(fixture_run.run_dir, name="laneb_probe")
    assert _headings(html) == ["Mini metadata", "Notes"]
    assert "<b>block world</b>" in html
    assert "fig-bottleneck" not in html  # not declared -> not rendered

    # 2. adding a block adds its section
    probe_template.write_text(yaml.safe_dump([
        {"type": "metric_table", "title": "Bottlenecks", "table": "bottleneck"},
        {"type": "plot_bottleneck", "title": "Attribution", "div": "fig-bottleneck"},
    ]))
    html = render_report(fixture_run.run_dir, name="laneb_probe")
    assert _headings(html) == ["Bottlenecks", "Attribution"]
    assert "fig-bottleneck" in html
    assert html.count("<h2>") == 2

    # 3. removing it removes the section
    probe_template.write_text(yaml.safe_dump([
        {"type": "metric_table", "title": "Bottlenecks", "table": "bottleneck"},
    ]))
    html = render_report(fixture_run.run_dir, name="laneb_probe")
    assert _headings(html) == ["Bottlenecks"]
    assert "fig-bottleneck" not in html

    # 4. reordering reorders sections
    probe_template.write_text(yaml.safe_dump([
        {"type": "markdown", "text": "first", "title": "First"},
        {"type": "metadata", "title": "Second"},
    ]))
    html = render_report(fixture_run.run_dir, name="laneb_probe")
    assert _headings(html) == ["First", "Second"]


def test_unknown_block_type_is_a_clear_error(fixture_run, probe_template) -> None:
    probe_template.write_text(yaml.safe_dump([{"type": "definitely_not_a_block"}]))
    with pytest.raises(ValueError, match="unknown block type"):
        render_report(fixture_run.run_dir, name="laneb_probe")


# --------------------------------------------------------------------- #
# every block type renders from fixture data                             #
# --------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "spec,expect",
    [
        ({"type": "metadata"}, ["t_end", "warmup_end"]),
        ({"type": "metric_table", "table": "latency_percentiles",
          "filter": {"source": "e2e", "kind": "latency"}}, ["e2e", "latency"]),
        ({"type": "metric_table", "table": "utilization_stats"}, ["utilization", "mean"]),
        ({"type": "metric_table", "table": "bottleneck"}, ["queue_share", "score"]),
        ({"type": "plot_series", "series": "utilization"}, ["fig-utilization"]),
        ({"type": "plot_series", "series": "tokens"}, ["fig-token-rate"]),
        ({"type": "plot_percentiles"}, ["fig-latency-percentiles"]),
        ({"type": "plot_bottleneck"}, ["fig-bottleneck"]),
        ({"type": "plot_distribution"}, ["fig-dist-e2e", ":\"histogram\"",]),
        ({"type": "plot_distribution", "source": "llm"}, ["fig-dist-llm"]),
        ({"type": "markdown", "text": "# Prose\n- item one"}, ["<h1>Prose</h1>", "<li>item one</li>"]),
    ],
)
def test_each_run_block_type_renders(fixture_run, spec, expect) -> None:
    html = compose_run_block(spec, fixture_run)
    for token in expect:
        assert token in html, token
    if "type" in spec and spec["type"] != "markdown":
        assert "Plotly" in html or "<table" in html


def compose_run_block(spec: dict, fixture_run) -> str:
    return _compose_one_block(spec, fixture_run.run_dir)


def _compose_one_block(spec: dict, run_dir: Path) -> str:
    # single-block render through the same public entry the templates use
    probe = TEMPLATES / "_laneb_single.blocks.yaml"
    try:
        probe.write_text(yaml.safe_dump([spec]))
        return render_report(run_dir, name="_laneb_single")
    finally:
        probe.unlink(missing_ok=True)


def test_metric_table_filter_narrows_rows(fixture_run) -> None:
    html = _compose_one_block(
        {"type": "metric_table", "table": "latency_percentiles",
         "filter": {"source": "e2e"}}, fixture_run.run_dir)
    # only e2e rows survive the filter; stage rows (llm/db) are gone
    data_rows = [line for line in html.splitlines() if "<tr><td>" in line]
    assert data_rows and all("e2e" in row for row in data_rows)


def test_plot_distribution_fixture_numbers(fixture_run) -> None:
    html = _compose_one_block({"type": "plot_distribution"}, fixture_run.run_dir)
    assert '"type": "histogram"' in html or '"type":"histogram"' in html


# --------------------------------------------------------------------- #
# sweep blocks over the synthetic 2-run sweep                             #
# --------------------------------------------------------------------- #

def test_load_sweep_reads_summary_and_runs(sweep: Path) -> None:
    data = load_sweep_convenience(sweep)
    assert data["name"] == "fixture-sweep"
    assert set(map(str, data["factors"])) == {
        "db.topology", "db.compute_nodes", "agents.count",
    }
    assert len(data["runs"]) == 2
    for run in data["runs"]:
        assert run["e2e_percentiles"]["n"] > 0
        assert run["bottleneck_winner"]["score"] is not None


def load_sweep_convenience(sweep_dir: Path):
    from agent_db_sim.viz.blocks import load_sweep

    return load_sweep(sweep_dir)


def test_sweep_compare_template_golden(sweep: Path) -> None:
    html = render_sweep_report(sweep, name="sweep_compare", standalone=True)
    headings = _headings(html)
    assert headings == [
        "Sweep runs — factors, e2e percentiles, bottleneck winner",
        "e2e p95 grouped by factor",
        "Paired deltas — same factors except one (other − base)",
    ]
    # run_table: both run ids and factor values appear
    assert "0000-db__topology=centralized" in html
    assert "0001-db__topology=distributed" in html
    assert "centralized" in html and "distributed" in html
    # factor_group_plot colored by factor, factor_delta chart rendered
    assert "fig-factor-p95-db-topology" in html
    assert "fig-delta-p95-db-topology" in html
    assert 'src="plotly.min.js"' in html


def test_sweep_contextual_numbers(sweep: Path) -> None:
    import sqlite3

    html = render_sweep_report(sweep, name="sweep_compare")
    for run_dir in sorted(sweep.iterdir()):
        if not run_dir.is_dir():
            continue
        conn = sqlite3.connect(f"file:{run_dir / 'metrics.db'}?mode=ro", uri=True)
        p95 = conn.execute(
            "SELECT p95 FROM latency_percentiles WHERE source='e2e' AND kind='latency'"
        ).fetchone()[0]
        conn.close()
        assert ("%.4g" % float(p95)) in html


@pytest.mark.parametrize(
    "spec,expect",
    [
        ({"type": "run_table", "title": "Runs & factors"},
         ["Runs &amp; factors", "0000-db__topology=centralized"]),
        ({"type": "factor_group_plot", "metric": "p95", "factor": "db.topology"},
         ["e2e p95 by db.topology", "■"]),
        ({"type": "factor_delta", "metric": "p95", "factor": "db.topology"},
         ["Δ p95 (other − base)", "fig-delta"]),
        ({"type": "markdown", "text": "sweep notes"}, ["<p>sweep notes</p>"]),
    ],
)
def test_each_sweep_block_type_renders(sweep: Path, spec, expect) -> None:
    html = compose_sweep_report([spec], sweep, name="laneb_sweep_probe")
    for token in expect:
        assert token in html, token


def test_factor_delta_paired_values(sweep: Path) -> None:
    """Both runs share db.compute_nodes/agents.count and differ only in
    topology -> one factor group with exactly one delta row."""
    entries = json.loads((sweep / "sweep_summary.json").read_text())
    p95s = {e["run_id"]: e["e2e_percentiles"]["p95"] for e in entries["runs"]}
    delta_expected = p95s["0001-db__topology=distributed"] - \
        p95s["0000-db__topology=centralized"]
    html = compose_sweep_report(
        [{"type": "factor_delta", "metric": "p95", "factor": "db.topology"}],
        sweep, name="probe")
    assert ("%.4g" % abs(delta_expected)) in html or (
        "%+.4g" % delta_expected).replace("+", "+") in html
    assert f"{delta_expected:+.4g}" in html


def test_sweep_block_in_run_context_is_rejected(fixture_run) -> None:
    with pytest.raises(ValueError, match="sweep-only"):
        compose_run_report(
            [{"type": "run_table"}], fixture_run.run_dir, name="probe")


def test_run_block_in_sweep_context_is_rejected(sweep: Path) -> None:
    with pytest.raises(ValueError, match="needs a run directory"):
        compose_sweep_report(
            [{"type": "plot_series", "series": "utilization"}],
            sweep, name="probe")


def test_sweep_compare_not_renderable_per_run(fixture_run) -> None:
    with pytest.raises(ValueError, match="sweep-only"):
        render_report(fixture_run.run_dir, name="sweep_compare")


# --------------------------------------------------------------------- #
# server routes                                                          #
# --------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def sweep_client(fixture_run, sweep: Path):
    app = create_app(fixture_run.run_dir, sweeps_root=sweep.parent)
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


def test_get_sweeps_lists_sweep_roots(sweep_client, sweep: Path) -> None:
    r = sweep_client.get("/sweeps")
    assert r.status_code == 200
    body = r.json()
    names = [s["name"] for s in body["sweeps"]]
    assert sweep.name in names
    entry = next(s for s in body["sweeps"] if s["name"] == sweep.name)
    assert entry["n_runs"] == 2


def test_get_sweeps_default_report(sweep_client, sweep: Path) -> None:
    r = sweep_client.get(f"/sweeps/{sweep.name}")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    assert "0001-db__topology=distributed" in r.text
    assert "fig-factor-p95-db-topology" in r.text


def test_get_sweeps_explicit_template(sweep_client, sweep: Path) -> None:
    r = sweep_client.get(f"/sweeps/{sweep.name}/reports/sweep_compare")
    assert r.status_code == 200
    assert "bottleneck" in r.text


def test_sweep_routes_404s(sweep_client) -> None:
    assert sweep_client.get("/sweeps/nope").status_code == 404
    assert sweep_client.get("/sweeps/nope/reports/sweep_compare").status_code == 404
    assert sweep_client.get(
        "/sweeps/../fixture-run/reports/standard").status_code in (404, 307)


def test_run_reports_still_work_alongside_sweeps(sweep_client, fixture_run) -> None:
    r = sweep_client.get("/reports/standard")
    assert r.status_code == 200
    assert "Latency percentiles (per stage)" in r.text
    r2 = sweep_client.get("/reports/sweep_compare")
    assert r2.status_code == 404
    assert "sweep" in r2.text  # 404 detail points at sweep rendering


# --------------------------------------------------------------------- #
# CLI --sweep mode                                                       #
# --------------------------------------------------------------------- #

def test_render_cli_sweep_mode(fixture_run, sweep: Path) -> None:
    result = runner.invoke(cli_app, ["render", str(fixture_run.run_dir),
                                     "--sweep", str(sweep)])
    assert result.exit_code == 0, result.output
    out = sweep / "sweep_compare.html"
    assert out.exists()
    html = out.read_text()
    assert 'src="plotly.min.js"' in html
    assert (sweep / "plotly.min.js").exists()
    assert "0000-db__topology=centralized" in html
    assert "0001-db__topology=distributed" in html


def test_render_cli_sweep_mode_named_template(fixture_run, sweep: Path, tmp_path: Path) -> None:
    out = tmp_path / "custom.html"
    result = runner.invoke(cli_app, [
        "render", str(fixture_run.run_dir), "--sweep", str(sweep),
        "--name", "sweep_compare", "--out", str(out),
    ])
    assert result.exit_code == 0, result.output
    assert out.exists() and "0000-db__topology=centralized" in out.read_text()


def test_render_cli_sweep_unknown_template_errors(fixture_run, sweep: Path) -> None:
    result = runner.invoke(cli_app, [
        "render", str(fixture_run.run_dir), "--sweep", str(sweep),
        "--name", "no_such_template",
    ])
    assert result.exit_code == 2


def test_render_cli_run_mode_unchanged(fixture_run) -> None:
    result = runner.invoke(cli_app, ["render", str(fixture_run.run_dir)])
    assert result.exit_code == 0, result.output
    out = fixture_run.run_dir / "standard.html"
    assert out.exists() and 'src="plotly.min.js"' in out.read_text()
