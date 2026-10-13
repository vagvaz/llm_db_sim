"""Fixture-based server/report/CLI tests (ticket 12).

The viz stack is validated standalone: a synthetic run directory (fixture
module) provides run.vcd + metrics.db without touching tickets 08-10.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from agent_db_sim.cli import app as cli_app
from agent_db_sim.viz.fixture import make_fixture_run
from agent_db_sim.viz.report import list_templates
from agent_db_sim.viz.server import create_app, sse_events
from agent_db_sim.viz.vcd import VcdSampleWriter, parse_vcd_bytes
from agent_db_sim.simkit.probes import Sample

runner = CliRunner()


def _age_file(path: Path, seconds: float) -> None:
    old = datetime.now().timestamp() - seconds
    os.utime(path, (old, old))


@pytest.fixture(scope="module")
def fixture_run(tmp_path_factory: pytest.TempPathFactory):
    run_dir = tmp_path_factory.mktemp("run") / "fixture-run"
    return make_fixture_run(run_dir, agents=2, until=30.0)


@pytest.fixture(scope="module")
def client(fixture_run):
    app = create_app(fixture_run.run_dir)
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


# --------------------------------------------------------------------- #
# /signals + /vcd                                                        #
# --------------------------------------------------------------------- #

def test_signals_catalog(client) -> None:
    r = client.get("/signals")
    assert r.status_code == 200
    body = r.json()
    names = {f"{s['scope']}.{s['name']}" for s in body["signals"]}
    for expected in (
        "agent_db_sim.llm.queue_depth",
        "agent_db_sim.llm.slot_busy",
        "agent_db_sim.llm.tokens",
        "agent_db_sim.llm.request_state",
        "agent_db_sim.db.request_state",
        "agent_db_sim.agent.loop_state",
    ):
        assert expected in names
    assert body["t_max"] > 0


def test_vcd_raw_bytes(client, fixture_run) -> None:
    r = client.get("/vcd")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/octet-stream")
    assert r.content == fixture_run.vcd.read_bytes()


# --------------------------------------------------------------------- #
# waveform shell                                                         #
# --------------------------------------------------------------------- #

def test_home_shell_local_vendored_js(client) -> None:
    r = client.get("/")
    assert r.status_code == 200
    html = r.text
    # offline-first: no CDN references
    assert "://cdn." not in html and "://unpkg" not in html
    for local in ("/static/d3.min.js", "/static/d3-wave.js", "/static/app.js",
                  "/static/fa-icons.js"):
        assert local in html or local.replace("/static/", "static/") in html
    # Surfer deep-dive link targets the /vcd route
    assert "surfer-project.org" in html and "/vcd" in html


def test_vendored_static_assets(client) -> None:
    for asset, min_len in (("d3.min.js", 100_000), ("d3-wave.js", 50_000),
                           ("plotly.min.js", 500_000), ("fa-icons.js", 300)):
        r = client.get(f"/static/{asset}")
        assert r.status_code == 200, asset
        assert len(r.content) >= min_len, asset


def test_app_js_stream_glue(client) -> None:
    r = client.get("/static/app.js")
    assert r.status_code == 200
    text = r.text
    assert "EventSource('/stream')" in text
    assert "d3wave.WaveGraph" in text
    assert "foldInit" in text and "foldDelta" in text


# --------------------------------------------------------------------- #
# SSE stream                                                             #
# --------------------------------------------------------------------- #

def test_stream_finished_run_init_then_done(client, fixture_run) -> None:
    _age_file(fixture_run.vcd, seconds=120.0)
    events: list[str] = []
    with client.stream("GET", "/stream") as r:
        assert r.status_code == 200
        assert "text/event-stream" in r.headers["content-type"]
        for chunk in r.iter_text():
            events.extend(chunk.split("\n\n"))
            if any(e.startswith("event: done") for e in events):
                break
    kinds = [
        e.split("\n", 1)[0].replace("event: ", "")
        for e in events if e.strip() and not e.startswith(":")
    ]
    assert kinds[0] == "init"
    assert kinds[-1] == "done"
    init = json.loads(next(e for e in events if e.startswith("event: init")).split("data: ", 1)[1])
    assert len(init["signals"]) >= 6
    assert init["t_max"] > 0
    # init snapshot carries value series identical to a full parse
    full = parse_vcd_bytes(fixture_run.vcd.read_bytes(), include_tail=True)
    assert set(init["series"]) <= set(full.signals)


def test_sse_events_live_deltas(fixture_run, tmp_path: Path) -> None:
    """The SSE generator: init, then delta rows as the file grows."""
    live = tmp_path / "live.vcd"
    w = VcdSampleWriter(live)
    w.feed(Sample(0.0, "slot_busy", "level", 0, {"component": "llm", "slots": 4}))
    w.flush()
    it = sse_events(live, fast=True, live_window=0.5)
    init_ev = next(it)
    assert init_ev.startswith("event: init")
    init = json.loads(init_ev.split("data: ", 1)[1])
    assert init["t_max"] == 0.0
    # append two more timestamp blocks, then a third: the first two commit
    w.feed(Sample(2.0, "slot_busy", "level", 1, {"component": "llm", "slots": 4}))
    w.feed(Sample(4.0, "slot_busy", "level", 2, {"component": "llm", "slots": 4}))
    w.feed(Sample(6.0, "slot_busy", "level", 3, {"component": "llm", "slots": 4}))
    w.flush()
    for _ in range(20):
        ev = next(it)
        if ev.startswith("event: delta"):
            delta = json.loads(ev.split("data: ", 1)[1])
            break
    else:
        pytest.fail("no delta event observed")
    samples = [tuple(row) for row in delta["samples"]]
    # the newest block (#6.0) is still held by safe-tail discipline
    assert (2.0, "agent_db_sim.llm.slot_busy", 1) in samples
    assert (4.0, "agent_db_sim.llm.slot_busy", 2) in samples
    assert all(row[0] != 6.0 for row in samples)
    # closing writer commits everything; after the live-window elapses the
    # stream ends with a `done` event
    w.close(final_t=6.0)
    finished = False
    for _ in range(300):
        ev = next(it)
        if ev.startswith("event: done"):
            finished = True
            break
    assert finished
    with pytest.raises(StopIteration):
        next(it)


def test_stream_missing_vcd_yields_error_and_done(tmp_path: Path) -> None:
    events = list(sse_events(tmp_path / "nothing.vcd", fast=True))
    kinds = []
    for e in events:
        if e.strip():
            kinds.append(e.split("\n", 1)[0])
    assert kinds == ["event: error", "event: done"]


# --------------------------------------------------------------------- #
# reports                                                                #
# --------------------------------------------------------------------- #

def test_reports_index_lists_auto_templates(client, fixture_run) -> None:
    r = client.get("/reports")
    assert r.status_code == 200
    assert "standard" in r.text
    # user-drop-in templates appear automatically
    new_tmpl = Path("src/agent_db_sim/viz/templates/_scaffold_test.html")
    user_tmpl = new_tmpl.parent / "custom_probe.html"
    try:
        user_tmpl.write_text("<p>custom {{ meta.run_name }}</p>")
        r2 = client.get("/reports")
        assert "custom_probe" in r2.text
        r3 = client.get("/reports/custom_probe")
        assert r3.status_code == 200
        assert "custom" in r3.text
    finally:
        user_tmpl.unlink(missing_ok=True)
    # underscore files are scaffolds, never listed
    r4 = client.get("/reports/_scaffold_test")
    assert r4.status_code == 404
    assert "reports.html" not in [t["name"] for t in list_templates()]
    assert "wave.html" not in [t["name"] for t in list_templates()]


def test_report_standard_contains_fixture_numbers(client, fixture_run) -> None:
    r = client.get("/reports/standard")
    assert r.status_code == 200
    html = r.text
    db_path = fixture_run.metrics_db
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    e2e = conn.execute(
        "SELECT p50, p95 FROM latency_percentiles WHERE source='e2e' AND kind='latency'"
    ).fetchone()
    conn.close()
    assert e2e is not None
    for value in e2e:
        assert ("%.4g" % float(value)) in html, value
    # figures are plotly json embedded server-side
    assert "fig-latency-percentiles" in html
    assert "fig-utilization" in html
    assert "fig-token-rate" in html
    assert "fig-bottleneck" in html
    assert "plotly.min.js" in html


def test_report_unknown_template_404(client) -> None:
    assert client.get("/reports/does_not_exist").status_code == 404


# --------------------------------------------------------------------- #
# CLI                                                                    #
# --------------------------------------------------------------------- #

def test_render_cli_standalone(fixture_run) -> None:
    result = runner.invoke(
        cli_app, ["render", str(fixture_run.run_dir), "--name", "standard"]
    )
    assert result.exit_code == 0, result.output
    out = fixture_run.run_dir / "standard.html"
    assert out.exists()
    html = out.read_text()
    assert 'src="plotly.min.js"' in html  # standalone: relative asset
    assert (out.parent / "plotly.min.js").exists()
    conn = sqlite3.connect(f"file:{fixture_run.metrics_db}?mode=ro", uri=True)
    p50 = conn.execute(
        "SELECT p50 FROM latency_percentiles WHERE source='e2e' AND kind='latency'"
    ).fetchone()[0]
    conn.close()
    assert ("%.4g" % float(p50)) in html


def test_render_cli_unknown_template_errors(fixture_run) -> None:
    result = runner.invoke(
        cli_app, ["render", str(fixture_run.run_dir), "--name", "nope"]
    )
    assert result.exit_code == 2


def test_serve_cli_requires_vcd(tmp_path: Path) -> None:
    empty = tmp_path / "empty-run"
    empty.mkdir()
    result = runner.invoke(cli_app, ["serve", str(empty)])
    assert result.exit_code == 2
    assert "run.vcd" in result.output
