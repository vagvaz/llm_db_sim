"""Tests for the /live animated view: launch guards, SSE delta shape, tail.

Endpoint tests avoid real subprocess waits where possible; one integration
test launches the tiny ``sanity_single`` scenario (finishes in ~1 s wall)
and drives the whole live loop: POST -> stream -> done -> report artifacts.
"""

from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from pathlib import Path
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from agent_db_sim.viz.live import LiveBusy, LiveManager, SamplesTail, zip_run_dir
from agent_db_sim.viz.server import create_app

REPO = Path(__file__).resolve().parents[1]
SCENARIOS = REPO / "scenarios"


# --------------------------------------------------------------------- #
# fixtures / helpers                                                     #
# --------------------------------------------------------------------- #

def _app(tmp_path: Path, **kw: Any) -> Any:
    run_dir = tmp_path / "served"
    run_dir.mkdir(exist_ok=True)
    return create_app(
        run_dir,
        live_root=str(tmp_path / "live"),
        scenarios_root=str(SCENARIOS),
        live_poll=kw.get("live_poll", 0.05),
    )


def _events(client: TestClient, path: str, timeout_events: int = 4000) -> List[Dict[str, Any]]:
    """Collect SSE ``event:/data:`` pairs until ``done`` (or a hard cap)."""
    out: List[Dict[str, Any]] = []
    with client.stream("GET", path) as resp:
        assert resp.status_code == 200
        current: Dict[str, str] = {}
        for line in resp.iter_lines():
            if line.startswith("event: "):
                current["event"] = line[7:]
            elif line.startswith("data: "):
                payload = json.loads(line[6:])
                out.append({"event": current.get("event", "message"), "data": payload})
                current = {}
                if payload.get("event") == "done" or out[-1]["event"] == "done":
                    break
            if len(out) >= timeout_events:
                break
    return out


def _wait_done(client: TestClient, limit: float = 60.0) -> Dict[str, Any]:
    import time

    t0 = time.time()
    while time.time() - t0 < limit:
        st = client.get("/live/state").json()
        if st["status"] in ("done", "error"):
            return st
        time.sleep(0.2)
    raise AssertionError("live run never finished")


# --------------------------------------------------------------------- #
# scenarios + state + validation                                         #
# --------------------------------------------------------------------- #

def test_scenarios_listing(tmp_path: Path) -> None:
    app = _app(tmp_path)
    client = TestClient(app)
    body = client.get("/live/scenarios").json()
    paths = {s["path"] for s in body["scenarios"]}
    assert "sanity/sanity_single.yaml" in paths
    # legacy dummy-loop files are not runnable component scenarios
    assert "smoke.yaml" not in paths
    for s in body["scenarios"]:
        assert s["agents"] and s["path"]


def test_state_idle(tmp_path: Path) -> None:
    client = TestClient(_app(tmp_path))
    assert client.get("/live/state").json() == {"status": "idle"}


def test_run_validation_errors(tmp_path: Path) -> None:
    client = TestClient(_app(tmp_path))
    assert client.post("/live/run", json={}).status_code == 422
    assert client.post(
        "/live/run", json={"scenario": "nope.yaml"}
    ).status_code == 422
    assert client.post(
        "/live/run", json={"scenario": "sanity/sanity_single.yaml",
                           "overrides": {"sim.warmup": 1}}
    ).status_code == 422  # not an allowed key
    assert client.post(
        "/live/run", json={"scenario": "sanity/sanity_single.yaml",
                           "overrides": {"db.topology": "star-trek"}}
    ).status_code == 422
    assert client.post(
        "/live/run", json={"scenario": "sanity/sanity_single.yaml",
                           "overrides": {"db.storage.backend": "tape"}}
    ).status_code == 422
    assert client.post(
        "/live/run", json={"scenario": "sanity/sanity_single.yaml",
                           "overrides": {"agents.count": -3}}
    ).status_code == 422


def test_scenario_path_escape(tmp_path: Path) -> None:
    mgr = LiveManager(tmp_path / "live", SCENARIOS)
    with pytest.raises(Exception):
        mgr.start("../src", {})


# --------------------------------------------------------------------- #
# concurrent-launch guard                                                #
# --------------------------------------------------------------------- #

class _AliveProc:
    def is_alive(self) -> bool:
        return True

    def join(self, timeout: float = 0) -> None:
        pass

    exitcode = None


def test_guard_rejects_second_launch(tmp_path: Path) -> None:
    client = TestClient(_app(tmp_path))
    r = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_single.yaml",
              "overrides": {"sim.until": 24}},
    )
    assert r.status_code == 202
    first = r.json()
    assert first["run_id"] and first["until"] == 24.0
    assert first["config"]["db"]["topology"] == "centralized"
    # while in flight a second launch is rejected
    busy = client.post("/live/run", json={"scenario": "sanity/sanity_single.yaml"})
    assert busy.status_code == 409
    assert "already in flight" in busy.json()["detail"]

    st = _wait_done(client)
    assert st["status"] == "done", st
    assert st["t_last"] > 0
    # slot freed: another launch now succeeds
    r2 = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_single.yaml",
              "overrides": {"sim.until": 12}},
    )
    assert r2.status_code == 202
    assert r2.json()["run_id"] != first["run_id"]
    _wait_done(client)


def test_guard_pure_unit(tmp_path: Path) -> None:
    mgr = LiveManager(tmp_path / "live", SCENARIOS)
    from agent_db_sim.viz.live import LiveState

    st = LiveState(run_id="x", run_dir=tmp_path / "x")
    st.status = "running"
    st.process = _AliveProc()  # type: ignore[assignment]
    mgr.state = st
    with pytest.raises(LiveBusy):
        mgr.start("sanity/sanity_single.yaml", {})


# --------------------------------------------------------------------- #
# SSE stream: init -> deltas -> done, plus report artifacts              #
# --------------------------------------------------------------------- #

def test_live_stream_end_to_end(tmp_path: Path) -> None:
    client = TestClient(_app(tmp_path))
    r = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_single.yaml",
              "overrides": {"sim.until": 24}},
    )
    assert r.status_code == 202
    run_id = r.json()["run_id"]

    events = _events(client, "/live/stream")
    kinds = [e["event"] for e in events]
    assert kinds[0] == "init"
    assert "delta" in kinds
    assert kinds[-1] == "done"

    init = events[0]["data"]
    assert init["run_id"] == run_id
    assert init["config"]["agents"]["count"] == 1
    assert init["until"] == 24.0

    rows: List[List[Any]] = []
    for e in events:
        if e["event"] == "delta":
            assert set(e["data"]) == {"rows"}
            rows.extend(e["data"]["rows"])
    assert len(rows) > 50
    t_prev = -1.0
    kinds_seen = set()
    for row in rows:
        t, signal, kind, value, tags = row
        assert isinstance(signal, str) and isinstance(tags, dict)
        assert t >= t_prev
        t_prev = t
        kinds_seen.add(kind)
    # lifecycle edges carry state/agent/request tags
    edges = [r for r in rows if r[2] == "edge"]
    assert edges
    assert any((r[4].get("state") or r[3]) and r[4].get("agent_id") for r in edges)
    # levels for queue cylinders and slot bars
    assert "queue_depth" in {r[1] for r in rows if r[2] == "level"}
    assert "slot_busy" in {r[1] for r in rows if r[2] == "level"}

    done = events[-1]["data"]
    assert done["reason"] == "done"
    assert done["t_max"] > 20.0
    assert done["report"] == f"/live/run/{run_id}/report"

    # artifacts of the finished live run are served
    rep = client.get(f"/live/run/{run_id}/report")
    assert rep.status_code == 200
    assert "<html" in rep.text.lower()
    vcd = client.get(f"/live/run/{run_id}/vcd")
    assert vcd.status_code == 200
    assert b"$var" in vcd.content
    # traversal / unknown run ids are rejected
    assert client.get("/live/run/nope/report").status_code == 404
    assert client.get("/live/run/..%2F..%2Fx%2F/report").status_code == 404


def test_stream_replay_after_done(tmp_path: Path) -> None:
    """A finished run streams in full on a fresh connect, then ends."""
    client = TestClient(_app(tmp_path))
    r = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_single.yaml",
              "overrides": {"sim.until": 16}},
    )
    assert r.status_code == 202
    _wait_done(client)
    # first stream: catch-up + done; a second connect replays too
    events = _events(client, "/live/stream")
    assert events[-1]["event"] == "done"
    total = sum(len(e["data"]["rows"]) for e in events if e["event"] == "delta")
    assert total > 20
    events2 = _events(client, "/live/stream")
    total2 = sum(len(e["data"]["rows"]) for e in events2 if e["event"] == "delta")
    assert total2 == total  # fresh per-connection cursor


def test_stream_without_run(tmp_path: Path) -> None:
    client = TestClient(_app(tmp_path))
    events = _events(client, "/live/stream")
    assert events[0]["event"] == "error"
    assert events[-1]["event"] == "done"
    assert events[-1]["data"]["reason"] == "idle"


# --------------------------------------------------------------------- #
# samples tail unit test                                                 #
# --------------------------------------------------------------------- #

def test_samples_tail_decodes_rows(tmp_path: Path) -> None:
    db = tmp_path / "metrics.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, t REAL, signal TEXT, "
        "kind TEXT, value TEXT, tags TEXT)"
    )
    conn.executemany(
        "INSERT INTO samples (t, signal, kind, value, tags) VALUES (?,?,?,?,?)",
        [
            (1.0, "agent_db_sim.llm.server.slot_busy", "level", "2", '{"component": "llm.server"}'),
            (2.0, "agent_db_sim.llm.tokens_input", "counter", "512", "{}"),
            (3.0, "agent_db_sim.llm.queued_state", "edge", '"queued"',
             '{"request_id": "r1", "state": "queued"}'),
        ],
    )
    conn.commit()
    conn.close()

    tail = SamplesTail(db)
    rows = tail.since()
    assert [r[1] for r in rows] == [1.0, 2.0, 3.0]
    assert rows[2][4] == "queued"
    assert rows[2][5]["request_id"] == "r1"
    assert tail.since() == []           # cursor advanced
    tail.reset()
    assert len(tail.since()) == 3       # fresh cursor re-reads
    tail.close()


def test_samples_tail_missing_db(tmp_path: Path) -> None:
    tail = SamplesTail(tmp_path / "nope.db")
    assert tail.since() == []
    tail.close()


# --------------------------------------------------------------------- #
# component capability metadata in the resolved init config              #
# --------------------------------------------------------------------- #

def test_display_cfg_capabilities_sanity_queue() -> None:
    """The live scene draws every component's processing capability from the
    run's RESOLVED config: LLM slots + tok/s caps, DB compute slots per node,
    the storage backend's IOPS/bandwidth ceilings, network bandwidth + pool."""
    from omegaconf import OmegaConf

    from agent_db_sim.viz.live import _display_cfg

    cfg = OmegaConf.load(SCENARIOS / "sanity" / "sanity_queue.yaml")
    out = _display_cfg(cfg)
    assert out["llm"] == {
        "slots": 1,
        "input_tokens_per_s": 1024.0,
        "output_tokens_per_s": 256.0,
        "ingress": "fifo",
    }
    db = out["db"]
    assert db["topology"] == "centralized"
    assert db["compute"] == {
        "slots": 2, "ingress": "fifo", "issue_ratio": 0.25,
        "phase_split": "three_phase",
    }
    # ssd preset ceilings, resolved through the very registry the sim uses
    assert db["storage"]["backend"] == "ssd"
    assert db["storage"]["iops_ceiling"] == 75000.0
    assert db["storage"]["bandwidth_ceiling"] == 0.4
    # centralized has no net section: schema defaults come through
    assert db["network"]["bandwidth"] == 1000.0
    assert db["network"]["connections"] == 0  # 0 = unlimited pool


def test_display_cfg_capabilities_distributed() -> None:
    from omegaconf import OmegaConf

    from agent_db_sim.viz.live import _display_cfg

    cfg = OmegaConf.load(SCENARIOS / "db_distributed.yaml")
    out = _display_cfg(cfg)
    db = out["db"]
    assert db["topology"] == "distributed"
    assert db["compute_nodes"] == 4
    assert db["coordinators"] == 2
    assert db["compute"]["slots"] == 2
    assert db["storage"]["backend"] == "nvme"
    assert db["storage"]["iops_ceiling"] == 750000.0
    assert db["storage"]["bandwidth_ceiling"] == 5.0
    assert db["network"]["latency"] == 0.001
    assert db["network"]["bandwidth"] == 1000.0
    assert db["network"]["coordinator_overhead"] == 1.1


def test_display_cfg_survives_unknown_backend() -> None:
    """A scenario with an unresolvable backend still renders (the run itself
    will fail loudly, but the launch/init payload must not break on display)."""
    from omegaconf import OmegaConf

    from agent_db_sim.viz.live import _display_cfg

    cfg = OmegaConf.create({"agents": {"count": 1}, "llm": {"slots": 2},
                            "db": {"topology": "centralized",
                                   "storage": {"backend": "tape-o-matic"}}})
    out = _display_cfg(cfg)
    assert out["db"]["backend"] == "tape-o-matic"
    assert out["db"]["storage"] == {"backend": "tape-o-matic"}


def test_init_payload_carries_capabilities(tmp_path: Path) -> None:
    """The streamed init config is the scene's rebuild source: capability
    fields must be present end-to-end, not only unit-side."""
    client = TestClient(_app(tmp_path))
    r = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_queue.yaml",
              "overrides": {"sim.until": 14}},
    )
    assert r.status_code == 202
    cfg = r.json()["config"]  # POST response carries the same public() shape
    assert cfg["db"]["compute"]["slots"] == 2
    assert cfg["db"]["storage"]["iops_ceiling"] == 75000.0
    assert cfg["db"]["network"]["bandwidth"] == 1000.0
    assert cfg["llm"]["ingress"] == "fifo"
    init = _events(client, "/live/stream")[0]["data"]
    assert init["config"]["db"]["storage"]["bandwidth_ceiling"] == 0.4
    _wait_done(client)


# --------------------------------------------------------------------- #
# live-fidelity regressions (loop review #1)                             #
# --------------------------------------------------------------------- #

def test_init_event_labels_finished_run_as_done(tmp_path: Path) -> None:
    """``init`` carries the SERVER run status so the page can show REPLAY
    (not LIVE) while playing back a run that already finished."""
    client = TestClient(_app(tmp_path))
    r = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_single.yaml",
              "overrides": {"sim.until": 12}},
    )
    assert r.status_code == 202
    st = _wait_done(client)
    assert st["status"] == "done"
    events = _events(client, "/live/stream")
    assert events[0]["event"] == "init"
    assert events[0]["data"]["status"] == "done"
    assert events[0]["data"]["run_id"] == st["run_id"]


def test_page_state_follows_new_run(tmp_path: Path) -> None:
    """/live/state + a fresh stream always describe the CURRENT run: the
    page's state-poller detects the changed run_id and re-adopts, and the
    new init carries the NEW run's resolved config (scene rebuild source)."""
    client = TestClient(_app(tmp_path))
    a = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_single.yaml",
              "overrides": {"sim.until": 10}},
    ).json()
    _wait_done(client)
    assert client.get("/live/state").json()["run_id"] == a["run_id"]

    b = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_single.yaml",
              "overrides": {"sim.until": 8, "agents.count": 3}},
    ).json()
    assert b["run_id"] != a["run_id"]
    st = client.get("/live/state").json()
    assert st["run_id"] == b["run_id"]          # state follows the server
    assert st["status"] in ("starting", "running", "done", "error")
    # a stream attached after the switch inits with run B's identity + config
    init = _events(client, "/live/stream")[0]["data"]
    assert init["run_id"] == b["run_id"]
    assert init["config"]["agents"]["count"] == 3
    assert init["until"] == 8.0
    _wait_done(client)


def test_counter_rows_reach_db_at_tick_boundaries(tmp_path: Path) -> None:
    """The streaming collector commits samples to ``metrics.db`` at
    ``sample_interval`` tick boundaries — mid-run, without finalize — so
    the page's tail poll sees token counters while the run is live."""
    from agent_db_sim.metrics.collect import RunCollector
    from agent_db_sim.simkit.probes import Sample

    run = tmp_path / "cadence"
    tail = SamplesTail(run / "metrics.db")
    collector = RunCollector(run, {"warmup": 0.0, "sample_interval": 1.0,
                                   "batch_size": 10_000})
    try:
        collector.push(Sample(t=0.0, signal="llm.tokens_input", kind="counter",
                              value=512.0, tags={"component": "llm"}))
        assert [r[1] for r in tail.since()] == [0.0]  # first tick flushes

        collector.push(Sample(t=0.5, signal="llm.tokens_input", kind="counter",
                              value=512.0, tags={"component": "llm"}))
        assert tail.since() == []  # inside the interval: still buffered

        collector.push(Sample(t=1.2, signal="llm.slot_busy", kind="level",
                              value=1.0, tags={"component": "llm"}))
        rows = tail.since()  # tick boundary: 0.5 + 1.2 committed, no finalize
        assert [r[1] for r in rows] == [0.5, 1.2]
        assert rows[0][3] == "counter" and rows[0][4] == 512.0
    finally:
        tail.close()
        collector.abort()


# --------------------------------------------------------------------- #
# run-dir export (the /live EXPORT button)                               #
# --------------------------------------------------------------------- #

def _launch(client: TestClient, until: float = 12) -> str:
    r = client.post(
        "/live/run",
        json={"scenario": "sanity/sanity_single.yaml",
              "overrides": {"sim.until": until}},
    )
    assert r.status_code == 202
    return r.json()["run_id"]


def test_zip_run_dir_walks_dir_and_snapshots_metrics_db(tmp_path: Path) -> None:
    run = tmp_path / "r"
    (run / "nested").mkdir(parents=True)
    (run / "config.yaml").write_text("sim:\n  until: 12\n")
    (run / "nested" / "extra.txt").write_text("anything is archived")
    conn = sqlite3.connect(run / "metrics.db")
    conn.execute("CREATE TABLE samples (id INTEGER PRIMARY KEY, t REAL)")
    conn.execute("INSERT INTO samples (t) VALUES (1.5)")
    conn.commit()
    conn.close()

    archive = tmp_path / "out.zip"
    names = zip_run_dir(run, archive)
    assert set(names) == {"config.yaml", "metrics.db", "nested/extra.txt"}
    with zipfile.ZipFile(archive) as zf:
        assert zf.testzip() is None
        assert b"anything is archived" in zf.read("nested/extra.txt")
        out = tmp_path / "copy.db"
        out.write_bytes(zf.read("metrics.db"))
    conn = sqlite3.connect(out)
    assert conn.execute("SELECT t FROM samples").fetchall() == [(1.5,)]
    conn.close()


def test_export_zip_after_done(tmp_path: Path) -> None:
    """DONE state: the zip carries the full artifact set, db included."""
    client = TestClient(_app(tmp_path))
    run_id = _launch(client)
    _wait_done(client)

    r = client.get("/live/export", params={"run_id": run_id})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    cd = r.headers["content-disposition"]
    assert cd.startswith("attachment;")
    assert f"agent-db-sim-{run_id}.zip" in cd

    zf = zipfile.ZipFile(io.BytesIO(r.content))
    assert zf.testzip() is None
    names = set(zf.namelist())
    assert {"config.yaml", "metrics.db", "run.vcd", "report.html",
            "summary.json"} <= names
    # the archived metrics.db opens and queries as a healthy SQLite copy
    out = tmp_path / "extracted.db"
    out.write_bytes(zf.read("metrics.db"))
    conn = sqlite3.connect(out)
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0] > 0
    conn.close()


def test_export_defaults_to_current_run(tmp_path: Path) -> None:
    client = TestClient(_app(tmp_path))
    run_id = _launch(client, until=8)
    _wait_done(client)
    r = client.get("/live/export")   # no ?run_id= → the server-current run
    assert r.status_code == 200
    assert f"agent-db-sim-{run_id}.zip" in r.headers["content-disposition"]
    assert zipfile.ZipFile(io.BytesIO(r.content)).testzip() is None


def test_export_during_run_is_valid_zip(tmp_path: Path) -> None:
    """EXPORT while the run is STILL executing: valid zip, consistent db."""
    import time

    client = TestClient(_app(tmp_path))
    run_id = _launch(client, until=3000)   # ~2.5 s wall: an export window
    assert client.get("/live/state").json()["status"] == "running"
    live_db = tmp_path / "live" / run_id / "metrics.db"
    t0 = time.time()                        # wait for the writer to land
    while not live_db.exists():
        assert time.time() - t0 < 10.0, "metrics.db never appeared"
        time.sleep(0.05)

    r = client.get("/live/export", params={"run_id": run_id})
    assert r.status_code == 200
    # the run really was mid-flight at click time (or barely finished): still
    # a complete, readable archive either way
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    assert zf.testzip() is None
    assert "metrics.db" in zf.namelist()
    out = tmp_path / "mid.db"
    out.write_bytes(zf.read("metrics.db"))
    conn = sqlite3.connect(out)
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    # the backup API snapshot is queryable even with the writer in flight
    conn.execute("SELECT COUNT(*) FROM samples").fetchone()
    conn.close()
    _wait_done(client)


def test_export_before_any_run_404(tmp_path: Path) -> None:
    client = TestClient(_app(tmp_path))
    r = client.get("/live/export")
    assert r.status_code == 404
    assert "/live" in r.json()["detail"]


def test_export_unknown_and_traversal_run_ids(tmp_path: Path) -> None:
    client = TestClient(_app(tmp_path))
    _launch(client, until=8)
    _wait_done(client)
    assert client.get("/live/export", params={"run_id": "nope"}).status_code == 404
    esc = client.get("/live/export", params={"run_id": "../../etc"})
    assert esc.status_code == 404
