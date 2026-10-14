"""Tests for the /live animated view: launch guards, SSE delta shape, tail.

Endpoint tests avoid real subprocess waits where possible; one integration
test launches the tiny ``sanity_single`` scenario (finishes in ~1 s wall)
and drives the whole live loop: POST -> stream -> done -> report artifacts.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from agent_db_sim.viz.live import LiveBusy, LiveManager, SamplesTail
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
