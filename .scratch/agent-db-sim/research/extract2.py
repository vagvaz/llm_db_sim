#!/usr/bin/env python3
"""Extract per-run study-2 metrics from runs/sensitivity2/*/ into one JSON.

Usage: .venv/bin/python .scratch/agent-db-sim/research/extract2.py
Writes: runs/sensitivity2/study2_extract.json
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "runs/sensitivity2"
DBFS = ("centralized", "disaggregated", "distributed")


def pct_row(conn, src_like, kind):
    rows = conn.execute(
        "SELECT source, p50, p95, p99, n FROM latency_percentiles "
        "WHERE source LIKE ? AND kind=?",
        (src_like, kind),
    ).fetchall()
    return rows


def run_metrics(run_dir: Path) -> dict:
    conn = sqlite3.connect(f"file:{run_dir/'metrics.db'}?mode=ro", uri=True)
    summary = json.loads((run_dir / "summary.json").read_text())
    until = sqlite3.connect(f"file:{run_dir/'config.yaml'}?mode=ro", uri=True) if False else None
    out = {}

    # config digest from summary (written by the pipeline)
    cfg = summary
    out["sweep"] = run_dir.parent.name
    out["run_id"] = run_dir.name
    out.update(
        {
            "agents": cfg.get("agents.count"),
            "llm_slots": cfg.get("llm.slots"),
            "topology": cfg.get("db.topology"),
            "compute_nodes": cfg.get("db.compute_nodes"),
            "storage_nodes": cfg.get("db.storage_nodes"),
            "coordinators": cfg.get("db.coordinators"),
        }
    )
    # caps + backend from the run's config.yaml snapshot
    import re

    text = (run_dir / "config.yaml").read_text()

    def cfg_val(pat):
        m = re.search(pat, text, re.M)
        return m.group(1) if m else None

    out["caps_in"] = float(cfg_val(r"^\s*input_tokens_per_s:\s*([\d.]+)"))
    out["caps_out"] = float(cfg_val(r"^\s*output_tokens_per_s:\s*([\d.]+)"))
    m = re.search(r"^\s*backend:\s*(\S+)", text, re.M)
    out["backend"] = m.group(1) if m else "ssd_enterprise"
    rungs = {(4000.0, 1000.0): 0.5, (8000.0, 2000.0): 1.0, (16000.0, 4000.0): 2.0,
             (32000.0, 8000.0): 4.0, (64000.0, 16000.0): 8.0, (128000.0, 32000.0): 16.0}
    out["rung"] = rungs.get((out["caps_in"], out["caps_out"]))

    counts = cfg
    out["inference_completed"] = counts.get("inference_completed")
    out["db_completed"] = counts.get("db_completed")
    out["goodput_per_100u"] = counts.get("db_completed", 0) / 6.0

    e2e = cfg.get("e2e_percentiles") or {}
    out["e2e"] = {k: e2e.get(k) for k in ("p50", "p95", "p99", "n")}

    bw = cfg.get("bottleneck_winner") or {}
    out["winner"] = {k: bw.get(k) for k in ("component", "stage", "score", "queue_share")}

    # llm service/queue percentiles
    lat = pct_row(conn, "llm.%", "latency")
    if lat:
        r = lat[0]
        out["llm_lat"] = {"p50": r[1], "p95": r[2], "p99": r[3], "n": r[4]}
    q = pct_row(conn, "llm.%", "queue")
    if q:
        out["llm_queue_p95"] = q[0][2]

    # db compute stages: all compute_issue latency/queue rows (per node)
    comp_lat = pct_row(conn, "%compute_issue", "latency")
    out["db_compute"] = sorted(
        [
            {"node": r[0], "p50": r[1], "p95": r[2], "p99": r[3], "n": r[4]}
            for r in comp_lat
        ],
        key=lambda d: -d["n"],
    )
    out["db_compute_p95_max"] = max([d["p95"] for d in out["db_compute"]], default=None)
    cq = pct_row(conn, "%compute_issue", "queue")
    out["db_queue_p95_max"] = max([r[2] for r in cq], default=None)

    # storage stages
    st_lat = pct_row(conn, "%storage%", "latency")
    out["db_storage_p95_max"] = max([r[2] for r in st_lat], default=None)
    out["db_storage_rows"] = len(st_lat)

    # network stages (segment/link transfers)
    net_lat = [
        r for r in conn.execute(
            "SELECT source, kind, p50, p95, p99, n FROM latency_percentiles "
            "WHERE (source LIKE '%segment%' OR source LIKE '%link%') AND kind='latency'"
        )
    ]
    out["net_rows"] = [
        {"source": r[0], "kind": r[1], "p50": r[2], "p95": r[3], "n": r[5]} for r in net_lat
    ]
    out["net_lat_p50_sum"] = sum(r[2] for r in net_lat)

    # utilizations: slot_busy per component + token rates
    utils = {}
    for sig, comp, mean, peak, _mu in conn.execute(
        "SELECT signal, component, mean, peak, mean_util FROM utilization_stats"
    ):
        utils[f"{sig}@{comp}"] = (mean, peak)
    out["llm_tokens_in_rate"] = utils.pop("llm.tokens_input.rate@", (None, None))[0]
    out["llm_tokens_out_rate"] = utils.pop("llm.tokens_output.rate@", (None, None))[0]
    slot = {k: v for k, v in utils.items() if k.startswith("slot_busy@")}
    out["llm_slot_busy_mean"] = slot.pop("slot_busy@llm.server", (None, None))[0]
    db_nodes = {}
    for k, (mean, peak) in slot.items():
        node = k.split("@")[1]
        db_nodes[node] = {"slot_busy_mean": mean, "peak": peak}
    out["db_nodes"] = db_nodes
    # db compute utilization fraction = mean slot_busy / 4 slots
    if db_nodes:
        out["db_util_mean"] = sum(v["slot_busy_mean"] for v in db_nodes.values()) / (
            4.0 * len(db_nodes)
        )
    conn.close()
    return out


def main() -> None:
    rows = []
    for sweep_dir in sorted(OUT.iterdir()):
        if not sweep_dir.is_dir() or sweep_dir.name == "study2_extract.json":
            continue
        for run_dir in sorted(sweep_dir.iterdir()):
            if not (run_dir / "metrics.db").exists():
                continue
            rows.append(run_metrics(run_dir))
    (OUT / "study2_extract.json").write_text(json.dumps(rows, indent=1) + "\n")
    print(f"extracted {len(rows)} runs -> {OUT/'study2_extract.json'}")


if __name__ == "__main__":
    main()
