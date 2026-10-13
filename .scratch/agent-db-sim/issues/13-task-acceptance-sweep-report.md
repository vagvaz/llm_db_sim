# 13 — Acceptance: topology sweep produces bottleneck report

Type: task
Status: resolved
Blocked by: 06, 08, 09, 10, 11, 12

## Question

Close the destination bar: run the full analysis loop once.

- A factorial sweep config comparing the three topologies (and at least one scale-out variant) across a small parameter set (e.g. agent count × inference profile × DB profile mix).
- Execute via `agent-db-sim` CLI (sweep runner), collect metrics for all runs.
- Produce the report: per-run latency percentiles (agent/DB/inference), utilization series, bottleneck attribution, scenario comparison — plus VCD output viewable in the viz server.
- Demonstrate the destination's analytical question in miniature: which component bottlenecks first per topology, and what scale-out changes.

This is the definition of done for the map; when it passes, the way to the destination is walked.

## Answer

The analysis loop is closed: `agent-db-sim run` / `sweep` both execute the full pipeline — `build_model` (AgentClient + LLMEndpoint + DBEndpoint wired end to end) → simulate → `metrics.db` (collect_run) + `run.vcd` (viz VcdSampleWriter, same shared sink) → standard `report.html` → `summary.json`.

### Integration shape

- **`components/assembly.py` (new glue)**: `wire_agent_llm_db(top, agents, llm, db)` records the four contract links in the top composite's connect graph (agent inference_requests→llm requests, llm responses→agent inference_responses, db responses→agent db_responses) and binds the runtime transport of ticket 08's contract: the agent's `bind_output` handlers call `llm.submit(req)` / `db.submit(req)` and spawn a per-request watcher process that yields the serving-side completion and `agent.deliver(port, shaped_req)`s the response into the waiting agent's rendezvous box (received t == serving completion t, by construction). The agent's `db_queries` port is declared kind `req` (ticket 08) while DBEndpoint's `queries` input is kind `query` (ticket 10); a small declarative `QueryAdapter("queries_bridge")` node bridges the two kind vocabularies in the connect graph (transport is unchanged: the agent's Request flows straight to `db.submit`) — no other ticket's files were edited.
- **`simkit/registry.py` `build_model`**: builds `top.db` always; when `agents:` is in the config it adds `top.agents` + `top.llm` (+ `top.queries_bridge`) and wires them (shared `top.probe_sink`, also handed to `run_model`/collect_run/write_vcd). `db:`-only configs keep the ticket-10 stub-caller contract, and the missing-`db:` RegistryError stays (ticket 10's `test_build_model_requires_db_section` supersedes the stale `test_build_model_is_a_skeleton`, no further fix needed).
- **`pipeline.py` (new)**: `execute_run(cfg, run_dir)` = config snapshot → simulate until `sim.until` → `collect_run` → `write_vcd` → `render_report("standard")` as `report.html` (+ vendored plotly) → `summary.json` (condensed destination numbers: e2e percentiles, ranked hybrid attribution, utilization overview, config/counts digest). `enumerate_sweep` reuses simkit's sweep enumeration (`read_sweep`/`apply_factors`/`run_id_for`, same `runs/<name>/<run-id>/` layout) and writes per-run full artifact sets + `sweep_summary.json` (+ per-run comparison table in `sweep_summary.html`).
- **`cli.py`**: `run <scenario> --out runs` → full pipeline when the config has `db:` (placeholder `loop:`-only scenarios keep ticket 06's smoke loop; CLI smoke tests untouched); `sweep` routes db-scenarios through `enumerate_sweep` and prints a per-run `e2e p95 / bottleneck winner` line each.
- **simkit hoist**: the ticket-10 fp-tolerant container moved to `simkit/levels.py::TolerantContainer` and `BandwidthPool` uses it as the default (exactly-representable volumes unaffected); DB's `NetPool` is now a plain alias subclass.
- **`scenarios/topology_compare.yaml` (new)**: the acceptance scenario. LLM generously provisioned (8 slots, cap 6000 tok/s); agents 8 closed-loop (fixed 256/128 tokens; mix 80% point reads (c=0.2, v=0.05) / 20% scans (c=0.6, v=2.0)); DB per node: 2 slots, nvme, 3-phase, net latency 0.01/bandwidth 500, coordinator overhead ×1.2. Sweep factors: `db.topology ∈ {centralized, disaggregated, distributed} × db.compute_nodes ∈ {4, 8} × agents.count ∈ {8, 16}` = 12 runs. Profiles: `agents_16`, `llm_tight` (starve the LLM: slots 2, rates 1500/750 → llm queue-share rises 0 → 17.5%, attribution responds).

### CLI usage

```
agent-db-sim run   scenarios/topology_compare.yaml            # runs/topology_compare/{config.yaml,metrics.db,run.vcd,report.html,summary.json}
agent-db-sim sweep scenarios/topology_compare.yaml            # runs/topology-sweep/<run-id>/... + sweep_summary.{json,html}
agent-db-sim serve runs/topology_sweep/0004-.../              # VCD waveforms + reports (served: verified /signals=34 signals)
agent-db-sim render <run-dir>                                 # standalone report (pipeline already emits report.html)
```

### Acceptance numbers (until=200, warmup 0.1 → 180 u window; ~1.8k–5.4k sampled requests per run)

e2e agent-facing latency p50/p95/p99 (tu), bottleneck winner (hybrid attribution, primary = queueing-delay share):

| run | agents | nodes | e2e p50 / p95 / p99 | n | bottleneck winner (score, queue share) |
|---|---|---|---|---|---|
| centralized ×4 | 8 | N=4 | 0.128 / **2.195** / 2.882 | 1850 | db compute-0.compute_issue (0.468, **66.8 %**) |
| centralized ×8 | 8 | N=8 (no-op) | 0.128 / **2.195** / 2.882 | 1850 | same, **identical** — scale-out is structurally a no-op |
| centralized ×4 | 16 | N=4 | 0.128 / **4.042** / 4.651 | 1840 | db compute-0 (0.584, **83.4 %**) |
| disaggregated ×4 | 8 | N=4 | 0.230 / 1.199 / 1.769 | 4304 | db compute-2 (0.019, 2.7 %) |
| disaggregated ×8 | 8 | N=8 | 0.230 / 1.284 / 1.774 | 4393 | shared segment net_req (0.011, 1.5 %) |
| disaggregated ×4 | 16 | N=4 | 0.371 / 1.803 / 2.576 | 5009 | db compute-3 (0.058, 8.3 %) |
| disaggregated ×8 | 16 | N=8 | 0.398 / **1.512** / 2.445 | 5420 | llm (0.046, 6.6 %) — compute relieved to the LLM token cap |
| distributed ×4 | 8 | 1 coord + 3 peers | 0.202 / **1.586** / 2.052 | 2925 | coordinator node-0.compute_issue (0.146, 20.9 %) |
| distributed ×8 | 8 | 1 coord + 7 peers | 0.207 / 1.556 / 2.219 | 3011 | coordinator node-0 (0.170, 24.2 %) — peer scale-out does **not** relieve: entry capacity is coordinator slots |
| distributed ×4 | 16 | 1 + 3 | 0.219 / 3.121 / 4.135 | 3001 | coordinator node-1 (0.316, 45.1 %) |
| distributed ×8 | 16 | 1 + 7 | 0.214 / 3.336 / 4.265 | 3030 | coordinator node-1 (0.429, **61.4 %**) |

**The destination's question, answered by the numbers:**

1. **Centralized bottles first** — the single entry compute node is the dominant share (66.8 % queue share at 8 agents → 83.4 % at 16 agents, e2e p95 2.195 → 4.042); its service time p50 is ~0.98 u vs ~0.04 u per node on disaggregated.
2. **Disaggregation relieves compute; network + storage hops become visible** — per-hop additivity appears in the tables: net_req p50 0.01002, net_resp 0.0101 (latency) + staged transfer, storage service stretches to p95 ≈ 0.7 u under scan contention; queue shares collapse to single digits.
3. **Scale-out changes the bottleneck** — disagg N=4→8 at 16 agents drops e2e p95 1.803 → 1.512 and the winner migrates from DB compute queues to the shared segment / the LLM aggregate token cap (6000 tok/s, llm service p50 stretched to ~0.175 u at 8 agents): the DB compute queue was the relieved target. With `llm_tight`, the LLM becomes a first-class queue (0 → 17.5 % share) immediately behind the DB.
4. **Distributed adds coordinator overhead and re-centralizes** — the coordinator's ×1.2 compute multiplier plus per-peer network shows in coordinator compute latencies (p50 ≈ 0.33–0.40 u), e2e p95 ≈ 1.56–1.59 u at 8 agents (vs 1.2–1.28 disagg/distributed-equivalent), and external queries all enter through coordinator slots: growing peers 3 → 7 (still 1 coordinator) does not relieve the entry bottleneck — the winner stays at the coordinator and grows with demand (20.9 % → 24.2 % share, and 45 % → 61 % at 16 agents).

**Artifact validation**: every sweep run dir carries the same set (`config.yaml`, `metrics.db`, `run.vcd`, `report.html`, `summary.json`, plotly.min.js). Spot-checks: `metrics.db` percentiles populated for e2e + every recorded hop (e.g. disagg run: llm.*, per-node compute_issue/segment net_* per-node storage-*), `bottleneck` non-empty, `utilization_stats` covers llm/DB servers + net segment queue depths, traces >100k rows in the 16-agent runs; `run.vcd` parses with own reader (34 signals incl. per-agent `inference_state`/`db_state`, llm 4-state machine, DB node stages; ~1.1 MB per sweep run); `report.html` renders (plotly + `fig-*`, ~40 KB); the viz server serves the run (`/signals`, `/reports/standard` verified on a real pipeline run dir).

### Notes / small deviations surfaced

- **Attribution `sat_score` is 0 in this integration** (score is queue-share-only): ticket 11's `derive_bottleneck` looks up saturation by trace component name, while `utilization_stats` rows are keyed by the probe-sink component tag (e.g. `top.llm.server`, and DB compute's `slot_busy` samples are tagged with the bare node name `compute-0`). Queue share (the primary signal) is unaffected; a later pass can join on normalized component prefixes. Left as-is per scope (metrics internals unchanged).
- **Network-hop queue labeling**: NetLink records its propagation latency inside `net_req`/`net_resp` queue delays (emitters landed that way in ticket 10); visible as the 0.01 p50 "queue" on net_* rows. Out of scope here.
- `simkit/sweep.py:102` has a pre-existing lazy-annotation `Optional` reference (ruff F821) — works at runtime (`from __future__ import annotations`); flagging for its owner, not touched.

**Validation**: `uv run pytest` → **133 passed** (124 baseline + 9 new `tests/test_integration_pipeline.py`), `ruff check` clean on all touched files; acceptance executed from the repo root (`runs/topology-compare`, `runs/topology-sweep`, both reproducible). The map's destination bar is walked.
