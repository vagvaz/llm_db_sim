# Decision journal — agent-db-sim wayfinding

Chronological log of orchestration decisions and stage completions. Tickets hold detail; this is the running history.

## 2026-10-13

**Charting (map created).** Destination: working SimPy-based simulation platform (3-component model, typer+OmegaConf, factorial sweeps, 3 DB topologies + scale-out, metrics + live VCD viz server + template reports). Framework research: desmod = dormant (2021) SimPy sugar → **SimPy locked** + project-local simkit replicating desmod's scaffolding (config, sweeps, probes, I/O validation). Tracker: local-markdown `.scratch/agent-db-sim/`. Python target 3.10 (user) — repo retargeted from 3.13.

**01 research (resolved).** No browser viewer streams VCD live. Architecture: FastAPI + SSE tailing pipeline, d3-wave renderer (live), Surfer WASM link (deep-dive), event-driven VCD with safe-tail boundary server-side, pyvcd writer.

**02 research (resolved).** Report layer: **Jinja2 + Plotly rendered server-side** as routes on the viz server; optional `.ipynb` kind via nbclient later. (Parquet/CSV assumption later superseded by 04's SQLite.)

**06 task (resolved).** Skeleton: pyproject ≥3.10, typer CLI `run` with OmegaConf profile overlay, package layout simkit/components/metrics/viz, smoke tests. fix-1.

**03 grilling (resolved).** Architecture contract: two-phase lifecycle construct→elaborate(validate)→start; typed ports; **ingress (none/fifo/priority) ∥ processing (slots + cost_fn)** with separate queue/service timing; explicit `build_model` + registry dicts; per-component named RNG child streams; sweep = dotted-path factors → `runs/<sweep>/<run-id>/`. Glossary ratified (Scenario vs QueryProfile).

**05 grilling (resolved).** Per-topology network: centralized = none; disaggregated = shared storage-network segment; distributed = coordinator-fronted per-node links (queries round-robin to coordinators, no coordinator↔coordinator links). Compute wraps storage (slot held), `issue_ratio` default 0.1 configurable; `coordinator_overhead` multiplier; StorageBackend = base_latency + per_gb_time + ceilings, presets; scale-out = round-robin compute, striped storage.

**07 task (resolved).** simkit kernel: full primitive set per 03 contract; 40 tests green. fix-2.

**04 grilling (resolved).** Metrics: per-run **SQLite** `metrics.db` (over parquet — mid-run batched appends); **request lifecycle traces** (`agent_id`+`request_id` end-to-end, states submitted|queued|processing|completed|received, parent_id sub-traces) as source of truth; derived tables (percentiles, time-weighted utilization, **hybrid attribution**: queue-delay share primary, saturation score supporting) at collect(); warm-up fraction 0.1; VCD = event-driven raw signals, tables = tick-sampled series. Last decision ticket — decision layer closed.

**Git.** Repo initialized: master + 2 research branches pushed to github.com:vagvaz/llm_db_sim.

**Stage: parallel component build (in progress).** Dispatched fix-3 (08 AgentClient), fix-4 (09 LLMEndpoint), fix-5 (10 DBEndpoint, reuses fix-2), fix-6 (11 Metrics collector). Guards: shared wire contract pinned in every brief (ports, attrs, trace-as-edge-probe-samples contract from 04); single-writer ownership (10 owns registry.py/build_model; others never touch simkit); no map.md edits by workers (orchestrator reconciles); no worker commits (orchestrator commits per completed ticket/stage). Commit policy: one commit per resolved ticket; push per stage.
