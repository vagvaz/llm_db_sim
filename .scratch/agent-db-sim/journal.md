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

**08 task (resolved).** AgentClient: N config-driven agents, closed loop inference→token generator→QueryProfile DB query; rendezvous ingress for exact response routing; named seeded RNG streams per agent; distributions registry + weighted profile mixes. 14 tests green. fix-3.

**09 task (resolved).** LLMEndpoint: two-phase input/output token service on SlotServer; slots + fair-shared aggregate token/s cap; submit/wait/response_of API; lifecycle edges + counters/occupancy probes. 16 tests green. fix-4.

**11 task (resolved).** Metrics collector: batched SQLite TraceWriter (bounded buffer, indexes at finalize); derived percentile/utilization/token-rate/hybrid-attribution tables at collect_run(); warm-up + sample_interval config; component-level stage fallback for split hops. 13 tests green (59 slice tests green post-landing). fix-6.

**10 task (resolved).** DBEndpoint: components/db/ tree — three topologies, per-link/segment/none networks, StorageBackend presets (hdd/ssd/nvme) with ceilings, PhaseSlotServer (compute slot held through storage round-trip; flagged: base SlotServer cost_fn can't express wrapped hops), coordinators + overhead multiplier, scale-out N/M. Two flagged findings: (1) simpy Container fp-precision wedge on 0.01 transfers — DB-local tolerant container subclass, noted for simkit fix; (2) PhaseSlotServer subclasses SlotServer overriding _serve only. 20 tests; full suite 101 green. fix-5.

**Stage 1 complete (components).** Tickets 08–11 all landed, committed (`296a2b1`, `0ccb9d1`, `08686e7`, `a4335b0`), map indexed, suite 101 green. Loop attempt 1 successful.

**Stage 2 dispatched (viz stack).** Ticket 12 → fix-6 (reused): VCD writer + FastAPI/SSE/d3-wave live server + Jinja2/Plotly reports on fixtures; integration with build_model (agent/LLM stub wiring) deferred to ticket 13.

**12 task (resolved).** Viz stack: viz/{vcd,server,report,fixture}.py; vendored d3-wave/plotly static assets (offline-first); SSE init/delta/keepalive/done with safe-tail; serve/render CLI; fixture factory for standalone tests. 23 tests; suite 124 green. fix-6. Committed `3061e3b`.

**Stage 3 dispatched (integration + acceptance).** Ticket 13 → fix-5 (reused, owns build_model): full agent/LLM/DB wiring, run dir pipeline (metrics.db + run.vcd + report), topology sweep acceptance demo.
