# Map: agent-db-sim simulation platform

## Destination

A working simulation platform in this repo: a SimPy-based discrete-event simulator with the three-component model (AgentClient, LLMEndpoint, DBEndpoint = DBStorage + DBCompute + internal network), configured via typer + OmegaConf profiles, that runs single scenarios and factorial sweeps across the three DB topologies (centralized / disaggregated / distributed, with scale-out knobs), and produces agent-facing DB latency percentiles, inference latency, per-component utilizations, and bottleneck attribution — plus live VCD signal output served to a browser visualization server with expandable template reports.

## Notes

- Domain: discrete-event simulation of an LLM-agent workload against a database. Python 3.10 target (repo currently pins 3.13 — retarget as part of the skeleton ticket). uv build.
- Framework: **SimPy locked** (desmod research: dormant since 2021, it is scaffolding over SimPy, not an LLM/DB simulator). The desmod capabilities we want become a project-local **simkit** layer: config namespaces, factorial sweep runner (`simulate_factors` analogue), probes/tracking, runtime component input/output validation.
- Standing modeling preferences (settled during charting):
  - All "processing" is a wait-time event computed from parameters — true for agents, inference, and DB.
  - Closed-loop agents for iteration 1: alternates LLM call → DB query, with a token generator feeding the DB hit; N agents configurable. Open-loop/time-varying is later.
  - Serving components (LLMEndpoint, DBCompute, …) have configurable slot capacity **and** aggregate token/s limits; they queue when saturated.
  - Queries are parameterized by complexity (compute time) × volume (data), drawn from configurable profile mixes; richer query workflows later.
  - Topologies: centralized (compute+storage in one machine), disaggregated (N compute × M storage + network), distributed (equal counts, networked exchange); scale-out = growing N or M on the disaggregated topology.
  - CLI: typer; config: OmegaConf with named profiles; factorial sweeps required.
  - Visualization: real VCD files, live; a dedicated visualization server (browser view served by a server, not static HTML); standard report templates via Jupyter-notebook-style technology, user-expandable.
- Skills every session should consult: `grilling` + `domain-modeling` on decision tickets; `research` on research tickets.
- This map carries execution itself (destination is a working platform): `task` tickets are build slices.
- Tracker: local-markdown — tickets at `.scratch/agent-db-sim/issues/NN-<slug>.md`. Claim = `Status: claimed`. Resolve = append `## Answer`, set `Status: resolved`, then append a context pointer here under Decisions so far. Concurrency: map edits are single-writer per reconciliation; check the file before appending.

## Decisions so far

<!-- one line per closed ticket: [ticket title](issues/NN-slug.md): gist of the answer -->

- [02 — Template report technology](issues/02-research-report-template-technology.md): Jinja2 + Plotly rendered server-side as a route on the viz server; optional `.ipynb` kind via nbclient later.
- [01 — Live VCD viewing in a browser](issues/01-research-browser-vcd-live-viewing.md): FastAPI + SSE tailing pipeline with d3-wave renderer for live; Surfer WASM link for deep-dive; event-driven VCD, safe-tail boundary server-side.
- [03 — Core architecture](issues/03-grilling-core-architecture.md): two-phase construct→elaborate(validate)→start lifecycle; typed ports; ingress (none/fifo/priority) feeding a slots+cost_fn processing primitive with separate queue/service timing; explicit build_model + registry dicts; named RNG child streams; sweep = dotted-path factors → run dirs.
- [05 — DB network & storage fidelity](issues/05-grilling-db-network-fidelity.md): per-topology network (centralized none / disaggregated shared segment / distributed per-node links via coordinators, queries round-robined); compute wraps storage with configurable issue_ratio split; StorageBackend = base_latency+per_gb_time+ceilings, presets; scale-out = round-robin compute, striped storage.
- [06 — Project skeleton](issues/06-task-skeleton-cli-config.md): Python 3.10, typer+OmegaConf skeleton with smoke scenario; tests pass.
- [07 — simkit kernel](issues/07-task-simkit-kernel.md): Component lifecycle (construct→elaborate→start→collect) + named RNG streams; typed ports validated at elaboration; ingress (none/fifo/priority) → SlotServer (slot pool, cost_fn, shared aggregate token cap) with queue/service records; Container BandwidthPool; sweep runner `run_sweep` + CLI `sweep` writes run dirs; ProbeSink API + in-memory sink; `build_model` stub; 40 tests pass.

## Not yet specified

- Staged / multi-query workflows and more sophisticated agent models (the "query workflows" the user deferred).
- Open-loop and time-varying workloads (changing agent behavior over simulated time).
- Extending the component model beyond the initial three components as fidelity grows.
- Which experiment matrices to run once the platform works (sweep designs, scale-out studies, profile mixes).

## Out of scope

- Nothing ruled out yet; anything that surfaces past the destination bar gets closed and logged here with its gist.
