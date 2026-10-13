# 04 — Metrics definitions: latencies, utilization, bottleneck attribution

Type: grilling
Status: resolved
Blocked by:

## Question

What exactly do we measure, and by what rule do we say "component X is the bottleneck"?

- Latency layering: agent-facing latency = queueing + service per hop; breakdown into DB latency vs inference latency; where does the network fit in the DB latency decomposition (compute wait, storage wait, network transfer)?
- Utilization signals: slot occupancy per serving component, aggregate token/s throughput vs cap, queue depth over time, storage bandwidth utilization.
- Percentile methodology: per-request latency distributions (p50/p95/p99) vs time-weighted metrics; warm-up handling; which metrics are recorded per-signal (feeds VCD) vs aggregated (feeds report).
- Bottleneck attribution rule: e.g. highest queueing-delay share in a request's end-to-end path, or component closest to saturation over a window — pick one primary rule, note fallbacks.

Outcome: a metrics spec the metrics-ticket implements and the report ticket renders.

## Answer

**Metrics store:** per-run SQLite `runs/<run-id>/metrics.db` (append-friendly mid-run; parquet export deferred — chosen over parquet for batched appends). Supersedes the parquet/CSV assumption in ticket 02's answer; pandas reads SQLite for the report layer.

**Source of truth — request lifecycle traces:** every request carries `agent_id` + `request_id` through the entire system; each state transition is one row `(agent_id, request_id, parent_id, component, stage, state, t, attrs)`; canonical states `submitted | queued | processing | completed | received` (`received` = agent observed the result). Hops (inference, db compute, network, storage) are sub-trace rows linked via `parent_id`. Memory stays bounded on long runs: events batched, flushed to SQLite at N-events-or-tick boundaries; indexes built at `collect()`.

**Derived metrics (precomputed at collect(), stored alongside traces):**
- Latency percentiles (p50/p95/p99) per stage and end-to-end — per-request distributions.
- Time-weighted utilization series (slots, bandwidth, token throughput vs cap) — tick-boundary samples at configurable `sample_interval`.
- Warm-up: configurable `warmup` fraction (default 0.1) excluded from all statistics.
- Bottleneck attribution: **hybrid** — primary = queueing-delay share of end-to-end agent latency; supporting = saturation score (utilization→1 + queue growth).

**VCD vs tables:** VCD = event-driven, tick-quantized raw signals (queue depths, slot occupancy, token counters, request-state enums — catalog from ticket 01); tables = sampled time-weighted series for the report.

**Canonical stage taxonomy (per-request):** `agent_queue → inference[ingress_queue → input_proc → output_gen] → db[ingress_queue → compute_issue → net_req → storage_queue → storage_service → net_resp → compute_finalize]`; centralized skips net_*; coordinator overhead folds into compute stages on the coordinator node.
