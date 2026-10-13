# 11 — Metrics collector: latency percentiles, utilization series, attribution

Type: task
Status: resolved
Blocked by: 04, 07

## Question

Implement the metrics layer per the metrics spec from ticket 04:

- Recorders for agent-facing / inference / DB latencies (per-hop decomposition), with percentile computation (p50/p95/p99, warm-up handling per spec).
- Time-series samplers for slot occupancy, token throughput vs cap, queue depths, storage/network utilization — the data both the report and the VCD signal writer consume.
- Bottleneck attribution computed per the chosen rule; output to a common result shape (tables on disk — pick format within the ticket, e.g. parquet/CSV) that the sweep runner and report layer aggregate.

Validated by tests: a fixture run's recorded percentiles and attribution match hand-computed expectations.

## Answer

Built the metrics layer entirely under `src/agent_db_sim/metrics/` as a pure consumer of the probe-sink contract from ticket 04 (no fixed component set; no simkit/helper edits needed — the concurrent ticket 08/09/10 emitters already match the `{component}.{stage}` edge shape). Modules:

- `trace.py` — `TraceWriter`: probe samples → trace rows. A lifecycle event is an `edge` sample tagged `agent_id`+`request_id` (state from the `state` tag, falling back to the sample value; unknown states persist as text). Signal `{component}.{stage}` is split at the last dot, so hierarchical component paths (`top.llm.ingress.ingress_queue`) decompose correctly. Rows are buffered to `batch_size` then flushed via one `executemany` (bounded memory, buffer always `< batch_size`); indexes (agent+request, component+stage, parent_id, t) are built only at `finalize()`. Exports `STATES` (`submitted|queued|processing|completed|received`), `TraceRow`, `sample_to_trace_row`, `split_signal`.
- `derive.py` — percentiles (numpy-style linear interpolation, `percentile()`), time-weighted utilization, hybrid attribution. State-machine rule per (agent, request, component, stage): start at `submitted` (fallback `queued`), queue_delay = `processing − start`, latency = `completed − start`. **Split-stage fallback**: when an emitter splits one hop across sibling stages (the landed DB writer puts submitted/queued in `storage_queue` and processing/completed in `storage_service`), the hop is recovered at component level and reported as stage `*` (full stage machines suppress the fallback — no double counting). Warm-up: requests starting before `t0 + warmup·(t_end−t0)` are excluded from every statistic; raw traces are never pruned.
- `collect.py` — `collect_run(run_dir, sink, cfg) -> Path`: one pass over `sink.samples` (lifecycle → TraceWriter; `level` samples → step functions with `capacity`/`slots` tags noted; `*token*` counters → cumulative→per-tick rates), then finalize + derived passes + `meta` table. Kills any existing `metrics.db` first (rerun-safe). `MetricsConfig.from_any` reads `cfg.metrics.<key>` else `cfg.<key>` else defaults — `warmup=0.1`, `sample_interval=1.0`, `batch_size=2048`; accepts OmegaConf or plain mappings.

Schema (SQLite `runs/<run-id>/metrics.db`):
- `trace(id, agent_id, request_id, parent_id, component, stage, state, t, attrs JSON)` + 4 indexes — source of truth.
- `latency_percentiles(source, kind, p50, p95, p99, n)` — `source` = `e2e` or `component.stage[.*]`; `kind` = `latency | queue`.
- `utilization_series(signal, component, t, value)` — last-value-hold evaluated at each `sample_interval` tick in `(warmup_end, t_end]`.
- `utilization_stats(signal, component, mean, peak, mean_util)` — mean/peak integrated time-weighted over `[warmup_end, t_end]`; `mean_util` = value/capacity when a capacity tag exists, else nonempty-queue time fraction for `queue_depth` signals, else NULL.
- `token_rate_series(component, t, rate)` — tokens/time unit per tick interval (supports "throughput vs cap": divide by the server's cap).
- `bottleneck(component, stage, queue_delay, queue_share, sat_score, score)` — hybrid: primary queueing-delay share of agent-facing e2e latency (denominator falls back to summed stage latency when no `received` rows), supporting component saturation score; `score = 0.7·queue_share + 0.3·sat_score`, sorted descending.
- `meta(key, value)` — `warmup_end`, `t_end`.

API: `collect_run(run_dir, sink, cfg)`; it is the only entry point the orchestrator needs to wire (tiny additive helper in simkit was not necessary). Report/sweep layers `import sqlite3` and query the tables directly.

Not built here: parquet export (deferred per ticket 04), VCD signal writing (viz ticket), the orchestrator's CLI hook.

Validation: 13 fixture tests in `tests/test_metrics_collect.py` (synthetic sink samples only, no dependency on tickets 08–10): trace schema + state machine + signal decomposition + parent_id/json attrs round-trip; batching respects the buffer cap (asserted `≤ batch_size` after every sample) and completes all 50 rows; percentiles hand-checked ([1..5] → p50=3, p95=4.8, p99=4.96; e2e [3..11] → 7 / 10.6 / 10.92); warm-up exclusion (boundary request dropped, n=1, traces untouched); time-weighted utilization integrated correctly on hand-built step series (7/12 mean over [0,12], tick-sampled series rows, capacity-normalized mean_util, queue-depth nonempty fraction fallback); token pacing 10 tok / 5 tu → 2.0 rate; attribution picks the hand-designed DB bottleneck (queue share 33/42 vs 6/42, sat 0.5 vs 0.1, ranked first); empty-sink and rerun safety. Full suite at time of resolution: metrics tests 13/13 green; 85 passed overall, 11 failures all from concurrent tickets' in-flight files (`test_agent_client.py`, `test_components_llm_endpoint.py`, `test_db_endpoint.py`) plus one stale ticket-07 test (`test_build_model_is_a_skeleton`) that ticket 10's landed `build_model` superseded — none in metrics code.
