# 04 — Metrics definitions: latencies, utilization, bottleneck attribution

Type: grilling
Status: open
Blocked by:

## Question

What exactly do we measure, and by what rule do we say "component X is the bottleneck"?

- Latency layering: agent-facing latency = queueing + service per hop; breakdown into DB latency vs inference latency; where does the network fit in the DB latency decomposition (compute wait, storage wait, network transfer)?
- Utilization signals: slot occupancy per serving component, aggregate token/s throughput vs cap, queue depth over time, storage bandwidth utilization.
- Percentile methodology: per-request latency distributions (p50/p95/p99) vs time-weighted metrics; warm-up handling; which metrics are recorded per-signal (feeds VCD) vs aggregated (feeds report).
- Bottleneck attribution rule: e.g. highest queueing-delay share in a request's end-to-end path, or component closest to saturation over a window — pick one primary rule, note fallbacks.

Outcome: a metrics spec the metrics-ticket implements and the report ticket renders.
