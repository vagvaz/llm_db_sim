# 11 — Metrics collector: latency percentiles, utilization series, attribution

Type: task
Status: open
Blocked by: 04, 07

## Question

Implement the metrics layer per the metrics spec from ticket 04:

- Recorders for agent-facing / inference / DB latencies (per-hop decomposition), with percentile computation (p50/p95/p99, warm-up handling per spec).
- Time-series samplers for slot occupancy, token throughput vs cap, queue depths, storage/network utilization — the data both the report and the VCD signal writer consume.
- Bottleneck attribution computed per the chosen rule; output to a common result shape (tables on disk — pick format within the ticket, e.g. parquet/CSV) that the sweep runner and report layer aggregate.

Validated by tests: a fixture run's recorded percentiles and attribution match hand-computed expectations.
