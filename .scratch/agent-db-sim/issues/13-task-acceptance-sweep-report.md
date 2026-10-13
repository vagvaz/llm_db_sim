# 13 — Acceptance: topology sweep produces bottleneck report

Type: task
Status: open
Blocked by: 06, 08, 09, 10, 11, 12

## Question

Close the destination bar: run the full analysis loop once.

- A factorial sweep config comparing the three topologies (and at least one scale-out variant) across a small parameter set (e.g. agent count × inference profile × DB profile mix).
- Execute via `agent-db-sim` CLI (sweep runner), collect metrics for all runs.
- Produce the report: per-run latency percentiles (agent/DB/inference), utilization series, bottleneck attribution, scenario comparison — plus VCD output viewable in the viz server.
- Demonstrate the destination's analytical question in miniature: which component bottlenecks first per topology, and what scale-out changes.

This is the definition of done for the map; when it passes, the way to the destination is walked.
