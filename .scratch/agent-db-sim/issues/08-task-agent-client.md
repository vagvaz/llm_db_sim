# 08 — AgentClient: closed-loop workload model

Type: task
Status: open
Blocked by: 07

## Question

Implement the AgentClient component for iteration 1:

- Closed loop per agent: **LLM call → token generator produces output tokens → DB query → repeat**, running constantly for the configured simulated duration.
- N agents configurable via config profile.
- Query drawn from a configurable complexity × volume profile mix; inference request parameterized by input/output token counts (output count from the token generator).
- Every step is a wait-time event per the shared primitive; emits latency records (agent-facing, inference, DB) and signal samples for VCD.

Validated by tests: a scripted single-agent scenario produces the expected request sequence and latency records with a seeded RNG.
