# 09 — LLMEndpoint: slots + aggregate token throughput

Type: task
Status: open
Blocked by: 07

## Question

Implement the LLMEndpoint component on the simkit server primitive:

- Configurable **slot capacity** (concurrent requests) **and aggregate tokens/s** cap (input processing rate + output generation rate), queue-when-saturated semantics per settled preferences.
- Serves AgentClient inference requests parameterized by input/output token counts: input tokens processed first, output tokens generated (streamed?) second — settle streaming vs post-hoc within the ticket's bounds (architectural default from ticket 03).
- Per-request latency records + utilization/throughput/queue-depth signals.

Validated by tests: saturation behavior (slots full → queue; aggregate cap → service time scales with token counts) under a seeded scenario.
