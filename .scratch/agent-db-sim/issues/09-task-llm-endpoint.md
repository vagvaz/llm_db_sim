# 09 — LLMEndpoint: slots + aggregate token throughput

Type: task
Status: resolved
Blocked by: 07

## Question

Implement the LLMEndpoint component on the simkit server primitive:

- Configurable **slot capacity** (concurrent requests) **and aggregate tokens/s** cap (input processing rate + output generation rate), queue-when-saturated semantics per settled preferences.
- Serves AgentClient inference requests parameterized by input/output token counts: input tokens processed first, output tokens generated (streamed?) second — settle streaming vs post-hoc within the ticket's bounds (architectural default from ticket 03).
- Per-request latency records + utilization/throughput/queue-depth signals.

Validated by tests: saturation behavior (slots full → queue; aggregate cap → service time scales with token counts) under a seeded scenario.

## Answer

Built `src/agent_db_sim/components/llm_endpoint.py` — `LLMEndpoint` on the simkit `Component` base, serving on the kernel's `SlotServer` exactly per the wired contract:

**Config keys** (`llm:` namespace; also readable at top level when the section is absent, and a `cfg_section=` constructor arg is accepted for the `build_model`-style convention):
- `slots` — slot capacity, concurrent requests (default 4; `ComponentError` if < 1)
- `input_tokens_per_s` — input-phase rate (required, > 0)
- `output_tokens_per_s` — output-phase rate (required, > 0)
- `ingress` — any simkit ingress spec, default `"fifo"`
- `quantum` — token-cap pacing granularity (optional, SlotServer default 0.01)

**Neighbors accepted as config:** `probes.sink` on the master config, a `sink=` constructor arg, or `NULL_SINK`.

**Service semantics:**
- Requests arrive with the wired attrs `{input_tokens, output_tokens, agent_id, request_id}` (plain attribute objects, or a simkit `Request` carrying them in its `params` mapping). Failure to carry the attrs is a `ComponentError` at `submit`.
- Typed ports: input `requests` (kind `req`), output `responses` (kind `response`) — validated at elaboration via the connect graph. Ports are validation/introspection only in simkit, so the transport API is: `ev = ep.submit(request)` → `yield ev` → `rec = yield ep.wait(request)` → `ep.response_of(request)` returns an `LLMResponse(request_id, agent_id, input_tokens, output_tokens, queue_time, service_time, start, end)`.
- Serving on a SlotServer (`{name}.server`): queue-when-saturated from the slot pool; within one slot hold the phases run **sequentially** — input tokens at `input_tokens_per_s`, then output tokens at `output_tokens_per_s` (`cost_fn` = `input/in_rate + output/out_rate`).
- **Aggregate token/s cap** = `input_tokens_per_s + output_tokens_per_s`, shared with fair share across in-flight token-consuming requests, exactly per ticket 07's SlotServer contract. Consequences, all test-asserted: a solo request takes exactly its phase floor (`N` input tokens → `N/input_rate`; an output-only request takes `N/output_rate`); under sharing each in-flight request earns `cap/|in-flight|`, so per-request time stretches to `k·tokens/cap` and aggregate throughput cannot exceed the cap.
- Per-request `queue_time` vs `service_time` split comes free from `SlotServer.records`; `collect()` returns per-request dicts (contract identity + timings, no rid), served count, input/output token totals and the config echo.

**Probe samples** (on the shared sink):
- Lifecycle trace edges: name `{component}.{stage}` with stage in `submitted | queued | processing | completed` (edge value = state), tags/attrs `{agent_id, request_id, state}`. `processing` is emitted from the `cost_fn` hook at the exact slot-acquisition instant; `completed` from a per-request completion watcher; `queued` = admitted-and-awaiting-a-slot (zero-duration possible when a slot is free at the same instant).
- Throughput counters `tokens_input` / `tokens_output` (tagged with agent/request identity); occupancy levels `slot_busy` and `queue_depth` from the underlying primitives (tagged `component={name}.server` / `{name}.server.ingress`).

**Callers' probes live on the same flat sink** — signal names are namespaced by component name (`{name}.{stage}`, `{name}.tokens_input`, …).

**Calling contract for ticket 08 (AgentClient)**: documented in the module docstring (submit/wait/response_of above); shaped `LLMResponse`s are the `responses`-port payload.

**Not built:** scenario YAML — `build_model` is ticket 10's and doesn't attach the agent/LLM side yet (its stub comment marks the wiring point); a declarative scenario YAML would encode a speculative top-level layout that ticket 11 may re-shape. The config keys above are the stable surface.

**Verification:** `tests/test_components_llm_endpoint.py` — 16 tests: typed ports + wiring validation at elaboration (valid edges pass, kind mismatch `WiringError`), phase-rate floors solo (split/mixed/input-only/output-only), saturation (slots=1 → `queue_time` 0/1.5/3.0, `service_time` unchanged at 1.5), aggregate cap (solo `N/rate` exact; pair each ≈ `2N/cap` ≈ 2× floor, fair-share ends together; 4 concurrent ≈ `4N/cap`), lifecycle edges present with correct names/value/attrs and per-request state ordering, throughput counters + occupancy levels, `response_of`/`collect`, fail-fast config/request validation, simkit-`Request` params-mapping acceptance, seeded determinism (bit-identical records + full sink sample stream across runs).

Full suite: all 68 tests outside the concurrent tickets' in-flight files pass (pre-existing simkit/CLI suite + ticket 11's metrics tests + these 16). Remaining failures in `tests/test_agent_client.py`, `tests/test_db_endpoint.py` and the superseded `test_build_model_is_a_skeleton` belong to tickets 08/10, which are mid-flight editing `registry.py`/`agent_client.py`/db components in this worktree — outside ticket 09's scope. No files outside my ticket were edited and nothing was committed.
