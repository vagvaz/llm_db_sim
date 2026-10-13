# 08 — AgentClient: closed-loop workload model

Type: task
Status: resolved
Blocked by: 07

## Question

Implement the AgentClient component for iteration 1:

- Closed loop per agent: **LLM call → token generator produces output tokens → DB query → repeat**, running constantly for the configured simulated duration.
- N agents configurable via config profile.
- Query drawn from a configurable complexity × volume profile mix; inference request parameterized by input/output token counts (output count from the token generator).
- Every step is a wait-time event per the shared primitive; emits latency records (agent-facing, inference, DB) and signal samples for VCD.

Validated by tests: a scripted single-agent scenario produces the expected request sequence and latency records with a seeded RNG.

## Answer

Implemented `src/agent_db_sim/components/agent_client.py` on the simkit Component base (construct→elaborate→start→collect), exported via `components/__init__.py`; scenario namespace locked in `scenarios/agents_smoke.yaml`.

**Component shape.** One `AgentClient` component spans N agent processes (N = `agents.count`); no simkit child components — processes share the component but never share RNG streams or transport buffers. Declared typed ports (kinds from ticket 03): outputs `inference_requests`, `db_queries` (kind `req`); inputs `inference_responses`, `db_responses` (kind `response`). Connectable via the composite's `ConnectGraph` (unknown ports/kinds fail at elaboration per ticket 03).

**Closed loop per agent** (constant for the whole simulated duration): submit inference request (`params`: `kind="inference"`, `agent_id`, `request_id`, `parent_id=None`, `input_tokens`, `output_tokens`; `req.tokens=input+output`) → yield the wired `submit()` event on `inference_requests` → park on the input buffer → observe response → token generator draws a `QueryProfile` → submit DB query (`params`: `kind="db_query"`, `agent_id`, `request_id`, `parent_id=<inference request_id>`, `complexity`, `volume`, `profile`=<class name>; `req.tokens=0`, DB volume is bandwidth not tokens) → yield `submit()` on `db_queries` → park → observe response → repeat with the next sequence number. No agent-side think time in v1; the loop period is entirely made of serving-side waits.

**Wired contract — runtime transport (what tickets 10/11 wire):**
- `bind_output(port, handler)`: the port's tx handler must be the uniform serving admission shape `endpoint.submit(req) -> event` (simkit Ingress/SlotServer shape). Missing bindings fail at elaboration (`WiringError`).
- `deliver(port, req)` (alias `submit_response(port, req)`): the serving side's response entry point. It routes `req.params["agent_id"]` into that agent's per-(agent, port) rendezvous buffer — an `IngressNone` per pair (an agent's waiting uses the ingress primitive with kind `none`, per the shared contract) — and returns the submit event to yield. Because each (agent, port) pair buffers separately, exact response→agent routing holds even with all agents sharing one client; the agent's `received` transition is exactly that handoff.
- Trace samples are emitted with explicit `t=env.now` so they are correct regardless of the sink's clock; response buffers probe with `NULL_SINK` (rendezvous depth is plumbing noise, not a metric).

**Lifecycle trace events** (probe edge samples on the attached sink): one per send/observation, signal `{component}.{stage}` with component = the agent's unique id `{client.full_path}.agent-{i}` and stage `inference` | `db`; sample value and tags carry exactly `state` (`submitted` at send, `received` at agent observation) + `agent_id`, `request_id`, `parent_id`. `request_id` format `{agent_id}#{n}` where n is a per-agent counter over both request kinds (iteration k: inference `#(2k-1)`, db `#(2k)`); DB queries' `parent_id` = the inference request that derived them (sub-trace linkage per ticket 04). Sink attach: `attach_sink(sink)` (defaults `NULL_SINK`).

**Config** (`agents:` subtree, read via `cfg_key` defaulting to the component name; OmegaConf/dict supported; all missing keys fail fast with key names): `count`, `input_tokens`, `output_tokens` (distribution specs or bare numbers = fixed>clamped int >= 1), `query_profiles` (list of `{name, complexity, volume, weight}`; names unique, weights >= 0, at least one > 0, weights normalized). Token distribution registry `TOKEN_DISTRIBUTIONS`: `fixed` `uniform`, `geometric`, `exponential`, `gamma`, `normal`, `lognormal` — each with declared required parameters and numeric bounds checked at parse/sample (unknown name → `ComponentError`).

**RNG (no global random):** per agent two named child streams derived deterministically as `random.Random(f"{master_seed}/{agent_id}/tokens")` and `.../queries` — the same seed/path derivation scheme as the simkit Component base; per-agent inference/token draws and profile draws are reproducible per seed and mutually independent across agents.

**collect():** `{agent_id: {inference_submitted, inference_received, db_submitted, db_received}}` keyed by agent id (merges under the client's name in a composite, i.e. `collected["agents"][agent_id]`).

Validation: 14 tests in `tests/test_agent_client.py` with seeded RNG and a serving-contract stub (`FakeServer` on the real `SlotServer` + response delivery through the wired port): scripted single-agent sequence exact to the time-unit (loop period = llm+db delay; submit→received per stage and per iteration; submit times 0,4,8…; fenced db submits landing exactly at inference-received instants), per-transition edge samples with full attrs, request attrs carried through the serving side (`tokens=input+output`, parent linkage, profile class), N-agent count honored with fan-out counters, per-agent stream independence, QueryProfile draws statistically matching a 0.7/0.3 mix over 1000 seeded draws and verbatim class values, run-level determinism (same seed → identical sample stream/counters; different seed → diverges), port declaration shape + wiring/kind/duplicate-input validation, and config fail-fast coverage. Full suite: my file plus the 40 pre-existing tests and tickets 09/11 tests stay green; the concurrent ticket-10 worktree failures (`test_db_endpoint.py`, stale `test_simkit_sweep.py::test_build_model_is_a_skeleton`) reproduce without my change and live entirely inside DBEndpoint's own wiring — not touched per ticket scope. `uv run ruff check` clean on both new files.
