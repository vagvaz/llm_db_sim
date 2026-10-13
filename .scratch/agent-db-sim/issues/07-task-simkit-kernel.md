# 07 — simkit kernel: component base, server primitive, sweep runner, probes

Type: task
Status: resolved
Blocked by: 03, 06

## Question

Build the project-local simkit layer (the desmod-equivalent scaffolding we committed to in Notes):

- Component base class per the architecture from ticket 03: hierarchical composition, **runtime input/output validation**, lifecycle hooks, seeded RNG access.
- Shared wait-time primitive and a **slot + aggregate token/s server** (configurable slot capacity, aggregate throughput cap, queue-when-saturated) — the primitive LLMEndpoint and DBCompute are built on.
- Bandwidth/level primitive (SimPy Container-based) for storage/network transfer.
- Config elaboration: OmegaConf namespaces → component/object construction.
- Factorial **sweep runner** (the `simulate_factors` analogue): given a base config + factor dict, enumerate the cross product, run each, collect results into a common output shape.
- Probe/tracking hooks: per-signal recording API feeding both the metrics collector and VCD emission.

Validated by unit tests on the primitives (slots, aggregate cap, sweep enumeration) — no model components yet.

## Answer

Built the simkit kernel in `src/agent_db_sim/simkit/` per the ticket-03 contract. Module layout:

- `errors.py` — `SimkitError` hierarchy (`WiringError`, `RegistryError`, `IngressError`, `BandwidthError`, `SweepError`, `ComponentError`); all raised pre-simulated-time (fail fast).
- `component.py` — `Component` base with the two-phase lifecycle `construct(env, cfg)` → `elaborate()` → `start()` → `collect()`; `Composite` owns child components + a connect graph and recurses each phase. Typed ports are class attrs `input_ports`/`output_ports` (name → kind tag). RNG: master seed `sim.seed` (default 0) derives one deterministic child stream per component via `random.Random(f"{seed}/{full_path}")` (hash-seed independent → stable across runs; full paths disambiguate same-named children).
- `ports.py` — `ConnectGraph.connect()` records `Link`s; full validation (unknown ports, kind mismatch, double-wired inputs) runs at elaboration. Graph is introspectable (`links`, `links_of(comp)`).
- `ingress.py` — `Request` dataclass; explicit registry `none | fifo | priority:<dotted-key>` (`open_ingress`); uniform `submit(req)`/`get()` API; `none` = rendezvous handoff (submits block), `fifo` = unbounded Store, `priority` = heap by `dig(req, key)` with FIFO tie-break; `queue_depth` sampled to the probe sink on every change.
- `processing.py` — `SlotServer`: explicit `simpy.Resource` slot pool; dispatcher consumes ingress in order; service = acquire slot → wait `max(cost_fn(req), token-paced time)` → release. `queue_time` = submit→slot-acquired, `service_time` = slot-acquired→done, recorded per request in `server.records` and fulfilled on `req.done`. Optional aggregate `token_cap`: in-flight token-consuming requests earn tokens at `cap / |in-flight|` (quantum 0.01 pacing) — what throttles LLMEndpoint/DBCompute aggregate token/s.
- `levels.py` — `BandwidthPool`: SimPy Container staging level; `transfer(volume)` blocks on `get(volume)` (queue time), serves `volume/bandwidth` (service time), returns volume to the level; own records + level probes.
- `registry.py` — explicit config-name→class dicts (`INGRESS_REGISTRY`, placeholder `TOPOLOGY_REGISTRY`/`STORAGE_BACKEND_REGISTRY` for tickets 08-10); `build_model(cfg) -> Top` is a stub raising `NotImplementedError`.
- `sweep.py` — `sweep: factors: {dotted.path: [values]}` → Cartesian product; `run_sweep(cfg)` writes `runs/<sweep-name>/<run-id>/config.yaml` snapshots + `sweep.json` summary; `runs/<run-id>/metrics.json` hook exists for a runner payload (metrics/VCD collectors land later).
- `probes.py` — probe API `Sample`/`ProbeSink` (`level`/`edge`/`counter`) + `InMemorySink` (flat samples, `series()`/`values()`/`counters`); primitives already emit `queue_depth`, `slot_busy`, `level` signals for VCD/metrics consumers.
- `runner.py` — `run_model(top, cfg, until)` drives construct-all→elaborate→start→env.run→collect and returns collected data + the probe sink (radial `run_minimal` smoke loop kept for the 06 CLI). CLI: `agent-db-sim sweep <scenario> [--out]` wired; `scenarios/sweep_smoke.yaml` added.

Not built (out of scope): model components, metrics percentiles, viz/VCD emission — hooks only.

Validation: 37 new unit tests across 7 files (ingress kinds/ordering, slots/cap/queue-service separation, token cap scaling + fair sharing, bandwidth levels, lifecycle order + RNG streams, port wiring validation, sweep cross product + dirs + snapshots, probes, registry errors, CLI sweep) — full suite `uv run pytest`: **40 passed** (3 pre-existing CLI smoke tests unchanged).
