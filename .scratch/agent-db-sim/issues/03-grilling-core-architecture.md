# 03 — Core architecture: composing the three components in SimPy

Type: grilling
Status: resolved
Blocked by:

## Question

How do AgentClient, LLMEndpoint, and DBEndpoint (DBStorage, DBCompute, internal network) compose into the SimPy simulation?

Decisions needed:

- The component base class: hierarchical composition (a 3-component model now, extensible later), **runtime input/output validation** of component interfaces (the user wants early I/O validation to catch bad wiring), lifecycle (init → elaborate → run → collect, desmod-style or simpler).
- How "processing = wait-time event computed from parameters" is expressed uniformly across agents, inference, and DB (one shared primitive? per-component helpers built on it?).
- Where the simkit layer sits relative to model components: config namespaces → component objects, factorial sweep runner, probes/tracking hooks, seeded RNG.
- Naming: the canonical domain terms (AgentClient, LLMEndpoint, DBEndpoint, slot, topology, profile, …) for CONTEXT.md.

Outcome: an architecture sketch small enough to react to in a session (diagram or skeleton interfaces), which the simkit and component tickets then build against.

## Answer

The architecture the simkit (07) and component tickets (08–11) build against:

1. **Lifecycle (two-phase, desmod-style):** `construct(env, config)` for all components → `elaborate()` connects ports, instantiates ingress/processing per config, **validates all wiring** (fail fast before any simulated time) → `start()` launches processes → `collect()` harvests metrics/signals. Runner drives these phases in order.
2. **Typed ports:** every component declares named inputs/outputs; `connect()` type-checks at elaboration. The connect graph is introspectable (future DOT rendering falls out for free).
3. **Ingress ∥ Processing split:** each serving component owns an `Ingress` object selected by config (`none | fifo | priority:<key>`) that feeds the shared processing primitive. Processing = acquire slot → `yield` wait of `cost_fn(params)` → release, with **queue time and service time instrumented separately** at both stages (feeds bottleneck attribution directly). One call shape for agents, LLM, compute, storage, network.
4. **Composition:** one explicit `build_model(cfg) -> Top`; the three top components are always constructed; **registry dicts** map config names → classes for swappable parts (ingress kinds, topologies, storage backends); unknown names fail at elaboration. No dotted-import magic.
5. **RNG:** single master seed in config → per-component **named child streams** (derived from seed + component name); topology-comparable, deterministic per component.
6. **Sweep:** `sweep:` section in scenario YAML with dotted-path `factors:` over the OmegaConf tree → Cartesian product; `run_sweep(cfg) -> list[RunResult]`; outputs `runs/<sweep-name>/<run-id>/` (config snapshot + metrics tables + `run.vcd` + report), plus a sweep summary; CLI `agent-db-sim sweep <scenario>`.
7. **Layout (ratified):** `simkit/` (base class, ports, ingress/processing primitives, runner, sweep, probes), `components/` (AgentClient, LLMEndpoint, DBEndpoint), `metrics/`, `viz/` — matches the 06 skeleton.
8. **Glossary ratified:** `CONTEXT.md` as written (Scenario vs QueryProfile distinction stands; Ingress/Processing/Slot definitions stand).
