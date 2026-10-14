# agent-db-sim — User Guide

A discrete-event simulation platform (SimPy) that models **AgentClients** — simulated
workers running a closed loop of LLM inference calls alternating with database
queries — against a simulated **DBEndpoint** (compute + storage + internal network),
to study where workloads queue and how different database **Topologies** absorb them.
It supports factorial **Sweeps**, produces per-run SQLite `metrics.db` + `run.vcd`
waveforms, template reports, and a live visualization server.

Audience: a technical user who just cloned the repo. Terminology follows the
project glossary in [`CONTEXT.md`](CONTEXT.md) — in particular we say
**AgentClient** / **LLMEndpoint** / **DBEndpoint** (not "agent", "inference
server", "database").

---

## 1. What the platform is

The platform composes three modeled resource families:

| Component | What it models |
|---|---|
| **AgentClient** | N closed-loop workers; each runs think (timeout) → inference request → token generation → DB query, for the whole simulated duration. Token sizes and per-draw **QueryProfile** mixes come from named distributions with per-agent seeded RNG streams. |
| **LLMEndpoint** | A simulated inference server: a pool of **Slots** (concurrent capacity), plus a fair-shared aggregate token/s cap across input and output token throughput. |
| **DBEndpoint** | The database under study: **DBCompute** (spends query complexity as service time), **DBStorage** (serves query volume through a swappable StorageBackend — `hdd` / `ssd` / `nvme` presets with IOPS and bandwidth ceilings), and internal network links. |

Three **Topologies** wire compute, storage and network differently
(`db.topology`):

- **centralized** — one colocated compute+storage node, no internal network stages;
- **disaggregated** — N compute nodes × M storage nodes over one shared
  storage-network segment; **Scale-out** means growing N or M (`db.compute_nodes` /
  `db.storage_nodes`), per-node parameters untouched;
- **distributed** — colocated compute + local-storage node pairs; queries enter
  through round-robined **Coordinators**, each coordinator links to every peer,
  and coordinator compute carries a `coordinator_overhead` multiplier.

Everything a request touches is recorded as a **Trace** (one row per lifecycle
state transition, keyed by `agent_id` + `request_id`), from which all metrics —
latency percentiles, time-weighted utilization, hybrid bottleneck attribution —
are derived. Reports are Jinja2 + Plotly; waveforms are a VCD file rendered live
in the browser by the bundled viz server (d3-wave, fully offline — no CDN).

---

## 2. Install & setup

Requirements: **Python ≥ 3.10** and [uv](https://docs.astral.sh/uv/).

```bash
git clone <repo-url> agent_db_sim
cd agent_db_sim
uv sync
```

Verify the toolchain:

```bash
% uv --version
uv 0.13.0
% uv run agent-db-sim --help
Usage: agent-db-sim [OPTIONS] COMMAND [ARGS]...
  agent-db-sim: SimPy-based simulation of agent-database workloads.
╭─ Commands ──────────────────────────────────────╮
│ run     Execute a scenario end to end …         │
│ sweep   Enumerate the scenario's sweep factors …│
│ serve   Serve a completed (or still-running) run│
│ render  Render a report as standalone HTML …    │
╰─────────────────────────────────────────────────╯
```

Optionally run the test suite: `uv run pytest` (133 tests green as of this writing).

---

## 3. Quickstart

### 3.1 Smoke run (wiring check, milliseconds)

```bash
% uv run agent-db-sim run scenarios/smoke.yaml --out /tmp/agent-db-sim-runs
agent-db-sim smoke: 2 agents x 3 timeouts (1.0u each) -> 6 timeouts, sim end t=10.0u
```

`smoke.yaml` uses the legacy `loop:` scenario shape: it just verifies the CLI +
SimPy + OmegaConf wiring. Try a profile overlay:

```bash
% uv run agent-db-sim run scenarios/smoke.yaml --profile fast --out /tmp/agent-db-sim-runs
agent-db-sim smoke: 4 agents x 2 timeouts (0.5u each) -> 8 timeouts, sim end t=5.0u
```

(A profile that lives in `profiles:` inside the scenario YAML can also be a
sibling file `<profile>.yaml`.)

### 3.2 A real run (full model pipeline)

`topology_compare.yaml` — the acceptance scenario — defines agents, an LLM
endpoint and a DB, so `run` drives the real pipeline:

```bash
% uv run agent-db-sim run scenarios/topology_compare.yaml --out /tmp/agent-db-sim-runs
run dir: /tmp/agent-db-sim-runs/topology_compare
  e2e p50/p95/p99 = 0.128/2.195/2.882 (n=1850)
  bottleneck: top.db.topology.compute-0.compute_issue (score=0.468, queue_share=0.668)
```

Every run directory contains the full artifact set:

```text
topology_compare/
├── config.yaml        # the effective config snapshot (after profile overlay)
├── metrics.db         # SQLite: trace table + derived metric tables (see §6)
├── run.vcd            # VCD waveforms, event-driven, one step per state change
├── report.html        # standard report (Jinja2 + Plotly, standalone)
├── plotly.min.js      # vendored next to the HTML for offline viewing
└── summary.json       # headline numbers: e2e percentiles, ranked bottleneck,
                      # utilization top-8, counts, config digest
```

Open `report.html` in a browser immediately — it needs the adjacent
`plotly.min.js` and nothing else.

If you re-run without `--out <fresh-dir>`, the runner picks the next free
directory name (`topology_compare-2`, `-3`, …) rather than overwriting. The same
applies inside sweeps (`enumerate_sweep` errors if the target run dir already
exists — remove it or change `--out`).

### 3.3 The acceptance sweep (what this project is about)

```bash
% uv run agent-db-sim sweep scenarios/topology_compare.yaml --out /tmp/agent-db-sim-runs
sweep 'topology-sweep': 12 runs written under /tmp/agent-db-sim-runs/topology-sweep/
  0000-db__topology=centralized,db__compute_nodes=4,agents__count=8: e2e p95=2.195 bottleneck=top.db.topology.compute-0.compute_issue (score=0.468)
  ...
```

Twelve runs (3 topologies × 2 scale-out widths × 2 agent counts), each with the
full artifact set under `runs/topology-sweep/<run-id>/`, plus sweep-level
comparison artifacts at the sweep root:

- `sweep_summary.json` — factors, per-run summaries, grouped scale-out deltas;
- `sweep_summary.html` — one comparison table: run id, factors, e2e
  p50/p95/p99, bottleneck winner, score, queue share.

§7 walks through exactly what those numbers mean.

### 3.4 Serve and render

```bash
uv run agent-db-sim serve /tmp/agent-db-sim-runs/topology_compare
# viz server: http://127.0.0.1:8123/  (/tmp/agent-db-sim-runs/topology_compare)
```

(point the `--port` flag anywhere; `serve` works on a single run dir or any
sweep member run dir) and, for a CI-friendly standalone report without a
server:

```bash
uv run agent-db-sim render /tmp/agent-db-sim-runs/topology_compare
# report 'standard' -> /tmp/agent-db-sim-runs/topology_compare/standard.html
```

---

## 4. Scenarios & configuration

A **Scenario** is one YAML file ("a complete simulation configuration,
optionally modified by a named config overlay"). See `scenarios/` for worked
examples: `smoke.yaml` and `sweep_smoke.yaml` (legacy `loop:` stub shape),
`db_centralized.yaml` / `db_disaggregated.yaml` / `db_distributed.yaml`
(DB-only examples), `agents_smoke.yaml` (documents the `agents:` namespace),
and `topology_compare.yaml` (the full acceptance scenario).

### 4.1 Anatomy of a full scenario

```yaml
sim:
  until: 200.0          # simulated time horizon (time units); run stops here
  seed: 11              # seed for the RNG stream tree

agents:                 # the AgentClient fleet (required for a full run)
  count: 8              # number of closed-loop agents
  input_tokens:         # prompt-size draw: {distribution, ...params} or bare int
    distribution: fixed
    value: 256
  output_tokens:        # response-size draw; likewise
    distribution: fixed
    value: 128
  query_profiles:       # the per-agent QueryProfile mix (weights normalize)
    - name: point_read   # cheap complexity, tiny volume
      complexity: 0.2    # compute complexity charged at DBCompute
      volume: 0.05       # GB touched at DBStorage
      weight: 0.8
    - name: wide_scan    # heavier complexity, bulky volume
      complexity: 0.6
      volume: 2.0
      weight: 0.2

llm:                    # the LLMEndpoint
  slots: 8              # Slot pool size (per-endpoint)
  input_tokens_per_s: 4000   # aggregate input-token throughput cap
  output_tokens_per_s: 2000  # aggregate output-token throughput cap
  ingress: fifo         # admission stage: none | fifo | priority

db:                     # the DBEndpoint (required — run errors without it)
  topology: centralized # centralized | disaggregated | distributed
  compute_nodes: 4      # N (scale-out axis on disaggregated; node pairs on
                        # distributed; must stay 1 on centralized)
  storage_nodes: 2      # M (scale-out axis on disaggregated; striping)
  coordinators: 2       # distributed only: entry Coordinator slots
  compute:
    slots: 2            # DBCompute Slot pool, per compute node
    ingress: fifo       # admission stage in front of issue
    issue_ratio: 0.2    # compute-issue phase share of total query work
    phase_split: three_phase   # issue / finalize / storage round-trip split
  storage:
    backend: nvme       # hdd | ssd | nvme (see §8.1 to add presets)
    # overrides:        # optional per-parameter overrides of the preset:
    #   per_gb_time: 0.1
  network:
    latency: 0.01       # per-hop propagation delay
    bandwidth: 500.0    # GB/s link bandwidth
    capacity: 8.0       # concurrent-transfer capacity of the shared segment
    request_size: 0.01  # GB per request leg
    coordinator_overhead: 1.2  # multiplier on coordinator compute

metrics:                # optional; defaults shown
  warmup: 0.1           # fraction of the run excluded from every statistic
  sample_interval: 1.0  # tick spacing of the time-sampled utilization series

sweep:                  # optional; makes `sweep` the natural command
  name: topology-sweep
  factors:              # dotted config paths → enumerated value lists
    db.topology: [centralized, disaggregated, distributed]
    db.compute_nodes: [4, 8]
    agents.count: [8, 16]

profiles:               # optional named overlays, deep-merged via --profile/-p
  agents_16:
    agents:
      count: 16
  llm_tight:            # starve the LLM to make it the first bottleneck
    llm:
      slots: 2
      input_tokens_per_s: 1500
      output_tokens_per_s: 750
```

Token distributions available to `agents.input_tokens` / `output_tokens`
(fixed | uniform | geometric | exponential | gamma | normal | lognormal —
lookup is in `src/agent_db_sim/components/agent_client.py`, the `agents_smoke`
YAML documents each).

### 4.2 Profiles

A profile is a *partial* config deep-merged on top of the scenario. Deep-merge
means you only write the keys you want to change — everything else keeps the
scenario's value. Resolve order:
1. a key under the scenario's `profiles:` block (`--profile agents_16`);
2. a sibling file `<profile>.yaml` next to the scenario.

Unknown profiles fail fast with a `ConfigError` listing the available ones.

### 4.3 Sweeps

The `sweep:` block enumerates the cross product of factor values. Factors are
dotted paths into the config (`agents.count`, `db.compute_nodes`,
`db.topology`, …) and values are lists. Run dirs are
`<out>/<sweep-name>/<index>-<assignments>` where assignments are
config-path/value pairs with `.`/`__` sanitization, e.g.
`0007-db__topology=disaggregated,db__compute_nodes=8,agents__count=16`.
Note that on `centralized` a `db.compute_nodes` factor is a documented no-op
(the topology structurally has one node) — see the acceptance table §7.

---

## 5. Commands reference

```text
agent-db-sim run <scenario.yaml> [--out DIR] [--profile NAME]
agent-db-sim sweep <scenario.yaml> [--out DIR] [--profile NAME]
agent-db-sim serve <run-dir> [--port N] [--host ADDR]
agent-db-sim render <run-dir> [--name NAME] [--out PATH]
```

### `run` — one scenario, one run directory

Executes a scenario end to end. Configs with a `db:` (and `agents:`) section
run the full pipeline (build model → simulate → `metrics.db` + `run.vcd` +
`report.html` + `summary.json`); `loop:`-only configs keep the placeholder
smoke loop. Two caveats verified while writing this guide:

- `db_*`-only scenarios (`db_centralized.yaml` etc., no `agents:` section) run
  the pipeline but nothing ever submits queries, so percentile tables are
  empty (`e2e p50/p95/p99 = nan/nan/nan (n=0)`). Use them as component-config
  references, not as workload runs.
- `agents_smoke.yaml` (which documents the `agents:` namespace but defines
  neither `db:` nor `loop:`) currently errors under `run`.

`run` prints the run dir, the e2e percentiles and the hybrid-attribution
winner. There is one run dir per invocation under `<out>/<scenario-stem>/` or
the `scenario-stem-<n>` increments.

### `sweep` — factorial enumeration

One full-pipeline run per factor combination (configs with a `db:` section),
or simkit's stub runner for `loop:`-only configs. Writes per-run dirs plus
`sweep_summary.json` / `sweep_summary.html` at the sweep root. Stdout: one
line per run (`e2e p95`, winner `component.stage (score)`).

### `serve` — waveforms + reports for one run dir

```bash
uv run agent-db-sim serve runs/topology_compare --port 8060
```

Binds `http://127.0.0.1:<port>/` (port 8060 default, `--port`/`--host` to
change). Routes:

| Route | What you get |
|---|---|
| `/` | d3-wave live waveform view over SSE (init snapshot → ~1 Hz deltas → done). Works on finished runs (emits `done` immediately) and on still-running ones (deltas tail the VCD, holding back a possibly-mid-write last block). |
| `/signals` | Signal catalog JSON — names, scopes, types parsed from the run's VCD (see §6.4). |
| `/reports` | Auto-indexed report list (any `*.html` template shipped in `viz/templates/` — see §8.3). |
| `/reports/<name>` | The report rendered now, from this run's `metrics.db`. |
| `/vcd` | The raw VCD file (`run.vcd`), for feeding Surfer (deep-dive link is embedded on the page when serving publicly-reachable URLs). |

### `render` — standalone report HTML

```bash
uv run agent-db-sim render <run-dir> --name standard --out report.html
```

Renders any indexed template to a standalone HTML file (vendored
`plotly.min.js` is copied next to the output). With `--name <template>`,
any report template under `viz/templates/` works; default output is
`<run-dir>/<name>.html`. `run` already emits `report.html`, so `render` is
mainly useful for other templates or CI artifacts. Sweep mode: add
`--sweep <sweep-dir>` to render a sweep-comparison report over a sweep root
(defaults to `--name sweep_compare`; see §8.5).

### `analyze` — analytical cross-check of one run

```bash
uv run agent-db-sim analyze <run-dir>
```

Builds the closed-network model (MVA / exact Gordon–Newell chain, ported
from the repo-root `core2.js` reference) from the run's config snapshot and
prints it side by side with the run's measured means: throughput, cycle,
per-hop latencies, waits, utilizations — with a relative-error column and
a qualitative verdict line. Means only: percentiles have no analytical
counterpart. Details in §9.

---

## 6. Reading results

### 6.1 `metrics.db` schema

Open with any SQLite client:

```bash
sqlite3 <run-dir>/metrics.db ".tables"
# bottleneck  latency_percentiles  meta  token_rate_series
# trace       utilization_series   utilization_stats
```

| Table | Contents |
|---|---|
| `trace` | Source of truth: one row per lifecycle state transition — `agent_id`, `request_id`, `parent_id` (sub-traces), `component`, `stage`, `state` (`submitted`/`queued`/`processing`/`completed`/`received`), `t`, `attrs` (JSON: sizes, costs). |
| `latency_percentiles` | `(source, kind)` → p50/p95/p99/n. `source` = `e2e` (agent-facing latency: inference + response + DB round-trip) or `<component>.<stage>`; `kind` = `latency` (wait + service) or `queue` (wait only). |
| `utilization_series` | Tick-sampled level series per `(signal, component)` — slots busy, queue depths. |
| `utilization_stats` | Per `(signal, component)`: time-weighted `mean`, `peak`, and capacity-normalized `mean_util` (NULL when capacity is not observable). |
| `token_rate_series` | Per `component` and tick: tokens/second throughput observed (LLM quotas, generator output). |
| `bottleneck` | One row per `(component, stage)`: `queue_delay` (summed queueing delay post-warmup), `queue_share` (share of agent-facing latency), `sat_score`, `score = 0.7 * queue_share + 0.3 * sat_score`. This is the **hybrid attribution**. |
| `meta` | Run metadata: `t_end`, `warmup_end`, warmup/sample_interval settings. |

**Warm-up**: a `warmup` fraction (default **0.1**) of the run duration is
excluded everywhere — stage/e2e samples starting at or before the warm-up
boundary are dropped, utilization integrates over the post-warm-up window
only. The top of `report.html` shows `t_end` / `warmup_end`.

**Sampling**: derived utilization series are tick-sampled at
`metrics.sample_interval` (default 1.0 time units); the raw VCD is event-driven
instead (a value-change only when something actually changes).

### 6.2 Latency percentiles

Sorted by `kind` then source; the row you care about first is
`e2e | latency` — agent-observed end-to-end latency:

```text
source                          kind      p50      p95      p99      n
e2e                             latency   0.128    2.195    2.882    1850
top.db.topology.compute-0.compute_issue   latency  0.982    1.930    2.696    924
top.db.topology.compute-0.storage.*       latency  0.0100   0.533    0.746    930
llm.*                           latency   0.128    0.128    0.128    928
top.db.topology.compute-0.compute_issue   queue    0.9378   1.8682   2.5998   924
```

Read it additively: at 8 agents on `centralized`, agent p95 latency 2.195 is
almost entirely the DB compute-issue stage (p95 1.93, of which 1.87 is queue
**wait**, `queue` kind), while the LLM hop is flat at 0.128 (well provisioned).
The p50 vs p95 *shape* is the other signal: e2e p50 is tiny (0.128 u — most
point-reads sail through) while p95 explodes — contention lives in the tail
(the `wide_scan` QueryProfile is 20% of draws and blocks the 2-slot compute).

### 6.3 Utilization & bottleneck attribution

`utilization_stats` gives time-weighted means/peaks of the underlying signals
(`slot_busy`, `queue_depth`, token rates, net-segment fills). Record only
what you need — 34 signals is a complete full run.

**Hybrid attribution** ranks every `(component, stage)` by

```text
score = 0.7 × queue_share + 0.3 × saturation
```

- **`queue_share` (primary signal)** — what fraction of agent-facing latency
  is spent waiting at that stage. This is the column that decides the winner:
  whatever has the largest share is what agents perceive first.
- **`saturation` (supporting signal)** — time-weighted utilization fraction
  of that component's capacity (or the queue-nonempty fraction when capacity
  is not observable). It catches *steady-state near-capacity* even before
  queues explode; it cannot raise a stage to the top alone, and is often NULL
  on components without observable capacity.

`summary.json` mirrors this: `bottleneck_winner` (highest score) and the top-5
`bottleneck` rows, plus `e2e_percentiles` and the top `utilization_stats` rows.

Known gap (being fixed at time of writing): `sat_score` reads 0 for all rows
in the integrated pipeline — the lookup join on probe/component naming is out
of sync (ticket-13 note). Queue share remains fully valid; check
`utilization_stats` manually for the saturation picture meanwhile.

### 6.4 Reading the VCD signal catalog

`/signals` (or `zg`-style greps over `run.vcd`) yields the catalog. Scope
naming: every scope hangs under `agent_db_sim` = the run's top-level composite:

```text
agent_db_sim.compute-0                requests / served / slot_busy
agent_db_sim.compute-0.ingress        queue_depth            ← DBCompute node 0
agent_db_sim.top.db.topology.compute-0.storage   storage_queue_state /
                                      storage_service_state  ← DB node's DBStorage
agent_db_sim.llm                      submitted_state / queued_state /
                                      processing_state / completed_state  (string states)
agent_db_sim.llm.server               requests / served / slot_busy / tokens_input /
                                      tokens_output / ingress.queue_depth
agent_db_sim.top.agents.agent-0 … -7  inference_state + db_state (per AgentClient)
```

Types are `string` for state machines and `integer` for levels/counters. The
waveform view (`/`) renders state changes as steps — useful to *watch* the
loop: an AgentClient's `inference_state` cycles `submitted → queued →
processing → completed` while `db_state` does its own cycle, and you can see
which side is stuck in `queued`. `token_input`/`token_output` cumulative
counters give the LLM throughput visually.

---

## 7. Worked example — the topology comparison

`scenarios/topology_compare.yaml` swept
`db.topology × db.compute_nodes × agents.count` (until=200, warmup 0.1 →
180 u post-warmup window, ~1.8k–5.4k sampled requests per run). All numbers
below were regenerated while writing this guide and reproduce the
acceptance run. e2e agent-facing latency p50/p95/p99, and the bottleneck
winner (hybrid attribution — primary column is queue share):

| run | agents | nodes | e2e p50 / p95 / p99 | n | bottleneck winner (score, queue share) |
|---|---|---|---|---|---|
| centralized ×4 | 8 | N=4 (one node) | 0.128 / **2.195** / 2.882 | 1850 | db compute-0.compute_issue (0.468, **66.8%**) |
| centralized ×8 | 8 | N=8 | 0.128 / **2.195** / 2.882 | 1850 | same, **identical** — scale-out is a structural no-op |
| centralized ×4 | 16 | N=4 | 0.128 / **4.042** / 4.651 | 1840 | db compute-0 (0.584, **83.4%**) |
| disaggregated ×4 | 8 | N=4 | 0.230 / 1.199 / 1.769 | 4304 | db compute-2 (0.019, 2.7%) |
| disaggregated ×8 | 8 | N=8 | 0.230 / 1.284 / 1.774 | 4393 | shared segment net_req (0.011, 1.5%) |
| disaggregated ×4 | 16 | N=4 | 0.371 / 1.803 / 2.576 | 5009 | db compute-3 (0.058, 8.3%) |
| disaggregated ×8 | 16 | N=8 | 0.398 / **1.512** / 2.445 | 5420 | llm (0.046, 6.6%) — compute relieved down to the LLM token cap |
| distributed ×4 | 8 | 1 coord + 3 peers | 0.202 / **1.586** / 2.052 | 2925 | coordinator node-0.compute_issue (0.146, 20.9%) |
| distributed ×8 | 8 | 1 coord + 7 peers | 0.207 / 1.556 / 2.219 | 3011 | coordinator node-0 (0.170, 24.2%) — peer scale-out does **not** relieve |
| distributed ×4 | 16 | 1 + 3 | 0.219 / 3.121 / 4.135 | 3001 | coordinator node-1 (0.316, 45.1%) |
| distributed ×8 | 16 | 1 + 7 | 0.214 / 3.336 / 4.265 | 3030 | coordinator node-1 (0.429, **61.4%**) |

What the platform is telling you here:

1. **Centralized bottlenecks first, at the single entry compute node.** Queue
   share there is 66.8% at 8 agents and worsens to 83.4% at 16 agents (e2e
   p95 2.195 → 4.042). Its compute service p50 is ~0.98 u against ~0.04 u per
   node disaggregated — the entire workload funnels through one DBCompute
   Slot pool of 2. Also note it processes the *fewest* requests (n=1850 vs
   4304 on disaggregated): the entry node caps the closing loop's pace.
2. **Disaggregation spreads compute over N nodes and the per-hop network
   becomes visible.** With three phases and a shared storage-network segment,
   `net_req` / `net_resp` hops add their 0.01 u each and DB compute queue
   shares collapse to single digits. e2e p95 drops to 1.2–1.8 in every
   disaggregated run.
3. **Scale-out changes *who* bottlenecks, not just how much.** Disaggregated
   N=4 → N=8 at 16 agents moves e2e p95 1.803 → 1.512 and the winner migrates
   from a DB compute queue (compute-3, 8.3%) to the shared segment (1.5%)
   and finally to the LLM endpoint itself (6.6%) — the DB compute queue was
   what scale-out relieved, so the next queue down the path surfaces as the
   bottleneck. Overlay `--profile llm_tight` (slots 2, rates 1500/750) makes
   this explicit: the LLM's share rises 0 → 17.5% and becomes a first-class
   queue.
4. **Distributed re-centralizes at the Coordinators.** All external queries
   enter through round-robined coordinator slots, with the ×1.2 coordinator
   multiplier on top; the p95 (1.56–1.59 u at 8 agents — worse than
   disaggregated by the coordinator's compute overhead) and the winner share
   both sit on the *coordinator* compute node. Crucially, growing peers
   3 → 7 while keeping 1 coordinator does **not** relieve anything: the entry
   capacity is coordinator Slots, so the bottleneck stays put and grows with
   demand — 20.9% → 24.2% share at 8 agents, 45.1% → 61.4% at 16.

This is the platform's core pattern: **bottlenecks are found by queue share,
and changing capacity moves the bottleneck somewhere else** — which is
exactly what a factorial Sweep is for.

---

## 8. Extending the platform

### 8.1 Adding a StorageBackend preset

Presets live in one dict: `STORAGE_BACKEND_REGISTRY` in
`src/agent_db_sim/components/db/backend.py`. Each entry is a
`BackendParams(name=…, base_latency, per_gb_time, iops_ceiling,
bandwidth_ceiling)` — time units, GB units, GB/s and ops/s ceilings; ceilings
must be > 0 and latency params ≥ 0 (checked at registration). The contention
model is defined in the module docstring (each storage node owns one ceiling
budget, shared rate-fairly across in-flight ops). Add the preset, and any
scenario can then say `db.storage.backend: tape` — with optional
`db.storage.overrides: {per_gb_time: …}` for per-run tweaks (unknown override
fields fail fast with the list of valid fields).

### 8.2 Adding a QueryProfile mix

Nothing to register — a QueryProfile is just data. Add weighted entries to
any scenario's `agents.query_profiles:` (`name`, `complexity`, `volume`,
`weight`) or to `profiles:` overlays to swap mixes per run without touching
the base scenario. Weights normalize, and each agent draws its mix from its
own seeded `queries` RNG stream. (Known classes: named point-read / wide-scan
entries are the convention; names are free-form labels — only `complexity`
and `volume` are charged.)

### 8.3 Adding a new report template

Reports are plain Jinja2 files dropped into `src/agent_db_sim/viz/templates/`
(one file per report, named `<name>.html`). Any file that is not a scaffold
(`base.html`, `reports.html`, `wave.html`) and doesn't start with `_` is
**auto-indexed**: it shows up at `GET /reports` and works with
`agent-db-sim render --name <stem>`. The template context (from
`viz/report.py::render_report`) provides:

- `meta` — run metadata dict: `run_name`, `run_dir`, plus the `meta` table's
  keys (`t_end`, `warmup_end`, …), the run's `config` snapshot, and
  `vcd_signals` / `vcd_t_end` when `run.vcd` exists;
- `figures` — prebuilt Plotly figure JSON keyed by div id:
  `fig-latency-percentiles`, `fig-utilization`, `fig-token-rate`,
  `fig-bottleneck`;
- `rows_percentiles`, `rows_utilization` — ready-rendered table rows;
- `plotly_src` — use `{{ plotly_src }}` as the `<script src>` so the same
  template works both served and standalone;
- `template_name`.

Copy an existing plain template as a starting point; see §8.5 for the
preferred blocks-file way of composing report content.

### 8.4 Adding signals

Signals are edge samples emitted by component code through the shared
`ProbeSink` (see e.g. `StorageNode._edge` and the per-stage `_edge` calls in
the DB compute/network components: `f"{self.full_path}.{stage}", state,
agent_id=…, request_id=…`). Everything emitted lands in both the `trace`
table and the VCD catalog, so:

1. pick a component and a stage name following the existing `<stage>_state`
   conventions;
2. emit the state lifecycle (typically `submitted` / `queued` / `processing`
   / `completed`) via `env.process` body `_edge` calls with the standard
   tags so traces stay joinable by `agent_id` / `request_id`;
3. counters / levels of interest should also be emitted as `level` /
   `counter` samples for utilization and token-rate derivation
   (`derive_utilization` turns `level` samples into step functions, and
   `counter` samples whose names contain `token` into throughput rates).

### 8.5 Composable reports

A report does not have to be one hard-coded template file. Any report can be
a **block list**: an ordered list of named sections declared in a small YAML
file next to the built-in template scaffolds, `<name>.blocks.yaml` in
`src/agent_db_sim/viz/templates/`. Add, remove, or reorder blocks by editing
that file — no code. The built-in `standard` report is blocks-driven with the
same sections as before (`standard.blocks.yaml`), and `sweep_compare`
renders a whole sweep (`runs/<sweep>/`) instead of a single run.

**Block types** (a block is a YAML mapping with a mandatory `type`, optional
`title` / `name` (stable section id) / `inline` (glue under the previous
section instead of starting a new one), plus per-type parameters):

| type (scope) | what it renders | parameters |
|---|---|---|
| `metadata` (run) | run metadata key/value table | `fields` (list of meta keys; default `t_end`, `warmup_end`, VCD stats) |
| `metric_table` (run) | a derived table | `table` = `latency_percentiles` \| `utilization_stats` \| `bottleneck`; `filter` (e.g. `{source: e2e, kind: latency}`); optional `columns` |
| `plot_series` (run) | Plotly time series | `series` = `utilization` (of `utilization_series`) \| `tokens` (of `token_rate_series`) |
| `plot_percentiles` (run) | grouped per-stage p50/p95/p99 bars | `div` |
| `plot_bottleneck` (run) | hybrid bottleneck attribution bars | `div` |
| `plot_distribution` (run) | latency histogram from `trace` | `source` = `e2e` (default) or a component name |
| `markdown` (both) | prose | `text` (headings, bullets, bold, inline code) |
| `run_table` (sweep) | per-run factors + e2e percentiles + bottleneck winner | — |
| `factor_group_plot` (sweep) | e2e metric colored/grouped by one factor | `metric` = `p50`\|`p95`\|`p99`; `factor` (default: the first factor that distinguishes the runs) |
| `factor_delta` (sweep) | paired deltas between runs equal on every other factor | `metric`; `factor` |

Example — a run report (`src/agent_db_sim/viz/templates/standard.blocks.yaml`):

```yaml
blocks:
  - type: metadata
    title: Run metadata
  - type: plot_percentiles
    title: "Latency percentiles (per stage)"
    div: fig-latency-percentiles
  - type: metric_table
    table: latency_percentiles
    inline: true          # table under the previous figure's section
  - type: plot_series
    series: utilization
    title: "Utilization (slots / token rate)"
    div: fig-utilization
  - type: plot_series
    series: tokens
    div: fig-token-rate
    inline: true
  - type: metric_table
    table: utilization_stats
    inline: true
  - type: plot_bottleneck
    title: "Bottleneck attribution (hybrid: queue share 70% + saturation 30%)"
    div: fig-bottleneck
```

Drop the same shape into any `myreport.blocks.yaml` and it is auto-indexed
like every template (§8.3) at `GET /reports`.

**Sweep / multi-run analysis.** `sweep_compare.blocks.yaml` declares the
sweep block types `run_table`, `factor_group_plot`, and `factor_delta`; they
read `sweep_summary.json` plus each run's `metrics.db` under the sweep root
(a run missing from the summary is picked up from its `metrics.db` /
`config.yaml` instead). Render over a sweep (`runs/<sweep>/`):

```bash
# viz server, served against any sibling run dir
uv run agent-db-sim serve runs/topology_compare
#   GET /sweeps                      -> list of sweep roots under runs/
#   GET /sweeps/topology-sweep/      -> sweep_compare over runs/topology-sweep
#   GET /sweeps/topology-sweep/reports/sweep_compare

# CI-friendly standalone HTML (defaults to --name sweep_compare in sweep mode)
uv run agent-db-sim render runs/topology_compare --sweep runs/topology-sweep
# report 'sweep_compare' -> runs/topology-sweep/sweep_compare.html
```

`factor_delta` is for paired questions like *does scale-out relieve the
bottleneck?*: it groups runs that agree on every factor except the chosen
one and reports `Δ metric = other − base` per group (the `sweep_summary.json`
`comparison` table is still available alongside if you want raw numbers).

---

## 9. Analytical cross-check — `analyze` (and the `analytical_compare` block)

Every run can be cross-checked against **closed-network queueing theory**
(mean value analysis / exact Gordon–Newell chain — the very model in the
repo-root `core2.js` reference analyzer, ported to
`src/agent_db_sim/analysis/`): the workload is a closed loop of N agents
whose cycle is one LLM inference then one DB query, and the analyzer
decomposes it into M/M/c chain stations evaluated exactly.

```sh
agent-db-sim analyze runs/topology_compare
# analytical cross-check: runs/topology_compare (N=8 agents, ...)
#
# side-by-side (rel err = (sim - model) / model):
#   metric                 simulated  analytical  rel err
#   ---------------------  ---------  ----------  -------
#   throughput (cycles/s)  ...        ...
#   cycle (N/X)            ...
#   inference hop (R_llm)  ...
#   db hop (R_db)          ...
#   util llm slots         ...        ...
#   ...
# verdict: bottleneck MATCH (sim=llm / model=llm) | means tight (cycle within 5%)
```

### 9.1 What the analytical side assumes

- **Means only, exponential service** — per-station service means come from
  the run's `config.yaml` snapshot alone (configured token-distribution
  means, QueryProfile classes, storage backend presets + overrides), never
  from trace data. Simulated **percentiles have no analytical counterpart**
  (p50/p95/p99 are never compared).
- **The DB is one "slot-held-through" station**: the sim holds a DB compute
  slot from admission through issue → net → storage → finalize, so the
  model treats the DB as a single M/M/c station whose mean service
  includes the middle legs, with a mean-field stretch on the storage
  pacing work at the observed middle concurrency (shared-ceiling
  contention, as the backend budget shares its rate in flight).
- **The aggregate LLM token cap is a second bottleneck**: when model demand
  exceeds the cap, throughput pins at `cap / E[tokens]` and the check
  reports the `token-cap` regime — the exact regime where the sim's
  queue-share attribution is blind (saturation shows on the token rate,
  not on queues).
- Scope notes: legacy `[inference, db]` loops are exact; workflow
  templates are folded into per-cycle visit counts (exact only when every
  template alternates 1:1); `two_phase` split is approximated;
  connection-pool queueing is not modeled.

### 9.2 How to read the error column

`rel err = (sim − model) / model` per metric. The verdict line combines
two qualitative calls:

- **bottleneck MATCH/MISMATCH** — family-level (llm vs db) agreement
  between the simulated attribution and the model's argmax response time.
  In **queue-free** runs both sides degenerate to saturation proxies, so
  the call falls back to the utilization argmax; in the **token-cap**
  regime the comparison is the sim's token rate vs the configured cap.
- **means band** — the cycle (N/X) relative error: `tight ≤5%`, `ok
  ≤15%`, otherwise `off`.

Treat <1–3% as "the model reproduces the measured system"; larger errors at
occupied stations just below saturation are the classic knee
sensitivity (wait ∝ 1/(1−ρ)), and deterministic-service sanity scenarios
carry a documented exponential-assumption error (the exact-per-hand
`sanity_single`/`sanity_disaggregated` runs cross-check to ~0%).

The same table renders inside reports: add an
`- {type: analytical_compare}` block to any `<name>.blocks.yaml`
(`src/agent_db_sim/viz/templates/analytical.blocks.yaml` is the standard
report plus this section), or use `agent-db-sim render <run-dir>
--name analytical`.

---

## 10. Known limitations (as of this writing)

- `sat_score` in the `bottleneck` table reads 0 (attribution is
  queue-share-only) — component/probe naming join mismatch, being fixed;
  queue share (the primary signal) is unaffected.
- `db_*`-only scenario YAMLs run the pipeline but produce empty metric
  tables (no AgentClients submit) — they document DBEndpoint config shape.
- `agents_smoke.yaml` documents the `agents:` namespace but fails under
  `agent-db-sim run` (neither `db:` nor `loop:` present; the CLI falls into
  the smoke branch and crashes on the missing `loop` key).
- Network-hop queue labeling: `NetLink` propagation latency is recorded
  inside the `net_req` / `net_resp` queue delays (visible as a flat ~0.01 u
  "queue" row on net_* hops in the percentile tables). Timing correctness is
  exact; the wait-vs-propagation *label* is coarse.
- On `centralized`, `db.compute_nodes>1` is a no-op by design (one colocated
  node) — reflected identically in the acceptance table above.

---

## 11. Where to go next

- Glossary & canonical terms: `CONTEXT.md`
- Decision journal & ticket tracker (how the system was built and why):
  `.scratch/agent-db-sim/` (`journal.md`, `issues/`)
- Component-level code: `src/agent_db_sim/components/` (AgentClient,
  LLMEndpoint, DB sub-tree), kernel & registry in
  `src/agent_db_sim/simkit/`, metrics in `src/agent_db_sim/metrics/`,
  viz in `src/agent_db_sim/viz/`
- Scenarios to copy: `scenarios/topology_compare.yaml`
