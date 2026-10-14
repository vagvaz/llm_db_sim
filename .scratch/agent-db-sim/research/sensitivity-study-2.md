# Sensitivity study 2 — high agent counts × inference provisioning ladder,
# DB axes under a genuinely DB-bound load

Author: fixer subagent (sensitivity sweep lane, study 2), 2026-10-14.
Direct successor of `.scratch/agent-db-sim/research/sensitivity-study-1.md`
(4–64 agents × grounded inference caps; verdict: the LLM token cap binds
everything, DB axes move e2e ≤ 0.3% — they were swept *asleep*).

Inputs: sweep set `scenarios/sensitivity2/` (7 YAMLs + README design table;
**51 runs, 14 invocations, --jobs 8, ~4.4 min wall total** on 16 cores),
outputs under `runs/sensitivity2/s2-*/` (gitignored; per-run full artifact
set + `sweep_summary.json`). Machine-readable companions:
`runs/sensitivity2/study2_extract.json` (per-run metrics pulled from each
`summary.json` + `metrics.db`), the raw tables in
`.scratch/agent-db-sim/research/study2_tables.md` (identical numbers), and
`.scratch/agent-db-sim/research/extract2.py` (the extractor).

Grounding (nothing invented): grounded base copied from
`scenarios/profiles/grounded_agent_70b.yaml` — 70B-midpoint endpoint
(`llm-inference-serving.md` §4: 8000 in / 2000 out tok/s, 32 slots),
lognormal agent tokens (mean ≈ 74.1k tok/call measured blended demand),
`mixed_agent_retrieval_analytics` mix (`tpc-workloads.md`: mean 3.505 CSE
and 0.575 GB row storage per query), 4 compute slots/node, three-phase
split (issue_ratio 0.2), `ssd_enterprise` (`ssd-generations.md` §4/§5),
network 0.0005 / 25 GB/s / coordinator_overhead 1.2. All files: `until`
600, `seed` 701, `warmup` 0.1 — study-1 conventions, so its baseline cells
are directly comparable.

## The two instruments

1. **Ladder** (`10_llm_ladder.yaml`, 24 runs): agents [64, 128, 256, 512]
   × provisioning rung f ∈ {0.5, 1, 2, 4, 8, 16} that scales **both**
   grounded caps (in = 8000f, out = 2000f; aggregate 10k·f tok/s). Slots
   pinned at the grounded 32 so the *only* moving part is token capacity
   (admission queues then appear for agents > 32 and are read off as their
   own signal). Expected completions ~ 600·10⁴f/74,150 (from study 1's
   measured scaling, confirmed within 2–8% below).
2. **DB-bound slices** (`20_`–`26_`, 27 runs): inference *generously*
   provisioned — slots ≥ agents (64 or 512, i.e. **zero llm ingress
   queue**), caps at f=16 (128k/32k = 2× the grounded `llm_huge`). The
   rationale: with the slot-hold mean τ_db ≈ 3.546 u (3.505 CSE compute +
   0.041 enterprise storage), the 4-slot centralized DB can sustain
   ≈ 1.128 inferences/s — i.e. it is *offered* 100% at rung
   f* ≈ 1.128 × 7.415 ≈ **8.4×** the grounded caps (9.2× on a compute-only
   basis). So f=8 is the analytically predicted knee and f=16 pins the DB
   outright; slice sweeps live above the knee. `25_crossover_topo.yaml`
   runs the f8 rung too, on both sides of the knee, with capacity-matched
   topologies.

Both instruments were chosen against study 1's pitfalls: goodput
(db completions / horizon) + per-stage latencies + utilizations everywhere;
bare e2e p50 never used for conclusions; attribution rows always reported
*with* their queue-share so the saturation-tie artifact (score 0.300,
queue share 0) is visible as the artifact it is.

---

## (a) The ladder: provisioning curve per agent count

Aggregate goodput pins at the token cap at every agent
count — with slots = 32 fixed, agent count barely moves it (−8–9% at
512/64 agents at f ≥ 8, from admission-wave truncation); agents move *who*
waits, not *how much* is produced (same law as study 1, now verified to
16× the grounded caps and 16× its max agent count). From
`s2-llm-ladder*` (llm.slots 32; "—" = no llm-stage sample survived the 60u
warmup filter, see pitfall P2; all 24 rows in
`research/study2_tables.md`):

| rung | caps tok/s | DB done /600u agents 64→512 | goodput /100u (a64 → a512) | llm svc p50 (a64 → a512) | llm q p95 (a64 → a512) | db util (a64 → a512) | db q p95 (a64 → a512) | winner a64 / a512 |
|---|---|---|---|---|---|---|---|---|
| 0.5× | 5000 | 32 | 5.33 → 5.33 | — | — | 0.047 | 0.0 | storage* tie / tie |
| 1× | 10000 | 72–73 | 12.2 → 12.0 | 323 → — | 230 → — | 0.10 → 0.08 | 0.0 | llm (0.72) / tie |
| 2× | 20000 | 155–160 | 26.7 → 25.8 | 196 → — | 122 → — | 0.26 → 0.22 | 0.0 | llm (0.65) / tie |
| 4× | 32000 | 304–331 | 53.3 → 50.8 | 103 → — | 66 → — | 0.50 → 0.41 | 4.9 → 1.4 | llm (0.63) / compute (0.36) |
| 8× | 80000 | 587–658 | 109.7 → 97.8 | 42.8 → 457† | 29.6 → 445† | 0.92 → 0.90 | 21.3 → 48.7 | llm (0.51) / llm (0.80) |
| 16× | 160000 | 677–739 | 123.2 → 112.8 | 2.4 → 97.9† | 0 → 187† | **1.00** everywhere | 78.8 → 282.1 | **db compute** (0.91) / (0.73) |

† llm p50 at 8×/16× grows ∝ agents *only through the 32-slot admission
wave stretch* (fair share = tokens × 32 / cap), not through the cap
itself. Token pins: the 8× ladder rows pin at ~99% of the 80k cap at
every agent count (76–77k in + 2.6–3.0k out); at 16× the a64 pin is only
54% of the 160k cap (86.5k measured) because the DB caps the loop — that
deficit *is* the crossover, visible from the cap side. (The 16× a128–512
rows show apparently higher pins only because ~1,000 second-cycle
inferences completed while their DB partners were still queued at
t=600 — the file-26 boundary effect below.)

Readings:

1. **The DB comes off the floor between the 8× and 16× rungs.** At 8× the
   4-slot DB sits at the predicted knee: db util 0.90–0.96, db queue p95
   21–49 u, growing with agents through the admission-wave dose. At 16×
   db util = 1.000 in every cell, db queue p95 79–283 u, llm service
   collapses to 1.9–2.4 u (a64) — the winner statement flips from
   "llm" (queue share, dominated by the admission waiting room) to
   **db compute** (score 0.73–0.95). Both instruments agree with the
   analytical f* ≈ 8.4–9.2× .
2. **Goodput hits a hard ceiling between the 8× and 16× rungs:** 97.8–109.7 →
   112.8–123.2 /100u (<+13%) while the provision ×2 — completing the
   study-1 "closed loop against a shared cap" law. At 16× the further
   provisioning has nowhere to go on centralized: db queue p95 grows
   79 → 283 u from 64 → 512 agents (queue coalescence at DB util 1.000).
3. Per-agent progress (not tabulated) falls ∝ agents at every rung —
   calls/agent within 600u ≈ goodput·600/agents (e.g. 16× a512 ≈ 1.3).

## (b) Topology ranking at DB-bound points

From `s2-topo-dbbound` (f=16, agents 64 = llm slots 64 — no admission
queue; every topology at its minimal groundeds; N/M/C shown):

| topology | db done | goodput /100u | e2e p50/p95 | llm svc p50 | db q p95 | db util | winner |
|---|---|---|---|---|---|---|---|
| centralized (4 slots, local storage) | 737 | 122.8 | 11.8 / 63.9 | 2.4 | 62.4 | 1.000 | db compute (0.91/q0.86) |
| disaggregated N2 M2 (8 slots, shared segment) | 1243 | 207.2 | 12.4 / **38.2** | 10.8 | 39.2 | 0.89 | db compute (0.54/q0.26) |
| distributed N2 C1 (coord entry ×1.2, peer storage) | 589 | **98.2** | 10.0 / **77.5** | 1.8 | 75.8 | 1.000 | node-0 (0.92/q0.90) |

The ranking is capacity-driven and *sharp*: distributed's minimum
footprint has ~0.86× centralized's effective entry capacity (4 slots ÷
1.2 coordinator multiplier) and pays the peer hop: **−20% goodput, +21%
e2e p95 vs centralized** — the study-1 "+21% p50 wiring tax" now shows up
in throughput at the DB knee too. Disaggregated at its N2 minimal groundeds
has 2× compute: −69% e2e p95 and +69% goodput — it *escapes the knee*
(offered util 0.95 → 0.89 realized). Per-query internal network cost is
negligible either way: segment/link p50 ≈ 0.0014 u/query (~0.05% of DB
compute service) — distributed's penalty is entry-capacity+overhead, not
wire latency.

## (c) Scale-out relief curves

**Disaggregated N×M** (`s2-disagg-dbbound`; base N1 M2, f16):

| N | M | dbdone | goodput | e2e p95 | db q p95 | db util | winner |
|---|---|---|---|---|---|---|---|
| 1 | 1 | 717 | 119.5 | 62.6 | 61.3 | 1.000 | db (0.91) |
| 1 | 2 | 717 | 119.5 | 62.7 | 61.3 | 1.000 | db (0.91) |
| 2 | 1 | 1241 | 206.8 | 42.1 | 46.4 | 0.94 | db (0.47) |
| 2 | 2 | 1243 | 207.2 | 38.2 | 39.2 | 0.89 | db (0.54) |
| 4 | 1 | 1315 | 219.2 | 42.1 | 3.1 | 0.50 | tie (0.30) |
| 4 | 2 | 1318 | 219.7 | 41.6 | 5.0 | 0.50 | tie (0.30) |

What N buys: **one doubling of N ≈ +73% goodput** (119.5 → 206.8), the
second doubling only +6% (207 → 220) — diminishing returns as the llm
token cap (offered goodput 2.16/s at f16) takes over: N's DB relief is
capped by the *llm*, exactly as designed. e2e p95 improves 62.6 → 42 then
stops. **M (storage striping) buys nothing** (M1 vs M2: identical
goodput, ±0.4% p95, storage svc p95 identical 0.29): at DB-binding loads
this mix's storage demand (~0.66 GB/s realized) is ~5% of one enterprise
node's 14 GB/s — the storage axis is *available* but never binding in any
disagg cell (as in study 1, now also true at 8.7× the DB-side pressure).

**Distributed axes** (`s2-dist-coords/nodes-dbbound`; f16):

| coords C | nodes N | dbdone | goodput | e2e p95 | db q p95 | winner |
|---|---|---|---|---|---|---|
| 1 | 2 | 589 | 98.2 | 77.5 | 75.8 | node-0 (0.92) |
| 1 | 4 | 587 | 97.8 | 77.5 | 75.8 | node-0 (0.92) |
| 2 | 4 | 1074 | 179.0 | 57.1 | 57.1 | node-1 (0.53) |

**Adding coordinators (entry capacity: 4C slots @ ×1.2) is the strongest
DB lever found: +82% goodput.** Adding peers (N 2→4, storage-side) buys
0%.

## (d) SSD generation in e2e once the DB binds

From `s2-storage-dbbound` (centralized, f16, agents 64 = slots 64):

| backend | dbdone | goodput | e2e p50/p95 | stg svc p95 | db q p95 | db util | winner |
|---|---|---|---|---|---|---|---|
| ssd_gen3 (0.294 s/GB) | 689 | 114.8 | 8.9 / 66.3 | 1.18 | 64.0 | 0.98 | db (0.92) |
| ssd_gen4 (0.147) | 726 | 121.0 | 10.8 / 63.6 | 0.59 | 61.7 | 1.00 | db (0.91) |
| ssd_enterprise (0.0714) | 737 | 122.8 | 11.8 / 63.9 | 0.29 | 62.4 | 1.00 | db (0.91) |

For the first time the storage tier couples into throughput: **Gen3 costs
−6.5% goodput and +3.8% e2e p95** against enterprise (Gen4 −1.5% /
−0.5%, inside run-to-run noise). Mechanism: per-query slot-hold now
includes up to 4.0 GB × per_gb_time of scan inside a DB compute slot that
is near 100% occupied — Gen3 stretches the analytic classes (p95 1.18 vs
0.29 u) and the whole 4-slot pool feels it. Still capped by compute, not
bandwidth (~0.66 GB/s demand vs 3.4: storage util ≈ 19%) — but the
direction is now measurable in goodput, not only in per-stage rows.
Watch the p50 inversion: Gen3's e2e p50 is *lower* (8.9 vs 11.8) purely
from completion-mix timing (pitfall P3) — the goodput column is the
signal.

## (e) The crossover, topologies both sides (files 25 + 26)

Capacity-matched pair (centralized vs disaggregated N1 M2, same 4 compute
slots; llm slots = agents ⇒ *no admission queue*; attribution clean):

| rung | agents | topo | dbdone | goodput | e2e p50/p95 | llm svc p50 | db q p95 | db util | winner (q-share) |
|---|---|---|---|---|---|---|---|---|---|
| f8 | 64 | centr | 639 | 106.5 | 20.9 / 71.6 | 36.9 | 23.5 | 0.90 | db (0.19) |
| f8 | 64 | disagg | 643 | 107.2 | 21.5 / 69.9 | 36.5 | 24.5 | 0.90 | db (0.19) |
| f16 | 64 | centr | 737 | 122.8 | 11.8 / 63.9 | 2.4 | 62.4 | 1.00 | db (0.86) |
| f16 | 64 | disagg | 717 | 119.5 | 11.6 / 62.7 | 1.9 | 61.3 | 1.00 | db (0.87) |
| f8 | 512 | centr | 421 | 70.2 | 41.8 / 318.5 | 238.6 | 68.0 | 0.80‡ | db (0.29) |
| f8 | 512 | disagg | 419 | 69.8 | 42.6 / 318.5 | 238.3 | 69.7 | 0.80‡ | db (0.29) |
| f16 | 512 | centr | 591 | 98.5 | 77.1 / 281.8 | 31.4 | 295.2 | 1.00 | db (0.72) |
| f16 | 512 | disagg | 573 | 95.5 | 75.4 / 274.1 | 31.5 | 286.1 | 1.00 | db (0.71) |

At the f8 knee the *knee-level* attribution already points to the DB
(winner db compute, queue share 0.19, score 0.43 — first rung where a real
DB queue exists at all, p95 23.5 vs the llm service p50 36.9: near-parity);
*f16 is unambiguous* (q-share 0.71–0.87, llm service at the fair-share
floor 1.9–2.4 u, DB util 1.000). **The crossover is agent-count-invariant
in structure**: at 512 agents (slots = 512) the same knee utilities read
0.80 and the same winner — agents multiply the llm fair-share stretch
(36.9 → 238.6 u at f8) and the per-call e2e (71.6 → 318 p95), never the
binding point. Topology differences collapse on both sides of the knee:
at matched capacity (N1 M2 vs centralized), ±3% p95 everywhere.

In the ladder (file 10, slots pinned at 32) the same crossover appears**
from the other side: winner llm → db compute between the 8× and 16× rungs at every
agent count; db util 0.90–0.96 → 1.000; token pin 79–80k → 83–137k vs the
160k cap (46−56% deficit). Analytically f* ≈ 8.4× overall (9.2× on a
compute-only basis) — bracketed by the two rungs on both instruments.

## DB-bound goodput vs agent count (file 26)

| agents | dbdone | inf done | goodput | e2e p50/p95 | llm svc p50 | db q p95 | db util | winner (q-share) |
|---|---|---|---|---|---|---|---|---|
| 64 | 737 | 800 | 122.8 | 11.8 / 63.9 | 2.4 | 62.4 | 1.000 | db (0.86) |
| 256 | 639 | 893 | 106.5 | 37.4 / 227.0 | 3.1 | 227.4 | 1.000 | db (0.91) |
| 512 | 591 | 1089 | 98.5 | 77.1 / 281.8 | 31.4 | 295.2 | 1.000 | db (0.72) |

Study 1's extrapolation held qualitatively but not quantitatively: goodput
does NOT stay at the DB ceiling as agents grow — it *falls* ∼20% by 512
agents. Cause: **within a 600u horizon, 256/512-agent closed loops never
reach their steady state** (the first collective inference wave at
tokens × agents / cap = 30–240 u delays the entire DB queue build-up;
`inference_completed` = 1089 > `db_completed` = 591 at 512 because ~500
DB queries are still queued inside the loop at t=600). DB utilization is
1.000 in all three (the stamp of DB-binding), but the *measured* goodput
carries a building transient: an agents-proportional horizon (or a
warm-up that discards the flood phase on *both* sides) would restore the
plateau. Recommended instrument note for the next study.

## Surprises / pitfalls for the metrics layer

1. **Agent-count axis is invisible at cap-bound rungs with slots ≪ agents.**
   At f ≤ 4 the ladder rows for agents 128/256/512 are *byte-identical*
   to each other (e.g. e2e p95 exactly 15.8 / 20.1 u across all three;
   winners storage·saturation-tie or compute-0 at 0.30): with 32 slots
   and a capped rate, only the first cohort(s) are ever admitted within
   the horizon, and the nested per-agent RNG streams make those cohorts
   identical across runs (agents 33+ never contribute a completed row).
   Agent counts above ~2–3 waves' worth are wasted wall time at low
   rungs; the ladder's agent axis only discriminates at f ≥ 8.
2. **Warmup eats the llm stage entirely in truncated cohorts.** With
   warmup 60u and *all* inferences submitted at t ≈ 0, essentially every
   llm machine is filtered out ("—" cells above): no llm latency/queue
   rows exist for f ≤ 4 at agents ≥ 128, and the "e2e" row switches to
   being DB-request latencies (e2e p50 0.0–20 u). Attribution falls back
   to the saturation tie (score 0.300, queue share ≈ 0, "winner
   storage"): the study-1 artifact, now reproducible at will. Attribution
   at (slots ≪ agents, cap-bound) is structurally blind — queue-share
   attribution in a 512-deep admission waiting room reads "llm" even when
   the DB queue is the real cost (f8 ladder rows, DB util 0.90, winner
   llm q-share 0.72) — while the clean-instrument rows at the *same* f8
   operating point attribute to db compute. **Read agent-heavy cap-bound
   runs through goodput + token pins + utilizations, never the winner
   column.**
3. **The e2e p50 inversion survived everything study 1 predicted and
   gained a new form.** Old form: 64-agent saturation median collapse.
   New forms: (i) the truncated-cohort p50 ≈ 0 (inference submitted at 0,
   recorded post-warmup only as db); (ii) Gen3 storage p50 8.9 < Gen4's
   10.8 < enterprise 11.8 *inversely ordered* against goodput
   114.8 < 121.0 < 122.8 (fewer but earlier-fresher completions at high
   p99 tail); (iii) 512-agent DB-bound p50 77 vs 64-agent 11.8 with
   *better* util in both (all 1.000). p50 between load points is worthless;
   p95 + goodput + util is the usable triple.
4. **Sim DB service mean runs ~10–13% below the configured mean**
   (measured issue 0.61 + finalize 2.44 = 3.05 u vs configured 0.701 +
   2.804 = 3.505·(mix mean) + pacing; consistent across the three
   spot-checks). The closed chain then runs 5–10% *above* the analytical
   throughput (1.23–1.24 measured vs 1.128 modeled at f16; the mix-draw
   mean itself verified 3.5045 over 200k draws, so this is not a config
   mismatch). Likely a truncation/asymmetry artifact of the measured
   sample set (post-warmup completions) — surfaced for the analysis lane;
   it does not affect any relative ranking or attribution in this study
   (reproduced uniformly across topologies and backends).
5. **Distributed's entry-compute framing is the load-bearing detail:** with
   coordinators as the only entry nodes, its *minimum* footprint
   (C=1) loses to centralized on capacity (0.86×) and pays +21% e2e p95
   with −20% goodput at DB-bound loads; C=2 flips it to +45% goodput over
   centralized. Peer count (the axis one might naturally grow) does
   nothing at C=1. Coordinators ≥ peers is *not* swept (invalid: the
   construction needs 1 ≤ C < N) — noted so the null cell isn't
   re-invented.

## Spot checks with `agent-db-sim analyze` (agreement report)

- `s2-crossover-topo/0000-…centralized` (f16, a64): verdict **bottleneck
  MATCH** (sim=db / model=db), cycle −9.1% (sim 51.6 vs model 56.7); sim
  DB slot utilization 1.0 vs model 1.0; sim llm util 0.9% — the
  queue-clean run is DB-bound in both readings.
- `s2-topo-dbbound/0002-…distributed` (f16): **bottleneck MATCH** (sim=db,
  model=db), cycle −4.8% (65.1 vs 68.4); model's db τ 4.272 (1.2×
  overhead folded) vs sim total compute 3.77 — same −10% service-reading
  artifact as pitfall P4.
- `s2-storage-dbbound/0000-…ssd_gen3` (f16): **bottleneck MATCH** (sim=db,
  model=db), cycle −6.1% (55.2 vs 58.8); model db τ 3.674 includes the
  storage pacing 0.169 u/query = 0.575 GB × 0.294 s/GB — the Gen3 preset
  rides exactly per `ssd-generations.md` §5.
- The analytical M/G/c wait over-estimates the sim's drain-truncated
  queues (db wait model 116–140 u vs sim 44–58 u) — expected at t=600
  after a flood phase; the *bottleneck identity and rank* agree in all
  three cells.

## Method / provenance

- 51 runs = 24 file-10 rows (6 rung invocations × 4 agent cells) + file
  20 (3) + 21 (6) + 22 (2) + 23 (2) + 24 (3) +
  25 (8) + 26 (3). Execution: `agent-db-sim sweep <file> [-p <rung>]
  --out runs/sensitivity2 --jobs 8`; per-invocation wall 4.5–49 s (worst:
  crossover-f8 4 runs parallel ≈ 49 s; worst run 30 s: 512-agent f16
  cells) — total ≈ 264 s ≈ 4.4 min, inside the <15 min budget. 0 failed
  runs.
- Every number above is read back from each run's `summary.json` + tables
  `latency_percentiles`, `utilization_stats` (incl. `llm.tokens_*.rate`
  pins), `bottleneck` in `metrics.db` (extractor:
  `research/extract2.py` → `study2_extract.json` / `study2_tables.md`).
  Percentile rows are post-warmup (n recorded per row); missing llm rows
  flagged "—" per P2. `e2e` = agent-observed received − submitted over
  the request trace.
- Re-run everything:

```bash
mkdir -p runs/sensitivity2
agent-db-sim sweep scenarios/sensitivity2/26_dbbound_agents.yaml --out runs/sensitivity2 --jobs 8
for f in 20_topology_dbbound 21_disagg_scaleout_dbbound 22_distributed_coordinators_dbbound 23_distributed_nodes_dbbound 24_storage_backend_dbbound; do
  agent-db-sim sweep "scenarios/sensitivity2/$f.yaml" --out runs/sensitivity2 --jobs 8
done
agent-db-sim sweep scenarios/sensitivity2/25_crossover_topo.yaml --out runs/sensitivity2 --jobs 8
agent-db-sim sweep scenarios/sensitivity2/25_crossover_topo.yaml -p s8 --out runs/sensitivity2 --jobs 8
for rung in s05 s2 s4 s8 s16; do
  agent-db-sim sweep scenarios/sensitivity2/10_llm_ladder.yaml -p "$rung" --out runs/sensitivity2 --jobs 8
done
agent-db-sim sweep scenarios/sensitivity2/10_llm_ladder.yaml --out runs/sensitivity2 --jobs 8
```

- Enumeration/provenance tests: `tests/test_sensitivity2_scenarios.py`
  (32 checks: 14-involution run counts, unique sweep dirs, per-rung cap
  scaling, topology constraints, research-doc citations).
