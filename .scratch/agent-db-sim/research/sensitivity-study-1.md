# Sensitivity study 1 — agent/LLM/DB simulator: what moves what

Author: fixer subagent (sensitivity sweep lane), 2026-10-13.
Consumes the sweep set `scenarios/sensitivity/0*.yaml` (designed factors, 40
runs, one shared grounded baseline) executed under
`runs/sensitivity/sens-*/` with `--jobs 8`. All numbers below are read back
from the runs' `sweep_summary.json` / `metrics.db` (`latency_percentiles`,
`utilization_stats`, `bottleneck`) — every table cites real sweep output.

Grounding: every baseline parameter is cited in
`scenarios/profiles/grounded_agent_70b.yaml` → `llm-inference-serving.md`
(§4: 8000 in / 2000 out tok/s aggregate caps, 32 slots), `tpc-workloads.md`
(mixed_agent mix + CSE volumes), `ssd-generations.md` (§4 ssd_enterprise,
§5 preset table). None of the numbers were invented; the sweep only *varies*
grounded family members.

## The baseline regime in one paragraph

With grounded agent tokens (~78k input + ~2.5k output tokens per call,
lognormal, p95 input ~160k), four agents — let alone sixteen or sixty-four —
already demand **~21k tok/s aggregate** against a **10k tok/s** endpoint cap:
the inference token cap is the throughput binder from the first run of the
design. DB utilization confirms it: compute `slot_busy` mean is 0.28–0.78 of
4 slots (7–20%) across the entire design, storage is idle to within its base
latencies. Consequently **every sweep cell completes the same inference
count (±15% horizon noise) regardless of DB-side factors**, and the
"elevator" ranking of design levers is:

```
llm tokens/s caps  >  llm slots  >  agents (progress per agent only)
         >>  topology/scale-out  =  ssd generation  ≈  nothing
```

Runs: **40 runs, 47 s wall** (`--jobs 8`; per-file 2–10 s). Each run keeps
the full pipeline (metrics.db + run.vcd + report.html + summary.json).

## Table 1 — axis 1: agents × LLM output-token rate (the inference-vs-db axis)

From `sens-agents-inference` (9 runs, until 600, llm.slots 32 fixed; total
token cap = 8000 in + out out rates). Latencies in time units, goodput =
completed inferences over the ~600 u horizon.

| agents | out tok/s (cap) | inf. done | goodput /100u | e2e p50 | e2e p95 | e2e p99 | llm lat p50 | llm queue p95 | db compute svc p95 | winner (queue share) |
|---|---|---|---|---|---|---|---|---|---|---|
| 4 | 500 (8.5k) | 72 | 12.0 | 11.22 | 50.59 | 60.86 | 24.7 | 0 | 2.40 | storage* (0.000) |
| 4 | 2000 (10k) | 85 | 14.2 | 8.64 | 42.63 | 60.60 | 21.3 | 0 | 2.56 | storage* (0.000) |
| 4 | 8000 (16k) | 134 | 22.3 | 6.58 | 25.87 | 40.21 | 11.6 | 0 | 4.00 | storage* (0.000) |
| 16 | 500 (8.5k) | 63 | 10.5 | 12.29 | 194.54 | 203.61 | 102.6 | 0 | 4.00 | storage* (0.000) |
| 16 | 2000 (10k) | 80 | 13.3 | 12.29 | 164.80 | 235.12 | 92.7 | 0 | 4.00 | storage* (0.000) |
| 16 | 8000 (16k) | 127 | 21.2 | 20.05 | 107.67 | 178.13 | 62.9 | 0 | 4.00 | storage* (0.000) |
| 64 | 500 (8.5k) | 65 | 10.8 | 0.02 | 364.97 | 424.81 | 366.2 | 271.3 | 4.00 | **llm** (0.612) |
| 64 | 2000 (10k) | 75 | 12.5 | 1.54 | 345.45 | 426.96 | 323.1 | 230.3 | 4.00 | **llm** (0.604) |
| 64 | 8000 (16k) | 126 | 21.0 | 1.54 | 316.24 | 361.95 | 235.2 | 155.7 | 4.00 | **llm** (0.520) |

Readings:

1. **Aggregate inference goodput ≈ (in+out token caps) ÷ mean tokens per
   call, independent of agent count.** 8500/85000, 10000/85000,
   16000/85000 tok/s → 10.0, 11.8, 18.8 inferences/100 u predicted;
   10.5–12.0, 12.5–14.2, 21–22.3 measured. Doubling the cap doubles agent
   throughput; adding agents changes *who* waits, not *how much* is
   produced. This is the paper-law crosscheck the design wanted: the
   closed loop cannot escape a shared-rate cap.
2. **DB queueing never develops anywhere in the design.** db compute
   service p95 ≤ 4.0 u with queue p95 = 0.0 in every cell (worst-case
   wide_scan ≈ 20 CSE ÷ 4 slots ÷ issue phases). The DB "side" answers in
   ≤ 0.29 u storage p95; it is never the winner on capacity grounds.
3. The winner flips storage* → llm exactly when **agents (64) > slots
   (32)**: a genuine llm ingress queue of p95 230–271 u at score
   0.52–0.90 (`sens-llm-slots` below pins the mechanism). At ≤ 16 agents
   the winner row is a **saturation tie** (score 0.300, queue share 0) —
   see "surprises" below.

## Table 2 — axis 2: llm.slots (admission capacity)

From `sens-llm-slots` (6 runs; out rate fixed 2000; identical-sim prior to
the cap axis, slots 32 rows replicate Table 1 cells).

| agents | slots | e2e p50 | e2e p95 | llm queue p50 | llm queue p95 | inf. done | winner (queue share) |
|---|---|---|---|---|---|---|---|
| 4 | 8 | 8.64 | 42.63 | 0 | 0 | 85 | storage* (0.000) |
| 4 | 32 | 8.64 | 42.63 | 0 | 0 | 85 | storage* (0.000) |
| 16 | 8 | 16.17 | 160.29 | 50.9 | 86.7 | 78 | **llm** (0.471) |
| 16 | 32 | 12.29 | 164.80 | 0 | 0 | 80 | storage* (0.000) |
| 64 | 8 | 0.02 | 433.89 | 381.5 | 393.6 | 82 | **llm** (0.861) |
| 64 | 32 | 1.54 | 345.45 | 202.8 | 230.3 | 75 | **llm** (0.604) |

- Slots 8 at 4 agents are free (8 > 4). At 16 agents they add a real
  51 u median llm queue yet **goodput moves 80 → 78 inferences (−2%)**:
  with a capped token rate, admission slots are not throughput levers,
  they are latency/queue-shape levers.
- At 64 agents / 8 slots admission serializes badly (8 slots × ~290 u
  mean service ≫ the horizon): e2e p95 433.9 (+26% vs 32 slots),
  queue share 0.861, score 0.903 — the only >0.9 attribution in the study.

## Table 3 — axis 3: topology and (Table 3b) its 2-way with load

From `sens-topology` (N=4, M=2, coordinators=2) and
`sens-agents-topology` (the same 16-agent cells re-measured at 64).
DB-stage latencies per topology differ by construction — centralized
never pays internal hops; disaggregated pays segment req/resp; distributed
pays per-peer-link hops *plus* the 1.2× coordinator compute multiplier.

| cells (p50 / p95 e2e) | centralized | disaggregated | distributed |
|---|---|---|---|
| 16 agents | 12.29 / 164.80 | 12.45 / 164.77 | **14.85** / 163.87 |
| 64 agents | 1.54 / 345.45 | 1.56 / 345.38 | **1.86** / 344.31 |

| dbg (16 agents) | centralized | disaggregated | distributed |
|---|---|---|---|
| compute svc p95 | 4.00 | 2.48–4.00 (4 nodes) | 3.17–4.80 (2 coordinators @ 1.2×) |
| storage svc p95 | 0.286 | 0.039–0.286 (2 striped nodes) | 0.054–0.286 (node-local) |
| net hops p95 | — | net_req 0.001, net_resp 0.16 | net_req 0.001, net_resp 0.02–0.16 |

What scale-out relieves: **nothing in this envelope** — `sens-disagg-scaleout`
shows the six (N ∈ {1,2,4}) × (M ∈ {1,2}) disaggregated cells to be
*bit-for-bit identical in the comparisons that matter*: e2e p50 12.447 /
p95 164.770, 80 inferences completed in every cell. `sens-dist-nodes`
(N ∈ {2,4}) and `sens-dist-coordinators` (C ∈ {1,2}) are likewise
indistinguishable (p95 163.865 in all four). Because DB compute is ≤20%
occupied, N adds capacity nobody spends; M stripes load that is already
trivial; and the search is capped by the LLM, which no DB node change can
relieve. What scale-out *does* do in principle (visibly working per-stage):
disperses per-node compute p95 (2.48–4.0 u across four nodes vs 4.0 on one)
and halves per-node storage pressure — the machinery that would matter once
a heavy-DB mix (cf. `grounded_workloads.yaml`'s join-heavy analytical mix)
replaces the LLM cap as the binder.

The only reproducible topology effect is **distributed costs +21% on e2e
p50 at both loads** (12.29→14.85, 1.54→1.86): every coordinator-fronted
peer query legally pays two network hops + 1.2× coordinator overhead
purely for wiring, with zero compensating benefit at this load.

## Table 4 — axis 4: SSD generation (× topology 2-way)

From `sens-storage-backend` (16 agents; enterprise preset = baseline per the
grounded scenario's "honest DB choice" comment).

| backend (centralized, svc p95 / e2e p95) | ssd_gen3 | ssd_gen4 | ssd_enterprise |
|---|---|---|---|
| storage svc p95 (u) | 1.177 | 0.588 | 0.286 |
| e2e p95 (u) | 164.67 | 164.75 | 164.80 |
| e2e p50 (u) | 13.18 | 12.59 | 12.29 |

**Does the storage generation ever bind? No.** The per-stage tail moves
*exactly* as the presets prescribe — 1.177 ≈ 0.294 s/GB × 4 GB (the
join_heavy volume on Gen3), 0.588 = 0.1471×4 (Gen4), 0.286 = 0.0714×4
(enterprise, the preset table's own numbers) — a clean 4.1×/2.06× tier
cascade on the analytic tail. But even the *slowest* preset keeps storage
service at 1.2 u p95 against 4.0 u compute p95 and a ~93 u median llm
service: a 10-minute-wide gap that the e2e tail (Δ ≤ 0.08%) cannot see.
On disaggregated the striping already shows the split (storage-0 carried
the 0.5 GB small_analytics → 0.162/0.081/0.039; storage-1 carried the 4 GB
joins). Storage would only bind under mixes with ~10× more volume per call
or endpoints ≥ 10× faster than the grounded 70B.

## (a) At what agent count does inference vs db flip?

**It never flips to the DB within the tested envelope (4–64 agents × caps
8500–16000 tok/s).** Quantitatively: DB-side demand = goodput × mean
complexity/call (3.5 CSE ≈ 3.5 u slot-seconds) ÷ 4 slots ≈ 0.11–0.19
slot-u/s against 4.0 available — ≤ 5% (measured: ≤20% peak). Extrapolating
linearly, DB *compute* would approach binding only at ~300+ agents *and*
only if the token cap were scaled up ∝ agents (otherwise the cap caps
progress first). What does flip — visibly, mechanically — is the LLM's own
binding mode: at agents ≤ slots the cap binds as *service stretching*
(llm lat p50 92.7 u, no queue); at agents > slots it binds as *admission
queueing* on top (queue p50 381 u at 8 slots). Raising inference capacity
never uncaps agents into DB saturation: the entire design stays
inference-bound, and DB-side sweeps (topology, scale-out, SSD) move ≤0.3%
of e2e.

## (d) Surprises / pitfalls for the metrics layer

1. **e2e p50 is a mix artifact.** At 64 agents (per-call ~465–500 u vs a
   600 u horizon) the median e2e collapses to 0.02–1.5 u — it's a db
   point-lookup — while *few inferences complete at all* (65–75 across 64
   agents in 600 u ≈ 1 call per agent). Cross-file lesson: compare
   completions + per-stage llm latencies, never bare e2e percentiles,
   between load points. Raising the output cap 2000→8000 even *raised*
   e2e p50 at 16 agents (12.29→20.05) purely because the completion mix
   shifts toward long inference samples — with goodput up 59% (80→127).
2. **Queue-based bottleneck attribution is blind to the cap regime.** At
   agents ≤ slots every row pins at score 0.300, queue share ≈0 and the
   winner (storage) is a saturation tie, although llm service alone
   explains ~92–200 u of each ~100–200 u e2e. Attribution only lights up
   when a *real ingress queue* exists (agents > slots → llm winner,
   queue share 0.47–0.86). Suggested improvement (matches the queued
   M/G/c analytical crosscheck lane): treat sustained llm service-stretch
   (slot_busy = min(agents, slots) with tokens pinned at cap) as a
   first-class bottleneck signal.
3. **Token rates expose the cap:** `utilization_stats` shows
   llm.tokens_input.rate + tokens_output.rate pinning within ~1–5% of the
   configured in+out caps in every cell (e.g. 8181+345 ≈ 8526 vs 8500 at
   4/500; 15829+581 ≈ 16410 vs 16000 at 16/8000).
4. **Distributed is worse-in-every-cell for point loads:** +21% p50 at
   both loads with zero upside; coordinators 1→2 changes nothing (both
   cells byte-identical at 163.865 p95).

## Method / provenance

- 40 runs = 9+6+3+6+2+2+6+6 across the eight files in
  `scenarios/sensitivity/` (design table in `scenarios/sensitivity/
  README.md`); run thesis header comment per file (grounded base, why not
  a full cross product, invalid-combo avoidance for distributed
  coordinators < compute_nodes).
- Execution: `agent-db-sim sweep <file> --out runs/sensitivity --jobs 8`
  (spawn-context bounded-core pool; `nproc` = 16, capped at 8). Wall per
  file: 10/6/5/5/4/4/5/7 s; total 47 s; 0 failed runs.
- All analysis numbers pulled from each run's summary.json +
  `metrics.db` (tables `latency_percentiles`, `utilization_stats`,
  `bottleneck`; `sim.until` 600, `metrics.warmup` 0.1 so percentile rows
  are post-warmup). Percentile sample sizes are recorded per row in the
  runs; note the small llm.* ns (7–77) at cap-heavy cells — the llm-bound
  conclusions above rest on goodput + queue-delay attribution, not on the
  limited tail rows.
- Re-run everything: `for f in scenarios/sensitivity/0*.yaml; do
  agent-db-sim sweep "$f" --out runs/sensitivity --jobs 8; done`.
