# sensitivity2/ — study 2: high agent counts x an inference-provisioning
# ladder, with DB axes under a genuinely DB-bound load

Building directly on `../sensitivity/` (study 1 — 40 runs, 4–64 agents;
findings in `.scratch/agent-db-sim/research/sensitivity-study-1.md`).
Study 1's conclusion: with the grounded 70B endpoint (8000 in / 2000 out
tok/s aggregate = 10k tok/s, 32 slots) the LLM token cap binds everything;
DB queueing never develops (compute ≤ 20% busy anywhere) and
topology / scale-out / SSD axes move e2e by ≤ 0.3%. The DB axes were
tested while *asleep*.

Study 2 wakes them up. The DB-compute demand rate of this workload is

    G * 3.505 slot-u/s            (G = completed inferences per second,
                                   mean mix complexity 3.505 CSE/call,
                                   tpc-workloads.md mixed_agent mix)

against 4 generalized-slots of centralized compute. Inference supply
limits G from above: with caps scaled to aggregate 10k*f tok/s and the
grounded mean of ~74.1k tokens/call, G_max(f) = 10^4*f/74,150. The DB
compute station is *offered* 100% when

    f* = (4/3.505) * (74,150/10^4) ≈ 9.2× the grounded caps.

So the provisioning ladder [0.5×, 1×, 2×, 4×, 8×] of the grounded endpoint
reaches the knee at f=8 (offered DB utilization ≈ 95%, queues forming) and
one f=16 rung (2× the grounded `llm_huge` profile) pins the DB outright.
DB-side factors are then swept at f=16, where goodput is DB-capped at
4/3.505 ≈ 1.14 inferences/s.

Grounded base in every file (identical to `scenarios/profiles/
grounded_agent_70b.yaml`): grounded lognormal tokens (mean ~74.1k tok/call),
mixed_agent_retrieval_analytics mix, db.compute.slots 4, issue_ratio 0.2,
three_phase split, ssd_enterprise, network latency 0.0005 / bandwidth
25 GB/s / coordinator_overhead 1.2, `sim.until: 600.0`, `seed: 701`,
`metrics.warmup: 0.1` — all study-1 conventions, so baseline cells are
directly comparable across studies.

Design table (**51 runs**) — seven files, run with `--jobs 8`:

| file | sweep name | factors | runs | question |
|---|---|---|---|---|
| `10_llm_ladder.yaml` | s2-llm-ladder{-f05,-f2,-f4,-f8,-f16,base=f1} | agents [64,128,256,512] × rung f ∈ {0.5,1,2,4,8,16} (profile per rung scales BOTH caps 8000f/2000f) | 24 | the provisioning curve per agent count; where the DB takes over (slots pinned at the grounded 32) |
| `20_topology_dbbound.yaml` | s2-topo-dbbound | db.topology [centralized, disaggregated(N2,M2), distributed(N2,C1)] | 3 | topology ranking with the DB compute saturated (minimal grounded-ish footprint per topology) |
| `21_disagg_scaleout_dbbound.yaml` | s2-disagg-dbbound | db.compute_nodes [1,2,4] × db.storage_nodes [1,2] @ disaggregated | 6 | scale-out relief curve; what N vs M buys |
| `22_distributed_coordinators_dbbound.yaml` | s2-dist-coords-dbbound | db.coordinators [1,2] @ N=4 | 2 | coordinator fan-in (entry-compute capacity axis) |
| `23_distributed_nodes_dbbound.yaml` | s2-dist-nodes-dbbound | db.compute_nodes [2,4] @ C=1 | 2 | peer-count axis (serialization relief only) |
| `24_storage_backend_dbbound.yaml` | s2-storage-dbbound | db.storage.backend [ssd_gen3, ssd_gen4, ssd_enterprise] @ centralized | 3 | SSD generation, e2e-visible this time? |
| `25_crossover_topo.yaml` | s2-crossover-topo{-f8,base=f16} | agents [64,512] × db.topology [centralized, disaggregated(N1,M2)] × rung {16 (base), 8 (`-p s8`)} | 8 | the crossing itself, capacity-matched topologies on both sides (llm.slots = agents ⇒ zero admission queue) |
| `26_dbbound_agents.yaml` | s2-dbbound-agents | agents [64,256,512] @ rung f=16 | 3 | is the DB-bound goodput agent-count-invariant? |

Instruments, per file (two deliberately different ones):

- **Ladder** (`10_`): llm.slots 32 = grounded endpoint shape for every rung;
  the llm admission queue appears at agents > 32 and is reported as its own
  signal, NOT confused with resource binding (study-1 pitfall #2).
- **DB-bound slices** (`20_`–`26_`): inference *generously* provisioned —
  slots ≥ agents (64 or 512) and caps at rung f=16 (128k in / 32k out = 2×
  grounded `llm_huge`) — so llm ingress queues are structurally impossible
  and the token cost factor floor is < 1 u. `25_` also runs the f=8 rung
  per profile. `26_` re-provisions slots per agent count (512 everywhere).

Deliberately NOT swept: rungs beyond f=16, db.compute.slots, llm ingress
order, ssd_gen5 and the legacy hdd/ssd/nvme presets, agents < 64, and
distributed coordinators ≥ compute_nodes (invalid by construction —
validation demands 1 ≤ C < N).

Run all (~4.5 min wall on 16 cores, `--jobs 8`, worst case measured first):

```bash
mkdir -p runs/sensitivity2
agent-db-sim sweep scenarios/sensitivity2/26_dbbound_agents.yaml --out runs/sensitivity2 --jobs 8   # worst case first
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

Analytical cross-checks used throughout (report cites them): `G(f) =
10^4·f/74,150 inferences/s` capped by `min(..., 4/3.505)` on centralized;
per-call DB demand 3.505 CSE (compute) + 0.575 GB (row storage);
`agent-db-sim analyze <run_dir>` for spot verification.

Tables + findings: `.scratch/agent-db-sim/research/sensitivity-study-2.md`.
