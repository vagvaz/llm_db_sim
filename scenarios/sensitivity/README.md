# sensitivity/ — one designed sensitivity sweep, eight intentional files

Grounded base (identical in every file, from `scenarios/profiles/grounded_agent_70b.yaml`):

- **LLM endpoint**: 70B-class midpoint — aggregate caps 8000 tok/s input +
  2000 tok/s output, 32 slots (ref: `../research/llm-inference-serving.md` §4)
- **Agents**: 16 (unless swept) with grounded lognormal tokens — median
  input ~60k, mean ~78k, output mean ~2.5k per call — and the
  `mixed_agent_retrieval_analytics` TPC-derived query mix
  (ref: `../research/tpc-workloads.md`)
- **DB**: 4 compute slots / node, `ssd_enterprise` storage
  (ref: `../research/ssd-generations.md` §4), three-phase split with
  `issue_ratio` 0.2, 100 Gb-class internal network
- all files `sim.until: 600.0`, `seed: 701` so cross-file baselines are
  directly comparable

Axes (one-at-a-time + designed 2-way interactions; **40 runs total**):

| file | sweep name | factors | runs | question |
|---|---|---|---|---|
| `01_agents_x_inference.yaml` | sens-agents-inference | agents [4,16,64] × llm.output_tokens_per_s [500,2000,8000] | 9 | inference-vs-db axis: where is the bottleneck per (load, capacity)? |
| `02_llm_slots.yaml` | sens-llm-slots | agents [4,16,64] × llm.slots [8,32] | 6 | admission capacity vs token-rate capacity |
| `03_topology.yaml` | sens-topology | db.topology [centralized, disaggregated, distributed] @ N=4 | 3 | wiring cost/ranking at 16 agents |
| `04_disagg_scaleout.yaml` | sens-disagg-scaleout | db.compute_nodes [1,2,4] × db.storage_nodes [1,2] | 6 | disaggregated scale-out frontier |
| `05_distributed_nodes.yaml` | sens-dist-nodes | db.compute_nodes [2,4] (coordinators=1) | 2 | distributed peer count |
| `06_distributed_coordinators.yaml` | sens-dist-coordinators | db.coordinators [1,2] @ N=4 | 2 | coordinator fan-in |
| `07_storage_backend.yaml` | sens-storage-backend | db.storage.backend [ssd_gen3, ssd_gen4, ssd_enterprise] × db.topology [centralized, disaggregated] | 6 | storage generation vs wiring interaction |
| `08_agents_x_topology.yaml` | sens-agents-topology | agents [16,64] × db.topology [3] | 6 | does the topology ranking change at 64 agents? |

Deliberately NOT swept: `db.compute_nodes` on centralized (no-op), `ssd_gen5`
(hdd/ssd/nvme legacy presets, llm.input_tokens_per_s beyond the groundeds —
factor files keep the product intentional, not blind).

Run all eight (~47 s wall on 16 cores, `--jobs 8`):

```bash
for f in scenarios/sensitivity/0*.yaml; do
  agent-db-sim sweep "$f" --out runs/sensitivity --jobs 8
done
```

Findings + tables: `.scratch/agent-db-sim/research/sensitivity-study-1.md`.
