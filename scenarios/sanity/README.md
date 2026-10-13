# sanity/ — 1-of-everything verification scenarios

Four minimal deterministic scenarios whose latency math a human can verify
on paper. Every parameter is a constant (fixed token draws, one fixed
query profile, pinned seed `sim.seed: 0`), each run stops ~50 time units,
and the test suite (`tests/test_sanity_scenarios.py`) recomputes the paper
numbers from the YAMLs and asserts them to `1e-9` — per request, per hop,
plus the derived tables (`latency_percentiles`, `bottleneck`) equal the
percentiles of the hand multisets.

Run any of them directly:

```sh
agent-db-sim run scenarios/sanity/sanity_single.yaml --out runs
```

## The paper math (same constants in all scenarios)

```
L            = 512/1024 + 128/256                = 1.0      LLM service (solo slot hold)
issue        = 0.25 × 0.4 complex                = 0.1
storage hop  = 0.0001 + max(2.5×0.08,            = 0.2001   ssd preset: base 0.0001,
               0.08/0.4, 1/75000)                              per_gb 2.5 GB/tu,
                                                               bw 0.4 GB/s, iops 75000
final        = 0.75 × 0.4 complex                = 0.3
D (centralized db latency)                       = 0.6001
tau = L + D (loop period, nothing queues)        = 1.6001
```

## Scenario table (expected = hand math, actual = measured by the tests)

| scenario | what it proves | expected | actual |
|---|---|---|---|
| `sanity_single.yaml` — 1 agent, 1 LLM slot, centralized (1 compute slot, 1 ssd storage) | every stage of a task-free system and the exact loop | L=1.0, D=0.6001, τ=1.6001; 32 inf sent/received, 32/31 db; every hop exact (issue 0.1, storage 0.2001 incl. base 0.0001 queue, finalize 0.3); no `net_*` stages | identical `float`-for-`float` (≤1e-9 across all 64 requests); e2e p50/p95/p99 = 1.000 (n=63) |
| `sanity_queue.yaml` — 2 agents, LLM 1 slot | FIFO serialization exactly as hand-traced | a0 inf #1 queue 0; a1 inf #1 queue 1.0, e2e 2.0 = service + wait; every later inference (both agents) queues L−D=0.3999, service stays 1.0; dbs never queue | identical for all 51 LLM requests and 51 dbs; e2e p50/p95/p99 = 1.000/1.400/1.400 (n=101); bottleneck `llm.*` queue_share 0.203 = hand sum 20.5951/101.6001 |
| `sanity_parallel.yaml` — 2 agents, LLM 2 slots (storage ceiling-override, see YAML) | parallelism with zero queueing; e2e = single-request latency | L=1.0 (token cap 2×1280/2 = phase floor), D=0.7201, τ=1.7201; all queue times 0; both agents in perfect lockstep lockstep (overlapping spans) | identical (30+30 infs, 29+29 dbs); overlapping verified per request index; e2e p50/p95/p99 = 1.000 (n=118) |
| `sanity_disaggregated.yaml` — 1 agent, disaggregated (1 compute, 1 storage, 1 segment) | the net hops exist and are exact | net_req = 0.02 + 0.01/20 = 0.0205; net_resp = 0.02 + 0.08/20 = 0.024; D = 0.6446, τ = 1.6446 | identical per request (31/31 dbs); e2e p50/p95/p99 = 0.822/1.000/1.000 (n=62) |

## Notes for reading the derived tables

- `metrics.warmup: 0.0` in every sanity scenario: the loop is exactly
  periodic, so the derived statistics include every completed request.
- The `storage_*` trace-machine "queue" is the backend's fixed
  `base_latency` charge (0.0001 – charged before the paced service). The
  centralized scenarios never touch the pool; on disaggregated the
  propagation_wait is stage-machine queue (pool queue time is 0 here).
- The hybrid `bottleneck` supporting term (`sat_score`) derives from
  the ingress occupancy probes; in these queue-free systems the queue
  shares are hand-derived (LLM in `sanity_queue`, storage base charges
  elsewhere), and `score = 0.7·queue_share + 0.3·sat_score` per row is
  what the tests assert.

## Artifacts

Each run directory contains the full 6-artifact set:
`config.yaml`, `metrics.db`, `run.vcd`, `report.html`,
`summary.json`, `plotly.min.js`.
