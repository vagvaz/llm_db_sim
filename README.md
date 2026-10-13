# agent_db_sim — DES simulator (SimPy)

Discrete-event simulation of closed-loop LLM agents doing *think → LLM call → DB query*
against a DB endpoint, built on **SimPy** (Python ≥ 3.10, uv-managed).

> The **analytical/probabilistic model** with the browser analyzer lives in
> [`analysis/`](analysis/README.md) (queueing theory + Monte Carlo cross-check, no dependency
> on this simulator).

## Status: placeholder kernel

The current implementation is a SimPy smoke-test wiring (ticket #07 will replace it with the
real kernel: AgentClient / LLMEndpoint / DBEndpoint with slot capacity, token/s limits and
queueing). Per-program output is only a `LoopResult` dataclass and a stdout summary line —
no metrics tables on disk yet (planned parquet/CSV contract in ticket #11).

## Usage

```bash
uv run agent-db-sim run scenarios/smoke.yaml [-p fast]
# tests
uv run pytest
```

## Scenario parameters (`scenarios/smoke.yaml`)

| Key | Default | Meaning |
|---|---|---|
| `sim.until` | 10.0 | simulated time horizon |
| `loop.agents` | 2 | concurrent agent processes |
| `loop.n_timeouts` | 3 | timeouts per agent |
| `loop.timeout` | 1.0 | wait per timeout event |

Profiles (`-p fast`) deep-merge an overlay from the `profiles:` block
(fast: `until 5.0, agents 4, n_timeouts 2, timeout 0.5`).

## CLI

`agent-db-sim run <scenario.yaml> [-p <profile>]` — entry point `agent_db_sim.cli:main`.

## Intended model (roadmap)

Closed-loop agents; per-hop queueing latencies (agent-facing = inference + DB latency;
DB = compute wait / storage wait / network transfer), slot occupancy, token throughput vs
cap, three DB topologies (centralized / disaggregated N×M / distributed), factorial sweeps
over agents × profiles, and agent-facing DB latency percentiles p50/p95/p99.
See `.scratch/agent-db-sim/map.md` and the ticket tracker `.scratch/agent-db-sim/issues/`.
