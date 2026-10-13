# agent_db_sim — DES simulator (SimPy)

A discrete-event simulation platform that models **closed-loop LLM agents**
working against a simulated **database** (compute + storage + internal
network) to study bottlenecks and the behavior of different database
**topologies** (centralized, disaggregated, distributed) under agent
workloads — with factorial sweeps, per-run SQLite metrics, VCD waveforms,
report generation, and a live in-browser visualization server.

Built on **SimPy** (Python ≥ 3.10, uv-managed).

> The **analytical/probabilistic model** with the browser analyzer lives in
> [`analysis/`](analysis/README.md) (queueing theory + Monte Carlo cross-check, no dependency
> on this simulator).

## Documentation

**📖 [USER_GUIDE.md](USER_GUIDE.md)** — install, quickstart, scenario/config
reference, command reference, how to read results, the topology-comparison
worked example, and extension recipes.

## 60-second tour

```bash
uv sync                                            # install
uv run agent-db-sim run scenarios/smoke.yaml       # wiring check
uv run agent-db-sim run scenarios/topology_compare.yaml   # full pipeline:
                                                   # config.yaml, metrics.db,
                                                   # run.vcd, report.html, summary.json
uv run agent-db-sim sweep scenarios/topology_compare.yaml # 12-run factorial:
                                                   # 3 topologies × scale-out × agents
uv run agent-db-sim serve runs/topology_compare/    # live waveforms + reports
                                                   # at http://127.0.0.1:8060/
uv run pytest                                      # test suite
```

## Snippets

| Piece | Where |
|---|---|
| CLI (`run`, `sweep`, `serve`, `render`) | `src/agent_db_sim/cli.py` |
| Model components (AgentClient, LLMEndpoint, DBEndpoint) | `src/agent_db_sim/components/` |
| Simulation kernel (slots, ingress, sweeps, registry) | `src/agent_db_sim/simkit/` |
| Metrics (traces → percentiles/utilization/attribution) | `src/agent_db_sim/metrics/` |
| Viz (VCD writer, FastAPI/SSE server, Jinja2+Plotly reports) | `src/agent_db_sim/viz/` |
| Scenarios | `scenarios/*.yaml` |
| Design history & glossary | `CONTEXT.md`, `.scratch/agent-db-sim/` |
