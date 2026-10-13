# 06 — Project skeleton: Python 3.10, typer CLI, OmegaConf profiles

Type: task
Status: resolved
Blocked by:

## Question

Set up the runnable project foundation:

- Retarget to **Python 3.10**: `requires-python`, `.python-version`, any tooling pins (repo currently pins 3.13).
- **typer** CLI with a `run` command executing a scenario from an **OmegaConf** config file (named profiles: e.g. `scenarios/*.yaml` + profile overlay selection).
- Repo layout: `src/agent_db_sim/` package structure anticipating components/simkit/metrics/viz, `scenarios/`, `tests/`, `pyproject.toml` scripts.
- Minimal smoke test: `agent-db-sim run <config>` executes a trivial SimPy loop end-to-end and exits 0 (proves SimPy + typer + OmegaConf wiring on 3.10).

AFK: purely mechanical, no design decisions beyond layout (layout defers to ticket 03 if it lands first).

## Answer

Built the runnable project foundation on Python 3.10.

**Retarget**: `requires-python = ">=3.10"` in `pyproject.toml`, `.python-version` → `3.10` (uv picked up CPython 3.10.19 already provisioned; no `uv python install` needed).

**Dependencies**: simpy 4.1, typer 0.27, omegaconf 2.4 (runtime); pytest 8 in a `[dependency-groups] dev` group.

**Layout** (conventional baseline, reshaping deferred to ticket 03):
- `src/agent_db_sim/` — `config.py` (OmegaConf scenario loading + named-profile overlay resolution), `cli.py` (typer app)
- `src/agent_db_sim/simkit/` — `runner.py` (minimal SimPy loop: N dummy closed-loop processes each timeout `n_timeouts` times); `components/`, `metrics/`, `viz/` — empty `__init__.py` placeholders
- `scenarios/smoke.yaml` — base config (`sim.until`, `loop.{agents,n_timeouts,timeout}`) + inline `profiles: fast:` overlay
- `tests/test_cli_smoke.py` — CliRunner smoke tests

**CLI**: `agent-db-sim run scenarios/smoke.yaml [--profile fast]`. Loads the scenario via OmegaConf, deep-merges the named profile (inline `profiles:` key first, then sibling `<profile>.yaml` file), runs the minimal SimPy loop, prints a one-line summary, exits 0. Entry point fixed to `agent-db-sim = "agent_db_sim.cli:main"`. Note: an explicit `@app.callback()` was needed because typer collapses a single-command app — this keeps `run` a required subcommand.

**Commands to run**:
```
uv sync
uv run agent-db-sim run scenarios/smoke.yaml --profile fast
uv run pytest
```

**Verification**: `uv sync` on CPython 3.10.19 builds the package and installs all deps; `uv run pytest` → 3 passed (base scenario, `--profile fast` overlay, unknown-profile error path); installed CLI ran end-to-end printing `agent-db-sim smoke: 4 agents x 2 timeouts (0.5u each) -> 8 timeouts, sim end t=5.0u`, exit 0.
