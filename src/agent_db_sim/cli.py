"""typer CLI entry point for agent-db-sim.

Usage: ``agent-db-sim run scenarios/smoke.yaml [--profile fast]``
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from agent_db_sim.config import ConfigError, load_scenario
from agent_db_sim.simkit.errors import SweepError
from agent_db_sim.simkit.runner import run_minimal
from agent_db_sim.simkit.sweep import run_sweep

app = typer.Typer(
    add_completion=False,
    help="agent-db-sim: SimPy-based simulation of agent-database workloads.",
)


@app.callback()
def _root() -> None:
    """agent-db-sim: SimPy-based simulation of agent-database workloads."""


@app.command()
def run(
    scenario: Path = typer.Argument(  # noqa: ARG001 - typer handles exists checks
        ..., exists=True, file_okay=True, dir_okay=False, readable=True,
        help="Scenario config (YAML) to execute.",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Named profile overlay to merge on top of the scenario config.",
    ),
) -> None:
    """Execute a scenario end to end and print a one-line summary."""
    try:
        cfg = load_scenario(scenario, profile)
    except ConfigError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    loop = cfg.loop
    result = run_minimal(
        agents=int(loop.agents),
        n_timeouts=int(loop.n_timeouts),
        timeout=float(loop.timeout),
        until=float(cfg.sim.until),
    )

    typer.echo(
        f"agent-db-sim smoke: {result.agents} agents x {result.timeouts_per_agent} "
        f"timeouts ({result.timeout}u each) -> {result.total_timeouts} timeouts, "
        f"sim end t={result.end_time}u"
    )


@app.command()
def sweep(
    scenario: Path = typer.Argument(  # noqa: ARG001 - typer handles exists checks
        ..., exists=True, file_okay=True, dir_okay=False, readable=True,
        help="Scenario config (YAML) with a 'sweep:' section.",
    ),
    out: Path = typer.Option(
        "runs",
        "--out",
        "-o",
        help="Root output directory; run dirs land under <out>/<sweep-name>/<run-id>/.",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Named profile overlay to merge on top of the scenario config.",
    ),
) -> None:
    """Enumerate the scenario's sweep factors and write one run dir per combo.

    Metrics tables and run.vcd per run are stubs in this ticket; the run
    directories and per-run config snapshots are the contract.
    """
    try:
        cfg = load_scenario(scenario, profile)
    except ConfigError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    try:
        results = run_sweep(cfg, out_root=out)
    except SweepError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    name = results[0].directory.parent.name if results else "sweep"
    typer.echo(
        f"sweep {name!r}: {len(results)} runs written under "
        f"{out / name}/ (config snapshot per run)"
    )


def main() -> None:
    """Console-script entry point."""
    app()


if __name__ == "__main__":
    main()
