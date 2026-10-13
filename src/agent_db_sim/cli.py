"""typer CLI entry point for agent-db-sim.

Usage: ``agent-db-sim run scenarios/smoke.yaml [--profile fast]``
       ``agent-db-sim serve <run-dir> [--port]``   (viz server)
       ``agent-db-sim render <run-dir> [--name]``  (standalone report HTML)
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

DEFAULT_VIZ_PORT = 8060



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

def _require_viz_run(run_dir: Path) -> None:
    import agent_db_sim.viz.server as viz_server
    if not (run_dir / viz_server.VCD_NAME).exists():
        typer.secho(
            f"error: {run_dir} has no {viz_server.VCD_NAME} "
            "(run a scenario/sweep first, or check the run directory)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

@app.command()
def serve(
    run_dir: Path = typer.Argument(
        ..., exists=True, file_okay=False, readable=True,
        help="Run directory (with run.vcd [+ metrics.db]) to serve.",
    ),
    port: int = typer.Option(
        DEFAULT_VIZ_PORT, "--port", "-p",
        help="Port to bind the visualization server to.",
    ),
    host: str = typer.Option(
        "127.0.0.1", "--host",
        help="Host interface to bind.",
    ),
) -> None:
    """Serve a completed (or still-running) run: waveforms + reports."""
    import uvicorn

    _require_viz_run(run_dir)
    from agent_db_sim.viz.server import create_app
    typer.echo(f"viz server: http://{host}:{port}/  ({run_dir})")
    uvicorn.run(create_app(run_dir), host=host, port=port, log_level="info")

@app.command()
def render(
    run_dir: Path = typer.Argument(
        ..., exists=True, file_okay=False, readable=True,
        help="Run directory (with metrics.db) to render the report from.",
    ),
    name: str = typer.Option(
        "standard", "--name", "-n",
        help="Report template name (see its templates/ dir, auto-indexed).",
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", "-o",
        help="Output HTML path (default: <run-dir>/<name>.html).",
    ),
) -> None:
    """Render a report as standalone HTML (no server; CI-friendly)."""
    from agent_db_sim.viz import report as viz_report
    try:
        html = viz_report.render_report(run_dir, name=name, standalone=True)
    except FileNotFoundError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    out_path = Path(out) if out is not None else run_dir / f"{name}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html)
    _ship_plotly(out_path.parent)  # script tag points at the local copy
    typer.echo(f"report '{name}' -> {out_path}")

def _ship_plotly(directory: Path) -> Path:
    """Copy the vendored plotly.min.js next to standalone HTML output."""
    from agent_db_sim.viz.report import static_dir
    dest = directory / "plotly.min.js"
    if not dest.exists():
        src = static_dir / "plotly.min.js"
        dest.write_bytes(src.read_bytes())
    return dest


def main() -> None:
    """Console-script entry point."""
    app()


if __name__ == "__main__":
    main()
