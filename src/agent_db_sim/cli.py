"""typer CLI entry point for agent-db-sim.

Usage: ``agent-db-sim run scenarios/topology_compare.yaml [--out runs]``
       ``agent-db-sim sweep scenarios/topology_compare.yaml [--out runs]``
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

app = typer.Typer(
    add_completion=False,
    help="agent-db-sim: SimPy-based simulation of agent-database workloads.",
)

DEFAULT_VIZ_PORT = 8060
DEFAULT_OUT = "runs"



@app.callback()
def _root() -> None:
    """agent-db-sim: SimPy-based simulation of agent-database workloads."""


@app.command()
def run(
    scenario: Path = typer.Argument(  # noqa: ARG001 - typer handles exists checks
        ..., exists=True, file_okay=True, dir_okay=False, readable=True,
        help="Scenario config (YAML) to execute.",
    ),
    out: Path = typer.Option(
        DEFAULT_OUT,
        "--out",
        "-o",
        help="Root output directory; the run dir lands under <out>/<scenario-stem>/. ",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Named profile overlay to merge on top of the scenario config.",
    ),
) -> None:
    """Execute a scenario end to end and write one complete run directory.

    Scenario configs with a ``db:`` section run the full model pipeline
    (build_model -> simulate -> metrics.db + run.vcd + report.html +
    summary.json); the minimal ``loop:``-only smoke scenario keeps its
    placeholder loop.
    """
    try:
        cfg = load_scenario(scenario, profile)
    except ConfigError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if "db" not in cfg:
        # placeholder smoke loop (ticket 06; real scenarios carry `db:`)
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
        return

    from agent_db_sim.pipeline import execute_run

    run_dir = _next_run_dir(out, scenario.stem)
    try:
        summary = execute_run(cfg, run_dir)
    except Exception as exc:  # fail visible: report and exit non-zero
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

    e2e = summary.get("e2e_percentiles") or {}
    winner = summary.get("bottleneck_winner") or {}
    typer.echo(f"run dir: {run_dir}")
    typer.echo(
        f"  e2e p50/p95/p99 = "
        f"{e2e.get('p50', float('nan')):.3f}/{e2e.get('p95', float('nan')):.3f}/"
        f"{e2e.get('p99', float('nan')):.3f} (n={e2e.get('n', 0)})"
    )
    typer.echo(
        f"  bottleneck: {winner.get('component', '<none>')}.{winner.get('stage', '*')} "
        f"(score={winner.get('score', 0):.3f}, queue_share={winner.get('queue_share', 0):.3f})"
    )


def _next_run_dir(out_root: Path, stem: str) -> Path:
    """First free ``<out_root>/<stem>[-<n>]`` directory for a single run."""
    base = out_root / stem
    if not base.exists():
        return base
    n = 2
    candidate = out_root / f"{stem}-{n}"
    while candidate.exists():
        n += 1
        candidate = out_root / f"{stem}-{n}"
    return candidate



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
    jobs: Optional[int] = typer.Option(
        None,
        "--jobs",
        "-j",
        min=1,
        help=(
            "Maximum parallel worker processes for the sweep runs "
            "(default: sweep.max_jobs from the scenario, else 1 = sequential)."
        ),
    ),
    fail_fast: bool = typer.Option(
        False,
        "--fail-fast",
        help="Abort the remaining queued runs after the first failed run.",
    ),
) -> None:
    """Enumerate the scenario's sweep factors: one full run per combination.

    Every run dir gets the complete artifact set (config snapshot, metrics.db,
    run.vcd, report.html, summary.json) plus a sweep-level
    ``sweep_summary.json`` / ``sweep_summary.html`` comparing the runs. The
    placeholder ``loop:``-only scenarios keep simkit's stub sweep runner.

    ``--jobs N`` bounds the cores used: the factorial runs execute in a
    process pool of ``min(N, len(runs))`` workers; the sweep summary stays in
    enumeration order (identical to the sequential ``--jobs 1`` output).
    A failed run marks its entry failed in the summary; a failed-dominated
    sweep exits non-zero.
    """
    from omegaconf import OmegaConf

    try:
        cfg = load_scenario(scenario, profile)
    except ConfigError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    # workers: --jobs N > sweep.max_jobs config key > 1 (sequential)
    from agent_db_sim.simkit.sweep import validate_jobs

    if jobs is None:
        configured = OmegaConf.select(cfg, "sweep.max_jobs")
        jobs = 1 if configured is None else int(configured)
    workers = validate_jobs(jobs)

    try:
        if "db" in cfg:
            from agent_db_sim.pipeline import enumerate_sweep

            results, summary = enumerate_sweep(
                cfg, out_root=out, jobs=workers, fail_fast=fail_fast
            )
        else:
            from agent_db_sim.simkit.sweep import run_sweep

            results = run_sweep(cfg, out_root=out)
            summary = {}
    except SweepError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    name = results[0].directory.parent.name if results else "sweep"
    n_failed = sum(1 for run in summary.get("runs", []) if run.get("failed"))
    aborted = bool(summary.get("aborted"))
    if aborted:
        typer.echo(
            f"sweep aborted early: {summary.get('skipped', '?')} queued run(s) skipped"
        )
    typer.echo(
        f"sweep {name!r}: {len(results)} runs written under "
        f"{out}/{name}/ (workers={workers})"
    )
    for run in summary.get("runs", []):
        if run.get("failed"):
            typer.secho(
                f"  {run['run_id']}: FAILED: {run.get('error', '<unknown>')}",
                fg=typer.colors.RED,
            )
            continue
        e2e = run.get("e2e_percentiles") or {}
        winner = run.get("bottleneck_winner") or {}
        typer.echo(
            f"  {run['run_id']}: e2e p95={e2e.get('p95', float('nan')):.3f} "
            f"bottleneck={winner.get('component', '<none>')}."
            f"{winner.get('stage', '*')} (score={winner.get('score', 0):.3f})"
        )
    if n_failed:
        typer.secho(
            f"sweep finished with {n_failed} failed run(s) "
            f"(of {len(summary.get('runs', []))} executed)",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

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
    sweep: Optional[Path] = typer.Option(
        None, "--sweep",
        exists=True, file_okay=False, readable=True,
        help=(
            "Render a sweep-comparison report over a sweep root "
            "(contains sweep_summary.json) instead of the run dir; "
            "--name defaults to sweep_compare."
        ),
    ),
) -> None:
    """Render a report as standalone HTML (no server; CI-friendly)."""
    from agent_db_sim.viz import report as viz_report
    try:
        if sweep is not None:
            if name == "standard":
                name = "sweep_compare"
            html = viz_report.render_sweep_report(sweep, name=name, standalone=True)
            dest = sweep
        else:
            html = viz_report.render_report(run_dir, name=name, standalone=True)
            dest = run_dir
    except (FileNotFoundError, ValueError) as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    out_path = Path(out) if out is not None else dest / f"{name}.html"
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
