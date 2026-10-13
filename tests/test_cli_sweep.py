"""CLI test for the `agent-db-sim sweep` command."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from agent_db_sim.cli import app

runner = CliRunner()

SCENARIO_TEXT = """\
sim:
  until: 5.0

loop:
  agents: 1
  n_timeouts: 1
  timeout: 1.0

sweep:
  name: cli-sweep
  factors:
    loop.agents: [1, 2]
    loop.timeout: [0.25, 0.5]
"""


def test_sweep_command_writes_run_dirs(tmp_path: Path) -> None:
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(SCENARIO_TEXT)
    out = tmp_path / "out"

    result = runner.invoke(app, ["sweep", str(scenario), "--out", str(out)])

    assert result.exit_code == 0, result.output
    assert "cli-sweep" in result.output
    assert "4 runs" in result.output

    sweep_root = out / "cli-sweep"
    run_dirs = sorted(
        d for d in sweep_root.iterdir() if d.is_dir()
    )
    assert len(run_dirs) == 4
    for run_dir in run_dirs:
        assert (run_dir / "config.yaml").is_file()
        assert (sweep_root / "sweep.json").is_file()


def test_sweep_without_sweep_section_fails_cleanly(tmp_path: Path) -> None:
    scenario = tmp_path / "nosweep.yaml"
    scenario.write_text("sim:\n  until: 1.0\n")

    result = runner.invoke(app, ["sweep", str(scenario), "--out", str(tmp_path)])

    assert result.exit_code == 2
    assert "no 'sweep:' section" in result.output
