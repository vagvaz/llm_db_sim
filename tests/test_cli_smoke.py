"""Smoke tests for the typer CLI against scenarios/smoke.yaml."""

from pathlib import Path

from typer.testing import CliRunner

from agent_db_sim.cli import app

runner = CliRunner()
SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"
SMOKE = str(SCENARIOS / "smoke.yaml")


def test_run_smoke_scenario_end_to_end() -> None:
    result = runner.invoke(app, ["run", SMOKE])
    assert result.exit_code == 0, result.output
    assert "2 agents x 3 timeouts" in result.output
    assert "-> 6 timeouts" in result.output


def test_run_with_profile_overlay() -> None:
    result = runner.invoke(app, ["run", SMOKE, "--profile", "fast"])
    assert result.exit_code == 0, result.output
    assert "4 agents x 2 timeouts" in result.output
    assert "-> 8 timeouts" in result.output


def test_unknown_profile_fails_cleanly() -> None:
    result = runner.invoke(app, ["run", SMOKE, "--profile", "nope"])
    assert result.exit_code == 2
