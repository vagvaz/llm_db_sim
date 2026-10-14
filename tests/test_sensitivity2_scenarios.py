"""Light tests for `scenarios/sensitivity2/` — parse + enumeration only.

Sibling of `tests/test_sensitivity_utils.py`-style checks (compare
``test_grounded_profiles.py``): the sweep set must parse, its run
enumerations must match the README design table (51 runs across 14
invocations with unique sweep dirs), every profile overlay must scale BOTH
token caps by its rung factor, and every factored topology combo must
satisfy the topology validation constraints. No pipeline execution — the
real runs live in ``runs/sensitivity2`` (gitignored).
"""

from __future__ import annotations

from itertools import product
from pathlib import Path

import pytest
from omegaconf import OmegaConf

from agent_db_sim.config import load_scenario
from agent_db_sim.simkit.sweep import apply_factors, read_sweep

REPO = Path(__file__).resolve().parents[1]
SCENARIO_DIR = REPO / "scenarios" / "sensitivity2"
RESEARCH_DIR = REPO / ".scratch" / "agent-db-sim" / "research"

#: (file stem, profile, expected sweep dir, expected run count)
INVOCATIONS: list[tuple[str, str | None, str, int]] = [
    ("10_llm_ladder", None, "s2-llm-ladder", 4),
    ("10_llm_ladder", "s05", "s2-llm-ladder-f05", 4),
    ("10_llm_ladder", "s2", "s2-llm-ladder-f2", 4),
    ("10_llm_ladder", "s4", "s2-llm-ladder-f4", 4),
    ("10_llm_ladder", "s8", "s2-llm-ladder-f8", 4),
    ("10_llm_ladder", "s16", "s2-llm-ladder-f16", 4),
    ("20_topology_dbbound", None, "s2-topo-dbbound", 3),
    ("21_disagg_scaleout_dbbound", None, "s2-disagg-dbbound", 6),
    ("22_distributed_coordinators_dbbound", None, "s2-dist-coords-dbbound", 2),
    ("23_distributed_nodes_dbbound", None, "s2-dist-nodes-dbbound", 2),
    ("24_storage_backend_dbbound", None, "s2-storage-dbbound", 3),
    ("25_crossover_topo", None, "s2-crossover-topo", 4),
    ("25_crossover_topo", "s8", "s2-crossover-topo-f8", 4),
    ("26_dbbound_agents", None, "s2-dbbound-agents", 3),
]

#: per-profile token-cap rungs: (file, profile, in, out) — base = f1/f16
CAPS: dict[tuple[str, str], tuple[float, float]] = {
    ("10_llm_ladder", None): (8000.0, 2000.0),
    ("10_llm_ladder", "s05"): (4000.0, 1000.0),
    ("10_llm_ladder", "s2"): (16000.0, 4000.0),
    ("10_llm_ladder", "s4"): (32000.0, 8000.0),
    ("10_llm_ladder", "s8"): (64000.0, 16000.0),
    ("10_llm_ladder", "s16"): (128000.0, 32000.0),
    ("25_crossover_topo", None): (128000.0, 32000.0),
    ("25_crossover_topo", "s8"): (64000.0, 16000.0),
}

#: ratio of profile caps to the grounded base caps (10k aggregate)
RUNG = {(4000.0, 1000.0): 0.5, (8000.0, 2000.0): 1.0, (16000.0, 4000.0): 2.0,
        (32000.0, 8000.0): 4.0, (64000.0, 16000.0): 8.0, (128000.0, 32000.0): 16.0}


@pytest.mark.parametrize(("stem", "profile", "sweep_name", "n_runs"), INVOCATIONS)
def test_invocation_enumerates(stem: str, profile: str | None, sweep_name: str, n_runs: int) -> None:
    """Each (file, profile) invocation parses and enumerates the designed
    run count under its unique sweep directory."""
    cfg = load_scenario(SCENARIO_DIR / f"{stem}.yaml", profile)
    name, factors = read_sweep(cfg)
    assert name == sweep_name
    assert factors, f"{stem}({profile}): no sweep factors"
    n = len(list(product(*factors.values())))
    assert n == n_runs, f"{stem}({profile}): enumerated {n}, design says {n_runs} ({factors})"


def test_design_table_1472_total() -> None:
    """The README's 51-run / 14-invocation design table, no duplicate
    sweep dirs (profile overlays rename their sweep to avoid run-dir
    collisions when re-invoking the same scenario file)."""
    dirs = {(stem, profile): sweep_name for stem, profile, sweep_name, _ in INVOCATIONS}
    assert len(set(dirs.values())) == len(dirs), "duplicate sweep dirs in design"
    total = sum(n for *_, n in INVOCATIONS)
    assert total == 51
    assert len(INVOCATIONS) == 14


@pytest.mark.parametrize(("stem", "profile"), sorted(CAPS, key=lambda kv: kv[0]))
def test_rung_scales_both_caps(stem: str, profile: str | None) -> None:
    """Rung profiles scale BOTH caps by the rung factor (aggregate cap =
    10k tok/s * f, never one cap alone)."""
    cfg = load_scenario(SCENARIO_DIR / f"{stem}.yaml", profile)
    caps = (float(OmegaConf.select(cfg, "llm.input_tokens_per_s")),
            float(OmegaConf.select(cfg, "llm.output_tokens_per_s")))
    assert caps == CAPS[(stem, profile)]
    assert RUNG[caps] == pytest.approx(caps[0] / 8000.0) == pytest.approx(caps[1] / 2000.0)


@pytest.mark.parametrize(
    ("stem", "profile", "sweep_name", "_n"),
    [inv for inv in INVOCATIONS if inv[2].rstrip("-f8") and inv[0] != "10_llm_ladder"],
)
def test_topology_constraints_hold(stem: str, profile: str | None, sweep_name: str, _n: int) -> None:
    """Every factored DB combo satisfies the topology validation rules:
    disaggregated N>=1/M>=1; distributed 1 <= coordinators < compute_nodes
    and compute_nodes >= 2; slots >= 1."""
    cfg = load_scenario(SCENARIO_DIR / f"{stem}.yaml", profile)
    _name, factors = read_sweep(cfg)
    for combo in product(*factors.values()):
        run = apply_factors(cfg, dict(zip(factors, combo)))
        topo = OmegaConf.select(run, "db.topology")
        nodes = OmegaConf.select(run, "db.compute_nodes")
        coords = OmegaConf.select(run, "db.coordinators")
        stor = OmegaConf.select(run, "db.storage_nodes")
        if topo == "distributed":
            assert nodes >= 2 and 1 <= coords < nodes, (combo, nodes, coords)
        if topo == "disaggregated":
            assert nodes >= 1 and stor >= 1, (combo, nodes, stor)
        assert OmegaConf.select(run, "llm.slots") >= 1


def test_scenario_files_cite_research() -> None:
    """Every sensitivity2 header cites its three grounding research docs
    (the study-1 provenance contract)."""
    expected = {"llm-inference-serving.md", "tpc-workloads.md", "ssd-generations.md"}
    found = sorted(RESEARCH_DIR.glob("*.md"))
    assert expected <= {p.name for p in found}, found
    for path in sorted(SCENARIO_DIR.glob("*.yaml")):
        header = path.read_text().split("\nsim:")[0]
        for doc in expected:
            assert doc in header, f"{path.name}: header must cite {doc}"
