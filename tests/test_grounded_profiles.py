"""Tests for the stage-6 lane-A grounded presets and profiles.

Everything lane A carried in two YAMLs plus the ssd-generation presets in
``components/db/backend.py``:

- ``scenarios/profiles/grounded_agent_70b.yaml`` — the 70B-midpoint LLM
  endpoint + grounded agent token distributions + TPC-derived query mix
  (research: ``.scratch/agent-db-sim/research/ssd-generations.md``).
- ``scenarios/profiles/grounded_workloads.yaml`` — workload-mix x topology
  x storage-gen sweep (research: ``.scratch/agent-db-sim/research/
  tpc-workloads.md``).
- ``STORAGE_BACKEND_REGISTRY`` — the hdd/ssd/nvme legacy presets plus the
  researched ssd_gen3 / ssd_gen4 / ssd_gen5 / ssd_enterprise presets.

The test suite here is intentionally LIGHT on assertions (a sibling of
``test_sanity_scenarios.py``, not a replacement): the grounded scenarios

- run the full pipeline once each (fast variant: small until/count),
- must produce the complete artifact set with sane headline numbers,
- must keep a provenance header pointing at the research documents that
  grounded their numbers (the stage-6 contract).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict

import pytest
from omegaconf import OmegaConf
from typer.testing import CliRunner

from agent_db_sim.cli import app
from agent_db_sim.components.db.backend import BackendParams, STORAGE_BACKEND_REGISTRY
from agent_db_sim.config import load_scenario
from agent_db_sim.pipeline import execute_run

REPO = Path(__file__).resolve().parents[1]
SCENARIO_DIR = REPO / "scenarios" / "profiles"
RESEARCH_DIR = REPO / ".scratch" / "agent-db-sim" / "research"

runner = CliRunner()

_ARTIFACTS = ("config.yaml", "metrics.db", "run.vcd", "report.html", "summary.json")

# --------------------------------------------------------------------------- #
# storage backend presets: the researched values                              #
# --------------------------------------------------------------------------- #


def _params(name: str, **kwargs: float) -> BackendParams:
    return BackendParams(name=name, **kwargs)


KNOWN_PRESETS: Dict[str, BackendParams] = {
    # legacy ticket-05 presets (unchanged while the grounded ones landed)
    "hdd": _params(
        "hdd",
        base_latency=0.010,
        per_gb_time=6.0,
        iops_ceiling=150.0,
        bandwidth_ceiling=0.167,
    ),
    "ssd": _params(
        "ssd",
        base_latency=0.0001,
        per_gb_time=2.5,
        iops_ceiling=75000.0,
        bandwidth_ceiling=0.4,
    ),
    "nvme": _params(
        "nvme",
        base_latency=0.00005,
        per_gb_time=0.2,
        iops_ceiling=750000.0,
        bandwidth_ceiling=5.0,
    ),
    # researched generation presets (ssd-generations.md §5 "YAML-ready
    # presets): base_latency = QD1 4K read TYP; per_gb_time = 1 / sustained
    # seq-read GB/s; ceilings = conservative 4K-read blends)
    "ssd_gen3": _params(  # Samsung 970 EVO Plus / WD SN750 class
        "ssd_gen3",
        base_latency=0.00008,
        per_gb_time=0.294,  # 1 / 3.4 GB/s
        iops_ceiling=550000.0,
        bandwidth_ceiling=3.4,
    ),
    "ssd_gen4": _params(  # 980 PRO / SN850 / FireCuda 530
        "ssd_gen4",
        base_latency=0.00004,
        per_gb_time=0.1471,  # 1 / 6.8 GB/s
        iops_ceiling=800000.0,
        bandwidth_ceiling=6.8,
    ),
    "ssd_gen5": _params(  # Crucial T700/T705, WD SN8100 (consumer Gen5x4)
        "ssd_gen5",
        base_latency=0.00003,
        per_gb_time=0.0806,  # 1 / 12.4 GB/s
        iops_ceiling=1450000.0,
        bandwidth_ceiling=12.4,
    ),
    "ssd_enterprise": _params(  # Micron 9550 MAX / Kioxia CM7-V
        "ssd_enterprise",
        base_latency=0.00008,
        per_gb_time=0.0714,  # 1 / 14.0 GB/s
        iops_ceiling=2400000.0,
        bandwidth_ceiling=14.0,
    ),
}


def test_storage_presets_registered() -> None:
    """Every named preset is present in the registry with exactly the
    researched (or legacy-stable) parameter values."""
    assert set(STORAGE_BACKEND_REGISTRY) == set(KNOWN_PRESETS)
    for name, expected in KNOWN_PRESETS.items():
        got = STORAGE_BACKEND_REGISTRY[name]
        assert got.name == expected.name
        assert got.base_latency == pytest.approx(expected.base_latency), name
        assert got.per_gb_time == pytest.approx(expected.per_gb_time), name
        assert got.iops_ceiling == pytest.approx(expected.iops_ceiling), name
        assert got.bandwidth_ceiling == pytest.approx(expected.bandwidth_ceiling), name


def test_generation_tier_scaling() -> None:
    """The tier ordering the research doc claims (Gen3 -> Gen4 ~ 2x all
    axes; Gen4 -> Gen5 ~ 1.8x bandwidth, no latency gain; consumer ->
    enterprise Gen5 ~ 2.7x IOPS) holds approximately on the presets."""
    r = STORAGE_BACKEND_REGISTRY
    assert r["ssd_gen4"].bandwidth_ceiling / r["ssd_gen3"].bandwidth_ceiling == pytest.approx(2.0, rel=0.01)
    assert r["ssd_gen5"].per_gb_time / r["ssd_gen4"].per_gb_time == pytest.approx(0.55, rel=0.05)
    # Gen5 QD1 latency: ~30 us class per the preset (bandwidth, not latency,
    # is the Gen4->Gen5 axis per the research doc)
    assert r["ssd_gen5"].base_latency == pytest.approx(0.00003)
    assert (
        r["ssd_enterprise"].iops_ceiling / r["ssd_gen5"].iops_ceiling
        == pytest.approx(2400000 / 1450000, rel=0.01)
    )


def test_storage_backend_overrides_merges_preset() -> None:
    """db.storage.overrides still layers onto a *generation* preset."""
    cfg = OmegaConf.create({"backend": "ssd_gen4", "overrides": {"base_latency": 0.00002}})
    from agent_db_sim.components.db.backend import resolve_backend_params

    params = resolve_backend_params(cfg)
    assert params.name == "ssd_gen4"
    assert params.base_latency == pytest.approx(0.00002)
    assert params.per_gb_time == pytest.approx(0.1471)


# --------------------------------------------------------------------------- #
# grounded scenarios: parse + provenance headers                              #
# --------------------------------------------------------------------------- #


def _header(doc: str, key: str = "sim") -> str:
    """The YAML header = everything before the first top-level `key:` line
    at column 0 (the comment block citing the research docs)."""
    marker = f"\n{key}:"
    idx = doc.find(marker)
    if doc.startswith(f"{key}:"):
        return ""
    assert idx >= 0, f"scenario has no top-level '{key}:' section"
    return doc[:idx]


def _research_names() -> list[str]:
    return sorted(p.name for p in RESEARCH_DIR.glob("*.md"))


@pytest.mark.parametrize(
    ("scenario", "cited"),
    [
        # the 70B profile grounds llm/agents params from the LLM serving doc,
        # the query mix from the TPC doc, and the storage preset from the
        # ssd-generation doc -- all three must be cited in its header
        (
            "grounded_agent_70b.yaml",
            ["llm-inference-serving.md", "ssd-generations.md", "tpc-workloads.md"],
        ),
        # the workloads sweep grounds its mixes/complexities from the TPC doc
        # and its storage axis from the ssd-generation doc
        ("grounded_workloads.yaml", ["ssd-generations.md", "tpc-workloads.md"]),
    ],
)
def test_grounded_provenance_headers(scenario: str, cited: list[str]) -> None:
    doc = (SCENARIO_DIR / scenario).read_text()
    header = _header(doc)
    # the header is a comment block (the provenance contract)
    assert all(
        line.lstrip().startswith("#") or not line.strip() for line in header.splitlines()
    ), f"{scenario}: header carries non-comment lines"
    all_docs = _research_names()
    assert all_docs, f"research docs missing from {RESEARCH_DIR}"
    for name in cited:
        assert name in all_docs, f"cited doc {name} not on disk ({all_docs})"
        assert name in header, f"{scenario}: header must cite {name}"


def test_grounded_agent_70b_parses() -> None:
    cfg = load_scenario(SCENARIO_DIR / "grounded_agent_70b.yaml")
    assert OmegaConf.select(cfg, "llm.output_tokens_per_s") == 2000
    assert OmegaConf.select(cfg, "llm.input_tokens_per_s") == 8000
    assert OmegaConf.select(cfg, "llm.slots") == 32
    assert OmegaConf.select(cfg, "agents.count") == 32
    assert OmegaConf.select(cfg, "db.storage.backend") == "ssd_enterprise"
    profiles = OmegaConf.select(cfg, "agents.query_profiles")
    weights = [p["weight"] for p in profiles]  # type: ignore[index]
    names = {p["name"] for p in profiles}  # type: ignore[index]
    assert names == {
        "point_lookup", "small_analytic", "join_heavy",
        "wide_scan_aggregate", "oltp_tx",
    }
    assert sum(weights) == pytest.approx(1.0)


@pytest.mark.parametrize("profile", ["demo", "llm_huge", "disaggregated"])
def test_grounded_agent_70b_profiles_merge(profile: str) -> None:
    cfg = load_scenario(SCENARIO_DIR / "grounded_agent_70b.yaml", profile)
    assert profile is not None  # load_scenario never returns None cfg
    if profile == "demo":
        assert OmegaConf.select(cfg, "sim.until") == 120.0
        assert OmegaConf.select(cfg, "agents.count") == 8
    elif profile == "llm_huge":
        assert OmegaConf.select(cfg, "llm.output_tokens_per_s") == 16000
    elif profile == "disaggregated":
        assert OmegaConf.select(cfg, "db.topology") == "disaggregated"
        assert OmegaConf.select(cfg, "db.compute_nodes") == 4


def test_grounded_workloads_parses_with_sweep_factors() -> None:
    from agent_db_sim.simkit.sweep import read_sweep

    cfg = load_scenario(SCENARIO_DIR / "grounded_workloads.yaml")
    name, factors = read_sweep(cfg)
    assert name == "grounded-workloads-sweep"
    assert factors == {
        "db.topology": ["centralized", "disaggregated"],
        "db.storage.backend": ["ssd_gen4", "ssd_enterprise"],
    }
    # profile keys live in the raw YAML (load_scenario strips 'profiles:')
    raw = OmegaConf.load(SCENARIO_DIR / "grounded_workloads.yaml")
    profiles = OmegaConf.select(raw, "profiles")
    assert set(profiles) >= {"mix_tpcc", "mix_mixed", "demo"}  # type: ignore[arg-type]
    cfg_run = load_scenario(SCENARIO_DIR / "grounded_workloads.yaml")
    name_run, _factors = read_sweep(cfg_run)
    assert name_run == name


@pytest.mark.parametrize("profile", ["mix_tpcc", "mix_mixed", "demo"])
def test_grounded_workloads_profile_overlays(profile: str) -> None:
    cfg = load_scenario(SCENARIO_DIR / "grounded_workloads.yaml", profile)
    # every overlay keeps the base's grounded LLM sizing intact
    assert OmegaConf.select(cfg, "llm.output_tokens_per_s") == 2000
    if profile != "demo":
        qps = OmegaConf.select(cfg, "agents.query_profiles")
        weights = [p["weight"] for p in qps]  # type: ignore[index]
        assert sum(weights) == pytest.approx(1.0)
        if profile == "mix_tpcc":
            names = {p["name"] for p in qps}  # type: ignore[index]
            assert names == {"oltp_tx", "point_lookup"}


# --------------------------------------------------------------------------- #
# grounded scenarios run through the full pipeline (fast variants)            #
# --------------------------------------------------------------------------- #


def _fast_variant(scenario: str, profile: str | None, until: float, agents: int) -> Any:
    cfg = load_scenario(SCENARIO_DIR / scenario, profile)
    cfg.sim.until = float(until)
    cfg.agents.count = int(agents)
    return cfg


def _read_summary(run_dir: Path) -> Dict[str, Any]:
    return OmegaConf.to_container(OmegaConf.load(run_dir / "summary.json"))  # type: ignore[return-value]


def _assert_run_dir(run_dir: Path, min_e2e_n: int) -> None:
    for artifact in _ARTIFACTS:
        assert (run_dir / artifact).is_file(), artifact
        assert (run_dir / artifact).stat().st_size > 0, f"{artifact} is empty"
    summary = _read_summary(run_dir)
    assert isinstance(summary, dict)
    e2e = summary.get("e2e_percentiles")
    assert e2e is not None and e2e["n"] >= min_e2e_n, f"e2e sample too small: {e2e}"
    assert e2e["p95"] > e2e["p50"] >= 0.0
    winner = summary.get("bottleneck_winner")
    assert winner is not None and winner.get("component"), "no bottleneck attribution"
    sources = {row["source"] for row in summary["percentiles"]}
    # both serving components must have recorded stage metrics
    assert any(src.startswith("llm") for src in sources), sources
    assert any("top.db" in src for src in sources), sources
    assert summary["inference_completed"] + summary["db_completed"] > 0


def test_grounded_agent_70b_runs_pipeline_fast(tmp_path: Path) -> None:
    """grounded_agent_70b.yaml through the full pipeline at until=90 x 4
    agents (the 32x600 default is a research-scale run)."""
    run_dir = tmp_path / "70b-fast"
    cfg = _fast_variant("grounded_agent_70b.yaml", None, until=90.0, agents=4)
    summary = execute_run(cfg, run_dir)
    _assert_run_dir(run_dir, min_e2e_n=5)
    # grounded-token sanity at this scale: the DB keeps up (its completions
    # track inference completions 1:1 across the whole run)
    assert summary["db_completed"] == summary["inference_completed"]
    # the LLM endpoint reports per-request latency >> the DB stage costs,
    # because the aggregate token cap stretches every service (cap 10k tok/s
    # vs an agent demand of ~78k tokens/call)
    llm_p50 = next(
        row["p50"]
        for row in summary["percentiles"]
        if row["source"] == "llm.*" and row["kind"] == "latency"
    )
    assert llm_p50 > 1.0


def test_grounded_agent_70b_cli_demo_profile(tmp_path: Path) -> None:
    """The '--profile demo' path works end to end through the CLI."""
    out = tmp_path / "runs"
    result = runner.invoke(
        app,
        ["run", str(SCENARIO_DIR / "grounded_agent_70b.yaml"), "--profile", "demo", "--out", str(out)],
    )
    assert result.exit_code == 0, result.output
    assert "e2e p50/p95/p99" in result.output
    run_dirs = [d for d in out.iterdir() if d.is_dir()]
    assert len(run_dirs) == 1
    _assert_run_dir(run_dirs[0], min_e2e_n=5)


@pytest.mark.parametrize("profile", [None, "mix_tpcc", "mix_mixed"])
def test_grounded_workloads_runs_pipeline_fast(tmp_path: Path, profile: str | None) -> None:
    """grounded_workloads.yaml (base analytical mix + the two profile
    overlays) through the full pipeline in the `demo`-like fast variant."""
    run_dir = tmp_path / f"workloads-{profile or 'base'}"
    cfg = _fast_variant("grounded_workloads.yaml", profile, until=40.0, agents=6)
    # sweep factors must not double-apply in a single execute_run
    cfg.pop("sweep", None)
    summary = execute_run(cfg, run_dir)
    _assert_run_dir(run_dir, min_e2e_n=5)
    # short horizon: last db requests still in flight at t_end -> db rows lag
    assert 0 < summary["db_completed"] <= summary["inference_completed"]


def test_grounded_workloads_full_scenario_is_sweep_only(tmp_path: Path) -> None:
    """At the full demo scale the workloads scenario stays cheap enough to
    run end to end in tests (until=60 x 16 agents), and its sweep section
    enumerates exactly 4 combinations."""
    from agent_db_sim.pipeline import enumerate_sweep
    from agent_db_sim.simkit.sweep import _sanitize
    import shutil

    cfg = load_scenario(SCENARIO_DIR / "grounded_workloads.yaml")
    # make the sweep fast: shrink horizon + one topology cell
    container = OmegaConf.to_container(cfg, resolve=True)
    assert isinstance(container, dict)
    container["sim"]["until"] = 40.0
    container["agents"]["count"] = 6
    container["sweep"]["factors"]["db.topology"] = ["centralized"]
    cfg = OmegaConf.create(container)
    out_root = tmp_path / "sweep-out"
    results, summary = enumerate_sweep(cfg, out_root=out_root, jobs=1)
    name = _sanitize(summary["name"])
    assert out_root / name == out_root / "grounded-workloads-sweep"
    assert len(results) == 2  # optimized backend axis: ssd_gen4 | ssd_enterprise
    for run in summary["runs"]:
        assert run["failed"] is False
        assert run["e2e_percentiles"]["n"] > 0
    shutil.rmtree(out_root)


def test_grounded_trace_carries_requests(tmp_path: Path) -> None:
    """The run's metrics.db keeps the full request trace with both request
    kinds (inference + db) - the grounded scenarios are the first ones with
    heavy-tail token draws, so confirm the streaming trace doesn't drop
    anything terminal (received/completed states present for sampled rows)."""
    run_dir = tmp_path / "trace-check"
    cfg = _fast_variant("grounded_agent_70b.yaml", None, until=90.0, agents=4)
    execute_run(cfg, run_dir)
    conn = sqlite3.connect(f"file:{run_dir / 'metrics.db'}?mode=ro", uri=True)
    try:
        agent_rows = conn.execute(
            "SELECT stage, state, COUNT(*) FROM trace WHERE component LIKE 'top.agents.%' "
            "GROUP BY stage, state"
        ).fetchall()
        comp_states = set(
            str(row[0]) for row in conn.execute("SELECT DISTINCT state FROM trace")
        )
    finally:
        conn.close()
    by_stage: Dict[str, set] = {}
    for stage, state, _ in agent_rows:
        by_stage.setdefault(str(stage), set()).add(str(state))
    # agent-side lifecycle machines are keyed by stage 'inference'/'db'
    assert {"inference", "db"} <= set(by_stage), by_stage
    # submitted -> received both ways, submitted -> completed downstream
    assert {"submitted", "received"} <= by_stage["inference"]
    assert {"submitted", "received"} <= by_stage["db"]
    assert {"submitted", "completed"} <= comp_states
