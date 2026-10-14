"""Analytical model: config -> station means (grounded promises).

The model is config-only (never trace data); these tests pin the decoded
means against hand math from the scenario YAMLs:

- sanity scenarios decode to exactly the paper numbers of
  ``scenarios/sanity/README.md`` (L = 1.0, D = 0.6001, tau = 1.6001 etc.);
- the grounded 70b profile decodes its lognormal token means and enters the
  token-cap regime with X pinned at cap/E[T];
- the ``llm_huge`` profile lifts the cap so the db station becomes
  binding; and the workflow visit-count folding works.
"""

from __future__ import annotations

import math
from pathlib import Path

import pytest
from omegaconf import OmegaConf

from agent_db_sim.analysis.model import build_model, expected_tokens

REPO = Path(__file__).resolve().parents[1]
SANITY = REPO / "scenarios" / "sanity"
PROFILES = REPO / "scenarios" / "profiles"


def _load(*parts: Path):
    return OmegaConf.load(str(Path(*parts)))


# --------------------------------------------------------------------------- #
# sanity paper math                                                            #
# --------------------------------------------------------------------------- #

def test_sanity_single_decodes_paper_numbers(tmp_path: Path) -> None:
    model = build_model(_load(SANITY, "sanity_single.yaml"))
    assert model.n_agents == 1
    assert model.visits == (1, 1)
    assert not model.cap_bound
    llm, db = model.unique_stations()
    assert math.isclose(llm.tau, 1.0, abs_tol=1e-12)          # 512/1024 + 128/256
    assert math.isclose(db.tau, 0.6001, abs_tol=1e-12)       # 0.1 + 0.2001 + 0.3
    assert math.isclose(model.e2e_mean, 1.6001, abs_tol=1e-12)
    assert math.isclose(model.stages_dict()["db storage pace"].tau,
                        0.2, abs_tol=1e-12)  # ssd preset: per_gb 2.5 x 0.08


def test_sanity_disaggregated_net_legs_exact() -> None:
    model = build_model(_load(SANITY, "sanity_disaggregated.yaml"))
    stages = model.stages_dict()
    # net_req = 0.02 + 0.01/20 = 0.0205; net_resp = 0.02 + 0.08/20 = 0.024
    assert math.isclose(stages["db net req"].tau, 0.0205, abs_tol=1e-12)
    assert math.isclose(stages["db net resp"].tau, 0.024, abs_tol=1e-12)
    middle = sum(st.tau for st in model.stages if st.kind in ("net", "storage"))
    assert math.isclose(middle, 0.2446, abs_tol=1e-9)
    assert math.isclose(model.e2e_mean, 1.6446, abs_tol=1e-9)


def test_sanity_queue_two_agents() -> None:
    model = build_model(_load(SANITY, "sanity_queue.yaml"))
    assert model.n_agents == 2
    llm, db = model.unique_stations()
    assert math.isclose(llm.tau, 1.0, abs_tol=1e-12)
    assert math.isclose(db.tau, 0.6001, abs_tol=1e-12)
    # the deterministic ping-pong sim queues L-D; the exponential chain
    # queues less — just check the LLM is the identified bottleneck
    assert model.bottleneck.name == "llm"
    assert 0 < model.x <= 1.0


def test_sanity_parallel_two_slots_no_queue() -> None:
    model = build_model(_load(SANITY, "sanity_parallel.yaml"))
    llm, db = model.unique_stations()
    assert llm.c == 2 and db.c == 2
    assert math.isclose(db.tau, 0.7201, abs_tol=1e-12)  # 0.1 + 0.3201 + 0.3
    # N=2 = c: no queueing anywhere; e2e = single-request latency
    assert model.total_queue_length() < 1e-9
    assert math.isclose(model.e2e_mean, 1.7201, abs_tol=1e-9)


# --------------------------------------------------------------------------- #
# grounded 70b: token means + the cap regime                                   #
# --------------------------------------------------------------------------- #

def test_grounded_lognormal_means() -> None:
    spec = expected_tokens({"distribution": "lognormal", "mean": 11.0,
                            "sigma": 0.6})
    assert math.isclose(spec.mean, math.exp(11.0 + 0.18), rel_tol=1e-12)
    out = expected_tokens({"distribution": "lognormal", "mean": 7.36,
                           "sigma": 1.0})
    assert math.isclose(out.mean, math.exp(7.36 + 0.5), rel_tol=1e-12)


def test_grounded_70b_enters_token_cap_regime() -> None:
    model = build_model(_load(PROFILES, "grounded_agent_70b.yaml"))
    assert model.cap_bound
    e_tokens = math.exp(11.18) + math.exp(7.86)
    assert math.isclose(model.e_tokens, e_tokens, rel_tol=1e-9)
    x_cap = 10000.0 / e_tokens
    assert model.x_cap is not None
    assert math.isclose(model.x_cap, x_cap, rel_tol=1e-6)
    # the chain reproduces the pinned goodput
    assert abs(model.x - x_cap) / x_cap < 0.001
    # the db station holds its entry slot pool size (4 compute slots)
    assert model.db.c == 4
    assert "db storage pace" in model.stages_dict()


def test_llm_huge_relaxes_cap_but_still_marginal() -> None:
    base = _load(PROFILES, "grounded_agent_70b.yaml")
    huge = OmegaConf.merge(base, OmegaConf.create(
        {"llm": {"slots": 64, "input_tokens_per_s": 64000,
                 "output_tokens_per_s": 16000}}))
    model = build_model(huge)
    # the cap bound (80k/74.3k ~ 1.077) sits just below the db saturation
    # (4 slots / 3.55 tau ~ 1.127): both bind; the pinned x is the cap one
    assert model.x_cap is not None
    assert model.x <= model.x_cap * 1.001


# --------------------------------------------------------------------------- #
# workflow visit counts                                                        #
# --------------------------------------------------------------------------- #

def test_workflow_visit_counts_fold_into_instances() -> None:
    base = _load(SANITY, "sanity_single.yaml")
    wf = OmegaConf.merge(base, OmegaConf.create({"agents": {"workflow": {
        "templates": [
            {"name": "multi", "steps": ["inference", "db",
                                        "inference", "db"]},
        ],
    }}}))
    model = build_model(wf)
    assert model.visits == (2, 2)
    assert len([s for s in model.stations if s.name == "llm"]) == 2


def test_bad_config_fails_loudly() -> None:
    with pytest.raises(ValueError):
        build_model(OmegaConf.create({"agents": {"count": 2}}))
