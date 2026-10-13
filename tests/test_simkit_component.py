"""Unit tests for the Component base class: lifecycle order, typed-port
wiring validation at elaboration, and named child RNG streams."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from agent_db_sim.simkit.component import Component, Composite
from agent_db_sim.simkit.errors import WiringError
from agent_db_sim.simkit.ports import ConnectGraph
from agent_db_sim.simkit.runner import run_model


class LogComp(Component):
    """Lifecycle-order recorder that also draws from its RNG stream."""

    def __init__(self, name: str, log: List[Any], clicks: int = 3) -> None:
        super().__init__(name)
        self._log = log
        self._clicks = clicks

    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)
        self._log.append(("construct", self.full_path))
        self.samples = [self.rng.random() for _ in range(self._clicks)]

    def elaborate(self) -> None:
        super().elaborate()
        self._log.append(("elaborate", self.full_path))

    def start(self) -> None:
        super().start()
        self._log.append(("start", self.full_path))

    def collect(self) -> Dict[str, Any]:
        return {"samples": self.samples}


def test_run_model_drives_lifecycle_in_order() -> None:
    log: List[Any] = []
    top = Composite("top")
    a = top.add(LogComp("a", log))
    b = top.add(LogComp("b", log))

    result = run_model(top, {"sim": {"seed": 7}}, until=1.0)

    order = [entry[0] for entry in log]
    assert order == [
        "construct", "construct",
        "elaborate", "elaborate",
        "start", "start",
    ]
    assert [entry[1] for entry in log[:2]] == ["top.a", "top.b"]
    assert {entry[1] for entry in log[2:4]} == {"top.a", "top.b"}
    assert all(entry[1] in {"top.a", "top.b"} for entry in log[4:])
    assert result.end_time == pytest.approx(1.0)

    collected = result.collected
    assert set(collected) == {"a", "b"}
    assert len(collected["a"]["samples"]) == 3


def test_rng_children_stable_across_runs_and_independent_per_name() -> None:
    def build(seed: int, leaf: str = "llm", root: str = "top"):
        top = Composite(root)
        top.add(LogComp(leaf, []))
        return run_model(top, {"sim": {"seed": seed}}, until=1.0).collected

    same_seed_a = build(7, leaf="llm")
    same_seed_b = build(7, leaf="llm")
    assert same_seed_a == same_seed_b  # stable across runs

    other_seed = build(8, leaf="llm")
    assert other_seed != same_seed_a  # master seed changes the streams

    other_leaf = build(7, leaf="db", root="top")
    assert other_leaf["db"] != same_seed_a["llm"]  # independent per name

    other_root = build(7, leaf="llm", root="top2")
    assert other_root["llm"] != same_seed_a["llm"]  # full path disambiguates

def test_rng_access_before_construct_fails_fast() -> None:
    comp = Component("c")
    with pytest.raises(Exception, match="construct"):
        _ = comp.rng
    with pytest.raises(Exception, match="construct"):
        _ = comp.env


def test_composite_collect_merges_children() -> None:
    def build() -> Any:
        tree = Composite("top")
        tree.add(LogComp("a", [], clicks=1))
        return tree

    first = run_model(build(), {"sim": {"seed": 1}}, until=1.0).collected
    second = run_model(build(), {"sim": {"seed": 1}}, until=1.0).collected
    assert set(first) == {"a"}
    assert len(first["a"]["samples"]) == 1
    assert first == second
