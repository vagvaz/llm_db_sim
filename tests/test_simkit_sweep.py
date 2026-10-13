"""Unit tests for the factorial sweep runner and the registry module."""

from __future__ import annotations

from pathlib import Path

import pytest
from omegaconf import OmegaConf

from agent_db_sim.simkit.errors import RegistryError, SweepError
from agent_db_sim.simkit.ingress import open_ingress
from agent_db_sim.simkit.registry import INGRESS_REGISTRY, build_model
from agent_db_sim.simkit.sweep import read_sweep, run_sweep


def test_sweep_enumerates_cross_product_and_writes_runs(tmp_path: Path) -> None:
    base = OmegaConf.create(
        {
            "sim": {"until": 10.0},
            "loop": {"agents": 1, "timeout": 1.0},
            "sweep": {
                "name": "my-sweep",
                "factors": {"loop.agents": [1, 2], "loop.timeout": [0.5, 1.0]},
            },
        }
    )
    results = run_sweep(base, out_root=tmp_path)

    assert len(results) == 4
    run_dirs = [r.directory for r in results]
    assert len(set(run_dirs)) == 4
    assert all(d.parent.parent == tmp_path for d in run_dirs)
    assert [r.index for r in results] == [0, 1, 2, 3]

    combos = [tuple(r.values.values()) for r in results]
    assert combos == [(1, 0.5), (1, 1.0), (2, 0.5), (2, 1.0)]

    for result in results:
        snapshot = OmegaConf.load(result.directory / "config.yaml")
        assert snapshot.loop.agents == result.values["loop.agents"]
        assert snapshot.loop.timeout == result.values["loop.timeout"]
        assert snapshot.sim.until == 10.0  # untouched base keys preserved

    summary = (tmp_path / "my-sweep" / "sweep.json").read_text()
    assert '"n_runs": 4' in summary


def test_sweep_snapshot_isolated_from_base(tmp_path: Path) -> None:
    base = OmegaConf.create(
        {
            "a": {"x": 1},
            "sweep": {"factors": {"a.y": [10]}},
        }
    )
    (result,) = run_sweep(base, out_root=tmp_path)
    snapshot = OmegaConf.load(result.directory / "config.yaml")
    assert snapshot.a.x == 1
    assert snapshot.a.y == 10
    assert "y" not in base.a  # base config untouched


def test_sweep_validation_errors(tmp_path: Path) -> None:
    with pytest.raises(SweepError, match="no 'sweep:'"):
        run_sweep(OmegaConf.create({"a": 1}), out_root=tmp_path)

    with pytest.raises(SweepError, match="'factors'"):
        run_sweep(OmegaConf.create({"sweep": {"name": "x"}}), out_root=tmp_path)

    with pytest.raises(SweepError, match="must be a list"):
        run_sweep(
            OmegaConf.create({"sweep": {"factors": {"a.b": 3}}}),
            out_root=tmp_path,
        )


def test_runner_hook_writes_metrics_stub(tmp_path: Path) -> None:
    base = OmegaConf.create({"sweep": {"factors": {"a": [1, 2]}}})
    results = run_sweep(
        base,
        out_root=tmp_path,
        runner=lambda cfg: {"captured_a": int(cfg.a)},
    )
    assert len(results) == 2
    import json

    for result in results:
        payload = json.loads((result.directory / "metrics.json").read_text())
        assert payload["captured_a"] == int(result.values["a"])

    plain = run_sweep(base, out_root=tmp_path / "no-hook")
    assert not list((tmp_path / "no-hook" / "sweep").glob("*/metrics.json"))


def test_read_sweep_fails_on_empty_factor(tmp_path: Path) -> None:
    with pytest.raises(SweepError, match="at least one value"):
        run_sweep(
            OmegaConf.create({"sweep": {"factors": {"a.b": []}}}),
            out_root=tmp_path,
        )


def test_ingress_registry_maps_names_to_classes() -> None:
    assert set(INGRESS_REGISTRY) >= {"none", "fifo", "priority"}
    env = OmegaConf.create({})  # placeholder env type; registry only checks names
    for kind in ("none", "fifo"):
        inst = open_ingress(env, kind, kind)
        assert type(inst).__name__ == {  # noqa: E721
            "none": "IngressNone",
            "fifo": "IngressFifo",
        }[kind]


def test_build_model_requires_db_section() -> None:
    # ticket 10 filled the build_model stub: the DB portion wires from the
    # 'db:' config namespace; anything without it fails fast
    with pytest.raises(RegistryError, match="requires a 'db:'"):
        build_model({"anything": "goes"})


def test_registry_error_is_a_registryerror() -> None:
    with pytest.raises(RegistryError):
        open_ingress({}, "warp-drive", "x")  # type: ignore[arg-type]
