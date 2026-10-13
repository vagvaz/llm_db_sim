"""Factorial sweep runner (the ``simulate_factors`` analogue).

Scenario YAML carries::

    sweep:
      name: my-sweep          # -> runs/<sweep-name>/<run-id>/
      factors:                # dotted config paths -> value lists
        loop.agents: [1, 2, 4]

``run_sweep`` enumerates the Cartesian product over the base config, writes
one run directory per combination with a ``config.yaml`` snapshot, and
returns the run list. Per-run execution plus metrics/VCD reporting are
stubs in this ticket (model components land in 08-10).
"""

from __future__ import annotations

import itertools
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

from omegaconf import ListConfig, OmegaConf

from agent_db_sim.simkit.errors import SweepError

__all__ = [
    "RunResult",
    "read_sweep",
    "apply_factors",
    "run_id_for",
    "run_sweep",
    "validate_jobs",
]

_SANITIZE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class RunResult:
    """One enumerated run of a sweep."""

    index: int
    run_id: str
    directory: Path
    values: Dict[str, Any]


def read_sweep(cfg: Any) -> Tuple[str, Dict[str, List[Any]]]:
    """Extract ``sweep.name`` (default ``sweep``) and the factor lists."""
    if not hasattr(cfg, "__contains__") or "sweep" not in cfg:
        raise SweepError("scenario config has no 'sweep:' section")
    sweep = cfg["sweep"]
    name = sweep["name"] if "name" in sweep else "sweep"
    if "factors" not in sweep:
        raise SweepError(
            "sweep section requires a 'factors' mapping of dotted config "
            "paths to value lists"
        )
    factors_raw = sweep["factors"]
    factors: Dict[str, List[Any]] = {}
    for key, value in factors_raw.items():
        if isinstance(value, (list, tuple, ListConfig)):
            if len(value) == 0:
                raise SweepError(
                    f"sweep factor {key!r} must list at least one value"
                )
            factors[str(key)] = list(value)
        else:
            raise SweepError(
                f"sweep factor {key!r} must be a list of values, got {value!r}"
            )
    if not factors:
        raise SweepError("sweep.factors is empty")
    return str(name), factors


def apply_factors(base_cfg: Any, assignments: Mapping[str, Any]) -> Any:
    """Deep-copy the base config and set each dotted-path factor on it."""
    out = OmegaConf.create(OmegaConf.to_container(base_cfg, resolve=False))
    for path, value in assignments.items():
        try:
            OmegaConf.update(out, path, value, force_add=True)
        except Exception as exc:  # omegaconf validation/struct errors
            raise SweepError(
                f"cannot set sweep factor {path!r} = {value!r}: {exc}"
            ) from exc
    return out


def _sanitize(text: str) -> str:
    return _SANITIZE.sub("-", text).strip("-.") or "_"


def run_id_for(index: int, assignments: Mapping[str, Any]) -> str:
    """Stable, filesystem-safe run id: index prefix + factor=value pairs."""
    parts = [
        f"{_sanitize(key.replace('.', '__'))}={_sanitize(str(value))}"
        for key, value in assignments.items()
    ]
    return f"{index:04d}-" + ",".join(parts)


def validate_jobs(jobs: Any) -> int:
    """Coerce ``--jobs`` / ``sweep.max_jobs`` to a positive worker count.

    ``1`` means fully sequential execution (the historical default); ``N > 1``
    runs the enumerated combinations in a process pool of
    ``min(N, len(runs))`` workers. Anything below 1 (or non-numeric) raises a
    :class:`SweepError` with the fix in the message.
    """
    try:
        value = int(jobs)
    except (TypeError, ValueError) as exc:
        raise SweepError(
            f"max_jobs must be an integer >= 1, got {jobs!r}"
        ) from exc
    if value < 1:
        raise SweepError(
            f"max_jobs must be an integer >= 1 (got {value}); "
            "1 = sequential, or pass more workers for a parallel sweep"
        )
    return value


def run_sweep(
    cfg: Any,
    out_root: Union[str, Path] = "runs",
    runner: Optional[Callable[[Any], Any]] = None,
) -> List[RunResult]:
    """Enumerate the factor cross product and materialize one run dir each.

    Each run directory receives a ``config.yaml`` snapshot. ``runner`` is
    the optional per-run execution hook (returns a payload dict or None); in
    ticket 07 it stays a stub — the real model components (tickets 08-10)
    and the metrics/VCD collectors plug in here later.
    """
    name, factors = read_sweep(cfg)
    combos = itertools.product(*factors.values())
    results: List[RunResult] = []
    sweep_dir = Path(out_root) / _sanitize(name)
    for index, combo in enumerate(combos):
        assignments = dict(zip(factors.keys(), combo))
        run_cfg = apply_factors(cfg, assignments)
        run_id = run_id_for(index, assignments)
        directory = sweep_dir / run_id
        try:
            directory.mkdir(parents=True)
        except FileExistsError as exc:
            raise SweepError(
                f"run directory already exists: {directory} "
                f"(remove it or change out_root to re-run)"
            ) from exc
        OmegaConf.save(run_cfg, directory / "config.yaml")
        if runner is not None:
            payload = runner(run_cfg)
            if isinstance(payload, Mapping):
                (directory / "metrics.json").write_text(
                    json.dumps(payload, indent=2, sort_keys=True) + "\n"
                )
        results.append(
            RunResult(
                index=index, run_id=run_id, directory=directory, values=assignments
            )
        )

    summary = {
        "name": name,
        "out_root": str(Path(out_root)),
        "n_runs": len(results),
        "factors": factors,
        "run_ids": [r.run_id for r in results],
    }
    sweep_dir.mkdir(parents=True, exist_ok=True)
    (sweep_dir / "sweep.json").write_text(json.dumps(summary, indent=2) + "\n")
    return results
