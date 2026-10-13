"""Component base class: two-phase lifecycle, typed ports, named RNG streams.

Lifecycle per the ticket-03 architecture contract::

    construct(env, cfg)   # called on ALL components first (no sim time yet)
    elaborate()           # wiring validated here (fail fast)
    start()               # launch processes
    collect()             # harvest metrics/signals after env.run()

RNG: one master seed (``sim.seed``, default 0) in the config derives one
deterministic child stream per component from ``seed + full component path``
(``random.Random(str)`` is hash-seed independent, so streams are stable
across runs). Stream independence follows from the unique full path.
"""

from __future__ import annotations

import random
from typing import Any, ClassVar, Dict, List, Optional

from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.ports import ConnectGraph

__all__ = ["DEFAULT_MASTER_SEED", "master_seed", "Component", "Composite"]

DEFAULT_MASTER_SEED = 0


def master_seed(cfg: Any) -> int:
    """Read ``cfg.sim.seed``; fall back to the default when absent/invalid."""
    node = cfg
    for key in ("sim", "seed"):
        if hasattr(node, "__contains__") and key in node:
            node = node[key]
        else:
            return DEFAULT_MASTER_SEED
    try:
        return int(node)
    except (TypeError, ValueError):
        return DEFAULT_MASTER_SEED


class Component:
    """Base class of every simulated component.

    Subclasses declare typed ports as class attributes::

        class LLMEndpoint(Component):
            input_ports = {"requests": "req"}
            output_ports = {"responses": "response"}

    and override the lifecycle hooks, calling ``super()`` first in
    ``construct``/``elaborate`` so bookkeeping, RNG setup and wiring
    validation always run.
    """

    input_ports: ClassVar[Dict[str, Any]] = {}
    output_ports: ClassVar[Dict[str, Any]] = {}

    def __init__(self, name: str) -> None:
        self.name = str(name)
        self._children: List["Component"] = []
        self._path = self.name
        self._env: Optional[Any] = None
        self._cfg: Optional[Any] = None
        self._rng: Optional[random.Random] = None
        self._graph: Optional[ConnectGraph] = None

    # ------------------------------------------------------------------ #
    # hierarchy                                                           #
    # ------------------------------------------------------------------ #
    @property
    def full_path(self) -> str:
        """Unique component path used for RNG stream derivation and errors."""
        return self._path

    def add(self, child: "Component") -> "Component":
        """Attach a child component; returns ``child`` for chaining."""
        child._path = f"{self._path}.{child.name}"
        self._children.append(child)
        return child

    # ------------------------------------------------------------------ #
    # fail-fast accessors                                                 #
    # ------------------------------------------------------------------ #
    @property
    def env(self) -> Any:
        if self._env is None:
            raise ComponentError(
                f"component {self.name!r} used before construct(env, cfg)"
            )
        return self._env

    @property
    def cfg(self) -> Any:
        if self._cfg is None:
            raise ComponentError(
                f"component {self.name!r} read config before construct(env, cfg)"
            )
        return self._cfg

    @property
    def rng(self) -> random.Random:
        """Deterministic child stream derived from master seed + full path."""
        if self._rng is None:
            raise ComponentError(
                f"component {self.name!r} used rng before construct(env, cfg)"
            )
        return self._rng

    # ------------------------------------------------------------------ #
    # lifecycle                                                           #
    # ------------------------------------------------------------------ #
    def construct(self, env: Any, cfg: Any) -> None:
        """Phase 1: assign env/config to ALL components (no sim time yet)."""
        self._env = env
        self._cfg = cfg
        self._rng = random.Random(f"{master_seed(cfg)}/{self._path}")
        for child in self._children:
            child.construct(env, cfg)

    def elaborate(self) -> None:
        """Phase 2: validate wiring, then recurse into children."""
        if self._graph is not None:
            self._graph.validate()
        for child in self._children:
            child.elaborate()

    def start(self) -> None:
        """Phase 3: launch processes (recurses children; override last)."""
        for child in self._children:
            child.start()

    def collect(self) -> Dict[str, Any]:
        """Phase 4: harvest counters/metrics; default merges child results."""
        result: Dict[str, Any] = {}
        for child in self._children:
            result[child.name] = child.collect()
        return result


class Composite(Component):
    """A component that owns child components and a connect graph."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.connect_graph = ConnectGraph()
        self._graph = self.connect_graph

    def elaborate(self) -> None:
        self.connect_graph.validate()
        super().elaborate()
