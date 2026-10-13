"""DBEndpoint: the simulated database under study (tickets 05 + 10).

Composite of DBCompute + internal network + (storage through the topology's
resolver chain). Wired contract (shared with tickets 08/09 — do not deviate):
input port ``queries``, output port ``responses``; requests carry
``complexity`` / ``volume`` / ``agent_id`` / ``request_id``.

Caller API: ``resp = yield endpoint.submit(query)`` where ``query`` is a
:class:`DBQuery` (or anything carrying the four contract attrs) — ``submit``
returns the started SimPy process; its value is the response. External
queries round-robin across the topology's entry nodes.
"""

from __future__ import annotations

import itertools
from typing import Any, Dict, Optional

from agent_db_sim.components.db._cfg import cstr
from agent_db_sim.components.db.topology import TOPOLOGY_REGISTRY
from agent_db_sim.simkit.component import Composite
from agent_db_sim.simkit.errors import RegistryError
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = ["DBEndpoint"]


class DBEndpoint(Composite):
    """The DBEndpoint top component wired from ``db.*`` config."""

    input_ports = {"queries": "query"}
    output_ports = {"responses": "response"}

    def __init__(self, name: str, db_cfg: Any, sink: Optional[ProbeSink] = None) -> None:
        super().__init__(name)
        if not hasattr(db_cfg, "__contains__") or "topology" not in db_cfg:
            raise RegistryError(
                f"{name}: db config requires a 'topology' key "
                f"(one of {sorted(TOPOLOGY_REGISTRY)})"
            )
        self.topology_name = cstr(db_cfg, "topology", "centralized")
        cls = TOPOLOGY_REGISTRY.get(self.topology_name)
        if cls is None:
            available = ", ".join(sorted(TOPOLOGY_REGISTRY))
            raise RegistryError(
                f"unknown db.topology {self.topology_name!r}; available: {available}"
            )
        self.db_cfg = db_cfg
        self._topo_cls = cls
        self._sink = sink if sink is not None else NULL_SINK
        self._rr = itertools.count()

    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)
        if isinstance(self._sink, ProbeSink) and self._sink.clock is None:
            self._sink.clock = lambda: self.env.now
        self.topology = self._topo_cls("topology", self.db_cfg, self._sink)
        self.add(self.topology)
        self.topology.construct(self.env, cfg)  # builds nodes with final paths
        # NOTE: no graph links are recorded for the internal boundary --
        # the endpoint declares input 'queries' / output 'responses' and the
        # topology owns the same contract (inputs cannot be link sources in
        # the connect graph; validation happens at each composite's own
        # boundary once agents wire in).

    def collect(self) -> Dict[str, Any]:
        data = super().collect()
        data["topology_name"] = self.topology_name
        data["path"] = self.topology.full_path
        return data

    # ------------------------------------------------------------------ #
    # caller API                                                          #
    # ------------------------------------------------------------------ #
    def submit(self, query: Any) -> Any:
        """Admit one query and serve it; returns a process that yields the
        :class:`QueryResponse`. Queries round-robin across entry nodes."""
        entries = self.topology.entry_nodes
        node = entries[next(self._rr) % len(entries)]
        return node.handle(query)
