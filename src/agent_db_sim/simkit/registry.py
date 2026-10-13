"""Config -> class registries (explicit dicts, no dotted-import magic).

Unknown config names fail immediately (elaboration-time error) rather than
resolving lazily at import. ``build_model`` is the explicit top-level
factory: the full three-component model (AgentClient + LLMEndpoint +
DBEndpoint) is wired when the scenario carries an ``agents:`` section
(ticket 13); with ``db:`` alone the DB portion wires alone for stub-caller
use (ticket 10 contract, kept for compatibility).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from agent_db_sim.simkit.component import Composite
from agent_db_sim.simkit.errors import RegistryError
from agent_db_sim.simkit.ingress import INGRESS_REGISTRY
from agent_db_sim.simkit.probes import InMemorySink, ProbeSink

__all__ = [
    "INGRESS_REGISTRY",
    "TOPOLOGY_REGISTRY",
    "STORAGE_BACKEND_REGISTRY",
    "build_model",
]

INGRESS_REGISTRY = INGRESS_REGISTRY

# Populated from the ticket-10 DB components. Ticket 05 / ticket 10 contract:
# these stay explicit dicts so unknown config names fail fast in one place.
from agent_db_sim.components.db.backend import (  # noqa: E402
    STORAGE_BACKEND_REGISTRY as _DB_STORAGE_BACKENDS,
)
from agent_db_sim.components.db.topology import (  # noqa: E402
    TOPOLOGY_REGISTRY as _DB_TOPOLOGIES,
)

TOPOLOGY_REGISTRY: Dict[str, Any] = dict(_DB_TOPOLOGIES)
STORAGE_BACKEND_REGISTRY: Dict[str, Any] = dict(_DB_STORAGE_BACKENDS)


def build_model(cfg: Any, sink: Optional[ProbeSink] = None) -> Composite:
    """Explicit factory building the Top component tree from config.

    - ``db:`` section (required) -> :class:`DBEndpoint` under ``top.db``.
    - ``agents:`` section -> the full workload model under ``top.agents``
      (:class:`AgentClient` closed-loop agents) and ``top.llm``
      (:class:`LLMEndpoint`), wired agent inference_requests -> llm
      requests / llm responses -> agent inference_responses / agent
      db_queries -> db queries / db responses -> agent db_responses via the
      connect graph, with the runtime transport of ticket 08's contract.
    - The shared probe sink is exposed as ``top.probe_sink``: pass the same
      sink object to ``run_model``, and hand it to the metrics collector and
      VCD writer after the run.
    """
    if not hasattr(cfg, "__contains__") or "db" not in cfg:
        raise RegistryError("scenario config requires a 'db:' section to build a model")
    db_cfg = cfg["db"]
    shared_sink = sink if sink is not None else InMemorySink()

    # lazy imports keep the module import-graph side effect free
    from agent_db_sim.components.assembly import wire_agent_llm_db
    from agent_db_sim.components.agent_client import AgentClient
    from agent_db_sim.components.db.endpoint import DBEndpoint
    from agent_db_sim.components.llm_endpoint import LLMEndpoint

    db = DBEndpoint("db", db_cfg, shared_sink)
    top = Composite("top")
    top.add(db)
    top.probe_sink = shared_sink  # introspection hook for tests/report layers

    if hasattr(cfg, "__contains__") and "agents" in cfg:
        agents = AgentClient("agents", cfg_key="agents")
        agents.attach_sink(shared_sink)
        llm = LLMEndpoint("llm", sink=shared_sink)
        top.add(agents)
        top.add(llm)
        # wire ports per the shared contract + runtime transport
        wire_agent_llm_db(top, agents, llm, db)

    return top
