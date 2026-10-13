"""Config -> class registries (explicit dicts, no dotted-import magic).

Unknown config names fail immediately (elaboration-time error) rather than
resolving lazily at import. ``build_model`` is the explicit top-level
factory: the DB portion (ticket 10) is implemented; agent/LLM wiring
(tickets 08/09) is a clearly marked stub point.
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
try:  # the registries live with the components that own them
    from agent_db_sim.components.db.backend import (
        STORAGE_BACKEND_REGISTRY as _DB_STORAGE_BACKENDS,
    )
    from agent_db_sim.components.db.topology import TOPOLOGY_REGISTRY as _DB_TOPOLOGIES

    TOPOLOGY_REGISTRY: Dict[str, Any] = dict(_DB_TOPOLOGIES)
    STORAGE_BACKEND_REGISTRY: Dict[str, Any] = dict(_DB_STORAGE_BACKENDS)
except ImportError:  # pragma: no cover - stub state kept for ticket 07 tests
    TOPOLOGY_REGISTRY = {}
    STORAGE_BACKEND_REGISTRY = {}


def build_model(cfg: Any, sink: Optional[ProbeSink] = None) -> Composite:
    """Explicit factory building the Top component tree from config.

    v1 scope (ticket 10): the DB portion - a :class:`DBEndpoint` built from
    the ``db.*`` namespace, wired onto a ``top`` composite. The probe sink is
    shared with the components (pass the same sink to ``run_model``).

    Tickets 08/09 (AgentClient + LLMEndpoint) wire in at the marked stub
    below; manual stub callers (tests / scenarios) attach themselves to the
    returned top after :func:`build_model`.

    Returns the top composite (``top.db`` holds the endpoint).
    """
    from agent_db_sim.components.db.endpoint import DBEndpoint  # lazy import

    if not hasattr(cfg, "__contains__") or "db" not in cfg:
        raise RegistryError("scenario config requires a 'db:' section to build a model")
    db_cfg = cfg["db"]
    shared_sink = sink if sink is not None else InMemorySink()

    db = DBEndpoint("db", db_cfg, shared_sink)
    top = Composite("top")
    top.add(db)
    top.probe_sink = shared_sink  # introspection hook for tests/report layers

    # ------------------------------------------------------------------ #
    # STUB (tickets 08/09): AgentClient + LLMEndpoint wiring lands here.  #
    # When those components exist, attach them to `top` and wire          #
    # agent.queries -> db.queries / db.responses -> agent.responses       #
    # (port kinds 'query' / 'response'), driven by the `agents:` config   #
    # namespace. Deliberately not imported here so concurrent tickets     #
    # never break this builder while they are in flight.                  #
    # ------------------------------------------------------------------ #

    return top
