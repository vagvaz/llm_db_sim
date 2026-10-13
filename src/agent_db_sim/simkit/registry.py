"""Config -> class registries (explicit dicts, no dotted-import magic).

Unknown config names fail immediately (elaboration-time error) rather than
resolving lazily at import. ``build_model`` is the explicit top-level
factory stub filled in by the component tickets (08-10).
"""

from __future__ import annotations

from typing import Dict

from agent_db_sim.simkit.ingress import INGRESS_REGISTRY

__all__ = [
    "INGRESS_REGISTRY",
    "TOPOLOGY_REGISTRY",
    "STORAGE_BACKEND_REGISTRY",
    "build_model",
]

# Explicit config-name -> class registry for ingress kinds
# (``none | fifo | priority:<key>``).
INGRESS_REGISTRY = INGRESS_REGISTRY

#: Config-name -> class registries for swappable parts; components of
#: tickets 08-10 populate these (topologies, storage backends). They live
#: here so elaboration can fail fast on unknown names from one place.
TOPOLOGY_REGISTRY: Dict[str, type] = {}
STORAGE_BACKEND_REGISTRY: Dict[str, type] = {}


def build_model(cfg: object) -> object:
    """Explicit factory building the Top component tree from config.

    Stub until the component tickets: AgentClient, LLMEndpoint and
    DBEndpoint land in tickets 08-10.
    """
    raise NotImplementedError(
        "build_model is a skeleton: model components land in tickets 08-10"
    )
