"""DBEndpoint components (ticket 10): compute, storage backend, internal
network, per-topology wiring and the phased slot server.

Conventions set in ticket 05 / the wired ticket-10 contract:

- Ports: input ``queries``, output ``responses``; kinds ``query`` /
  ``response``; requests carry ``complexity``, ``volume``, ``agent_id``,
  ``request_id`` (attr table in :mod:`query`).
- Lifecycle edge samples: ``{component path}.{stage}`` with
  ``agent_id``/``request_id`` tags and states
  ``submitted|queued|processing|completed``; stages follow ticket 04's
  taxonomy (``compute_issue``, ``net_req``, ``net_conn``,
  ``storage_queue``, ``storage_service``, ``net_resp``,
  ``compute_finalize``). ``net_conn`` appears onkt on links with a bounded
  connection pool (``db.network.connections``) and records the wait-in-
  connection-queue contribution.
"""

from agent_db_sim.components.db.backend import (
    STORAGE_BACKEND_REGISTRY,
    BackendParams,
    StorageNode,
)
from agent_db_sim.components.db.compute import DBCompute
from agent_db_sim.components.db.endpoint import DBEndpoint
from agent_db_sim.components.db.network import NetLink, NetworkConfig
from agent_db_sim.components.db.query import DBQuery, QueryResponse, StageTiming
from agent_db_sim.components.db.server import PhaseControl, PhaseSlotServer
from agent_db_sim.components.db.topology import TOPOLOGY_REGISTRY

__all__ = [
    "BackendParams",
    "STORAGE_BACKEND_REGISTRY",
    "TOPOLOGY_REGISTRY",
    "DBEndpoint",
    "DBCompute",
    "DBQuery",
    "QueryResponse",
    "StageTiming",
    "StorageNode",
    "NetLink",
    "NetworkConfig",
    "PhaseControl",
    "PhaseSlotServer",
]
