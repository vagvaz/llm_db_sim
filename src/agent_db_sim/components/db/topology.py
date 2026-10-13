"""The three DB topologies as configuration wiring (ticket 05).

All topologies are SimKit children of :class:`DBEndpoint`:
``{db.topology: centralized | disaggregated | distributed}``.

- ``centralized`` — one colocated compute+storage node, no internal network.
- ``disaggregated`` — N compute nodes x M storage nodes over ONE shared
  storage-network segment (single BandwidthPool + fixed latency); queries
  round-robin across compute nodes, storage ops stripe across storage nodes.
- ``distributed`` — compute+local-storage node pairs; a configurable number
  of Coordinator nodes front peer queries (queries round-robined across
  coordinators); every coordinator holds a per-link connection to every peer
  (own bandwidth/latency). ``db.network.coordinator_overhead`` multiplies the
  coordinator node's compute complexity.

Scale-out (map notes): growing ``db.compute_nodes`` (N) or ``db.storage_nodes``
(M) only changes the node counts — per-node params stay untouched.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from agent_db_sim.components.db._cfg import cint
from agent_db_sim.components.db.backend import StorageNode, resolve_backend_params
from agent_db_sim.components.db.compute import DBCompute
from agent_db_sim.components.db.network import NetLink, NetworkConfig
from agent_db_sim.components.db.routing import (
    CoordinatorResolver,
    LocalResolver,
    SegmentResolver,
)
from agent_db_sim.simkit.component import Composite
from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = ["BaseTopology", "CentralizedTopology", "DisaggregatedTopology", "DistributedTopology", "TOPOLOGY_REGISTRY"]


class BaseTopology(Composite):
    """Topology composite: builds children during ``construct`` (so every
    component's full_path — used for RNG streams and probe signal names — is
    final by then) and exposes its query entry nodes."""

    input_ports = {"queries": "query"}
    output_ports = {"responses": "response"}

    def __init__(self, name: str, db_cfg: Any, sink: Optional[ProbeSink] = None) -> None:
        super().__init__(name)
        self.db_cfg = db_cfg
        self._topo_sink = sink if sink is not None else NULL_SINK
        self.entry_nodes: List[DBCompute] = []
        self.compute_nodes: List[DBCompute] = []
        self.storage_nodes: List[StorageNode] = []
        self.net = NetworkConfig(db_cfg.get("network") if "network" in db_cfg else None)

    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)  # no children yet
        self._build(self.env, cfg)

    def elaborate(self) -> None:
        if not self.entry_nodes:
            raise ComponentError(f"{self.full_path}: topology built no entry nodes")
        for node in self.entry_nodes:
            if node.resolver is None:
                raise ComponentError(
                    f"{node.full_path}: topology built no storage resolver"
                )
        for node in self.compute_nodes:
            if node.colocated_storage and node.storage is None:
                raise ComponentError(f"{node.full_path}: missing colocated storage")
        super().elaborate()

    def collect(self) -> Dict[str, Any]:
        return super().collect()

    def _build(self, env: Any, cfg: Any) -> None:  # pragma: no cover - abstract
        raise NotImplementedError

    def _spawn_node(self, name: str, *, colocated: bool, coordinator: bool, cfg: Any) -> DBCompute:
        node = DBCompute(
            name,
            self.db_cfg,
            self._topo_sink,
            colocated_storage=colocated,
            coordinator=coordinator,
        )
        self.add(node)
        node.construct(self.env, cfg)  # real root cfg: RNG streams share the master seed
        return node

    # helper: assemble the per-node resolver as soon as children exist
    def _attach_local_storage_resolver(self, node: DBCompute) -> None:
        node.resolver = LocalResolver(node)


class CentralizedTopology(BaseTopology):
    """Colocated compute+storage, no internal network."""

    def _build(self, env: Any, cfg: Any) -> None:
        node = self._spawn_node("compute-0", colocated=True, coordinator=False, cfg=cfg)
        self._attach_local_storage_resolver(node)
        self.compute_nodes = [node]
        self.storage_nodes = [node.storage]
        self.entry_nodes = [node]


class DisaggregatedTopology(BaseTopology):
    """N compute nodes x M storage nodes over one shared network segment."""

    def _build(self, env: Any, cfg: Any) -> None:
        n = cint(self.db_cfg, "compute_nodes", 2)
        m = cint(self.db_cfg, "storage_nodes", 2)
        if n < 1 or m < 1:
            raise ComponentError(
                f"{self.full_path}: disaggregated needs compute_nodes >= 1, "
                f"storage_nodes >= 1 (got N={n}, M={m})"
            )

        for i in range(n):
            node = self._spawn_node(f"compute-{i}", colocated=False, coordinator=False, cfg=cfg)
            self.compute_nodes.append(node)
        link = NetLink("segment", self.net, self._topo_sink)
        self.add(link)
        link.construct(self.env, cfg)

        params = resolve_backend_params(self.db_cfg.get("storage"))
        for j in range(m):
            storage = StorageNode(f"storage-{j}", params, self._topo_sink)
            self.add(storage)
            storage.construct(self.env, cfg)
            self.storage_nodes.append(storage)

        for node in self.compute_nodes:
            node.resolver = SegmentResolver(
                node, link, self.storage_nodes, request_size=self.net.request_size
            )
        self.entry_nodes = list(self.compute_nodes)


class DistributedTopology(BaseTopology):
    """Node pairs (compute+local storage), coordinator-fronted peer links."""

    def _build(self, env: Any, cfg: Any) -> None:
        total = cint(self.db_cfg, "compute_nodes", 2)
        n_coord = cint(self.db_cfg, "coordinators", 1)
        if total < 2 or not 1 <= n_coord < total:
            raise ComponentError(
                f"{self.full_path}: distributed needs compute_nodes >= 2 and "
                f"1 <= coordinators < compute_nodes (got {total} nodes, {n_coord} coordinators)"
            )

        coordinators: List[DBCompute] = []
        peers: List[DBCompute] = []
        for i in range(total):
            is_coord = i < n_coord
            node = self._spawn_node(f"node-{i}", colocated=True, coordinator=is_coord, cfg=cfg)
            if is_coord:
                node.overhead = self.net.coordinator_overhead  # ticket 05 rule
                coordinators.append(node)
            else:
                peers.append(node)
            self.compute_nodes.append(node)

        links: Dict[int, Dict[int, NetLink]] = {}
        for c_index, coord in enumerate(coordinators):
            by_peer: Dict[int, NetLink] = {}
            for p_index, peer in enumerate(peers):
                link = NetLink(f"link-{c_index}-{p_index}", self.net, self._topo_sink)
                self.add(link)
                link.construct(self.env, cfg)
                by_peer[p_index] = link
                # NOTE: the coordinator->peer fan-out / peer->coordinator fan-in
                # is structural (not recorded in the connect graph): multiple
                # sources wire one peer input and the connect graph forbids
                # double-wired inputs. The DBEndpoint boundary declares the
                # same contract (queries in / responses out).
            links[c_index] = by_peer

        for c_index, coord in enumerate(coordinators):
            coord.resolver = CoordinatorResolver(
                coord, peers, links[c_index], request_size=self.net.request_size
            )
        self.entry_nodes = coordinators
        self.storage_nodes = [node.storage for node in self.compute_nodes]


#: Config-name -> topology composite class registry (merged into
#: ``simkit.registry``; unknown names fail fast at DBEndpoint construction).
TOPOLOGY_REGISTRY: Dict[str, type] = {
    "centralized": CentralizedTopology,
    "disaggregated": DisaggregatedTopology,
    "distributed": DistributedTopology,
}
