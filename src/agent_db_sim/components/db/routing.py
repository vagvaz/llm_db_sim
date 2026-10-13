"""Topology-injected resolvers: the middle "storage hop" legs of the query
path (ticket 05 per-topology network wiring).

- :class:`LocalResolver` — colocated (centralized / distributed-node-local):
  straight to the node's own storage, no network.
- :class:`SegmentResolver` — disaggregated: shared storage-network segment
  (net_req out, storage, volume back over one contendable BandwidthPool);
  storage ops are striped (round-robin) across the M storage nodes.
- :class:`CoordinatorResolver` — distributed: coordinator-fronted request to
  one round-robin strategy-chosen peer over the dedicated per-peer link, the
  peer's compute slot held during its local storage hop, response volume back.

All resolvers emit the lifecycle stages belonging to their hops
(net_req/net_resp on the link objects, storage_* on the storage nodes) and
return ``{stage: StageTiming}`` for the phases they executed.
"""

from __future__ import annotations

from typing import Any, Dict, List

from agent_db_sim.components.db.compute import DBCompute
from agent_db_sim.components.db.network import NetLink
from agent_db_sim.components.db.query import StageTiming
from agent_db_sim.components.db.backend import StorageNode

__all__ = ["LocalResolver", "SegmentResolver", "CoordinatorResolver"]


class LocalResolver:
    """Colocated storage hop (no network stage)."""

    def __init__(self, compute: DBCompute) -> None:
        self.compute = compute

    def __call__(self, query: Any) -> Any:
        return self.compute.env.process(self._run(query))

    def _run(self, query: Any) -> Any:
        storage = self.compute.storage
        if storage is None:
            raise RuntimeError(f"{self.compute.full_path}: no colocated storage configured")
        stages = yield storage.serve(query)
        return dict(stages)


class SegmentResolver:
    """Disaggregated hop: shared segment out, storage, data back."""

    def __init__(
        self,
        compute: DBCompute,
        link: NetLink,
        storages: List[StorageNode],
        *,
        request_size: float,
    ) -> None:
        if not storages:
            raise RuntimeError(f"{compute.full_path}: disaggregated topology needs M >= 1")
        self.compute = compute
        self.link = link
        self.storages = storages
        self.request_size = request_size
        self._stripe = 0  # per-compute-node storage striping counter

    def __call__(self, query: Any) -> Any:
        return self.compute.env.process(self._run(query))

    def _run(self, query: Any) -> Any:
        stages: Dict[str, StageTiming] = {}
        node = self.storages[self._stripe % len(self.storages)]
        self._stripe += 1

        stages["net_req"] = yield self.link.transfer(query, "net_req", self.request_size)
        hops = yield node.serve(query)
        stages.update(hops)
        stages["net_resp"] = yield self.link.transfer(query, "net_resp")
        return stages


class CoordinatorResolver:
    """Distributed hop: coordinator fronts one peer over its dedicated link.

    Peer selection round-robins per coordinator; the peer's compute slot is
    held while its local storage serves the query (ticket 05 path:
    coordinator compute -> peer compute -> peer storage -> coordinator), so
    per-node compute occupancy and its queue show up on the peer leg.
    """

    def __init__(
        self,
        coordinator: DBCompute,
        peers: List[DBCompute],
        links: Dict[int, NetLink],
        *,
        request_size: float,
    ) -> None:
        self.coordinator = coordinator
        self.peers = peers
        self.links = links
        self.request_size = request_size
        self._rr = 0

    def __call__(self, query: Any) -> Any:
        return self.coordinator.env.process(self._run(query))

    def _run(self, query: Any) -> Any:
        stages: Dict[str, StageTiming] = {}
        index = self._rr % len(self.peers)
        self._rr += 1
        peer = self.peers[index]
        link = self.links[index]

        stages["net_req"] = yield link.transfer(query, "net_req", self.request_size)

        # peer compute leg: hold one of the peer's slots while its local
        # storage hop runs (peer compute -> peer local storage -> peer compute)
        peer_slot_wait = 0.0
        slot_req = peer.server.pool.request()
        yield slot_req
        t_acquired = peer.env.now
        peer_slot_wait = t_acquired - stages["net_req"].end
        peer_hops = yield peer.storage.serve(query)
        peer.server.pool.release(slot_req)

        queue_stage = peer_hops["storage_queue"]
        stages["storage_queue"] = StageTiming(
            component=queue_stage.component,
            stage="storage_queue",
            start=t_acquired - peer_slot_wait,
            end=queue_stage.end,
            queue_time=peer_slot_wait + queue_stage.queue_time,
            service_time=queue_stage.service_time,
            meta={**queue_stage.meta, "peer_node": peer.full_path},
        )
        stages["storage_service"] = peer_hops["storage_service"]

        stages["net_resp"] = yield link.transfer(query, "net_resp")
        return stages
