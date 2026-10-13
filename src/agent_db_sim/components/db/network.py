"""NetLink: one internal DB network hop — fixed propagation latency plus a
simkit BandwidthPool transfer (ticket 05: per-link bandwidth on distributed,
one shared segment pool on disaggregated), preceded by a bounded
**connection pool** admission (connections-as-a-resource model):

Each query acquires one connection from the link's pool before its first
transfer of the round trip (the control request) and holds it until the
last direction (``net_resp``) completes — "one query's connection" to the
data node. With no free connection the query waits in the link's
connection queue *before* any transfer; the wait is recorded as its own
``net_conn`` stage (trace edges ``{path}.net_conn``) and the pool's
occupancy as a ``conn_busy`` level signal. Per-hop bandwidth/level queueing
stays on the net stages (never double counted with the connection wait).
``connections: 0`` (default) models unlimited connections: no pool is
created and the old single-stage behavior is bit-identical.

The link emits ``{path}.net_req`` / ``{path}.net_resp`` lifecycle edge
samples (states submitted|queued|processing|completed, tagged with
``agent_id``/``request_id``) and returns the hop's :class:`StageTiming`.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import simpy

from agent_db_sim.components.db._cfg import cfloat, cint
from agent_db_sim.components.db.query import StageTiming, q_scalar, q_tag
from agent_db_sim.simkit.component import Component
from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.levels import BandwidthPool
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = ["NetLink", "NetworkConfig"]

StageName = str


class NetworkConfig:
    """Parsed ``db.network`` subtree (latency, bandwidth, staging capacity,
    control-request size, coordinator overhead multiplier, pooled connection
    count per link; ``connections: 0`` = unlimited)."""

    def __init__(self, net_cfg: Any) -> None:
        self.latency = cfloat(net_cfg, "latency", 0.01)
        self.bandwidth = cfloat(net_cfg, "bandwidth", 1000.0)
        self.capacity = cfloat(net_cfg, "capacity", 8.0)
        self.request_size = cfloat(net_cfg, "request_size", 0.01)
        self.coordinator_overhead = cfloat(net_cfg, "coordinator_overhead", 1.0)
        self.connections = cint(net_cfg, "connections", 0)
        if self.latency < 0 or self.bandwidth <= 0 or self.capacity <= 0:
            raise ComponentError("db.network: latency >= 0, bandwidth/capacity > 0 required")
        if self.request_size < 0 or self.coordinator_overhead <= 0:
            raise ComponentError(
                "db.network: request_size >= 0 and coordinator_overhead > 0 required"
            )
        if self.connections < 0:
            raise ComponentError(
                f"db.network: connections must be >= 0 (0 = unlimited), got {self.connections}"
            )


class NetPool(BandwidthPool):
    """BandwidthPool over the fp-safe staging container (now the level-layer
    default; see :class:`agent_db_sim.simkit.levels.TolerantContainer`)."""


class NetLink(Component):
    """Connection-pooled latency + BandwidthPool hop; ``transfer`` returns a
    process."""

    def __init__(self, name: str, net: NetworkConfig, sink: Optional[ProbeSink] = None) -> None:
        super().__init__(name)
        self.net = net
        self._sink_probe = sink if sink is not None else NULL_SINK
        self.pool: Optional[BandwidthPool] = None
        self.records: list = []
        # bounded connection pool (None = unlimited / backward-compatible)
        self.conn_pool: Optional[Any] = None
        self.conn_records: list = []
        self._conn_busy = 0
        self._holds: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)
        self.pool = NetPool(
            self.env,
            self.full_path,
            capacity=self.net.capacity,
            bandwidth=self.net.bandwidth,
            sink=self._sink_probe,
        )
        if self.net.connections > 0:
            self.conn_pool = simpy.Resource(self.env, capacity=self.net.connections)
            self._sink_probe.sample("conn_busy", 0, component=self.full_path)

    def transfer(self, query: Any, stage: StageName, volume: Optional[float] = None) -> Any:
        """One direction of the hop; returns a process yielding StageTiming.

        ``stage`` is ``net_req`` (control request out) or ``net_resp`` (data
        volume back). ``volume=None`` defaults to ``request_size`` for
        ``net_req`` and the query's ``volume`` for ``net_resp``. A volume of
        0 models a latency-only round trip.

        With a bounded connection pool, the first call of a query's round
        trip acquires a connection slot (the query waits in the link's
        connection queue until one is free) and keeps it held for the whole
        query engagement; the ``net_resp`` call releases it after its
        transfer completes.
        """
        if volume is None:
            volume = (
                self.net.request_size if stage == "net_req" else q_scalar(query, "volume")
            )
        return self.env.process(self._transfer(query, str(stage), float(volume)))

    # -- internals -------------------------------------------------------- #
    def _edge(self, stage: str, state: str, query: Any, t: float) -> None:
        self._sink_probe.edge(
            f"{self.full_path}.{stage}",
            state,
            t=t,
            agent_id=q_tag(query, "agent_id"),
            request_id=q_tag(query, "request_id"),
        )

    def _hold_key(self, query: Any) -> Tuple[str, str]:
        return (q_tag(query, "agent_id"), q_tag(query, "request_id"))

    def _acquire_connection(self, query: Any, t0: float) -> Any:
        """Begin admission into the link's connection pool.

        Returns the connection request to yield (a granted request must be
        handed to :meth:`_grant_connection`).
        """
        self._edge("net_conn", "submitted", query, t=t0)
        self._edge("net_conn", "queued", query, t=t0)
        return self.conn_pool.request()

    def _grant_connection(self, query: Any, req: Any, t0: float) -> float:
        """Mark a granted connection request as held; returns the wait."""
        t_acq = self.env.now
        self._edge("net_conn", "processing", query, t=t_acq)
        self._conn_busy += 1
        self._sink_probe.sample("conn_busy", self._conn_busy, component=self.full_path)
        self._holds[self._hold_key(query)] = {"request": req, "t0": t0, "acquired": t_acq}
        return t_acq - t0

    def _release_connection(self, query: Any, t_end: float) -> float:
        """Close the connection machine for this query; returns its wait."""
        hold = self._holds.pop(self._hold_key(query))
        conn_wait = hold["acquired"] - hold["t0"]
        self.conn_records.append(
            StageTiming(
                component=self.full_path,
                stage="net_conn",
                start=hold["t0"],
                end=t_end,
                queue_time=conn_wait,
                service_time=t_end - hold["acquired"],
                meta={},
            )
        )
        self._edge("net_conn", "completed", query, t=t_end)
        self._conn_busy -= 1
        self._sink_probe.sample("conn_busy", self._conn_busy, component=self.full_path)
        self.conn_pool.release(hold["request"])
        return conn_wait

    def _transfer(self, query: Any, stage: str, volume: float) -> Any:
        env = self.env
        if volume < 0:
            raise ComponentError(f"{self.full_path}: negative transfer volume {volume}")

        t0 = env.now
        conn_wait: Optional[float] = None
        if self.conn_pool is not None and self._hold_key(query) not in self._holds:
            # connection admission BEFORE any transfer of this query's round
            # trip on this link: no free connection -> wait in the
            # connection queue (the net_conn stage's queue_time)
            conn_req = self._acquire_connection(query, t0)
            yield conn_req
            conn_wait = self._grant_connection(query, conn_req, t0)

        # arrival at the link proper is post-admission, so the net stage's
        # own queue_time stays purely bandwidth/level queueing (never masked
        # by, nor double-counting, the connection wait — that lives on the
        # net_conn stage)
        t_arrival = env.now
        self._edge(stage, "submitted", query, t=t_arrival)
        self._edge(stage, "queued", query, t=t_arrival)

        rec = None
        if self.net.latency > 0:
            yield env.timeout(self.net.latency)
        if volume > 0:
            # created only after the propagation wait, so the pool queue
            # reflects real queueing time (not masked by the latency wait)
            rec = yield self.pool.transfer(volume=volume, label=stage)
        t_end = env.now

        processed_at = rec.start if rec is not None else env.now
        self._edge(stage, "processing", query, t=processed_at)
        self._edge(stage, "completed", query, t=t_end)

        queue_time = 0.0 if rec is None else rec.queue_time
        service_time = (
            t_end - t_arrival if rec is None else rec.service_time + self.net.latency
        )

        meta: Dict[str, float] = {"volume": volume, "latency": self.net.latency}
        if conn_wait is not None:
            meta["conn_queue_time"] = conn_wait

        # connection lifecycle: the net_resp direction completes (and
        # releases) the query's held connection — "one query's connection"
        # to the data node; net_req only acquires (and holds)
        if self.conn_pool is not None and stage == "net_resp" and self._hold_key(query) in self._holds:
            self._release_connection(query, t_end)

        timing = StageTiming(
            component=self.full_path,
            stage=stage,
            start=t_arrival,
            end=t_end,
            queue_time=queue_time,
            service_time=service_time,
            meta=meta,
        )
        self.records.append(timing)
        return timing

    def collect(self) -> Any:
        return {
            "latency": self.net.latency,
            "hops": len(self.records),
            "connections": self.net.connections,
            "conn_records": list(self.conn_records),
        }
