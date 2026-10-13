"""NetLink: one internal DB network hop — fixed propagation latency plus a
simkit BandwidthPool transfer (ticket 05: per-link bandwidth on distributed,
one shared segment pool on disaggregated).

The link emits ``{path}.net_req`` / ``{path}.net_resp`` lifecycle edge
samples (states submitted|queued|processing|completed, tagged with
``agent_id``/``request_id``) and returns the hop's :class:`StageTiming`.
"""

from __future__ import annotations

from typing import Any, Optional

from agent_db_sim.components.db._cfg import cfloat
from agent_db_sim.components.db.query import StageTiming, q_scalar, q_tag
from agent_db_sim.simkit.component import Component
from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.levels import BandwidthPool
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = ["NetLink", "NetworkConfig"]

StageName = str


class NetworkConfig:
    """Parsed ``db.network`` subtree (latency, bandwidth, staging capacity,
    control-request size, coordinator overhead multiplier)."""

    def __init__(self, net_cfg: Any) -> None:
        self.latency = cfloat(net_cfg, "latency", 0.01)
        self.bandwidth = cfloat(net_cfg, "bandwidth", 1000.0)
        self.capacity = cfloat(net_cfg, "capacity", 8.0)
        self.request_size = cfloat(net_cfg, "request_size", 0.01)
        self.coordinator_overhead = cfloat(net_cfg, "coordinator_overhead", 1.0)
        if self.latency < 0 or self.bandwidth <= 0 or self.capacity <= 0:
            raise ComponentError("db.network: latency >= 0, bandwidth/capacity > 0 required")
        if self.request_size < 0 or self.coordinator_overhead <= 0:
            raise ComponentError(
                "db.network: request_size >= 0 and coordinator_overhead > 0 required"
            )


class NetPool(BandwidthPool):
    """BandwidthPool over the fp-safe staging container (now the level-layer
    default; see :class:`agent_db_sim.simkit.levels.TolerantContainer`)."""


class NetLink(Component):
    """Latency + BandwidthPool hop; ``transfer`` returns a process."""

    def __init__(self, name: str, net: NetworkConfig, sink: Optional[ProbeSink] = None) -> None:
        super().__init__(name)
        self.net = net
        self._sink_probe = sink if sink is not None else NULL_SINK
        self.pool: Optional[BandwidthPool] = None
        self.records: list = []

    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)
        self.pool = NetPool(
            self.env,
            self.full_path,
            capacity=self.net.capacity,
            bandwidth=self.net.bandwidth,
            sink=self._sink_probe,
        )

    def transfer(self, query: Any, stage: StageName, volume: Optional[float] = None) -> Any:
        """One direction of the hop; returns a process yielding StageTiming.

        ``stage`` is ``net_req`` (control request out) or ``net_resp`` (data
        volume back). ``volume=None`` defaults to ``request_size`` for
        ``net_req`` and the query's ``volume`` for ``net_resp``. A volume of
        0 models a latency-only round trip.
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

    def _transfer(self, query: Any, stage: str, volume: float) -> Any:
        env = self.env
        if volume < 0:
            raise ComponentError(f"{self.full_path}: negative transfer volume {volume}")

        t0 = env.now
        self._edge(stage, "submitted", query, t=t0)
        self._edge(stage, "queued", query, t=t0)

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
        service_time = t_end - t0 if rec is None else rec.service_time + self.net.latency
        timing = StageTiming(
            component=self.full_path,
            stage=stage,
            start=t0,
            end=t_end,
            queue_time=queue_time,
            service_time=service_time,
            meta={"volume": volume, "latency": self.net.latency},
        )
        self.records.append(timing)
        return timing

    def collect(self) -> Any:
        return {"latency": self.net.latency, "hops": len(self.records)}
