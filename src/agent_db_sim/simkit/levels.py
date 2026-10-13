"""Bandwidth/level primitive (SimPy Container-based) for storage / network
transfer.

Model: a shared staging level (``simpy.Container``) bounded by
``capacity``. ``transfer(volume)`` blocks in a container ``get(volume)``
while the shared level is insufficient (recorded as queue time), then serves
``volume / bandwidth`` time units (recorded as service time), then returns
the volume to the shared level. Concurrent transfer throughput is therefore
limited both by the level (volume-wise) and by the bandwidth rate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import simpy

from agent_db_sim.simkit.errors import BandwidthError
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = ["TransferRecord", "TolerantContainer", "BandwidthPool"]


class TolerantContainer(simpy.Container):
    """Container whose ``put`` accepts when the level is within an epsilon of
    the requested amount after a ``get`` (imperfect floats make
    ``capacity - level`` fall ~1e-15 short of the returned volume, which
    otherwise wedges the staging level permanently).

    Hoisted from the ticket-10 DB ``NetPool`` workaround so every shared
    staging level gets the same fp guard. The epsilon is only exercised when
    the exact-put check would spuriously fail, so it cannot change behavior
    for exactly-representable volumes.
    """

    fp_tol = 1e-9

    def _do_put(self, event):  # noqa: ANN001 - simpy signature
        if self._capacity - self._level >= event.amount - self.fp_tol:
            self._level = min(self._capacity, self._level + event.amount)
            event.succeed()
            return True
        return None



@dataclass(frozen=True)
class TransferRecord:
    """Per-transfer timing record; queue and service time stay separated."""

    label: str
    arrival: float
    start: float
    end: float
    queue_time: float
    service_time: float
    volume: float


class BandwidthPool:
    """Container-based staging level with a bandwidth-limited transfer."""

    def __init__(
        self,
        env: Any,
        name: str,
        *,
        capacity: float,
        bandwidth: float,
        init: Optional[float] = None,
        sink: Optional[ProbeSink] = None,
    ) -> None:
        if capacity <= 0:
            raise BandwidthError(f"{name!r}: capacity must be > 0, got {capacity}")
        if bandwidth <= 0:
            raise BandwidthError(f"{name!r}: bandwidth must be > 0, got {bandwidth}")
        if init is None:
            init = capacity
        if not 0 <= init <= capacity:
            raise BandwidthError(
                f"{name!r}: init {init!r} must be within [0, capacity={capacity}]"
            )

        self.name = name
        self.env = env
        self.capacity = float(capacity)
        self.bandwidth = float(bandwidth)
        self.container = TolerantContainer(env, capacity=self.capacity, init=init)
        self._sink = sink if sink is not None else NULL_SINK
        self.records: list[TransferRecord] = []
        self._sink.sample("level", self.container.level, component=self.name)

    @property
    def level(self) -> float:
        return self.container.level

    def transfer(self, *, volume: float, label: str = "") -> Any:
        """Launch the transfer as a process; yield the returned Process.

        Caller is a process doing ``rec = yield pool.transfer(volume=...)``;
        the Process value holds the :class:`TransferRecord` with separated
        queue/service timings for this transfer.
        """
        return self.env.process(self._transfer(volume=volume, label=label))

    def _transfer(self, *, volume: float, label: str) -> Any:
        if volume <= 0:
            raise BandwidthError(
                f"{self.name!r}: transfer volume must be > 0, got {volume}"
            )

        t_arrival = self.env.now
        get = self.container.get(volume)
        yield get
        t_start = self.env.now
        queue_time = t_start - t_arrival

        self._sink.sample("level", self.container.level, component=self.name)
        yield self.env.timeout(volume / self.bandwidth)
        t_end = self.env.now
        service_time = t_end - t_start

        yield self.container.put(volume)
        self._sink.sample("level", self.container.level, component=self.name)

        rec = TransferRecord(
            label=label,
            arrival=t_arrival,
            start=t_start,
            end=t_end,
            queue_time=queue_time,
            service_time=service_time,
            volume=volume,
        )
        self.records.append(rec)
        return rec
