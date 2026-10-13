"""Processing primitive: slot capacity pool + cost_fn + optional aggregate
token-throughput cap, fed by an Ingress.

Semantics (the shape LLMEndpoint and DBCompute are built on):

- ``submit(req)`` records ``req.submitted_at`` and returns the ingress event
  to yield (blocks only for ingress ``none``).
- One dispatcher consumes the ingress in order; each request gets a service
  process that first acquires a slot from an explicit ``simpy.Resource``
  capacity pool. Requests waiting for a slot keep their ingress order
  (pool acquisition is FIFO among simultaneous requests).
- ``queue_time`` = submit -> slot acquired (ingress wait + slot wait, never
  double counted); ``service_time`` = slot acquired -> done. Both are
  recorded per request in ``server.records`` and the request's ``done``
  event is fulfilled with its :class:`ServiceRecord`.
- Service duration is ``max(cost_fn(req), token-paced duration)``. When the
  aggregate ``token_cap`` is set and the request carries tokens, the tokens
  are earned at ``token_cap / |in-flight set|`` per time unit while the
  request holds its slot — i.e. the cap is shared across all in-flight
  token-consuming requests (it is what throttles aggregate token/s).
  ``quantum`` is the pacing re-check granularity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Set, Union

import simpy

from agent_db_sim.simkit.errors import ComponentError, RegistryError
from agent_db_sim.simkit.ingress import Ingress, Request, open_ingress
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = ["ServiceRecord", "CostFn", "SlotServer"]

CostFn = Callable[[Request], float]


@dataclass(frozen=True)
class ServiceRecord:
    """Per-request timing record; queue and service time stay separated."""

    rid: int
    arrival: float
    start: float
    end: float
    queue_time: float
    service_time: float
    tokens: float


class SlotServer:
    """Slot-based server: acquire slot -> wait cost_fn -> release."""

    def __init__(
        self,
        env: Any,
        name: str,
        *,
        slots: int,
        cost_fn: CostFn,
        ingress: Optional[Union[Ingress, str]] = None,
        token_cap: Optional[float] = None,
        quantum: float = 0.01,
        sink: Optional[ProbeSink] = None,
    ) -> None:
        if slots < 1:
            raise ComponentError(f"server {name!r}: slots must be >= 1, got {slots}")
        if not callable(cost_fn):
            raise ComponentError(f"server {name!r}: cost_fn must be callable")
        if quantum <= 0:
            raise ComponentError(f"server {name!r}: quantum must be > 0")
        if token_cap is not None and token_cap <= 0:
            raise ComponentError(
                f"server {name!r}: token_cap must be > 0, got {token_cap}"
            )
        if ingress is None:
            raise ComponentError(
                f"server {name!r} requires an explicit ingress "
                f"(a registry kind like 'fifo' or an Ingress instance)"
            )

        self.name = name
        self.env = env
        self.slots = int(slots)
        self.cost_fn = cost_fn
        self.token_cap = token_cap
        self.quantum = float(quantum)
        self._sink = sink if sink is not None else NULL_SINK

        if isinstance(ingress, str):
            self.ingress: Ingress = open_ingress(
                env, ingress, name=f"{name}.ingress", sink=self._sink
            )
        elif isinstance(ingress, Ingress):
            self.ingress = ingress
        else:  # pragma: no cover - defensive
            raise RegistryError(
                f"server {name!r}: ingress must be a registry spec or Ingress"
            )

        self.pool = simpy.Resource(env, capacity=self.slots)
        self._pacing: Set[Request] = set()
        self._busy = 0
        self._started = False
        self.records: list[ServiceRecord] = []

    # ------------------------------------------------------------------ #
    # serving API                                                         #
    # ------------------------------------------------------------------ #
    def submit(self, req: Request) -> Any:
        """Uniform uniform admission: set timing stamps, return event to yield.

        Callers do ``ev = server.submit(req); yield ev``; the event blocks
        only when the ingress kind is ``none``. Completion is awaited via
        ``yield req.done`` (fulfilled with the ServiceRecord).
        """
        if req.submitted_at is not None or req.done is not None:
            raise ComponentError(f"request {req.rid} already submitted once")
        req.submitted_at = self.env.now
        req.done = self.env.event()
        self._sink.count("requests", 1.0, component=self.name)
        return self.ingress.submit(req)

    def wait(self, req: Request) -> Any:
        """The event fulfilled with the request's ServiceRecord."""
        if req.done is None:
            raise ComponentError(
                f"request {req.rid} not yet submitted; cannot wait on it"
            )
        return req.done

    def start(self) -> None:
        """Launch the ingress dispatcher process (idempotent)."""
        if self._started:
            return
        self._started = True
        self.env.process(self._dispatcher())

    # ------------------------------------------------------------------ #
    # internals                                                           #
    # ------------------------------------------------------------------ #
    def _dispatcher(self) -> Any:
        while True:
            req = yield self.ingress.get()
            self.env.process(self._serve(req))

    def _serve(self, req: Request) -> Any:
        # acquire from the explicit capacity pool; FIFO among simultaneous
        # requests preserves ingress ordering
        pool_req = self.pool.request()
        yield pool_req

        self._busy += 1
        self._sink.sample("slot_busy", self._busy, component=self.name)

        t_start = self.env.now
        queue_time = t_start - req.submitted_at

        base_wait = float(self.cost_fn(req))
        if base_wait < 0:
            raise ComponentError(
                f"server {self.name!r}: cost_fn returned {base_wait!r} < 0"
            )
        finish_after = t_start + base_wait

        remaining = 0.0
        if self.token_cap is not None and req.tokens > 0:
            remaining = float(req.tokens)
            self._pacing.add(req)

        # pacing loop: earn tokens at the shared rate up to the aggregate cap
        while remaining > 0 or self.env.now < finish_after:
            if remaining > 0:
                rate = self.token_cap / max(1, len(self._pacing))
                step = min(self.quantum, remaining / rate)
                if self.env.now < finish_after:
                    step = min(step, finish_after - self.env.now)
                yield self.env.timeout(step)
                # rate is re-read next iteration; in-flight set shrinks when
                # other requests release their slots
                remaining -= step * self.token_cap / max(1, len(self._pacing))
                if remaining <= 1e-9:
                    remaining = 0.0
            else:
                yield self.env.timeout(finish_after - self.env.now)

        self._pacing.discard(req)
        t_end = self.env.now
        service_time = t_end - t_start

        rec = ServiceRecord(
            rid=req.rid,
            arrival=req.submitted_at,
            start=t_start,
            end=t_end,
            queue_time=queue_time,
            service_time=service_time,
            tokens=req.tokens,
        )
        self.records.append(rec)

        self._busy -= 1
        self._sink.sample("slot_busy", self._busy, component=self.name)
        self._sink.count("served", 1.0, component=self.name)

        self.pool.release(pool_req)
        req.done.succeed(value=rec)
        return rec
