"""Phase-capable slot server: a simkit SlotServer whose service is a
multi-phase script executed while the slot is held (option b: compute wraps
storage).

The base :class:`agent_db_sim.simkit.processing.SlotServer` serves a single
cost_fn wait; DBCompute needs the three-phase path
``compute_issue -> (net/storage round-trip) -> compute_finalize`` with the
slot held throughout and an aggregate token cap paced across the *compute*
phases only. This subclass keeps every base mechanism (explicit slot pool,
ingress dispatcher, per-request ServiceRecord, shared in-flight token set,
probe samples) and swaps only ``_serve`` for a script-driven body.
"""

from __future__ import annotations

from typing import Any, Callable, Generator, Optional

from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.processing import ServiceRecord, SlotServer
from agent_db_sim.simkit.probes import ProbeSink

__all__ = ["PhaseControl", "PhaseSlotServer"]


class PhaseControl:
    """Handle passed to a serve script, exposing paced waits and hops."""

    def __init__(self, env: Any, server: "PhaseSlotServer") -> None:
        self.env = env
        self.server = server

    def call(self, event: Any) -> Any:
        """Participate in one foreign hop: ``yield ctl.call(event)``.

        ``event`` may be a SimPy event or a Process; the script receives its
        value.
        """
        return event

    def wait_compute(self, query: Any, duration: float) -> Any:
        """Compute wait of ``duration`` while pacing the aggregate token cap.

        The request earns tokens at ``token_cap / |in-flight|`` per time unit
        (mirroring SlotServer's shared-rate pacing), so the wait also covers
        token throttling when a cap is configured.
        """
        return self.env.process(self.server._wait_compute(query, duration))


class PhaseSlotServer(SlotServer):
    """SlotServer variant driving a multi-phase ``script`` per request.

    ``script(control, request)`` is a SimPy generator function; its return
    value becomes the request's ``done`` event value (the caller's response
    payload). Queue/service envelope timing and slot bookkeeping follow the
    base SlotServer contract exactly.
    """

    def __init__(
        self,
        env: Any,
        name: str,
        *,
        slots: int,
        ingress: Any,
        script: Optional[Callable[..., Generator[Any, Any, Any]]],
        token_cap: Optional[float] = None,
        quantum: float = 0.01,
        sink: Optional[ProbeSink] = None,
    ) -> None:
        # base cost_fn is unused (the script drives every wait); keep it a
        # valid callable so base validators pass unchanged.
        super().__init__(
            env,
            name,
            slots=slots,
            cost_fn=lambda req: 0.0,
            ingress=ingress,
            token_cap=token_cap,
            quantum=quantum,
            sink=sink,
        )
        if script is None or not callable(script):
            raise ComponentError(f"server {name!r}: script must be callable")
        self.script = script

    # -- internals -------------------------------------------------------- #
    def _serve(self, req: Any) -> Any:
        pool_req = self.pool.request()
        yield pool_req

        self._busy += 1
        self._sink.sample("slot_busy", self._busy, component=self.name)

        t_start = self.env.now
        queue_time = t_start - req.submitted_at

        if self.token_cap is not None and getattr(req, "tokens", 0.0) > 0:
            self._pacing.add(req)

        ctl = PhaseControl(self.env, self)
        stages_proc = self.env.process(self._run_script(ctl, req))
        yield stages_proc
        stages = stages_proc.value if stages_proc.value is not None else {}

        self._pacing.discard(req)
        t_end = self.env.now

        rec = ServiceRecord(
            rid=req.rid,
            arrival=req.submitted_at,
            start=t_start,
            end=t_end,
            queue_time=queue_time,
            service_time=t_end - t_start,
            tokens=getattr(req, "tokens", 0.0),
        )
        self.records.append(rec)

        self._busy -= 1
        self._sink.sample("slot_busy", self._busy, component=self.name)
        self._sink.count("served", 1.0, component=self.name)

        self.pool.release(pool_req)
        req.done.succeed(value=(rec, stages))
        return rec

    def _run_script(self, ctl: PhaseControl, req: Any) -> Any:
        result = yield from self.script(ctl, req)
        return result

    def _wait_compute(self, req: Any, duration: float) -> Any:
        """Paced compute wait; token cap shared across all in-flight holders."""
        env = self.env
        if duration < 0:
            raise ComponentError(f"server {self.name!r}: negative compute wait {duration}")
        if duration == 0:
            return  # not a process; scripts never yield it
        finish_after = env.now + duration
        remaining = 0.0
        if self.token_cap is not None and getattr(req, "tokens", 0.0) > 0:
            remaining = float(req.tokens)
        while remaining > 0 or env.now < finish_after:
            if remaining > 0:
                rate = self.token_cap / max(1, len(self._pacing))
                step = min(self.quantum, remaining / rate)
                if env.now < finish_after:
                    step = min(step, finish_after - env.now)
                yield env.timeout(step)
                remaining -= step * self.token_cap / max(1, len(self._pacing))
                if remaining <= 1e-9:
                    remaining = 0.0
            else:
                yield env.timeout(finish_after - env.now)
