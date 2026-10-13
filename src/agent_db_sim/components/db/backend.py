"""Storage backend model (ticket 05 spec: "(a)+(c)").

``base_latency + per_gb_time x volume`` with shared ``iops_ceiling`` /
``bandwidth_ceiling`` across in-flight ops; named presets (hdd / ssd /
``nvme``) are parameter sets. Latency distributions are deferred.

Contention model: every storage node owns one ceiling budget; in-flight
operations each earn IOPS and bandwidth at ``ceiling / |in-flight|`` per
time unit (shared-rate pacing, same shape as SlotServer's token pacing).
Contention therefore stretches ``storage_service``; a lone op finishes at
``max(base_latency + per_gb_time x volume, volume/bandwidth_ceiling,
1/iops_ceiling)``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Dict, List, Optional, Set

from agent_db_sim.components.db._cfg import cget, cstr
from agent_db_sim.components.db.query import StageTiming, q_scalar, q_tag
from agent_db_sim.simkit.component import Component
from agent_db_sim.simkit.errors import ComponentError, RegistryError
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = [
    "BackendParams",
    "STORAGE_BACKEND_REGISTRY",
    "resolve_backend_params",
    "StorageNode",
]


@dataclass(frozen=True)
class BackendParams:
    """One storage backend parameter set (a named preset)."""

    name: str
    base_latency: float  # fixed per-op latency (time units)
    per_gb_time: float  # seconds-ish per GB touched (time units)
    iops_ceiling: float  # ops/s shared across in-flight operations
    bandwidth_ceiling: float  # GB/s shared across in-flight operations

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _checked(
    name: str,
    *,
    base_latency: float,
    per_gb_time: float,
    iops_ceiling: float,
    bandwidth_ceiling: float,
) -> BackendParams:
    if base_latency < 0 or per_gb_time < 0:
        raise RegistryError(f"storage backend {name!r}: latency params must be >= 0")
    if iops_ceiling <= 0 or bandwidth_ceiling <= 0:
        raise RegistryError(
            f"storage backend {name!r}: ceilings must be > 0 "
            f"(iops={iops_ceiling}, bandwidth={bandwidth_ceiling})"
        )
    return BackendParams(
        name=name,
        base_latency=base_latency,
        per_gb_time=per_gb_time,
        iops_ceiling=iops_ceiling,
        bandwidth_ceiling=bandwidth_ceiling,
    )


#: Explicit config-name -> parameter set registry (ticket 05 presets, plus
#: ``hdd`` as the classic baseline). Merged into ``simkit.registry``.
STORAGE_BACKEND_REGISTRY: Dict[str, BackendParams] = {
    "hdd": _checked(
        "hdd",
        base_latency=0.010,
        per_gb_time=6.0,
        iops_ceiling=150.0,
        bandwidth_ceiling=0.167,
    ),
    "ssd": _checked(
        "ssd",
        base_latency=0.0001,
        per_gb_time=2.5,
        iops_ceiling=75000.0,
        bandwidth_ceiling=0.4,
    ),
    "nvme": _checked(
        "nvme",
        base_latency=0.00005,
        per_gb_time=0.2,
        iops_ceiling=750000.0,
        bandwidth_ceiling=5.0,
    ),
}


def resolve_backend_params(storage_cfg: Any) -> BackendParams:
    """Config -> BackendParams: ``db.storage.backend`` preset name plus the
    optional ``db.storage.overrides`` mapping of parameter overrides."""
    name = cstr(storage_cfg, "backend", "nvme")
    try:
        params = STORAGE_BACKEND_REGISTRY[name]
    except KeyError:
        available = ", ".join(sorted(STORAGE_BACKEND_REGISTRY))
        raise RegistryError(
            f"unknown storage backend {name!r}; available backends: {available}"
        ) from None
    overrides = cget(storage_cfg, "overrides", None)
    if overrides is not None:
        fields = {f for f in BackendParams.__dataclass_fields__ if f != "name"}
        known = {k: float(v) for k, v in dict(overrides).items() if k in fields}
        unknown = set(dict(overrides)) - fields
        if unknown:
            raise RegistryError(
                f"db.storage.overrides has unknown fields {sorted(unknown)}; "
                f"overridable fields: {sorted(fields)}"
            )
        params = replace(params, **known)
    return params


class StorageNode(Component):
    """One storage node: one backend budget, ceiling-paced service.

    Scale-out striping (ticket 05): storage volume work is striped across
    storage nodes — each node keeps its own backend budget, so concurrent
    queries distribute over the nodes and the aggregate bandwidth/IOPS scale
    with M.
    """

    def __init__(self, name: str, params: BackendParams, sink: Optional[ProbeSink] = None) -> None:
        super().__init__(name)
        self.params = params
        self._sink_probe = sink if sink is not None else NULL_SINK
        self._inflight: Set[Any] = set()
        self.records: List[Dict[str, Any]] = []

    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)

    def serve(self, query: Any) -> Any:
        """Storage round-trip for one query, as a SimPy process.

        Returns ``{storage_queue: StageTiming, storage_service: StageTiming}``
        and emits ``{path}.storage_queue`` / ``{path}.storage_service`` edge
        samples tagged with ``agent_id``/``request_id``.
        """
        return self.env.process(self._serve(query))

    # -- internals -------------------------------------------------------- #
    def _edge(self, stage: str, state: str, query: Any, t: Optional[float] = None) -> None:
        self._sink_probe.edge(
            f"{self.full_path}.{stage}",
            state,
            t=t,
            agent_id=q_tag(query, "agent_id"),
            request_id=q_tag(query, "request_id"),
        )

    def _serve(self, query: Any) -> Any:
        env = self.env
        volume = q_scalar(query, "volume")
        if volume <= 0:
            raise ComponentError(
                f"{self.full_path}: storage op needs volume > 0, got {volume}"
            )

        t_arrival = env.now
        self._inflight.add(query)
        self._edge("storage_queue", "submitted", query, t=t_arrival)
        self._edge("storage_queue", "queued", query, t=t_arrival)

        service_start = t_arrival

        # fixed per-op base latency first (charged before any ceiling pacing)
        if self.params.base_latency > 0:
            yield env.timeout(self.params.base_latency)
            service_start = env.now

        # pacing: earn volume at the shared bandwidth rate and one op at the
        # shared IOPS rate; the op completes when the nominal work window
        # (per_gb x volume floored by the lone-op ceiling times) AND both
        # earned budgets are satisfied.
        needed = max(
            self.params.per_gb_time * volume,
            volume / self.params.bandwidth_ceiling,
            1.0 / self.params.iops_ceiling,
        )
        finish_at = env.now + needed
        earned_bw = 0.0
        earned_iop = 0.0
        quantum = 0.01
        while True:
            n = max(1, len(self._inflight))
            bw_rate = self.params.bandwidth_ceiling / n
            iop_rate = self.params.iops_ceiling / n
            candidates: List[float] = []
            if earned_bw < volume:
                candidates.append((volume - earned_bw) / bw_rate)
            if earned_iop < 1.0:
                candidates.append((1.0 - earned_iop) / iop_rate)
            if env.now < finish_at:
                candidates.append(finish_at - env.now)
            if not candidates:
                break
            step = min(quantum, *candidates)
            yield env.timeout(step)
            earned_bw += bw_rate * step
            earned_iop += iop_rate * step

        t_end = env.now
        service_time = t_end - service_start
        queue_time = service_start - t_arrival

        self._inflight.discard(query)
        self._edge("storage_service", "processing", query, t=service_start)
        self._edge("storage_service", "completed", query, t=t_end)

        stages = {
            "storage_queue": StageTiming(
                component=self.full_path,
                stage="storage_queue",
                start=t_arrival,
                end=service_start,
                queue_time=queue_time,
                service_time=0.0,
                meta={"volume": volume, "inflight": float(n)},
            ),
            "storage_service": StageTiming(
                component=self.full_path,
                stage="storage_service",
                start=service_start,
                end=t_end,
                queue_time=0.0,
                service_time=service_time,
                meta={
                    "volume": volume,
                    "base_latency": self.params.base_latency,
                    "per_gb_time": self.params.per_gb_time,
                },
            ),
        }
        self.records.append(
            {
                "agent_id": q_tag(query, "agent_id"),
                "request_id": q_tag(query, "request_id"),
                "arrival": t_arrival,
                "start": service_start,
                "end": t_end,
                "queue_time": queue_time,
                "service_time": service_time,
                "volume": volume,
                "backend": self.params.name,
            }
        )
        return stages

    def collect(self) -> Dict[str, Any]:
        return {"params": self.params.as_dict(), "records": list(self.records)}
