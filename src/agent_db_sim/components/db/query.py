"""Query / response shapes flowing through DBEndpoint.

Request contract (shared with tickets 08/09 — do not deviate): a request
carries the attrs ``complexity`` (compute time units), ``volume`` (data
units, "GB"), ``agent_id`` and ``request_id``; the wired ports are ``queries``
(input) and ``responses`` (output).

:class:`DBQuery` is the concrete shape the DB endpoint serves. Callers may
also submit any object exposing those attrs (or a simkit ``Request`` with a
``params`` dict holding them) — :func:`q_scalar` resolves both shapes.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

__all__ = ["QueryResponse", "StageTiming", "DBQuery", "q_scalar", "next_request_id"]

_request_ids = itertools.count(1)


def next_request_id() -> int:
    return next(_request_ids)


def q_scalar(query: Any, name: str) -> float:
    """Read attribute ``name`` off a request; falls back to a params dict."""
    value = getattr(query, name, None)
    if value is None:
        params = getattr(query, "params", None)
        if isinstance(params, Mapping):
            value = params.get(name)
    if value is None:
        raise KeyError(
            f"request {query!r} is missing required attribute {name!r}"
        )
    value_f = float(value)
    if value_f < 0:
        raise ValueError(
            f"request {query!r} attribute {name!r} must be >= 0, got {value_f}"
        )
    return value_f


def q_tag(query: Any, name: str, default: Any = "") -> str:
    """Read an identifier attribute (agent_id/request_id) for trace tags."""
    value = getattr(query, name, None)
    if value is None:
        params = getattr(query, "params", None)
        if isinstance(params, Mapping):
            value = params.get(name)
    return str(value) if value is not None else str(default)


@dataclass(eq=False)
class DBQuery:
    """A DB query request (hashable by identity; used in in-flight sets)."""

    agent_id: Any
    request_id: Any = field(default=None)
    complexity: float = 0.0
    volume: float = 0.0
    priority: float = 0.0
    tokens: float = 0.0

    # internal per-request slot-server id (mirrors simkit.Request.rid)
    rid: int = field(default_factory=next_request_id)

    # runtime fields assigned by the serving node (mirrors simkit.Request)
    submitted_at: Optional[float] = field(default=None, init=False, repr=False)
    done: Any = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.request_id is None:
            self.request_id = self.rid


@dataclass(frozen=True)
class StageTiming:
    """Timing of one hop in the query path (per ticket 04 stage taxonomy).

    ``component`` is the owning component's full path; ``queue_time`` and
    ``service_time`` stay separated; ``meta`` carries extras such as the
    propagation latency (network hops) or the transfer volume.
    """

    component: str
    stage: str
    start: float = 0.0
    end: float = 0.0
    queue_time: float = 0.0
    service_time: float = 0.0
    meta: Dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryResponse:
    """What a caller receives for one submitted query.

    ``stages`` maps stage names from ticket 04's taxonomy
    (``compute_issue``, ``net_req``, ``storage_queue``, ``storage_service``,
    ``net_resp``, ``compute_finalize``) to their per-hop timings. ``queue_time``
    is the request's compute-slot wait; ``service_time`` is the whole slot-hold
    envelope (issue + storage round-trip + finalize, option b).
    """

    agent_id: Any
    request_id: Any
    db_latency: float
    queue_time: float
    service_time: float
    submitted_at: float
    completed_at: float
    stages: Dict[str, StageTiming] = field(default_factory=dict)
