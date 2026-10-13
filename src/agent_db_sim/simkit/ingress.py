"""Configurable ingress stage in front of a serving component's Processing.

Registry keys (explicit dict registry; unknown names are ``RegistryError``):

- ``none``            — no buffering: ``submit`` blocks until a consumer
  picks the request up (rendezvous handoff).
- ``fifo``            — unbounded FIFO buffering.
- ``priority:<path>`` — unbounded priority queue keyed by a dotted
  attribute/key path on the request; ties break FIFO by insertion order.

Uniform API: callers ``yield ingress.submit(req)`` (only ``none`` blocks),
consumers ``req = yield ingress.get()``. ``queue_depth`` is sampled into the
probe sink whenever the queue size changes.
"""

from __future__ import annotations

import itertools
from collections import deque
from dataclasses import dataclass, field
from heapq import heappop, heappush
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple

from simpy.resources.store import PriorityStore, Store

from agent_db_sim.simkit.errors import IngressError, RegistryError
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = [
    "Request",
    "dig",
    "Ingress",
    "IngressNone",
    "IngressFifo",
    "IngressPriority",
    "INGRESS_REGISTRY",
    "parse_ingress_spec",
    "open_ingress",
]

_issued_ids = itertools.count(1)


@dataclass(eq=False)
class Request:
    """A unit of work flowing through Ingress -> Processing.

    ``params`` carries whatever the component's cost function reads
    (complexity, volume, ...); ``tokens`` is the token volume drawn from an
    aggregate throughput cap (0 disables it); ``priority`` is the default
    sort-key attribute for ``priority`` ingress. Hashed by identity (used
    in the server's in-flight set).
    """

    params: Dict[str, Any] = field(default_factory=dict)
    tokens: float = 0.0
    priority: float = 0.0
    rid: int = field(default_factory=lambda: next(_issued_ids))

    # assigned at submit() by the owning server (not constructor arguments)
    submitted_at: Optional[float] = field(default=None, init=False, repr=False)
    done: Any = field(default=None, init=False, repr=False)  # simpy Event


def dig(req: Any, path: str) -> Any:
    """Resolve a dotted attribute/key path on a request (fail fast)."""
    node = req
    for part in path.split("."):
        if isinstance(node, Mapping) and part in node:
            node = node[part]
            continue
        try:
            node = node[part]
        except (TypeError, LookupError):
            try:
                node = getattr(node, part)
            except AttributeError:
                raise IngressError(
                    f"priority key path {path!r} not resolvable on request "
                    f"{req!r}: missing {part!r}"
                ) from None
    return node


class Ingress:
    """Base class of the ingress registry: uniform submit/get API."""

    def __init__(self, env: Any, name: str, sink: Optional[ProbeSink] = None) -> None:
        self.env = env
        self.name = name
        self._sink = sink if sink is not None else NULL_SINK

    def submit(self, req: Request) -> Any:
        """Companion event to yield; blocks only for the ``none`` kind."""
        raise NotImplementedError

    def get(self) -> Any:
        """Yield this event; the value is the dequeued request."""
        raise NotImplementedError

    @property
    def queue_depth(self) -> int:
        raise NotImplementedError

    def _sample_depth(self) -> None:
        self._sink.sample("queue_depth", self.queue_depth, component=self.name)


class IngressNone(Ingress):
    """No buffering: submits block until a consumer takes the request."""

    def __init__(self, env: Any, name: str, sink: Optional[ProbeSink] = None) -> None:
        super().__init__(env, name, sink)
        self._puts: Deque[Tuple[Any, Any]] = deque()
        self._gets: Deque[Any] = deque()
        self._sample_depth()

    def submit(self, req: Request) -> Any:
        put_ev = self.env.event()
        self._puts.append((req, put_ev))
        self._pump()
        return put_ev

    def get(self) -> Any:
        get_ev = self.env.event()
        self._gets.append(get_ev)
        self._pump()
        return get_ev

    @property
    def queue_depth(self) -> int:
        return len(self._puts)

    def _pump(self) -> None:
        while self._puts and self._gets:
            req, put_ev = self._puts.popleft()
            get_ev = self._gets.popleft()
            get_ev.succeed(value=req)
            put_ev.succeed()
        self._sample_depth()


class _SampledFifoStore(Store):
    """Unbounded Store that samples queue depth into the probe sink."""

    def __init__(self, env: Any, name: str, sink: Optional[ProbeSink]) -> None:
        super().__init__(env)
        self._name = name
        self._sink = sink if sink is not None else NULL_SINK

    def _do_put(self, event: Any) -> None:
        super()._do_put(event)
        self._sink.sample("queue_depth", len(self.items), component=self._name)

    def _do_get(self, event: Any) -> None:
        had_items = bool(self.items)
        super()._do_get(event)
        if had_items:
            self._sink.sample("queue_depth", len(self.items), component=self._name)


class IngressFifo(Ingress):
    """Unbounded FIFO buffering."""

    def __init__(self, env: Any, name: str, sink: Optional[ProbeSink] = None) -> None:
        super().__init__(env, name, sink)
        self._store = _SampledFifoStore(env, name, sink)

    def submit(self, req: Request) -> Any:
        return self._store.put(req)

    def get(self) -> Any:
        return self._store.get()

    @property
    def queue_depth(self) -> int:
        return len(self._store.items)


class _SampledPriorityStore(PriorityStore):
    """PriorityStore over ``(key, seq, req)`` tuples; returns bare requests."""

    def __init__(self, env: Any, name: str, sink: Optional[ProbeSink]) -> None:
        super().__init__(env)
        self._name = name
        self._sink = sink if sink is not None else NULL_SINK

    def _do_put(self, event: Any) -> None:
        if len(self.items) < self._capacity:
            heappush(self.items, event.item)
            event.succeed()
            self._sink.sample("queue_depth", len(self.items), component=self._name)

    def _do_get(self, event: Any) -> None:
        if self.items:
            _key, _seq, req = heappop(self.items)
            event.succeed(req)
            self._sink.sample("queue_depth", len(self.items), component=self._name)


class IngressPriority(Ingress):
    """Unbounded priority queue; key from a dotted path on the request."""

    def __init__(
        self, env: Any, name: str, key_path: str, sink: Optional[ProbeSink] = None
    ) -> None:
        super().__init__(env, name, sink)
        self._key_path = key_path
        self._store = _SampledPriorityStore(env, name, sink)
        self._seq = itertools.count()

    def submit(self, req: Request) -> Any:
        key = dig(req, self._key_path)
        item = (key, next(self._seq), req)
        return self._store.put(item)

    def get(self) -> Any:
        return self._store.get()

    @property
    def queue_depth(self) -> int:
        return len(self._store.items)


INGRESS_REGISTRY: Dict[str, type] = {
    "none": IngressNone,
    "fifo": IngressFifo,
    "priority": IngressPriority,
}


def parse_ingress_spec(spec: str) -> Tuple[str, Optional[str]]:
    """Parse ``none | fifo | priority:<key>`` into (kind, key)."""
    if not isinstance(spec, str):
        raise RegistryError(f"ingress spec must be a string, got {spec!r}")
    kind, _, key = spec.partition(":")
    if kind not in INGRESS_REGISTRY:
        available = ", ".join(sorted(INGRESS_REGISTRY))
        raise RegistryError(
            f"unknown ingress kind {kind!r} (spec {spec!r}); available kinds: {available}"
        )
    if kind == "priority":
        if not key:
            raise RegistryError(
                "ingress 'priority' requires a dotted request-key path: "
                "'priority:<path>'"
            )
        return kind, key
    if key:
        raise RegistryError(f"ingress kind {kind!r} takes no key (spec {spec!r})")
    return kind, None


def open_ingress(
    env: Any, spec: str, name: str, sink: Optional[ProbeSink] = None
) -> Ingress:
    """Registry lookup: spec string -> ingress instance (fail fast)."""
    kind, key = parse_ingress_spec(spec)
    cls = INGRESS_REGISTRY[kind]
    if kind == "priority":
        assert key is not None
        return cls(env, name, key, sink=sink)
    return cls(env, name, sink=sink)
