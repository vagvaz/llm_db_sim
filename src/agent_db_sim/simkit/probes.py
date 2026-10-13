"""Probe/signal recording API plus an in-memory sink.

Serving primitives (ingress, slot server, bandwidth pool) push named signal
samples into a :class:`ProbeSink`. The VCD emitter (``viz``) and the metrics
collector (percentiles) will consume sinks in later tickets; both should be
built against the :class:`Sample` / :class:`ProbeSink` interface only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

__all__ = ["Sample", "ProbeSink", "NullSink", "InMemorySink", "NULL_SINK"]

#: States at which a request leaves the active index of a bounded sink: the
#: agent-observed ``received`` edge, or ``completed`` for endpoint-only
#: requests whose lifecycle nobody observes past a component's completion.
TERMINAL_STATES = ("received", "completed")


@dataclass(frozen=True)
class Sample:
    """One recorded probe sample at a point in simulated time.

    ``kind`` is one of ``"level"`` (sampled state, e.g. slot occupancy),
    ``"edge"`` (state change, e.g. process entered a state) or ``"counter"``
    (increment of a named counter; value holds the delta).
    """

    t: float
    signal: str
    kind: str
    value: Any
    tags: Dict[str, Any] = field(default_factory=dict)


class ProbeSink:
    """Abstract push sink for probe samples.

    Primitives sample signals like ``queue_depth``, ``slot_busy`` and
    ``level``; counters are bumped via :meth:`count`. Call sites pass
    simulated time implicitly through the ``clock`` callable, or explicitly
    via ``t=``.

    Streaming consumers (ticket 12/13 follow-up: the live SQLite batch
    writer) attach with :meth:`subscribe`; every recorded sample is pushed
    to each subscriber synchronously, and a subscriber exception propagates
    to the emitter (fail the run loudly, no silent loss).
    """

    def __init__(self, clock: Optional[Callable[[], float]] = None) -> None:
        self.clock = clock
        self._subscribers: List[Callable[[Sample], None]] = []

    def subscribe(self, callback: Callable[[Sample], None]) -> Callable[[Sample], None]:
        """Attach a streaming consumer; it sees every future sample in
        emission order. Returns the callback for unsubscribing."""
        self._subscribers.append(callback)
        return callback

    def unsubscribe(self, callback: Callable[[Sample], None]) -> None:
        self._subscribers.remove(callback)

    def _notify(self, sample: Sample) -> None:
        """Push one sample to every subscribed consumer (errors propagate)."""
        for cb in list(self._subscribers):
            cb(sample)

    def _now(self, t: Optional[float]) -> float:
        if t is not None:
            return float(t)
        return float(self.clock()) if self.clock is not None else 0.0

    def record(self, sample: Sample) -> None:
        """Store or forward one sample. Subclasses must implement."""
        raise NotImplementedError

    # -- convenience API for primitives -------------------------------------
    def sample(
        self,
        signal: str,
        value: Any,
        kind: str = "level",
        t: Optional[float] = None,
        **tags: Any,
    ) -> Sample:
        sample = Sample(
            t=self._now(t), signal=signal, kind=kind, value=value, tags=dict(tags)
        )
        self.record(sample)
        return sample

    def edge(self, signal: str, value: Any, t: Optional[float] = None, **tags: Any) -> Sample:
        return self.sample(signal, value, kind="edge", t=t, **tags)

    def count(self, name: str, delta: float = 1.0, t: Optional[float] = None, **tags: Any) -> Sample:
        return self.sample(name, delta, kind="counter", t=t, **tags)


class NullSink(ProbeSink):
    """Drops everything; used when probing is disabled."""

    def record(self, sample: Sample) -> None:  # noqa: ARG002
        pass


NULL_SINK = NullSink()


class InMemorySink(ProbeSink):
    """The concrete in-memory sink.

    Two retention modes:

    - **Legacy / full-history** (default): flat sample list (``samples``) +
      counter totals (``counters``). Everything since sink creation is
      retained — used by unit fixtures and the viz synthetic fixture.
    - **Bounded** (``bounded=True``): finished-request history is the
      downstream consumer's job (the streaming TraceWriter pushing into
      ``metrics.db``, via :meth:`ProbeSink.subscribe`). The sink keeps only
      an ACTIVE-request index: request ids observed through a non-terminal
      edge (state outside :data:`TERMINAL_STATES`) plus the samples of those
      in-flight requests; a terminal edge (``received``, or ``completed``
      for endpoint-only requests) evicts the request and its samples.
      Un-attributed samples (``level``/``counter`` without a request id)
      collapse to a fixed-size latest-value store (``latest`` keyed by
      ``(signal, component)``) and counter totals. Retained count is
      therefore O(active concurrency), not O(total requests).
    """

    def __init__(
        self,
        clock: Optional[Callable[[], float]] = None,
        *,
        bounded: bool = False,
        terminal_states: Tuple[str, ...] = TERMINAL_STATES,
    ) -> None:
        super().__init__(clock)
        self.samples: List[Sample] = []
        self.counters: Dict[str, float] = {}
        self.bounded = bool(bounded)
        self.terminal_states = frozenset(terminal_states)
        # bounded-mode state (all O(active concurrency) / O(signal count))
        self.active_requests: Dict[str, str] = {}  # request_id -> last state
        self.latest: Dict[Tuple[str, str], Sample] = {}  # (signal, comp) -> last

    # ------------------------------------------------------------------ #
    # ingestion                                                            #
    # ------------------------------------------------------------------ #
    def record(self, sample: Sample) -> None:
        self._notify(sample)  # streaming consumer first; its failures are loud
        if sample.kind == "counter":
            self.counters[sample.signal] = (
                self.counters.get(sample.signal, 0.0) + float(sample.value)
            )
        if not self.bounded:
            self.samples.append(sample)
            return
        rid = sample.tags.get("request_id")
        if rid is None:
            # not attributable to a request: fixed-size latest-value cache
            self.latest[(sample.signal, str(sample.tags.get("component", "")))] = sample
            return
        if str(sample.tags.get("state", sample.value)) in self.terminal_states:
            # finished: drop the active entry and everything retained for it;
            # the sample itself is already with the streaming consumer
            self.active_requests.pop(str(rid), None)
            self.samples = [s for s in self.samples
                            if s.tags.get("request_id") != rid]
            return
        self.active_requests[str(rid)] = str(sample.tags.get("state", sample.value))
        self.samples.append(sample)

    def in_flight(self) -> int:
        """Number of requests currently retained (bounded mode introspection)."""
        return len(self.active_requests)

    # ------------------------------------------------------------------ #
    # reads (legacy full-history semantics in unbounded mode; in bounded    #
    # mode ``series``/``values`` see only the in-flight window)             #
    # ------------------------------------------------------------------ #
    def series(self, signal: str) -> List[Sample]:
        return [s for s in self.samples if s.signal == signal]

    def values(self, signal: str) -> List[Any]:
        return [s.value for s in self.series(signal)]
