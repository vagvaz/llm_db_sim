"""Probe/signal recording API plus an in-memory sink.

Serving primitives (ingress, slot server, bandwidth pool) push named signal
samples into a :class:`ProbeSink`. The VCD emitter (``viz``) and the metrics
collector (percentiles) will consume sinks in later tickets; both should be
built against the :class:`Sample` / :class:`ProbeSink` interface only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

__all__ = ["Sample", "ProbeSink", "NullSink", "InMemorySink", "NULL_SINK"]


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
    """

    def __init__(self, clock: Optional[Callable[[], float]] = None) -> None:
        self.clock = clock

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
    """The concrete in-memory sink: flat sample list + counter totals.

    VCD emission and the metrics collector will read from this shape (or an
    equivalent streaming sink) in later tickets.
    """

    def __init__(self, clock: Optional[Callable[[], float]] = None) -> None:
        super().__init__(clock)
        self.samples: List[Sample] = []
        self.counters: Dict[str, float] = {}

    def record(self, sample: Sample) -> None:
        self.samples.append(sample)
        if sample.kind == "counter":
            self.counters[sample.signal] = (
                self.counters.get(sample.signal, 0.0) + float(sample.value)
            )

    def series(self, signal: str) -> List[Sample]:
        return [s for s in self.samples if s.signal == signal]

    def values(self, signal: str) -> List[Any]:
        return [s.value for s in self.series(signal)]
