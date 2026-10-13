"""Unit tests for the probe/signal recording API + in-memory sink."""

from __future__ import annotations

from agent_db_sim.simkit.probes import InMemorySink, NULL_SINK, NullSink


def test_samples_levels_edges_and_counters() -> None:
    now = [0.0]
    sink = InMemorySink(clock=lambda: now[0])

    sink.sample("slot_busy", 2, component="llm")
    now[0] = 1.5
    sink.edge("state", "busy", component="llm")
    sink.count("requests", 1.0, component="llm")
    sink.count("requests", 2.0, component="llm")

    assert len(sink.samples) == 4
    assert [(s.t, s.kind) for s in sink.samples] == [
        (0.0, "level"),
        (1.5, "edge"),
        (1.5, "counter"),
        (1.5, "counter"),
    ]
    assert sink.values("slot_busy") == [2]
    assert sink.series("requests") == sink.samples[-2:]
    assert sink.counters == {"requests": 3.0}


def test_explicit_t_overrides_clock() -> None:
    sink = InMemorySink(clock=lambda: 99.0)
    sink.sample("level", 5, t=1.0)
    assert sink.samples[0].t == 1.0

    null = NULL_SINK or NullSink()
    null.sample("anything", 1)  # never raises, never stores
