"""Unit tests for the SlotServer processing primitive.

Covers: slot capacity respected; aggregate token cap scales service time and
is shared across in-flight requests; queue vs service timing separated;
per-request records and completion events.
"""

from __future__ import annotations

import simpy
import pytest

from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.probes import InMemorySink
from agent_db_sim.simkit.processing import SlotServer
from agent_db_sim.simkit.ingress import Request


def submit(env, server, req, at=0.0):
    def _agent():
        if at:
            yield env.timeout(at)
        yield server.submit(req)
        yield req.done

    env.process(_agent())
    return req


def test_slot_capacity_respected() -> None:
    env = simpy.Environment()
    sink = InMemorySink(clock=lambda: env.now)
    server = SlotServer(
        env, "svc", slots=2, cost_fn=lambda r: 1.0, ingress="fifo", sink=sink
    )
    server.start()

    for _ in range(6):
        submit(env, server, Request())

    env.run(until=20.0)

    busy = [v for v in sink.values("slot_busy")]
    assert max(busy) <= 2
    assert max(busy) == 2  # capacity fully used at peak

    starts = sorted(rec.start for rec in server.records)
    assert starts == [0.0, 0.0, 1.0, 1.0, 2.0, 2.0]


def test_queue_and_service_time_separated() -> None:
    env = simpy.Environment()
    server = SlotServer(env, "svc", slots=1, cost_fn=lambda r: 1.0, ingress="fifo")
    server.start()

    for _ in range(3):
        submit(env, server, Request())

    env.run(until=20.0)

    recs = server.records
    assert [(rec.queue_time, rec.service_time) for rec in recs] == [
        (0.0, 1.0),  # submitted t=0, served 0..1
        (1.0, 1.0),  # submitted t=0, slot at t=1
        (2.0, 1.0),  # submitted t=0, slot at t=2
    ]


def test_aggregate_token_cap_scales_service_time() -> None:
    # solo in-flight: tokens = 1000 at a cap of 1000 tok/time-unit -> 1.0
    env = simpy.Environment()
    solo = SlotServer(
        env,
        "solo",
        slots=4,
        cost_fn=lambda r: 0.0,
        ingress="fifo",
        token_cap=1000.0,
    )
    solo.start()
    submit(env, solo, Request(tokens=1000.0))
    env.run(until=20.0)
    (rec,) = solo.records
    assert rec.service_time == pytest.approx(1.0, abs=1e-6)
    assert rec.queue_time == 0.0

    # two concurrent in-flight requests share the cap: 2x1000/1000 -> 2.0 each
    env2 = simpy.Environment()
    shared = SlotServer(
        env2,
        "shared",
        slots=4,
        cost_fn=lambda r: 0.0,
        ingress="fifo",
        token_cap=1000.0,
    )
    shared.start()
    submit(env2, shared, Request(tokens=1000.0))
    submit(env2, shared, Request(tokens=1000.0))
    env2.run(until=20.0)

    assert len(shared.records) == 2
    for rec in shared.records:
        assert rec.service_time == pytest.approx(2.0, abs=1e-6)
        assert rec.end == pytest.approx(2.0, abs=1e-6)
        assert rec.queue_time == 0.0


def test_token_cap_does_not_shorten_cost_fn_floor() -> None:
    # service time = max(cost_fn, token-paced time); with a generous cap the
    # cost_fn wait dominates
    env = simpy.Environment()
    server = SlotServer(
        env,
        "svc",
        slots=2,
        cost_fn=lambda r: 3.0,
        ingress="fifo",
        token_cap=1.0e9,
    )
    server.start()
    submit(env, server, Request(tokens=1.0))
    env.run(until=20.0)
    (rec,) = server.records
    assert rec.service_time == pytest.approx(3.0)


def test_done_event_carries_record_and_double_submit_fails() -> None:
    env = simpy.Environment()
    server = SlotServer(env, "svc", slots=1, cost_fn=lambda r: 0.5, ingress="fifo")
    server.start()
    req = submit(env, server, Request())
    env.run(until=5.0)

    (rec,) = server.records
    assert req.done.value is rec
    assert rec.rid == req.rid

    with pytest.raises(ComponentError, match="already submitted"):
        server.submit(req)

    fresh = Request()
    with pytest.raises(ComponentError, match="not yet submitted"):
        server.wait(fresh)


def test_invalid_configuration_fails_fast() -> None:
    env = simpy.Environment()
    with pytest.raises(ComponentError, match="slots must be >= 1"):
        SlotServer(env, "s", slots=0, cost_fn=lambda r: 1.0, ingress="fifo")
    with pytest.raises(ComponentError, match="explicit ingress"):
        SlotServer(env, "s", slots=1, cost_fn=lambda r: 1.0)
    with pytest.raises(ComponentError, match="token_cap"):
        SlotServer(
            env, "s", slots=1, cost_fn=lambda r: 1.0, ingress="fifo", token_cap=-1.0
        )
