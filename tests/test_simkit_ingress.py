"""Unit tests for the simkit ingress registry kinds (none | fifo | priority)."""

from __future__ import annotations

import simpy
import pytest

from agent_db_sim.simkit.errors import IngressError, RegistryError
from agent_db_sim.simkit.ingress import Request, open_ingress
from agent_db_sim.simkit.processing import SlotServer


def submit(env, server, req, at=0.0):
    """Spawn a process that submits req at t=at and waits for completion."""
    received = []

    def _agent():
        if at:
            yield env.timeout(at)
        yield server.submit(req)
        yield req.done
        received.append(req.done.value)

    env.process(_agent())
    return received


def test_fifo_preserves_submit_order() -> None:
    env = simpy.Environment()
    server = SlotServer(env, "svc", slots=1, cost_fn=lambda r: 1.0, ingress="fifo")
    server.start()

    reqs = [Request() for _ in range(4)]
    for req in reqs:  # process creation order == submit order at t=0
        submit(env, server, req)

    env.run(until=20.0)

    assert [rec.rid for rec in server.records] == [r.rid for r in reqs]


def test_priority_serves_lowest_key_first() -> None:
    env = simpy.Environment()
    server = SlotServer(
        env, "svc", slots=1, cost_fn=lambda r: 1.0, ingress="priority:priority"
    )
    server.start()

    prio_feed = [3.0, 1.0, 2.0]
    reqs = [Request(priority=p) for p in prio_feed]
    for req in reqs:
        submit(env, server, req)

    env.run(until=20.0)

    served = [rec.rid for rec in server.records]
    assert served == [reqs[1].rid, reqs[2].rid, reqs[0].rid]


def test_priority_ties_break_fifo() -> None:
    env = simpy.Environment()
    server = SlotServer(
        env, "svc", slots=1, cost_fn=lambda r: 1.0, ingress="priority:priority"
    )
    server.start()

    reqs = [Request(priority=5.0) for _ in range(3)]
    for req in reqs:
        submit(env, server, req)

    env.run(until=20.0)
    assert [rec.rid for rec in server.records] == [r.rid for r in reqs]


def test_none_records_queue_delay_uniformly() -> None:
    env = simpy.Environment()
    server = SlotServer(env, "svc", slots=1, cost_fn=lambda r: 1.0, ingress="none")
    server.start()  # dispatcher first: slot busy before r2 arrives

    submit(env, server, Request())
    submit(env, server, Request(), at=0.5)

    env.run(until=20.0)

    rec1, rec2 = server.records
    assert rec1.queue_time == 0.0 and rec1.service_time == 1.0
    assert rec2.queue_time == pytest.approx(0.5)  # submit 0.5 -> slot at 1.0
    assert rec2.service_time == pytest.approx(1.0)


def test_ingress_registry_open_and_errors() -> None:
    env = simpy.Environment()
    assert isinstance(open_ingress(env, "none", "x"), open_ingress(env, "none", "x").__class__)
    with pytest.raises(RegistryError, match="unknown ingress kind"):
        open_ingress(env, "bogus", "x")
    with pytest.raises(RegistryError, match="requires a dotted"):
        open_ingress(env, "priority", "x")
    with pytest.raises(RegistryError, match="takes no key"):
        open_ingress(env, "fifo:oops", "x")
    with pytest.raises(RegistryError, match="must be a string"):
        open_ingress(env, 42, "x")  # type: ignore[arg-type]


def test_priority_key_path_missing_fails_fast() -> None:
    env = simpy.Environment()
    server = SlotServer(
        env, "svc", slots=1, cost_fn=lambda r: 0.1, ingress="priority:meta.class"
    )
    server.start()
    submit(env, server, Request())

    with pytest.raises(IngressError, match="not resolvable"):
        env.run(until=5.0)
