"""Tests for the LLMEndpoint component (ticket 09).

Covers: typed ports + wiring validation; phase rates (input vs output);
slot saturation (queue_time grows, service_time unchanged); aggregate token
cap (solo N/rate, shared fair-share stretch); lifecycle edge samples with
contract attrs; throughput/occupancy probes; seeded determinism.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
import simpy

from agent_db_sim.components.llm_endpoint import CFG_SECTION, LLMEndpoint, LLMResponse
from agent_db_sim.simkit.component import Component, Composite
from agent_db_sim.simkit.errors import ComponentError, WiringError
from agent_db_sim.simkit.ingress import Request
from agent_db_sim.simkit.probes import InMemorySink


@dataclass(eq=False)
class SReq:
    """Duck of the contract-shaped AgentClient request (ticket 08)."""

    input_tokens: float
    output_tokens: float
    agent_id: str = "a1"
    request_id: str = "r1"


def make_endpoint(
    *,
    slots: int = 4,
    in_rate: float = 1000.0,
    out_rate: float = 500.0,
    name: str = "llm",
) -> tuple[simpy.Environment, LLMEndpoint, InMemorySink]:
    env = simpy.Environment()
    sink = InMemorySink(clock=lambda: env.now)
    ep = LLMEndpoint(name, sink=sink)
    ep.construct(env, {"sim": {"seed": 5}, CFG_SECTION: {
        "slots": slots,
        "input_tokens_per_s": in_rate,
        "output_tokens_per_s": out_rate,
    }})
    ep.elaborate()
    ep.start()
    return env, ep, sink


def client(env: simpy.Environment, ep: LLMEndpoint, req: SReq, at: float = 0.0) -> None:
    def _go() -> None:
        if at:
            yield env.timeout(at)
        yield ep.submit(req)
        yield ep.wait(req)

    env.process(_go())


# --------------------------------------------------------------------- #
# ports and wiring                                                      #
# --------------------------------------------------------------------- #
def test_typed_ports_declared() -> None:
    assert LLMEndpoint.input_ports == {"requests": "req"}
    assert LLMEndpoint.output_ports == {"responses": "response"}


def test_wiring_validates_at_elaboration() -> None:
    class Producer(Component):
        output_ports = {"out": "req"}

    class Consumer(Component):
        input_ports = {"in": "response"}

    cfg = {"sim": {"seed": 1}, CFG_SECTION: {
        "slots": 1, "input_tokens_per_s": 100.0, "output_tokens_per_s": 100.0,
    }}

    # valid wiring elaborates fine
    top = Composite("top")
    ep = top.add(LLMEndpoint("llm"))
    p = top.add(Producer("p"))
    c = top.add(Consumer("c"))
    top.connect_graph.connect(p, "out", ep, "requests")
    top.connect_graph.connect(ep, "responses", c, "in")
    env = simpy.Environment()
    top.construct(env, cfg)
    top.elaborate()
    assert ep.server is not None

    # kind mismatch fails fast
    class XProducer(Component):
        output_ports = {"out": "x"}

    top2 = Composite("top")
    ep2 = top2.add(LLMEndpoint("llm"))
    bad = top2.add(XProducer("bad"))
    top2.connect_graph.connect(bad, "out", ep2, "requests")  # kind "x" != "req"
    env2 = simpy.Environment()
    top2.construct(env2, cfg)
    with pytest.raises(WiringError):
        top2.elaborate()


# --------------------------------------------------------------------- #
# phase rates                                                           #
# --------------------------------------------------------------------- #
def test_phase_rates_respected_solo() -> None:
    env, ep, _ = make_endpoint(slots=2, in_rate=1000.0, out_rate=500.0)
    client(env, ep, SReq(input_tokens=800, output_tokens=400, request_id="r1"))
    env.run(until=20.0)

    (rec,) = ep.records
    # input first at 1000 tok/s, then output at 500 tok/s: 0.8 + 0.8
    assert rec.service_time == pytest.approx(800 / 1000 + 400 / 500, abs=1e-6)
    assert rec.queue_time == 0.0


def test_input_only_and_output_only_solo_rates() -> None:
    env, ep, _ = make_endpoint(slots=2, in_rate=1000.0, out_rate=250.0)
    client(env, ep, SReq(input_tokens=1000, output_tokens=0, request_id="r-in"), at=0.0)
    # run the second request alone (staggered) so it gets the full cap
    client(env, ep, SReq(input_tokens=0, output_tokens=750, request_id="r-out", agent_id="a2"), at=5.0)
    env.run(until=20.0)

    recs = ep.records
    times = sorted(rec.service_time for rec in recs)
    # solo request of N tokens takes N/rate: 1000/1000 = 1.0, 750/250 = 3.0
    assert times[0] == pytest.approx(1.0, abs=1e-6)
    assert times[1] == pytest.approx(3.0, abs=1e-6)


# --------------------------------------------------------------------- #
# saturation: queue vs service separation                               #
# --------------------------------------------------------------------- #
def test_saturation_queues_but_service_time_unchanged() -> None:
    env, ep, _ = make_endpoint(slots=1, in_rate=1000.0, out_rate=2000.0)
    for i in range(3):
        client(env, ep, SReq(input_tokens=1000, output_tokens=1000, request_id=f"r{i}"))
    env.run(until=30.0)

    recs = sorted(ep.records, key=lambda r: r.start)
    # solo floor = 1000/1000 + 1000/2000 = 1.5; slots=1 -> run back to back
    for rec in recs:
        assert rec.service_time == pytest.approx(1.5, abs=1e-6)
    queue_times = sorted(rec.queue_time for rec in recs)
    assert queue_times == [pytest.approx(0.0), pytest.approx(1.5), pytest.approx(3.0)]
    assert queue_times[1] > 0 and queue_times[2] > 0


# --------------------------------------------------------------------- #
# aggregate token cap                                                   #
# --------------------------------------------------------------------- #
def test_aggregate_cap_solo_request_n_over_rate() -> None:
    # solo input-only request of N tokens takes N / input rate (the phase
    # floor; the aggregate cap cannot shorten it)
    env, ep, _ = make_endpoint(slots=2, in_rate=1000.0, out_rate=1.0)
    client(env, ep, SReq(input_tokens=1000, output_tokens=0, request_id="r1"))
    env.run(until=20.0)

    (rec,) = ep.records
    assert rec.service_time == pytest.approx(1000 / 1000, abs=1e-6)
    assert rec.queue_time == 0.0


def test_aggregate_cap_shared_pair_stretches_to_fair_share() -> None:
    env, ep, _ = make_endpoint(slots=4, in_rate=1000.0, out_rate=1.0)
    reqs = [SReq(input_tokens=1000, output_tokens=0, request_id=f"r{i}") for i in range(2)]
    for req in reqs:
        client(env, ep, req)
    env.run(until=20.0)

    cap = 1000.0 + 1.0  # aggregate tokens/s = input rate + output rate
    for rec in ep.records:
        # each earns cap/2 while in flight: ~2N/cap (well past the 1.0 floor)
        assert rec.service_time == pytest.approx(2 * 1000 / cap, abs=0.02)
        assert rec.service_time > 1.5
        assert rec.queue_time == 0.0
    ends = sorted(rec.end for rec in ep.records)
    assert ends[1] - ends[0] <= 0.05  # fair share: both finish ~together


def test_aggregate_cap_four_concurrent_requests() -> None:
    env, ep, _ = make_endpoint(slots=4, in_rate=1000.0, out_rate=1.0)
    reqs = [SReq(input_tokens=1000, output_tokens=0, request_id=f"r{i}") for i in range(4)]
    for req in reqs:
        client(env, ep, req)
    env.run(until=30.0)

    cap = 1000.0 + 1.0
    for rec in ep.records:
        assert rec.service_time == pytest.approx(4 * 1000 / cap, abs=0.05)
    assert max(rec.service_time for rec in ep.records) > 3.0


# --------------------------------------------------------------------- #
# lifecycle trace events (probe edge samples)                           #
# --------------------------------------------------------------------- #
def test_lifecycle_edges_present_with_contract_attrs() -> None:
    env, ep, sink = make_endpoint(slots=1, in_rate=1000.0, out_rate=2000.0)
    for i in range(3):
        client(env, ep, SReq(input_tokens=1000, output_tokens=1000, request_id=f"r{i}"))
    env.run(until=30.0)

    edges = [s for s in sink.samples if s.kind == "edge"]
    states = set()
    for edge in edges:
        state = edge.signal.rsplit(".", 1)[-1]
        assert state in ("submitted", "queued", "processing", "completed")
        assert edge.value == state
        assert edge.tags["state"] == state
        assert edge.tags["request_id"] in {"r0", "r1", "r2"}
        assert edge.tags["agent_id"] == "a1"
        states.add(state)
    assert states == {"submitted", "queued", "processing", "completed"}

    # per-request state order: submitted <= queued <= processing <= completed
    by_req: dict[str, dict[str, float]] = {}
    for edge in edges:
        rid = edge.tags["request_id"]
        state = edge.tags["state"]
        prev = by_req.setdefault(rid, {})
        if state in prev:
            continue
        prev[state] = edge.t
    for rid, times in by_req.items():
        order = [s for s in ("submitted", "queued", "processing", "completed") if s in times]
        assert order == ["submitted", "queued", "processing", "completed"]
        ts = [times[s] for s in order]
        assert ts == sorted(ts)
    # second/third requests really queued longer than zero
    late = [rec for rec in sorted(ep.records, key=lambda r: r.start)][1:]
    assert all(rec.queue_time > 0 for rec in late)


# --------------------------------------------------------------------- #
# throughput / occupancy probes                                         #
# --------------------------------------------------------------------- #
def test_throughput_counters_and_occupancy_levels() -> None:
    env, ep, sink = make_endpoint(slots=3, in_rate=1000.0, out_rate=500.0)
    reqs = [
        SReq(input_tokens=100, output_tokens=200, request_id=f"r{i}", agent_id=f"a{i}")
        for i in range(3)
    ]
    for req in reqs:
        client(env, ep, req)
    env.run(until=30.0)

    assert pytest.approx(sink.counters[f"llm.tokens_input"]) == 300.0
    assert pytest.approx(sink.counters[f"llm.tokens_output"]) == 600.0

    busy = sink.values("slot_busy")
    assert max(busy) == 3  # occupancy hit full capacity
    assert max(sink.values("queue_depth")) >= 0
    # counter samples carry identity
    token_samples = sink.series("llm.tokens_input")
    assert any(s.tags.get("agent_id") == "a2" for s in token_samples)


# --------------------------------------------------------------------- #
# records / responses / collect                                         #
# --------------------------------------------------------------------- #
def test_response_of_and_collect() -> None:
    env, ep, sink = make_endpoint(slots=2, in_rate=500.0, out_rate=500.0)
    req = SReq(input_tokens=500, output_tokens=500, request_id="r9", agent_id="a9")
    client(env, ep, req)
    env.run(until=20.0)

    resp = ep.response_of(req)
    assert isinstance(resp, LLMResponse)
    assert resp.request_id == "r9"
    assert resp.agent_id == "a9"
    assert resp.input_tokens == 500
    assert resp.output_tokens == 500
    assert resp.service_time == pytest.approx(2.0, abs=1e-6)
    assert resp.queue_time == 0.0

    collected = ep.collect()
    assert collected["served"] == 1
    assert collected["input_tokens"] == pytest.approx(500.0)
    assert collected["output_tokens"] == pytest.approx(500.0)
    assert collected["slots"] == 2
    assert collected["requests"][0]["request_id"] == "r9"
    assert collected["requests"][0]["queue_time"] == pytest.approx(resp.queue_time)
    assert collected["requests"][0]["service_time"] == pytest.approx(resp.service_time)


# --------------------------------------------------------------------- #
# fail-fast validation                                                  #
# --------------------------------------------------------------------- #
def test_bad_config_fails_fast() -> None:
    env = simpy.Environment()
    ep = LLMEndpoint("llm")
    ep.construct(env, {"sim": {"seed": 1}, CFG_SECTION: {
        "slots": 0, "input_tokens_per_s": 100.0, "output_tokens_per_s": 100.0,
    }})
    with pytest.raises(ComponentError, match="slots must be >= 1"):
        ep.elaborate()

    ep2 = LLMEndpoint("llm")
    ep2.construct(env, {"sim": {"seed": 1}, CFG_SECTION: {
        "slots": 2, "input_tokens_per_s": 0.0, "output_tokens_per_s": 100.0,
    }})
    with pytest.raises(ComponentError, match="input_tokens_per_s must be > 0"):
        ep2.elaborate()

    ep3 = LLMEndpoint("llm")
    with pytest.raises(ComponentError, match="required config"):
        ep3.construct(env, {"sim": {"seed": 1}, CFG_SECTION: {"slots": 1}})


def test_bad_requests_fail_fast() -> None:
    env, ep, _ = make_endpoint(slots=1, in_rate=100.0, out_rate=100.0)

    class Bare:
        pass

    with pytest.raises(ComponentError, match="missing required attribute"):
        ep.submit(Bare())
    with pytest.raises(ComponentError, match="must be >= 0"):
        ep.submit(SReq(input_tokens=-1, output_tokens=0))
    with pytest.raises(ComponentError, match="untracked"):
        ep.wait(SReq(input_tokens=1, output_tokens=1))
    with pytest.raises(ComponentError, match="not completed"):
        ep.response_of(SReq(input_tokens=1, output_tokens=1, request_id="never"))

    ep4 = LLMEndpoint("llm")
    with pytest.raises(ComponentError, match="before elaborate"):
        ep4.submit(SReq(input_tokens=1, output_tokens=1))


def test_simkit_request_with_params_mapping_accepted() -> None:
    env, ep, _ = make_endpoint(slots=2, in_rate=1000.0, out_rate=500.0)
    # simkit-native Request carrying the contract attrs in its params mapping
    wrapped = Request(
        params={
            "input_tokens": 500, "output_tokens": 250,
            "agent_id": "a1", "request_id": "w1",
        }
    )
    client(env, ep, wrapped)
    env.run(until=20.0)

    resp = ep.response_of(wrapped)
    assert resp.request_id == "w1"
    assert resp.service_time == pytest.approx(500 / 1000 + 250 / 500, abs=1e-6)
    collected = ep.collect()
    assert collected["requests"][0]["request_id"] == "w1"


def test_cfg_section_constructor_convention() -> None:
    # build_model-style convention: the config mapping passed directly
    env = simpy.Environment()
    sink = InMemorySink(clock=lambda: env.now)
    ep = LLMEndpoint("llm", sink=sink, cfg_section={
        "slots": 2, "input_tokens_per_s": 100.0, "output_tokens_per_s": 100.0,
    })
    ep.construct(env, {"sim": {"seed": 1}})
    ep.elaborate()
    ep.start()
    assert ep.server is not None and ep.server.slots == 2


# --------------------------------------------------------------------- #
# seeded determinism                                                    #
# --------------------------------------------------------------------- #
def test_seeded_determinism() -> None:
    def run() -> tuple[list[tuple], list[tuple]]:
        env, ep, sink = make_endpoint(
            slots=2, in_rate=800.0, out_rate=1200.0
        )
        plan = [
            (0.0, SReq(700, 300, "a1", "r1")),
            (0.0, SReq(700, 300, "a2", "r2")),
            (2.5, SReq(1000, 100, "a3", "r3")),
        ]
        for at, req in plan:
            client(env, ep, req, at=at)
        env.run(until=40.0)
        recs = sorted(
            (r.arrival, r.start, r.end, round(r.queue_time, 9), round(r.service_time, 9), r.tokens)
            for r in ep.records
        )
        samples = [
            (s.t, s.signal, s.kind, s.value, tuple(sorted(s.tags.items())))
            for s in sink.samples
        ]
        return recs, samples

    first_rec, first_samples = run()
    second_rec, second_samples = run()
    assert first_rec == second_rec
    assert first_samples == second_samples
