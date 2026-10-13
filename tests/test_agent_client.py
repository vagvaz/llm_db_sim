"""Tests for the AgentClient component (ticket 08): closed-loop behavior,
lifecycle trace events, N-agent fan-out, QueryProfile mix drawing and
seeded/deterministic token generation.

Harness: a minimal ``FakeServer`` stub implementing the serving-side contract
(uniform ``submit(req) -> event`` admission; completed requests delivered back
through ``AgentClient.deliver(port, req)``). The real endpoints are built on
SlotServer (tickets 09/10); the fake mirrors that shape with a constant delay.
"""

from __future__ import annotations

from collections import Counter
from random import Random
from typing import Any, Dict, List, Tuple

import pytest
import simpy

from agent_db_sim.components import AgentClient
from agent_db_sim.components.agent_client import build_mix, parse_token_generator
from agent_db_sim.simkit.component import Composite
from agent_db_sim.simkit.errors import ComponentError, WiringError
from agent_db_sim.simkit.ingress import Request
from agent_db_sim.simkit.processing import SlotServer
from agent_db_sim.simkit.probes import InMemorySink

AGENT_ID = "top.agents.agent-0"

AGENT_CFG = {
    "count": 1,
    "input_tokens": 10,  # bare-number shorthand for {distribution: fixed, value: 10}
    "output_tokens": {"distribution": "fixed", "value": 5},
    "query_profiles": [
        {"name": "only", "complexity": 2.0, "volume": 1.0, "weight": 1.0},
    ],
}


class FakeServer(Composite):
    """Serving-side contract stub: uniform submit + response delivery back port."""

    input_ports = {"requests": "req"}
    output_ports = {"responses": "response"}

    def __init__(self, name: str, host: AgentClient, port: str, delay: float,
                 input_name: str = "requests") -> None:
        super().__init__(name)
        self._host = host
        self._port = port
        self._delay = float(delay)
        self.input_ports = {input_name: "req"}  # instance-level port declaration
        self.server: Any = None
        self.submitted: List[Request] = []
        self.delivered: List[Request] = []

    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)
        self.server = SlotServer(
            self.env, self.name, slots=8,
            cost_fn=lambda _req: self._delay, ingress="fifo",
        )

    def start(self) -> None:
        super().start()
        self.server.start()

    def submit(self, req: Request) -> Any:
        self.submitted.append(req)
        self.env.process(self._deliver_after_done(req))
        return self.server.submit(req)

    def _deliver_after_done(self, req: Request) -> Any:
        yield req.done
        yield self._host.deliver(self._port, req)
        self.delivered.append(req)


def build_top(cfg_agents: Dict[str, Any], llm_delay: float, db_delay: float,
              ) -> Tuple[AgentClient, FakeServer, FakeServer, Composite]:
    top = Composite("top")
    client = AgentClient("agents")
    top.add(client)
    llm = FakeServer("llm", host=client, port="inference_responses", delay=llm_delay)
    top.add(llm)
    db = FakeServer("db", host=client, port="db_responses", delay=db_delay, input_name="queries")
    top.add(db)

    client.bind_output("inference_requests", llm.submit)
    client.bind_output("db_queries", db.submit)
    graph = top.connect_graph
    graph.connect(client, "inference_requests", llm, "requests")
    graph.connect(llm, "responses", client, "inference_responses")
    graph.connect(client, "db_queries", db, "queries")
    graph.connect(db, "responses", client, "db_responses")
    return client, llm, db, top


def run_agents(cfg_agents: Dict[str, Any] = AGENT_CFG, *, llm_delay: float = 3.0,
               db_delay: float = 1.0, until: float, seed: int = 7
               ) -> Tuple[InMemorySink, Dict[str, Any], AgentClient,
                          FakeServer, FakeServer]:
    env = simpy.Environment()
    sink = InMemorySink(clock=lambda: env.now)
    client, llm, db, top = build_top(cfg_agents, llm_delay, db_delay)
    client.attach_sink(sink)
    cfg = {"sim": {"seed": seed}, "agents": cfg_agents}
    top.construct(env, cfg)
    top.elaborate()
    top.start()
    env.run(until=until)
    collected = top.collect()
    return sink, collected, client, llm, db


def edges(sink: InMemorySink, signal: str) -> List[Any]:
    return sink.series(signal)


# --------------------------------------------------------------------------- #
# scripted single-agent scenario: exact request sequence + lifecycle events   #
# --------------------------------------------------------------------------- #

def test_single_agent_expected_sequence_and_lifecycle() -> None:
    # loop period = llm_delay + db_delay = 4.0; inference submits at 0,4,8,...
    sink, collected, client, llm, db = run_agents(
        AGENT_CFG, llm_delay=3.0, db_delay=1.0, until=17.0
    )

    assert client.agent_ids == (AGENT_ID,)

    # the 5th inference submit lands at t=16 and is still in flight;
    # four full loops (inf+db) complete inside the horizon
    counts = collected["agents"][AGENT_ID]
    assert counts == {
        "inference_submitted": 5,
        "inference_received": 4,
        "db_submitted": 4,
        "db_received": 4,
    }

    infinity = edges(sink, f"{AGENT_ID}.inference")
    assert [(s.t, s.value) for s in infinity] == [
        (0.0, "submitted"), (3.0, "received"),
        (4.0, "submitted"), (7.0, "received"),
        (8.0, "submitted"), (11.0, "received"),
        (12.0, "submitted"), (15.0, "received"),
        (16.0, "submitted"),
    ]

    dbh = edges(sink, f"{AGENT_ID}.db")
    assert [(s.t, s.value) for s in dbh] == [
        (3.0, "submitted"), (4.0, "received"),
        (7.0, "submitted"), (8.0, "received"),
        (11.0, "submitted"), (12.0, "received"),
        (15.0, "submitted"), (16.0, "received"),
    ]

    # one edge sample per transition, names {component}.{stage}, value = state
    for sample in infinity + dbh:
        assert sample.kind == "edge"
        assert sample.signal in (f"{AGENT_ID}.inference", f"{AGENT_ID}.db")

    # the db submit of each iteration is fenced by the inference response:
    # every db 'submitted' lands exactly at the matching inference 'received'
    received_times = [s.t for s in infinity if s.value == "received"]
    db_submit_times = [s.t for s in dbh if s.value == "submitted"]
    assert db_submit_times == received_times[: len(db_submit_times)]

    assert len(llm.submitted) == 5 and len(llm.delivered) == 4
    assert len(db.submitted) == 4 and len(db.delivered) == 4


# --------------------------------------------------------------------------- #
# trace attrs: agent_id / request_id / parent_id / state                       #
# --------------------------------------------------------------------------- #

def test_lifecycle_event_attrs_propagate() -> None:
    sink, collected, client, llm, db = run_agents(AGENT_CFG, until=9.0)

    infinity = edges(sink, f"{AGENT_ID}.inference")
    for j, sample in enumerate(infinity):
        # iteration k submits inference request #(2k-1) then db request #(2k)
        n = 2 * (j // 2) + 1
        assert sample.tags["agent_id"] == AGENT_ID
        assert sample.tags["request_id"] == f"{AGENT_ID}#{n}"
        assert sample.tags["parent_id"] is None
        assert sample.tags["state"] == sample.value

    dbh = edges(sink, f"{AGENT_ID}.db")
    for j, sample in enumerate(dbh):
        n = 2 * (j // 2) + 2
        assert sample.tags["agent_id"] == AGENT_ID
        # the db request follows the inference request of the same iteration
        assert sample.tags["request_id"] == f"{AGENT_ID}#{n}"
        assert sample.tags["parent_id"] == f"{AGENT_ID}#{n - 1}"
        assert sample.tags["state"] == sample.value


def test_request_attrs_carried_through_serving_side() -> None:
    _, collected, client, llm, db = run_agents(AGENT_CFG, llm_delay=2.0, until=3.5)

    assert len(llm.submitted) == 2  # second inference submit lands at t=3.0
    first, second = llm.submitted
    assert first.params["kind"] == "inference"
    assert first.params["agent_id"] == AGENT_ID
    assert first.params["request_id"] == f"{AGENT_ID}#1"
    assert first.params["parent_id"] is None
    assert first.params["input_tokens"] == 10
    assert first.params["output_tokens"] == 5
    assert first.tokens == 15.0  # aggregate token cap sees input+output

    # the db query of iteration 1 is derived from that iteration's response
    assert len(db.submitted) == 1
    query = db.submitted[0]
    assert query.params["kind"] == "db_query"
    assert query.params["agent_id"] == AGENT_ID
    assert query.params["request_id"] == f"{AGENT_ID}#2"
    assert query.params["parent_id"] == first.params["request_id"]
    assert query.params["complexity"] == 2.0
    assert query.params["volume"] == 1.0
    assert query.params["profile"] == "only"
    assert query.tokens == 0.0


# --------------------------------------------------------------------------- #
# N agents: fan-out honored, distinct ids, per-agent seeded streams             #
# --------------------------------------------------------------------------- #

def test_agent_count_configured_and_honored() -> None:
    cfg = dict(AGENT_CFG, count=3)
    sink, collected, client, llm, db = run_agents(cfg, until=8.5)

    assert client.count == 3
    assert client.agent_ids == tuple(f"top.agents.agent-{i}" for i in range(3))

    # inference submits at t = 0, 4, 8 (8 < until), the third still in flight;
    # two full loops (inf+db) complete inside the horizon
    for i in range(3):
        aid = f"top.agents.agent-{i}"
        assert collected["agents"][aid] == {
            "inference_submitted": 3,
            "inference_received": 2,
            "db_submitted": 2,
            "db_received": 2,
        }
        assert len(edges(sink, f"{aid}.inference")) == 5
        assert len(edges(sink, f"{aid}.db")) == 4

    assert len(llm.submitted) == 9 and len(db.submitted) == 6
    signal_agent_ids = {s.tags["agent_id"] for s in sink.samples}
    assert signal_agent_ids == {f"top.agents.agent-{i}" for i in range(3)}


def test_agent_streams_independent_between_agents() -> None:
    cfg = dict(
        AGENT_CFG,
        count=4,
        output_tokens={"distribution": "gamma", "shape": 2.0, "scale": 100.0},
    )
    _, collected, client, llm, db = run_agents(cfg, llm_delay=0.1, db_delay=0.1, until=0.99)

    per_agent: Dict[str, List[int]] = {}
    for req in llm.submitted:
        per_agent.setdefault(req.params["agent_id"], []).append(req.params["output_tokens"])
    assert set(per_agent) == set(client.agent_ids)
    assert all(len(v) == 5 for v in per_agent.values())  # submits at 0,0.2,0.4,0.6,0.8
    # each agent runs its own seeded child stream; no duplicates across agents
    assert len(set(tuple(v) for v in per_agent.values())) == 4


# --------------------------------------------------------------------------- #
# QueryProfile mix: statistical match (seeded) + determinism                   #
# --------------------------------------------------------------------------- #

def test_query_profile_draws_match_configured_mix() -> None:
    cfg = dict(
        AGENT_CFG,
        input_tokens={"distribution": "fixed", "value": 1},
        output_tokens={"distribution": "fixed", "value": 1},
        query_profiles=[
            {"name": "light", "complexity": 1.0, "volume": 0.1, "weight": 0.7},
            {"name": "heavy", "complexity": 9.0, "volume": 50.0, "weight": 0.3},
        ],
    )
    sink, collected, client, llm, db = run_agents(
        cfg, llm_delay=0.05, db_delay=0.05, until=100.0
    )

    names = Counter(r.params["profile"] for r in db.submitted)
    total = sum(names.values())
    assert total == 1000  # loop period 0.1 -> 1000 db queries
    assert abs(names["light"] / total - 0.7) < 0.05
    assert abs(names["heavy"] / total - 0.3) < 0.05

    by_name = {p["name"]: p for p in cfg["query_profiles"]}
    for req in db.submitted:
        prof = by_name[req.params["profile"]]
        assert req.params["complexity"] == prof["complexity"]
        assert req.params["volume"] == prof["volume"]


def test_draws_deterministic_per_seed_and_seed_sensitive() -> None:
    cfg = dict(
        AGENT_CFG,
        output_tokens={"distribution": "gamma", "shape": 2.0, "scale": 60.0},
        query_profiles=[
            {"name": "a", "complexity": 1.0, "volume": 1.0, "weight": 0.5},
            {"name": "b", "complexity": 2.0, "volume": 2.0, "weight": 0.5},
        ],
    )

    def capture(seed: int) -> Tuple[List[int], List[str], List[Any], Dict[str, Any]]:
        sink, collected, client, llm, db = run_agents(
            cfg, llm_delay=0.05, db_delay=0.05, until=2.0, seed=seed
        )
        tokens = [r.params["output_tokens"] for r in llm.submitted]
        profiles = [r.params["profile"] for r in db.submitted]
        fingerprint = [(s.t, s.signal, s.value) for s in sink.samples]
        return tokens, profiles, fingerprint, collected

    tokens_a1, profiles_a1, fp_a1, coll_a1 = capture(seed=42)
    tokens_a2, profiles_a2, fp_a2, coll_a2 = capture(seed=42)
    tokens_b, _, fp_b, _ = capture(seed=43)

    # same seed -> bit-identical run
    assert (tokens_a1, profiles_a1, fp_a1, coll_a1) == (tokens_a2, profiles_a2, fp_a2, coll_a2)
    # different seed -> the seeded draws diverge
    assert tokens_a1 != tokens_b


# --------------------------------------------------------------------------- #
# fail-fast wiring / delivery / config validation                              #
# --------------------------------------------------------------------------- #

def test_missing_output_binding_fails_at_elaborate() -> None:
    top = Composite("top")
    client = AgentClient("agents")
    top.add(client)
    llm = FakeServer("llm", host=client, port="inference_responses", delay=1.0)
    top.add(llm)
    client.bind_output("inference_requests", llm.submit)
    top.connect_graph.connect(client, "inference_requests", llm, "requests")
    top.construct(simpy.Environment(), {"sim": {"seed": 1}, "agents": AGENT_CFG})
    with pytest.raises(WiringError, match="db_queries"):
        top.elaborate()


def test_declared_ports_can_be_wired_and_kinds_checked() -> None:
    assert AgentClient.output_ports == {
        "inference_requests": "req",
        "db_queries": "req",
    }
    assert AgentClient.input_ports == {
        "inference_responses": "response",
        "db_responses": "response",
    }

    env = simpy.Environment()
    sink = InMemorySink(clock=lambda: env.now)
    client, llm, db, top = build_top(AGENT_CFG, llm_delay=1.0, db_delay=1.0)
    client.attach_sink(sink)
    top.construct(env, {"sim": {"seed": 1}, "agents": AGENT_CFG})
    top.elaborate()  # the four links + two output bindings validate cleanly


def test_unknown_output_bind_target() -> None:
    client = AgentClient("agents")
    with pytest.raises(WiringError, match="no output port"):
        client.bind_output("nope", lambda req: None)


def test_deliver_fails_fast_on_bad_requests() -> None:
    env = simpy.Environment()
    sink = InMemorySink()
    client, llm, db, top = build_top(AGENT_CFG, llm_delay=1.0, db_delay=1.0)
    client.attach_sink(sink)
    top.construct(env, {"sim": {"seed": 1}, "agents": AGENT_CFG})
    top.elaborate()

    with pytest.raises(WiringError, match="no input port"):
        client.deliver("nope", Request())

    with pytest.raises(ComponentError, match="agent_id"):
        client.deliver("inference_responses", Request())

    with pytest.raises(ComponentError, match="unknown agent_id"):
        client.deliver("inference_responses", Request(params={"agent_id": "nobody"}))


def test_config_fail_fast() -> None:
    env = simpy.Environment()

    def construct(cfg_agents: Dict[str, Any]) -> None:
        top = Composite("top")
        client = AgentClient("agents")
        top.add(client)
        llm = FakeServer("llm", host=client, port="inference_responses", delay=1.0)
        top.add(llm)
        client.bind_output("inference_requests", llm.submit)
        client.bind_output("db_queries", llm.submit)
        top.construct(env, {"sim": {"seed": 1}, "agents": cfg_agents})

    with pytest.raises(ComponentError, match="missing config section"):
        top = Composite("top")
        client = AgentClient("agents")
        top.add(client)
        top.construct(env, {"sim": {"seed": 1}})

    bad = {k: v for k, v in AGENT_CFG.items() if k != "output_tokens"}
    with pytest.raises(ComponentError, match="missing key"):
        construct(bad)

    with pytest.raises(ComponentError, match="count"):
        construct(dict(AGENT_CFG, count=-2))

    with pytest.raises(ComponentError, match="unknown token distribution"):
        construct(dict(AGENT_CFG, output_tokens={"distribution": "wat", "value": 3}))

    zero_weights = [
        {"name": "x", "complexity": 1.0, "volume": 1.0, "weight": 0.0},
        {"name": "y", "complexity": 1.0, "volume": 1.0, "weight": 0.0},
    ]
    with pytest.raises(ComponentError, match="weight"):
        construct(dict(AGENT_CFG, query_profiles=zero_weights))

    dup = [
        {"name": "x", "complexity": 1.0, "volume": 1.0, "weight": 0.5},
        {"name": "x", "complexity": 2.0, "volume": 2.0, "weight": 0.5},
    ]
    with pytest.raises(ComponentError, match="duplicate profile name"):
        construct(dict(AGENT_CFG, query_profiles=dup))


# --------------------------------------------------------------------------- #
# ProfileMix / TokenGenerator unit tests (no sim needed)                       #
# --------------------------------------------------------------------------- #

def test_profile_mix_inverse_cdf_boundaries_and_weights() -> None:
    mix = build_mix([
        {"name": "a", "complexity": 1, "volume": 1, "weight": 0.25},
        {"name": "b", "complexity": 2, "volume": 2, "weight": 0.75},
    ])
    assert mix.names() == ("a", "b")

    class Fixed:
        def __init__(self, value: float) -> None:
            self._value = value

        def random(self) -> float:
            return self._value

    assert mix.draw(Fixed(0.0)).name == "a"
    assert mix.draw(Fixed(0.24999)).name == "a"
    assert mix.draw(Fixed(0.25)).name == "b"
    assert mix.draw(Fixed(0.99)).name == "b"

    # weights are normalized, not required to sum to 1 in the YAML
    scaled = build_mix([
        {"name": "a", "complexity": 1, "volume": 1, "weight": 1},
        {"name": "b", "complexity": 2, "volume": 2, "weight": 3},
    ])
    assert scaled.draw(Fixed(0.2)).name == "a"
    assert scaled.draw(Fixed(0.3)).name == "b"


def test_parse_token_generator_registry_and_draws() -> None:
    gen = parse_token_generator(
        {"distribution": "gamma", "shape": 2.0, "scale": 120.0}, what="out"
    )
    draws = [gen.sample(Random(0)) for _ in range(4)]
    assert all(t >= 1 for t in draws)
    assert draws == [gen.sample(Random(0)) for _ in range(4)]  # stable per seed

    # bare-number shorthand = fixed distribution
    assert parse_token_generator(256, what="t").sample(Random(0)) == 256
    assert parse_token_generator(0.4, what="t").sample(Random(0)) == 1  # clamped >= 1

    with pytest.raises(ComponentError, match="unknown token distribution"):
        parse_token_generator({"distribution": "wat"}, what="x")
    with pytest.raises(ComponentError, match="missing parameter"):
        parse_token_generator({"distribution": "gamma", "shape": 2.0}, what="x")
    with pytest.raises(ComponentError, match="0 < p <= 1"):
        parse_token_generator({"distribution": "geometric", "p": 0.0}, what="x").sample(Random(0))
