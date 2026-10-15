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
from agent_db_sim.components.agent_client import (
    DEFAULT_WORKFLOW_NAME,
    build_mix,
    parse_token_generator,
    parse_workflow,
)
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


# --------------------------------------------------------------------------- #
# agent workflow templates (mix draw + step-sequence loop)                     #
# --------------------------------------------------------------------------- #

def construct_workflow(cfg_agents: Dict[str, Any]) -> Tuple[AgentClient, Any]:
    """Build one constructed (not run) AgentClient for config-level tests."""
    top = Composite("top")
    client = AgentClient("agents")
    top.add(client)
    llm = FakeServer("llm", host=client, port="inference_responses", delay=1.0)
    top.add(llm)
    client.bind_output("inference_requests", llm.submit)
    client.bind_output("db_queries", llm.submit)
    top.construct(simpy.Environment(), {"sim": {"seed": 3}, "agents": cfg_agents})
    return client, top


def test_workflow_template_assignment_honors_mix() -> None:
    cfg = dict(
        AGENT_CFG,
        count=30,
        workflow={
            "templates": [
                {"name": "a", "steps": ["inference", "db"]},
                {"name": "b", "steps": ["inference", "db", "inference", "db"]},
                {"name": "c", "steps": ["inference", "db", "db"]},
            ],
            "mix": [0.5, 0.3, 0.2],
        },
    )
    client, _ = construct_workflow(cfg)

    # one template per agent, drawn from the configured mix (seeded counts)
    assert client.workflow_templates == ("a", "b", "c")
    templates = Counter(client.agent_templates)
    assert sum(templates.values()) == 30
    counts_a = templates

    # assignment is reproducible per seed and tracks the mix proportions loosely
    counts_b = Counter(construct_workflow(cfg)[0].agent_templates)
    assert counts_a == counts_b  # same seed -> same draws
    for name, weight in (("a", 0.5), ("b", 0.3), ("c", 0.2)):
        got = counts_a[name] / 30
        assert abs(got - weight) <= 0.15, (name, got, weight)


def test_workflow_assignment_uniform_when_mix_omitted() -> None:
    cfg = dict(
        AGENT_CFG,
        count=4,
        workflow={
            "templates": [
                {"name": "x", "steps": ["inference", "db"]},
                {"name": "y", "steps": ["inference", "db", "db"]},
            ],
        },
    )
    client, _ = construct_workflow(cfg)
    names = Counter(client.agent_templates)
    assert set(names) == {"x", "y"} and sum(names.values()) == 4


def test_legacy_default_workflow_when_key_absent() -> None:
    client, _ = construct_workflow(AGENT_CFG)
    assert client.workflow_templates == ("inference_db",)
    assert client.agent_templates == ("inference_db",) * client.count


def test_workflow_loop_follows_sequence_order_and_step_indices() -> None:
    # template [inference, db, db], llm_delay=2, db_delay=1:
    # pass 1: inf#1 sub@0 rcv@2 -> db#2 sub@2 rcv@3 -> db#3 sub@3 rcv@4
    # pass 2: inf#4 sub@4 (in flight at the 5.5 horizon)
    cfg = dict(
        AGENT_CFG,
        workflow={
            "templates": [{"name": "double_db", "steps": ["inference", "db", "db"]}],
            "mix": [1.0],
        },
    )
    sink, collected, client, llm, db = run_agents(
        cfg, llm_delay=2.0, db_delay=1.0, until=5.5
    )
    assert client.agent_templates == ("double_db",)

    counts = collected["agents"][AGENT_ID]
    assert counts == {
        "inference_submitted": 2,
        "inference_received": 1,
        "db_submitted": 2,
        "db_received": 2,
    }

    infinity = edges(sink, f"{AGENT_ID}.inference")
    assert [(s.t, s.value, s.tags["workflow_step"], s.tags["request_id"]) for s in infinity] == [
        (0.0, "submitted", 0, f"{AGENT_ID}#1"),
        (2.0, "received", 0, f"{AGENT_ID}#1"),
        (4.0, "submitted", 0, f"{AGENT_ID}#4"),
    ]

    dbh = edges(sink, f"{AGENT_ID}.db")
    assert [(s.t, s.value, s.tags["workflow_step"], s.tags["request_id"]) for s in dbh] == [
        (2.0, "submitted", 1, f"{AGENT_ID}#2"),
        (3.0, "received", 1, f"{AGENT_ID}#2"),
        (3.0, "submitted", 2, f"{AGENT_ID}#3"),
        (4.0, "received", 2, f"{AGENT_ID}#3"),
    ]

    # both db steps of the pass link to the pass's inference request
    assert all(s.tags["workflow_template"] == "double_db" for s in infinity + dbh)
    db_params = [req.params for req in db.submitted]
    assert db_params[0]["parent_id"] == f"{AGENT_ID}#1"
    assert db_params[1]["parent_id"] == f"{AGENT_ID}#1"
    assert db_params[0]["workflow_template"] == "double_db"
    assert db_params[0]["workflow_step"] == 1
    assert db_params[1]["workflow_step"] == 2
    inf_params = [req.params for req in llm.submitted]
    assert all(p["parent_id"] is None for p in inf_params)
    assert inf_params[0]["workflow_template"] == "double_db"


def test_backward_compat_default_equals_old_alternating_behavior() -> None:
    """No ``workflow:`` key must reproduce the v1 inference->db loop exactly."""
    def fingerprint(cfg_agents: Dict[str, Any]) -> Tuple[
        List[Tuple[float, str, str]], Dict[str, Any], List[int], List[int]
    ]:
        sink, collected, client, llm, db = run_agents(
            cfg_agents, llm_delay=3.0, db_delay=1.0, until=17.0
        )
        times = [(s.t, s.signal, s.value) for s in sink.samples]
        tokens = [r.params["output_tokens"] for r in llm.submitted]
        profiles = [r.params["profile"] for r in db.submitted]
        return times, collected, tokens, profiles

    legacy = fingerprint(AGENT_CFG)
    explicit = fingerprint(
        dict(
            AGENT_CFG,
            workflow={"templates": [{"name": "x", "steps": ["inference", "db"]}]},
        )
    )
    assert legacy[0] == explicit[0]  # identical edge-sample stream
    assert legacy[1] == explicit[1]  # identical counters
    assert legacy[2] == explicit[2] and legacy[3] == explicit[3]  # identical draws

    # and the implicit template still tags samples with alternating steps
    times, collected, _, _ = legacy
    inference = [s for s in times if s[1] == f"{AGENT_ID}.inference"]
    assert inference == [
        (0.0, f"{AGENT_ID}.inference", "submitted"),
        (3.0, f"{AGENT_ID}.inference", "received"),
        (4.0, f"{AGENT_ID}.inference", "submitted"),
        (7.0, f"{AGENT_ID}.inference", "received"),
        (8.0, f"{AGENT_ID}.inference", "submitted"),
        (11.0, f"{AGENT_ID}.inference", "received"),
        (12.0, f"{AGENT_ID}.inference", "submitted"),
        (15.0, f"{AGENT_ID}.inference", "received"),
        (16.0, f"{AGENT_ID}.inference", "submitted"),
    ]


def test_workflow_attrs_in_emitted_samples_and_requests() -> None:
    cfg = dict(
        AGENT_CFG,
        count=4,
        workflow={
            "templates": [
                {"name": "simple_qa", "steps": ["inference", "db"]},
                {"name": "double_db", "steps": ["inference", "db", "db"]},
            ],
        },
    )
    sink, _, client, llm, db = run_agents(cfg, llm_delay=0.05, db_delay=0.05, until=1.0)

    template_by_agent = dict(zip(client.agent_ids, client.agent_templates))
    seen_templates = set()
    for sample in sink.samples:
        if sample.kind != "edge":
            continue
        template = sample.tags["workflow_template"]
        step = sample.tags["workflow_step"]
        assert isinstance(step, int) and 0 <= step <= 2
        assert template == template_by_agent[sample.tags["agent_id"]]
        seen_templates.add(sample.tags["workflow_template"])
    assert seen_templates == {"simple_qa", "double_db"}

    # every inference and db request carries the workflow attrs downstream
    for req in llm.submitted:
        agent_template = template_by_agent[req.params["agent_id"]]
        assert req.params["workflow_template"] == agent_template
        assert req.params["workflow_step"] == 0
    for req in db.submitted:
        agent_template = template_by_agent[req.params["agent_id"]]
        assert req.params["workflow_template"] == agent_template
        # db steps are at index 1 (simple_qa) or 1/2 (double_db)
        assert req.params["workflow_step"] in (1, 2)


def test_workflow_config_fail_fast() -> None:
    with pytest.raises(ComponentError, match="workflow"):
        construct_workflow(dict(AGENT_CFG, workflow="nope"))

    with pytest.raises(ComponentError, match="templates"):
        construct_workflow(dict(AGENT_CFG, workflow={}))

    with pytest.raises(ComponentError, match="unknown|must be one of"):
        construct_workflow(
            dict(
                AGENT_CFG,
                workflow={
                    "templates": [{"name": "x", "steps": ["inference", "llm"]}],
                },
            )
        )

    with pytest.raises(ComponentError, match="non-empty list of steps"):
        construct_workflow(
            dict(AGENT_CFG, workflow={"templates": [{"name": "x", "steps": []}]})
        )

    with pytest.raises(ComponentError, match="duplicate template name"):
        construct_workflow(
            dict(
                AGENT_CFG,
                workflow={
                    "templates": [
                        {"name": "x", "steps": ["inference", "db"]},
                        {"name": "x", "steps": ["db"]},
                    ],
                },
            )
        )

    with pytest.raises(ComponentError, match="list of 2 weights"):
        construct_workflow(
            dict(
                AGENT_CFG,
                workflow={
                    "templates": [
                        {"name": "x", "steps": ["inference", "db"]},
                        {"name": "y", "steps": ["db"]},
                    ],
                    "mix": [1.0, 1.0, 1.0],
                },
            )
        )

    with pytest.raises(ComponentError, match="mix\\[0\\]"):
        construct_workflow(
            dict(
                AGENT_CFG,
                workflow={
                    "templates": [
                        {"name": "x", "steps": ["inference", "db"]},
                        {"name": "y", "steps": ["db"]},
                    ],
                    "mix": [-1.0, 1.0],
                },
            )
        )

    with pytest.raises(ComponentError, match="at least one mix weight"):
        construct_workflow(
            dict(
                AGENT_CFG,
                workflow={
                    "templates": [
                        {"name": "x", "steps": ["inference", "db"]},
                        {"name": "y", "steps": ["db"]},
                    ],
                    "mix": [0.0, 0.0],
                },
            )
        )


def test_parse_workflow_unit_contract() -> None:
    # None (no workflow: key) -> legacy default template, weight 1
    legacy = parse_workflow(None)
    assert legacy.names() == (DEFAULT_WORKFLOW_NAME,)
    assert legacy.draw(Random(0)).steps == ("inference", "db")
    assert DEFAULT_WORKFLOW_NAME == "inference_db"

    # omitted mix -> uniform; weights are normalized, not required to sum to 1
    mix = parse_workflow(
        {
            "templates": [
                {"name": "a", "steps": ["inference", "db"]},
                {"name": "b", "steps": ["inference", "db", "db"]},
            ],
            "mix": [1, 3],
        }
    )
    class Fixed:
        def __init__(self, value: float) -> None:
            self._value = value
        def random(self) -> float:
            return self._value

    assert mix.draw(Fixed(0.0)).name == "a"
    assert mix.draw(Fixed(0.25)).name == "b"
    assert mix.draw(Fixed(0.99)).name == "b"


# --------------------------------------------------------------------------- #
# initial_step: configurable first-request kind (drawn once at construct)      #
# --------------------------------------------------------------------------- #

def test_initial_step_absent_starts_at_template_step_zero() -> None:
    # default legacy template [inference, db]: step 0 is inference -> "llm"
    client, _ = construct_workflow(AGENT_CFG)
    assert client.agent_initial_steps == ("llm",) * client.count

    # a template that starts with a db step -> step-0 kind is "db"
    cfg = dict(
        AGENT_CFG,
        count=2,
        workflow={"templates": [{"name": "dbfirst", "steps": ["db", "inference"]}]},
    )
    client, _ = construct_workflow(cfg)
    assert client.agent_initial_steps == ("db", "db")

    # and the loop really starts at step 0: first submit matches template[0] at t=0
    sink, _, _, _, db = run_agents(cfg, llm_delay=3.0, db_delay=1.0, until=5.0)
    dbh = edges(sink, f"{AGENT_ID}.db")
    assert dbh[0].t == 0.0 and dbh[0].value == "submitted"
    assert db.submitted[0].params["workflow_step"] == 0


def test_initial_step_attr_on_every_trace_event_and_request() -> None:
    # absent config: the attr is still present, named after step-0's kind
    sink, _, client, llm, db = run_agents(AGENT_CFG, until=5.0)
    assert all(s.tags["initial_step"] == "llm" for s in sink.samples)
    assert all(r.params["initial_step"] == "llm" for r in llm.submitted)
    assert all(r.params["initial_step"] == "llm" for r in db.submitted)

    # db-first template: the attr names the kind of the step that is walked first
    cfg = dict(
        AGENT_CFG,
        workflow={"templates": [{"name": "dbfirst", "steps": ["db", "inference"]}]},
    )
    sink, _, client, llm, db = run_agents(cfg, until=5.0)
    assert all(s.tags["initial_step"] == "db" for s in sink.samples)
    assert all(r.params["initial_step"] == "db" for r in llm.submitted)


def test_initial_step_db_weight_one_forces_db_first_submit_at_t0() -> None:
    # template [inference, db]; the db draw selects index 1 as the start
    cfg = dict(AGENT_CFG, initial_step={"llm": 0.0, "db": 1.0})
    sink, collected, client, llm, db = run_agents(
        cfg, llm_delay=3.0, db_delay=1.0, until=6.0
    )
    assert client.agent_initial_steps == ("db",)

    # first submit is a DB query at t=0 (no LLM head before it)
    dbh = edges(sink, f"{AGENT_ID}.db")
    infinity = edges(sink, f"{AGENT_ID}.inference")
    assert [(s.t, s.value, s.tags["workflow_step"]) for s in dbh] == [
        (0.0, "submitted", 1), (1.0, "received", 1),
        (4.0, "submitted", 1), (5.0, "received", 1),
    ]
    # then the template continues from the next step: inference at index 0
    assert [(s.t, s.value, s.tags["workflow_step"]) for s in infinity] == [
        (1.0, "submitted", 0), (4.0, "received", 0),
        (5.0, "submitted", 0),
    ]

    first_db = db.submitted[0]
    assert first_db.params["workflow_step"] == 1
    assert first_db.params["kind"] == "db_query"
    assert first_db.params["parent_id"] is None  # no inference seen yet
    assert first_db.params["initial_step"] == "db"
    # sequence numbering shared across kinds: the db hit took request #1,
    # so the first LLM call is #2
    assert llm.submitted[0].params["request_id"] == f"{AGENT_ID}#2"
    assert llm.submitted[0].params["initial_step"] == "db"


def test_initial_step_llm_weight_one_forces_inference_first_submit_at_t0() -> None:
    # template starts with a db step, but llm weight 1 forces the FIRST
    # inference step (index 1) as the start of the loop
    cfg = dict(
        AGENT_CFG,
        initial_step={"llm": 1.0},
        workflow={"templates": [{"name": "dbfirst", "steps": ["db", "inference"]}]},
    )
    sink, collected, client, llm, db = run_agents(
        cfg, llm_delay=3.0, db_delay=1.0, until=6.0
    )
    assert client.agent_initial_steps == ("llm",)

    infinity = edges(sink, f"{AGENT_ID}.inference")
    dbh = edges(sink, f"{AGENT_ID}.db")
    assert [(s.t, s.value, s.tags["workflow_step"]) for s in infinity] == [
        (0.0, "submitted", 1), (3.0, "received", 1),
        (4.0, "submitted", 1),
    ]
    assert [(s.t, s.value, s.tags["workflow_step"]) for s in dbh] == [
        (3.0, "submitted", 0), (4.0, "received", 0),
    ]
    first_llm = llm.submitted[0]
    assert first_llm.params["kind"] == "inference"
    assert first_llm.params["workflow_step"] == 1
    assert first_llm.params["initial_step"] == "llm"
    assert first_llm.params["request_id"] == f"{AGENT_ID}#1"


def test_initial_step_mix_draw_counts_match_the_named_stream_exactly() -> None:
    # 250 agents over 0.3/0.7; the counts are exact: replicate the same
    # named RNG stream the implementation draws from
    from random import Random

    from agent_db_sim.simkit.component import master_seed

    seed = master_seed({"sim": {"seed": 3}})  # construct_workflow seeds with 3
    cfg = dict(AGENT_CFG, count=250, initial_step={"llm": 0.3, "db": 0.7})

    client_a, _ = construct_workflow(cfg)
    client_b, _ = construct_workflow(cfg)  # same seed -> same draws
    assert client_b.agent_initial_steps == client_a.agent_initial_steps

    expected = [
        "llm" if Random(f"{seed}/{aid}/initial_step").random() < 0.3 else "db"
        for aid in client_a.agent_ids
    ]
    assert client_a.agent_initial_steps == tuple(expected)

    counts = Counter(client_a.agent_initial_steps)
    assert counts["llm"] == Counter(expected)["llm"]
    assert counts["db"] == Counter(expected)["db"]
    assert counts["llm"] > 0 and counts["db"] > 0  # the mix is really mixed


def test_initial_step_config_fail_fast() -> None:
    # negative weight
    with pytest.raises(ComponentError, match="must be >= 0"):
        construct_workflow(
            dict(AGENT_CFG, initial_step={"llm": -0.2, "db": 1.2})
        )

    # weights that don't sum to 1 (too short and too much)
    with pytest.raises(ComponentError, match="sum to 1"):
        construct_workflow(dict(AGENT_CFG, initial_step={"llm": 0.6, "db": 0.3}))
    with pytest.raises(ComponentError, match="sum to 1"):
        construct_workflow(
            dict(AGENT_CFG, initial_step={"llm": 0.6, "db": 0.5})
        )
    # a single key may only be omitted when the other weight is 1.0
    with pytest.raises(ComponentError, match="sum to 1"):
        construct_workflow(dict(AGENT_CFG, initial_step={"llm": 0.6}))

    # unknown key names
    with pytest.raises(ComponentError, match="unknown key"):
        construct_workflow(
            dict(AGENT_CFG, initial_step={"llm": 1.0, "sql": 0.0})
        )
    # spec must be a mapping (or None)
    with pytest.raises(ComponentError, match="mapping"):
        construct_workflow(dict(AGENT_CFG, initial_step=[0.3, 0.7]))

    # the template must have a step of the drawn kind — no silent fallback
    with pytest.raises(ComponentError, match="no 'db' step"):
        construct_workflow(
            dict(
                AGENT_CFG,
                initial_step={"llm": 0.0, "db": 1.0},
                workflow={
                    "templates": [
                        {"name": "llm_only", "steps": ["inference", "inference"]}
                    ]
                },
            )
        )
    with pytest.raises(ComponentError, match="no 'inference' step"):
        construct_workflow(
            dict(
                AGENT_CFG,
                initial_step={"llm": 1.0},
                workflow={"templates": [{"name": "db_only", "steps": ["db", "db"]}]},
            )
        )


def test_initial_step_stream_independent_of_token_draws() -> None:
    """The ``initial_step`` draw lives on its own named RNG child stream:
    adding an ``initial_step:`` config (one extra draw there) must not shift
    the per-agent token sequence of an agent that still starts at step 0."""

    def first_agent_tokens(extra: Dict[str, Any] | None) -> List[int]:
        cfg = dict(
            AGENT_CFG,
            count=2,
            output_tokens={"distribution": "gamma", "shape": 2.0, "scale": 80.0},
        )
        if extra is not None:
            cfg["initial_step"] = extra
        _, _, _, llm, _ = run_agents(cfg, llm_delay=0.1, db_delay=0.1, until=0.99)
        return [
            r.params["output_tokens"]
            for r in llm.submitted
            if r.params["agent_id"] == "top.agents.agent-0"
        ]

    baseline = first_agent_tokens(None)
    forced_llm = first_agent_tokens({"llm": 1.0, "db": 0.0})

    # identical agent configs apart from initial_step -> identical token stream
    assert baseline == forced_llm
    assert len(baseline) == 5  # inference submits at 0,0.2,0.4,0.6,0.8
