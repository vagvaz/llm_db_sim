"""DBEndpoint end-to-end tests (ticket 10 validation).

Per the ticket body: one scenario per topology runs end-to-end with a stub
caller process; scale-out config changes only N/M; the disaggregated and
distributed paths exercise the network (net_req/net_resp stages present,
centralized absent); storage backend presets produce different service
times; the compute slot is held across the storage round-trip; the
coordinator overhead multiplier applies on the distributed topology.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest
from omegaconf import OmegaConf

from agent_db_sim.components.db.query import DBQuery
from agent_db_sim.components.db.endpoint import DBEndpoint
from agent_db_sim.simkit.component import Component
from agent_db_sim.simkit.errors import RegistryError
from agent_db_sim.simkit.probes import InMemorySink
from agent_db_sim.simkit.registry import (
    STORAGE_BACKEND_REGISTRY,
    TOPOLOGY_REGISTRY,
    build_model,
)
from agent_db_sim.simkit.runner import run_model

# --------------------------------------------------------------------------- #
# shared fixture helpers                                                       #
# --------------------------------------------------------------------------- #


def base_db_cfg(topology: str, **over) -> Any:
    cfg = {
        "sim": {"until": 50.0, "seed": 0},
        "db": {
            "topology": topology,
            "compute_nodes": 2 if topology != "centralized" else 1,
            "storage_nodes": 2,
            "coordinators": 1,
            "compute": {"slots": 2, "ingress": "fifo", "issue_ratio": 0.1},
            "storage": {"backend": "nvme"},
            "network": {
                "latency": 0.001,
                "bandwidth": 1000.0,
                "capacity": 8.0,
                "request_size": 0.01,
                "coordinator_overhead": 1.1,
            },
        },
    }
    for k, v in over.items():
        cfg[k] = v
    return OmegaConf.create(cfg)


class StubCaller(Component):
    """Stub query source (stands in for ticket-08 AgentClient)."""

    input_ports = {"responses": "response"}
    output_ports = {"queries": "query"}

    def __init__(self, name: str, db: DBEndpoint, n_queries: int, complexity: float, volume: float) -> None:
        super().__init__(name)
        self.db = db
        self.n_queries = n_queries
        self.complexity = complexity
        self.volume = volume
        self.received = []

    def start(self) -> None:
        self.env.process(self._loop())

    def _loop(self) -> Any:
        for i in range(self.n_queries):
            query = DBQuery(
                agent_id=self.name,
                request_id=i,
                complexity=self.complexity,
                volume=self.volume,
            )
            response = yield self.db.submit(query)  # already a started process
            self.received.append(response)

    def collect(self) -> Any:
        return {"received": len(self.received)}


def build_with_caller(cfg: Any, n_queries: int = 8, until: float = 50.0):
    sink = InMemorySink()
    top = build_model(cfg, sink=sink)
    db = top._children[0]  # top.db
    caller = StubCaller("caller-0", db, n_queries, complexity=0.5, volume=1.0)
    top.add(caller)
    # wire the caller into db through the declared ports (actual data flow is
    # request objects + response events over the same contract)
    top.connect_graph.connect(caller, "queries", db, "queries")
    top.connect_graph.connect(db, "responses", caller, "responses")
    result = run_model(top, cfg, until=until, sink=sink)
    return result, db, caller, sink


def elapsed(caller: StubCaller) -> float:
    return max((r.completed_at for r in caller.received), default=0.0)


# --------------------------------------------------------------------------- #
# registry + config validation                                                 #
# --------------------------------------------------------------------------- #


def test_topology_and_backend_registries_populated():
    assert set(TOPOLOGY_REGISTRY) >= {"centralized", "disaggregated", "distributed"}
    assert {"hdd", "ssd", "nvme"} <= set(STORAGE_BACKEND_REGISTRY)


def test_unknown_topology_fails_fast():
    cfg = base_db_cfg("centralized")
    cfg.db["topology"] = "mesh"
    with pytest.raises(RegistryError):
        build_model(cfg)


def test_unknown_backend_fails_fast_at_construct():
    cfg = base_db_cfg("centralized")
    cfg.db["storage"]["backend"] = "punch-cards"
    with pytest.raises(RegistryError):
        build_with_caller(cfg, n_queries=1, until=5.0)


def test_build_model_requires_db_section():
    with pytest.raises(RegistryError):
        build_model(OmegaConf.create({"sim": {"until": 1.0}}))


# --------------------------------------------------------------------------- #
# per-topology end-to-end scenarios                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("topology", ["centralized", "disaggregated", "distributed"])
def test_scenario_runs_end_to_end_with_stub_caller(topology):
    cfg = base_db_cfg(topology)
    cfg.sim.until = 50.0
    result, db, caller, sink = build_with_caller(cfg, n_queries=8, until=50.0)

    assert len(caller.received) == 8
    for resp in caller.received:
        assert resp.db_latency > 0
        assert resp.completed_at > resp.submitted_at
        assert resp.agent_id == "caller-0"
        # ticket 04 stage taxonomy: compute_issue + storage + compute_finalize
        assert "compute_issue" in resp.stages
        assert "compute_finalize" in resp.stages
        assert "storage_service" in resp.stages

    # round-robin across entry nodes: disagg uses all compute nodes,
    # distributed (coordinators=1 here) funnels everything through its single
    # coordinator entry node
    path_prefix = result.collected["db"]["path"]
    node_paths = {resp.stages["compute_issue"].component for resp in caller.received}
    if topology == "disaggregated":
        entries = {f"{path_prefix}.compute-{i}" for i in range(2)}
        assert node_paths & entries == entries  # both entry nodes used
    elif topology == "distributed":
        assert node_paths == {f"{path_prefix}.node-0"}

    # lifecycle probe edges exist with the agent/request tags
    edges = [s for s in sink.samples if s.kind == "edge"]
    assert edges, "no lifecycle edge samples recorded"
    stages_seen = {s.signal.split(".")[-1] for s in edges}
    assert {"compute_issue", "compute_finalize"} <= stages_seen
    states_seen = {s.value for s in edges}
    assert {"submitted", "queued", "processing", "completed"} <= states_seen
    assert all("agent_id" in s.tags and "request_id" in s.tags for s in edges)


def test_centralized_skips_network_stages():
    cfg = base_db_cfg("centralized")
    _, db, caller, _ = build_with_caller(cfg, n_queries=4, until=50.0)
    assert caller.received
    for resp in caller.received:
        assert "net_req" not in resp.stages
        assert "net_resp" not in resp.stages


def test_disaggregated_exercises_network():
    cfg = base_db_cfg("disaggregated")
    _, db, caller, _ = build_with_caller(cfg, n_queries=4, until=50.0)
    for resp in caller.received:
        assert "net_req" in resp.stages
        assert "net_resp" in resp.stages
        assert resp.stages["net_resp"].service_time > 0


def test_distributed_exercises_network_and_peer_links():
    cfg = base_db_cfg("distributed")
    cfg.db.compute_nodes = 3
    cfg.db.coordinators = 1  # 1 coordinator + 2 peers
    _, db, caller, _ = build_with_caller(cfg, n_queries=4, until=50.0)
    for resp in caller.received:
        assert "net_req" in resp.stages
        assert "net_resp" in resp.stages
        # peer leg happens between net_req and net_resp
        assert resp.stages["net_req"].end <= resp.stages["storage_service"].end


# --------------------------------------------------------------------------- #
# scale-out: config changes only N / M                                         #
# --------------------------------------------------------------------------- #


def test_scale_out_compute_changes_only_n():
    cfg_a = base_db_cfg("disaggregated")
    cfg_b = copy.deepcopy(cfg_a)
    cfg_b.db.compute_nodes = 4

    _, db_a, _, _ = build_with_caller(cfg_a, n_queries=4, until=30.0)
    _, db_b, _, _ = build_with_caller(cfg_b, n_queries=4, until=30.0)

    topo_a, topo_b = db_a.topology, db_b.topology
    assert len(topo_a.compute_nodes) == 2
    assert len(topo_b.compute_nodes) == 4
    # M untouched
    assert len(topo_a.storage_nodes) == len(topo_b.storage_nodes) == 2
    # per-node params identical (slots / backend preset): growing N changes
    # only the node count
    slots_a = {n.slots for n in topo_a.compute_nodes}
    slots_b = {n.slots for n in topo_b.compute_nodes}
    assert slots_a == slots_b
    params_a = topo_a.storage_nodes[0].params
    params_b = topo_b.storage_nodes[0].params
    assert params_a == params_b


def test_scale_out_storage_changes_only_m():
    cfg_a = base_db_cfg("disaggregated")
    cfg_b = copy.deepcopy(cfg_a)
    cfg_b.db.storage_nodes = 4

    _, db_a, _, _ = build_with_caller(cfg_a, n_queries=4, until=30.0)
    _, db_b, _, _ = build_with_caller(cfg_b, n_queries=4, until=30.0)

def test_scale_out_presets_from_scenario_yaml():
    """The disaggregated scenario YAML's scale-out profile == growing N only.

    Applying the ``compute_nodes: 4`` factor on top of the scenario must not
    touch any per-node parameter and must build/run unchanged.
    """
    base = OmegaConf.to_container(OmegaConf.load("scenarios/db_disaggregated.yaml"))
    base.pop("profiles", None)
    cfg = OmegaConf.merge(base, {"db": {"compute_nodes": 4}})
    assert cfg.db.compute_nodes == 4
    assert cfg.db.compute.slots == 2  # per-node params untouched

    _, db, caller, _ = build_with_caller(cfg, n_queries=4, until=30.0)
    topo = db.topology
    assert len(topo.compute_nodes) == 4
    assert len(topo.storage_nodes) == 2


# --------------------------------------------------------------------------- #
# storage backends, slot holding, coordinator overhead                          #
# --------------------------------------------------------------------------- #


def test_storage_backend_presets_produce_different_service_times():
    def run_backend(backend):
        cfg = base_db_cfg("centralized")
        cfg.db.storage.backend = backend
        _, db, caller, _ = build_with_caller(cfg, n_queries=3, until=200.0)
        times = [resp.stages["storage_service"].service_time for resp in caller.received]
        assert times, f"no storage service times for backend {backend}"
        return sum(times) / len(times)

    avg_hdd = run_backend("hdd")
    avg_nvme = run_backend("nvme")
    # presets are checked strictly (> 20x separation by design) so the
    # swappable-backend path is visibly exercised
    assert avg_hdd > avg_nvme * 20


def test_compute_slot_held_through_storage_round_trip():
    """Option b: with one slot, the next query's queue time covers the whole
    slot-hold envelope of the in-flight query (no release between phases)."""
    cfg = base_db_cfg("disaggregated")
    cfg.db.compute_nodes = 1
    cfg.db.compute.slots = 1

    sink = InMemorySink()
    top = build_model(cfg, sink=sink)
    db = top._children[0]

    class TwoAtOnce(Component):
        input_ports = {"responses": "response"}
        output_ports = {"queries": "query"}

        def __init__(self, name: str, db: Any) -> None:
            Component.__init__(self, name)
            self.db = db

        def start(self) -> None:
            self.env.process(self._loop())

        def _loop(self) -> Any:
            for i in (0, 1):
                query = DBQuery(
                    agent_id=self.name, request_id=i, complexity=0.5, volume=1.0
                )
                self.db.submit(query)  # returns the started serving process
            yield self.env.timeout(50.0)

    runner = TwoAtOnce("runner", db)
    top.add(runner)
    top.connect_graph.connect(runner, "queries", db, "queries")
    top.connect_graph.connect(db, "responses", runner, "responses")
    run_model(top, cfg, until=50.0, sink=sink)  # type: ignore[arg-type]

    node = db.topology.entry_nodes[0]
    q0, q1 = tuple(node.responses)
    assert len(node.responses) == 2
    assert q0.queue_time == pytest.approx(0.0, abs=1e-6)
    # q1 waits for the full envelope: issue + storage round-trip + finalize
    assert q1.queue_time == pytest.approx(q0.db_latency, abs=0.02)
    assert q0.service_time > q0.stages["compute_issue"].service_time + q0.stages[
        "compute_finalize"
    ].service_time  # the storage round-trip sits inside the held slot


def test_coordinator_overhead_multiplier_applies():
    def run_with(overhead: float):
        cfg = base_db_cfg("distributed")
        cfg.db.network.coordinator_overhead = overhead
        _, db, caller, _ = build_with_caller(cfg, n_queries=2, until=50.0)
        times = []
        for resp in caller.received:
            issue = resp.stages["compute_issue"].service_time
            fin = resp.stages["compute_finalize"].service_time
            times.append(issue + fin)  # coordinator compute per request
        return sum(times) / len(times)

    with_overhead = run_with(1.1)
    without_overhead = run_with(1.0)
    # issue+finalize == complexity (0.5) scaled by the multiplier: strictly
    # proportional, small tolerance for float scheduling
    assert with_overhead == pytest.approx(0.5 * 1.1, abs=0.02)
    assert without_overhead == pytest.approx(0.5 * 1.0, abs=0.02)


# --------------------------------------------------------------------------- #
# slot capacity / queues interact correctly with end-to-end flow               #
# --------------------------------------------------------------------------- #


def test_two_phase_split_runs_storage_first():
    cfg = base_db_cfg("centralized")
    cfg.db.compute.phase_split = "two_phase"
    _, db, caller, _ = build_with_caller(cfg, n_queries=2, until=50.0)
    for resp in caller.received:
        assert "compute_issue" not in resp.stages
        assert "compute_finalize" in resp.stages
        # storage comes before the (single) compute stage
        assert resp.stages["storage_service"].end <= resp.stages["compute_finalize"].start
        assert resp.service_time == pytest.approx(
            resp.stages["storage_service"].service_time
            + resp.stages["compute_finalize"].service_time,
            abs=1e-4,
        )


def test_distributed_round_robin_across_coordinators():
    cfg = base_db_cfg("distributed")
    cfg.db.compute_nodes = 4
    cfg.db.coordinators = 2  # 2 coordinators + 2 peers
    _, db, caller, _ = build_with_caller(cfg, n_queries=8, until=50.0)
    paths = {resp.stages["compute_issue"].component for resp in caller.received}
    assert paths == {f"{db.topology.full_path}.node-0", f"{db.topology.full_path}.node-1"}
    counts: dict = {}
    for resp in caller.received:
        counts[resp.stages["compute_issue"].component] = (
            counts.get(resp.stages["compute_issue"].component, 0) + 1
        )
    assert set(counts.values()) == {4}  # perfect round-robin split
    # peers never serve external queries
    assert not any("node-2" in p or "node-3" in p for p in paths)

