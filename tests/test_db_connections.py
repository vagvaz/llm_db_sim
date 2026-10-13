"""Connections-as-a-resource: bounded connection pools on network links.

The contract under test:

- ``db.network.connections`` (int, default 0 = unlimited) bounds every
  network link's connection pool; pools apply to the disaggregated
  segment and to every distributed coordinator-peer link.
- A query acquires ONE connection from a link's pool before its first
  transfer of the round trip and holds it until the ``net_resp``
  direction completes (release after the last transfer). No free
  connection -> the query waits in the link's connection queue BEFORE
  any transfer.
- The wait appears as its own ``net_conn`` trace stage (edges
  ``{path}.net_conn`` submitted|queued|processing|completed) and the
  pool occupancy as a ``conn_busy`` level signal per link.
- Default config (no ``connections`` key): no pool, no ``net_conn``
  stage, no ``conn_busy`` signal, bit-identical behavior.

All e2e expectations below are hand-derived (latency/bandwidth/storage
numbers chosen to be exactly representable):

    NET_REQ_HOP  = latency + request_size/bandwidth = 0.5 + 0.5 = 1.0
    STORAGE_HOP  = base 0 + max(per_gb*vol, vol/bw, 1/iops) = 1.0
    NET_RESP_HOP = latency + volume/bandwidth       = 0.5 + 1.0 = 1.5
    LONE_D       = 3.5 (a lone query's db latency, no contention)
"""

from __future__ import annotations

from typing import Any, List, Tuple

import pytest
import simpy
from omegaconf import OmegaConf

from agent_db_sim.components.db.endpoint import DBEndpoint
from agent_db_sim.components.db.network import NetLink, NetworkConfig
from agent_db_sim.components.db.query import DBQuery
from agent_db_sim.simkit.component import Component
from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.probes import InMemorySink
from agent_db_sim.simkit.registry import build_model
from agent_db_sim.simkit.runner import run_model

TOL = 1e-9
NET_REQ_HOP = 1.0
STORAGE_HOP = 1.0
NET_RESP_HOP = 1.5
LONE_D = NET_REQ_HOP + STORAGE_HOP + NET_RESP_HOP  # 3.5


# --------------------------------------------------------------------------- #
# config parsing + fail-fast                                                    #
# --------------------------------------------------------------------------- #


def test_connections_default_is_unlimited():
    assert NetworkConfig(None).connections == 0
    assert NetworkConfig({"latency": 0.1}).connections == 0


def test_connections_parsed_from_db_network():
    assert NetworkConfig({"connections": 4}).connections == 4
    # integral floats parse as integers
    assert NetworkConfig({"connections": 2.0}).connections == 2


def test_connections_negative_fails_fast():
    with pytest.raises(ComponentError):
        NetworkConfig({"connections": -1})


def test_connections_non_numeric_fails_fast():
    with pytest.raises(TypeError):
        NetworkConfig({"connections": "two"})


def test_default_config_builds_no_connection_pool():
    cfg = _e2e_cfg("disaggregated")
    cfg["db"]["network"].pop("connections", None)
    links = _built_links(cfg)
    assert links, "no NetLink built"
    for link in links:
        assert link.conn_pool is None
        assert link.net.connections == 0
        assert link.conn_records == []


def test_connections_key_sizes_every_link_pool():
    cfg = _e2e_cfg("disaggregated")
    cfg["db"]["network"]["connections"] = 4
    links = _built_links(cfg)
    assert {getattr(link.conn_pool, "capacity", None) for link in links} == {4}

    cfg = _e2e_cfg("distributed")
    cfg["db"]["network"]["connections"] = 3
    cfg["db"]["compute_nodes"] = 3
    cfg["db"]["coordinators"] = 1
    links = _built_links(cfg)
    assert len(links) >= 1  # one per (coordinator, peer) pair
    assert {getattr(link.conn_pool, "capacity", None) for link in links} == {3}


# --------------------------------------------------------------------------- #
# unlimited default: golden behavior unchanged                                  #
# --------------------------------------------------------------------------- #


def test_unlimited_default_trace_is_golden():
    """No ``connections`` config -> no net_conn stage, no conn_busy signal,
    and the lone-query chain is exactly propagation + volume math."""
    cfg = _e2e_cfg("disaggregated")
    cfg["db"]["network"].pop("connections", None)
    sink, _, caller = _run_two_queries(cfg)

    assert [r.db_latency for r in caller.received] == pytest.approx([LONE_D, LONE_D])
    assert sink.series("conn_busy") == []
    edge_stages = {s.signal.split(".")[-1] for s in sink.samples if s.kind == "edge"}
    assert "net_conn" not in edge_stages
    assert {"net_req", "net_resp"} <= edge_stages


def test_connections_zero_is_bit_identical_to_absent():
    """``connections: 0`` (explicitly unlimited) == the key absent."""
    absent = _e2e_cfg("disaggregated")
    absent["db"]["network"].pop("connections", None)
    zero = _e2e_cfg("disaggregated")
    zero["db"]["network"]["connections"] = 0

    a = [(r.agent_id, r.request_id, r.db_latency, r.completed_at)
         for r in _run_two_queries(absent)[2].received]
    b = [(r.agent_id, r.request_id, r.db_latency, r.completed_at)
         for r in _run_two_queries(zero)[2].received]
    assert a == b


# --------------------------------------------------------------------------- #
# NetLink unit tests: exhaustion + release correctness (hand-computed)          #
# --------------------------------------------------------------------------- #


def _bare_link(net_kwargs: dict) -> Tuple[simpy.Environment, NetLink, InMemorySink]:
    env = simpy.Environment()
    sink = InMemorySink()
    sink.clock = lambda: env.now
    link = NetLink("seg", NetworkConfig(net_kwargs), sink)
    link.construct(env, {})
    return env, link, sink


def test_link_connection_exhaustion_waits_hand_wait():
    """1-connection pool, two concurrent transfers of the response direction
    (a ``net_resp`` call auto-releases at completion -- the round-trip
    terminal direction):

    q0: acquires at 0, hop = latency + volume/bandwidth = 0.5 + 1.0 = 1.5,
        releases at 1.5.
    q1: waits in the connection queue exactly 1.5, then its own 1.5 hop.
    """
    env, link, sink = _bare_link(
        {"latency": 0.5, "bandwidth": 1.0, "capacity": 8.0,
         "request_size": 0.1, "connections": 1}
    )
    link.transfer(DBQuery(agent_id="a", request_id=0, volume=1.0), "net_resp", 1.0)
    link.transfer(DBQuery(agent_id="a", request_id=1, volume=1.0), "net_resp", 1.0)
    env.run(until=10.0)

    rec0, rec1 = link.records
    conn0, conn1 = link.conn_records
    # q0: zero connection wait; the connection is held for the whole hop
    assert conn0.queue_time == pytest.approx(0.0, abs=TOL)
    assert conn0.start == pytest.approx(0.0, abs=TOL)
    assert conn0.end == pytest.approx(1.5, abs=TOL)
    assert conn0.service_time == pytest.approx(1.5, abs=TOL)
    # hand-computed connection wait for the queued query
    assert conn1.queue_time == pytest.approx(1.5, abs=TOL)
    assert conn1.start == pytest.approx(0.0, abs=TOL)
    assert conn1.end == pytest.approx(3.0, abs=TOL)
    assert conn1.service_time == pytest.approx(1.5, abs=TOL)
    # release correctness: the second query ran to completion and its net
    # stage only starts once the connection is granted (post-admission)
    assert rec0.start == pytest.approx(0.0, abs=TOL)
    assert rec1.start == pytest.approx(1.5, abs=TOL)
    assert rec1.end == pytest.approx(3.0, abs=TOL)
    # no leaked holds: the pool is released after every transfer
    assert link._holds == {}
    busy = [s.value for s in sink.series("conn_busy")]
    assert busy and max(busy) <= 1 and busy[-1] == 0


def test_link_release_under_concurrency_two_conns_four_queries():
    """connections=2, four same-instant transfers: the first two run now, the
    last two acquire exactly at the first pair's release (1.1) and finish
    at 2.2; busy peaks exactly at the pool size."""
    env, link, sink = _bare_link(
        {"latency": 0.1, "bandwidth": 1.0, "capacity": 8.0,
         "request_size": 0.1, "connections": 2}
    )
    for i in range(4):
        link.transfer(DBQuery(agent_id="a", request_id=i, volume=1.0), "net_resp", 1.0)
    env.run(until=10.0)

    assert len(link.records) == 4 and len(link.conn_records) == 4
    waits = [c.queue_time for c in link.conn_records]
    assert waits == pytest.approx([0.0, 0.0, 1.1, 1.1])
    ends = [r.end for r in link.records]
    assert ends == pytest.approx([1.1, 1.1, 2.2, 2.2])
    busy = [s.value for s in sink.series("conn_busy")]
    assert max(busy) == 2 and busy[-1] == 0
    assert link._holds == {}


def test_link_round_trip_holds_one_connection():
    """The net_req -> net_resp round trip acquires once (never re-queues for
    the second direction) and releases exactly after the net_resp transfer;
    a second query waits for that release."""
    env, link, sink = _bare_link(
        {"latency": 0.0, "bandwidth": 2.0, "capacity": 8.0,
         "request_size": 0.5, "connections": 1}
    )
    pq = DBQuery(agent_id="a", request_id=7, volume=1.0)
    p_req = link.transfer(pq, "net_req", 0.5)

    def round_trip() -> Any:
        yield p_req                      # net_req: 0.5/2.0 = 0.25
        yield env.timeout(0.25)          # the storage hop, conn still held
        p_resp = link.transfer(pq, "net_resp", 1.0)  # 1.0/2.0 = 0.5
        yield p_resp

    env.process(round_trip())

    def blocker() -> Any:
        p_other = link.transfer(  # noqa: F841 - second query needs the conn
            DBQuery(agent_id="a", request_id=8, volume=1.0), "net_resp", 1.0
        )
        yield env.timeout(10.0)

    env.process(blocker())
    env.run(until=10.0)

    req_rec, resp_rec = link.records[:2]
    assert req_rec.queue_time == pytest.approx(0.0, abs=TOL)
    assert req_rec.service_time == pytest.approx(0.25, abs=TOL)
    # net_resp ran 0.5 -> 1.0: the same connection stayed held across the gap
    assert resp_rec.start == pytest.approx(0.5, abs=TOL)
    assert resp_rec.end == pytest.approx(1.0, abs=TOL)
    own, other = link.conn_records
    assert own.queue_time == pytest.approx(0.0, abs=TOL)
    assert own.end == pytest.approx(1.0, abs=TOL)  # closed with net_resp
    # the blocker waited exactly until the hold was released at t=1.0 ...
    assert other.queue_time == pytest.approx(1.0, abs=TOL)
    # ... then its own 0.5 hop
    assert other.end == pytest.approx(1.5, abs=TOL)


# --------------------------------------------------------------------------- #
# e2e: connection pool exhaustion with hand-computed waits                      #
# --------------------------------------------------------------------------- #


def test_e2e_disaggregated_exhaustion_hand_timeline():
    """Two concurrent queries over one segment, connections=1.

    Paper timeline (compute.slots=2 so both are in flight):
      q0: conn 0.0 -> 3.5 (net_req 1.0, storage 1.0, net_resp 1.5), D=3.5
      q1: conn wait exactly q0's whole engagement (3.5), D=7.0
    """
    cfg = _e2e_cfg("disaggregated")
    cfg["db"]["network"]["connections"] = 1
    cfg["db"]["compute_nodes"] = 1
    cfg["db"]["storage_nodes"] = 1
    sink, links, caller = _run_two_queries(cfg)

    q0, q1 = caller.received
    assert len(caller.received) == 2
    assert q0.db_latency == pytest.approx(LONE_D, abs=TOL)      # never waited
    assert q1.db_latency == pytest.approx(2 * LONE_D, abs=TOL)  # + conn wait D

    assert len(links) == 1
    conn0, conn1 = links[0].conn_records
    assert conn0.queue_time == pytest.approx(0.0, abs=TOL)
    assert conn1.queue_time == pytest.approx(LONE_D, abs=TOL)
    assert conn0.service_time == pytest.approx(LONE_D, abs=TOL)
    assert conn0.end == pytest.approx(LONE_D, abs=TOL)          # release at 3.5
    assert conn1.end == pytest.approx(2 * LONE_D, abs=TOL)      # release at 7.0

    # q1's first transfer only STARTS after connection admission
    assert q1.stages["net_req"].start == pytest.approx(LONE_D, abs=TOL)

    # utilization: 0 -> 1 -> 0 -> 1 -> 0, never above the pool size
    busy = [s.value for s in sink.series("conn_busy")]
    assert busy == pytest.approx([0, 1, 0, 1, 0])


def test_e2e_distributed_exhaustion_hand_timeline():
    """Same hand math via the CoordinatorResolver: the coordinator's
    connection to a peer node is pooled on the dedicated per-peer link."""
    cfg = _e2e_cfg("distributed")
    cfg["db"]["network"]["connections"] = 1
    cfg["db"]["compute_nodes"] = 2  # 1 coordinator + 1 peer
    cfg["db"]["coordinators"] = 1
    _, links, caller = _run_two_queries(cfg)

    q0, q1 = caller.received
    assert q0.db_latency == pytest.approx(LONE_D, abs=TOL)
    assert q1.db_latency == pytest.approx(2 * LONE_D, abs=TOL)

    held = [link for link in links if link.conn_records]
    assert len(held) == 1  # both queries fronted to the single peer link
    conn0, conn1 = held[0].conn_records
    assert conn0.queue_time == pytest.approx(0.0, abs=TOL)
    assert conn1.queue_time == pytest.approx(LONE_D, abs=TOL)
    assert conn1.end == pytest.approx(2 * LONE_D, abs=TOL)


def test_e2e_distributed_per_link_isolation():
    """Two coordinators -> two peers over separate links: connections=1 per
    link, yet concurrent queries never contend (fan-out is per link)."""
    cfg = _e2e_cfg("distributed")
    cfg["db"]["network"]["connections"] = 1
    cfg["db"]["compute_nodes"] = 4  # 2 coordinators + 2 peers
    cfg["db"]["coordinators"] = 2
    _, links, caller = _run_two_queries(cfg)

    assert len(caller.received) == 2
    for resp in caller.received:
        assert resp.db_latency == pytest.approx(LONE_D, abs=TOL)

    used = [link for link in links if link.conn_records]
    assert len(used) == 2  # one distinct coordinator-peer link per query
    for link in used:
        (conn,) = link.conn_records
        assert conn.queue_time == pytest.approx(0.0, abs=TOL)


def test_trace_taxonomy_contains_net_conn_stage_when_pooled():
    cfg = _e2e_cfg("disaggregated")
    cfg["db"]["network"]["connections"] = 2
    cfg["db"]["compute_nodes"] = 1
    cfg["db"]["storage_nodes"] = 1
    sink, _, _caller = _run_two_queries(cfg)

    edges = [s for s in sink.samples if s.kind == "edge"]
    conn_edges = [s for s in edges if s.signal.split(".")[-1] == "net_conn"]
    assert conn_edges, "no net_conn lifecycle edges with pools configured"
    states = {s.value for s in conn_edges}
    assert {"submitted", "queued", "processing", "completed"} <= states
    assert all("agent_id" in s.tags and "request_id" in s.tags for s in conn_edges)
    # every request that used the link carries exactly one connection machine
    machines: dict[str, set] = {}
    for s in conn_edges:
        machines.setdefault(str(s.tags["request_id"]), set()).add(s.value)
    assert set(machines) == {"0", "1"}
    assert all(
        v == {"submitted", "queued", "processing", "completed"}
        for v in machines.values()
    )


# --------------------------------------------------------------------------- #
# helpers (mirroring the test_db_endpoint patterns)                             #
# --------------------------------------------------------------------------- #


def _e2e_cfg(topology: str) -> Any:
    cfg = {
        "sim": {"until": 50.0, "seed": 0},
        "db": {
            "topology": topology,
            "compute_nodes": 1,
            "storage_nodes": 1,
            "coordinators": 1,
            "compute": {"slots": 2, "ingress": "fifo", "issue_ratio": 0.1},
            "storage": {
                "backend": "nvme",
                "overrides": {
                    "base_latency": 0.0,
                    "per_gb_time": 1.0,
                    "bandwidth_ceiling": 10.0,
                    "iops_ceiling": 1e9,
                },
            },
            "network": {
                "latency": 0.5,
                "bandwidth": 1.0,
                "capacity": 8.0,
                "request_size": 0.5,
                "connections": 0,
            },
        },
    }
    return OmegaConf.create(cfg)


def _collect_links(node: Any, acc: List[NetLink]) -> List[NetLink]:
    for child in node._children:
        if isinstance(child, NetLink):
            acc.append(child)
        _collect_links(child, acc)
    return acc


def _built_links(cfg: Any) -> List[NetLink]:
    """Build the model (construct phase) and return its NetLinks."""
    top = build_model(cfg, sink=InMemorySink())
    db = top._children[0]  # top.db
    db.construct(simpy.Environment(), cfg)
    return _collect_links(db, [])


class TwoQueriesAtOnce(Component):
    """Two queries submitted in the same instant (contend for the link)."""

    input_ports = {"responses": "response"}
    output_ports = {"queries": "query"}

    def __init__(self, name: str, db: DBEndpoint) -> None:
        Component.__init__(self, name)
        self.db = db
        self.received: list = []

    def start(self) -> None:
        self.env.process(self._loop())

    def _loop(self) -> Any:
        for i in (0, 1):
            self.env.process(self._one(i))
        yield self.env.timeout(50.0)

    def _one(self, i: int) -> Any:
        query = DBQuery(agent_id=self.name, request_id=i, complexity=0.0, volume=1.0)
        response = yield self.db.submit(query)  # started serving process
        self.received.append(response)

    def collect(self) -> Any:
        return {"received": len(self.received)}


def _run_two_queries(cfg: Any) -> Tuple[InMemorySink, List[NetLink], TwoQueriesAtOnce]:
    sink = InMemorySink()
    top = build_model(cfg, sink=sink)
    db = top._children[0]
    caller = TwoQueriesAtOnce("caller-0", db)
    top.add(caller)
    top.connect_graph.connect(caller, "queries", db, "queries")
    top.connect_graph.connect(db, "responses", caller, "responses")
    run_model(top, cfg, until=50.0, sink=sink)  # type: ignore[arg-type]
    return sink, _collect_links(db, []), caller
