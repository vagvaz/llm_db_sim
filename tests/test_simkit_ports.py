"""Unit tests for typed ports and connect-graph elaboration validation."""

from __future__ import annotations

import pytest

from agent_db_sim.simkit.component import Component, Composite
from agent_db_sim.simkit.errors import WiringError
from agent_db_sim.simkit.runner import run_model


class Producer(Component):
    output_ports = {"out": "req"}


class Consumer(Component):
    input_ports = {"in": "req"}


class BadConsumer(Consumer):
    input_ports = {"in": "token"}  # different kind


def _tree(mismatch: bool = False, src_port: str = "out", dst_port: str = "in"):
    top = Composite("top")
    p = top.add(Producer("p"))
    c = top.add(BadConsumer("c") if mismatch else Consumer("c"))
    top.connect_graph.connect(p, src_port, c, dst_port)
    return top, p, c


def test_matching_kinds_pass_and_graph_is_introspectable() -> None:
    top, p, c = _tree()
    top.elaborate()  # fail-fast validation: no exception

    graph = top.connect_graph
    assert len(graph) == 1
    (link,) = graph.links
    assert link.src is p and link.dst is c
    assert link.describe() == "top.p.out -> top.c.in"
    assert graph.links_of(p) == [link]

    elaborated = run_model(top, {}, until=1.0)
    assert elaborated.end_time == pytest.approx(1.0)


def test_port_kind_mismatch_fails_at_elaboration() -> None:
    top, _, _ = _tree(mismatch=True)
    with pytest.raises(WiringError, match="kind mismatch"):
        top.elaborate()


def test_unknown_ports_fail_at_elaboration() -> None:
    top, _, _ = _tree(src_port="missing")
    with pytest.raises(WiringError, match="no output port"):
        top.elaborate()

    top, _, _ = _tree(dst_port="missing")
    with pytest.raises(WiringError, match="no input port"):
        top.elaborate()


def test_double_wired_input_fails() -> None:
    top, _, c = _tree()
    p2 = top.add(Producer("p2"))
    top.connect_graph.connect(p2, "out", c, "in")
    with pytest.raises(WiringError, match="more than once"):
        top.elaborate()


def test_connect_records_links_without_raising_until_elaboration() -> None:
    """Contract: connect() records; elaboration type-checks (fail fast)."""
    top, _, _ = _tree(mismatch=True)
    assert len(top.connect_graph) == 1  # recorded, not yet checked
