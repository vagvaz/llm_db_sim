"""Model assembly glue (ticket 13): AgentClient + LLMEndpoint + DBEndpoint.

Declarative side — the four wired links of the shared contract are recorded
in the top composite's :class:`ConnectGraph` (elaboration validates ports and
kinds, fail fast):

    agents.inference_requests -> llm.requests
    llm.responses             -> agents.inference_responses
    agents.db_queries         -> database.queries
    database.responses        -> agents.db_responses

The agent's ``db_queries`` output is declared kind ``req`` (ticket 08) while
the DBEndpoint ``queries`` input is kind ``query`` (ticket 10); the
:class:`QueryAdapter` node bridges the two vocabularies in the graph. It is a
pure declarative node: the actual transport is the runtime handlers below.

Runtime side — simkit ports are validation/introspection only, so transport
follows the ticket-08 wired contract:

- ``agent.bind_output(port, handler)``: the handler admits the request on the
  serving side (``llm.submit(req)`` / ``db.submit(req)``) and returns the
  event the agent yields (the admission event for the LLM, the started serve
  process for the DB).
- ``agent.deliver(port, req)``: a per-request watcher process (spawned inside
  the handler) yields the serving-side completion, shapes a simkit
  ``Request`` carrying the response payload and the request identity, and
  delivers it into the waiting agent's rendezvous buffer. The agent's
  ``received`` trace transition is exactly that handoff.

Both components share one flat :class:`ProbeSink` (signal names carry the
component path), which is the same sink handed to the metrics collector and
the VCD writer.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from agent_db_sim.components.agent_client import AgentClient
from agent_db_sim.components.db.endpoint import DBEndpoint
from agent_db_sim.components.llm_endpoint import LLMEndpoint
from agent_db_sim.simkit.component import Component, Composite
from agent_db_sim.simkit.ingress import Request

__all__ = ["QueryAdapter", "wire_agent_llm_db"]


class QueryAdapter(Component):
    """Declarative bridge ``agents.db_queries`` (kind ``req``) ->
    ``database.queries`` (kind ``query``).

    It carries no simulated behavior: simkit ports are the introspection
    surface, and the runtime transport is the agent's ``bind_output`` handler
    wired straight onto ``db.submit``. The node exists so the full agent ↔ DB
    contract is visible in the connect graph without rewriting either
    component's ticket-declared port kinds.
    """

    input_ports = {"requests": "req"}
    output_ports = {"queries": "query"}


def wire_agent_llm_db(
    top: Composite,
    agents: AgentClient,
    llm: LLMEndpoint,
    db: DBEndpoint,
) -> None:
    """Record the four contract links and bind the runtime transport.

    Call between model construction and elaboration (i.e. inside
    ``build_model``): bindings and graph links must exist before elaboration.
    """
    graph = top.connect_graph
    graph.connect(agents, "inference_requests", llm, "requests")
    graph.connect(llm, "responses", agents, "inference_responses")
    adapter = QueryAdapter("queries_bridge")
    top.add(adapter)
    graph.connect(agents, "db_queries", adapter, "requests")
    graph.connect(adapter, "queries", db, "queries")
    graph.connect(db, "responses", agents, "db_responses")

    def inference_tx(request: Any) -> Any:
        """LLM output port transport: admit, then deliver on completion."""
        admission = llm.submit(request)
        llm.env.process(_deliver_inference(llm, agents, request))
        return admission

    def db_tx(request: Any) -> Any:
        """DB output port transport: admit, then deliver on completion."""
        serving_process = db.submit(request)
        agents.env.process(_deliver_db(agents, request, serving_process))
        return serving_process

    agents.bind_output("inference_requests", inference_tx)
    agents.bind_output("db_queries", db_tx)


def _respond(
    agents: AgentClient,
    port: str,
    request: Any,
    extra: Optional[Dict[str, Any]] = None,
) -> Request:
    params: Dict[str, Any] = {
        "agent_id": _param(request, "agent_id"),
        "request_id": _param(request, "request_id"),
    }
    parent = _param(request, "parent_id", None)
    if parent is not None:
        params["parent_id"] = parent
    if extra:
        params.update(extra)
    return Request(params=params, tokens=0.0)


def _param(request: Any, name: str, default: Any = None) -> Any:
    params = getattr(request, "params", None)
    if isinstance(params, Dict):
        return params.get(name, default)
    return getattr(request, name, default)


def _deliver_inference(llm: LLMEndpoint, agents: AgentClient, request: Any) -> Any:
    """Watcher: llm completion -> agent rendezvous (nearest causal instant)."""
    response = yield llm.wait(request)
    shaped = _respond(
        agents,
        "inference_responses",
        request,
        extra={
            "response_kind": "inference",
            "queue_time": response.queue_time,
            "service_time": response.service_time,
        },
    )
    yield agents.deliver("inference_responses", shaped)


def _deliver_db(agents: AgentClient, request: Any, serving_process: Any) -> Any:
    """Watcher: DB completion -> agent rendezvous (nearest causal instant)."""
    response = yield serving_process
    shaped = _respond(
        agents,
        "db_responses",
        request,
        extra={
            "response_kind": "db",
            "db_latency": response.db_latency,
        },
    )
    yield agents.deliver("db_responses", shaped)
