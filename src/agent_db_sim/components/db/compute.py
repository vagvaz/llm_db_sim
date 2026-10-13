"""DBCompute: one compute node — ingress admission, a slot pool held through
the whole storage round-trip (option b), the phased query script, and a
pluggable topology resolver for the middle storage hop.

Query path per ticket 05 (three-phase): issue-compute (``issue_ratio`` of
complexity) -> resolver (network + storage, injected by the topology) ->
finalize (remaining complexity). ``phase_split: two_phase`` runs the storage
hop first and a single compute stage of the full complexity afterwards.
``coordinator_overhead`` multiplies a coordinator node's compute complexity
(folded into its compute stages per ticket 04).
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from agent_db_sim.components.db._cfg import cfloat, cstr
from agent_db_sim.components.db.backend import StorageNode, resolve_backend_params
from agent_db_sim.components.db.query import (
    QueryResponse,
    StageTiming,
    q_scalar,
    q_tag,
)
from agent_db_sim.components.db.server import PhaseControl, PhaseSlotServer
from agent_db_sim.simkit.component import Component
from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.ingress import open_ingress
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = ["DBCompute"]

COMPUTE_STAGES = ("compute_issue", "compute_finalize")


class DBCompute(Component):
    """One DB compute (processing) node of DBEndpoint."""

    input_ports = {"queries": "query", "peer_queries": "query"}
    output_ports = {"responses": "response", "peer_queries": "query"}

    def __init__(
        self,
        name: str,
        db_cfg: Any,
        sink: Optional[ProbeSink] = None,
        *,
        colocated_storage: bool = False,
        coordinator: bool = False,
    ) -> None:
        super().__init__(name)
        self.db_cfg = db_cfg
        self._sink_probe = sink if sink is not None else NULL_SINK
        self.colocated_storage = colocated_storage
        self.coordinator = coordinator

        compute_cfg = db_cfg.get("compute") if db_cfg is not None and "compute" in db_cfg else None

        self.slots = cfloat(compute_cfg, "slots", 1.0)
        if self.slots < 1:
            raise ComponentError(f"{name}: db.compute.slots must be >= 1, got {self.slots}")
        self.slots = int(self.slots)
        self.ingress_spec = cstr(compute_cfg, "ingress", "fifo")
        self.issue_ratio = cfloat(compute_cfg, "issue_ratio", 0.1)
        if not 0.0 <= self.issue_ratio <= 1.0:
            raise ComponentError(
                f"{name}: db.compute.issue_ratio must be in [0, 1], got {self.issue_ratio}"
            )
        self.phase_split = cstr(compute_cfg, "phase_split", "three_phase")
        if self.phase_split not in ("three_phase", "two_phase"):
            raise ComponentError(
                f"{name}: db.compute.phase_split must be 'three_phase' or 'two_phase', "
                f"got {self.phase_split!r}"
            )
        self.token_cap = cfloat(compute_cfg, "token_cap", 0.0) or None

        # per-ticket-05: coordinator overhead folds into the coordinator's
        # own compute stages; default ×1 applies to every other node.
        self.overhead = 1.0

        self.resolver: Optional[Callable[[Any], Any]] = None
        self.storage: Optional[StorageNode] = None
        self.server: Optional[PhaseSlotServer] = None
        self.responses: List[QueryResponse] = []

    # ------------------------------------------------------------------ #
    # lifecycle                                                          #
    # ------------------------------------------------------------------ #
    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)
        self.ingress = open_ingress(
            self.env, self.ingress_spec, name=f"{self.name}.ingress", sink=self._sink_probe
        )
        self.server = PhaseSlotServer(
            self.env,
            self.name,
            slots=self.slots,
            ingress=self.ingress,
            script=self._script,
            token_cap=self.token_cap,
            sink=self._sink_probe,
        )
        if self.colocated_storage:
            params = resolve_backend_params(self.db_cfg.get("storage"))
            self.storage = StorageNode("storage", params, self._sink_probe)
            self.add(self.storage)
            self.storage.construct(self.env, cfg)  # late-added child: construct it here

    def start(self) -> None:
        super().start()
        self.server.start()

    def collect(self) -> Dict[str, Any]:
        data = super().collect()
        data["responses"] = list(self.responses)
        data["service_records"] = list(self.server.records)
        return data

    # ------------------------------------------------------------------ #
    # caller API (wired contract: queries in, responses out)              #
    # ------------------------------------------------------------------ #
    def handle(self, query: Any) -> Any:
        """Serve one query; returns a process yielding :class:`QueryResponse`."""
        return self.env.process(self._handle(query))

    def _handle(self, query: Any) -> Any:
        admission = self.server.submit(query)  # the ingress admission event
        self._edge("compute_issue", "submitted", query)
        yield admission
        record, stages = yield query.done
        response = QueryResponse(
            agent_id=q_tag(query, "agent_id"),
            request_id=q_tag(query, "request_id"),
            db_latency=record.end - record.arrival,
            queue_time=record.queue_time,
            service_time=record.service_time,
            submitted_at=record.arrival,
            completed_at=record.end,
            stages=stages,
        )
        self.responses.append(response)
        return response

    # ------------------------------------------------------------------ #
    # probe emission                                                      #
    # ------------------------------------------------------------------ #
    def _edge(self, stage: str, state: str, query: Any, t: Optional[float] = None) -> None:
        self._sink_probe.edge(
            f"{self.full_path}.{stage}",
            state,
            t=t,
            agent_id=q_tag(query, "agent_id"),
            request_id=q_tag(query, "request_id"),
        )

    # ------------------------------------------------------------------ #
    # the phased service script (runs inside the held slot)               #
    # ------------------------------------------------------------------ #
    def _script(self, ctl: PhaseControl, query: Any) -> Any:
        env = self.env
        complexity = q_scalar(query, "complexity")
        stages: Dict[str, StageTiming] = {}

        if self.phase_split == "two_phase":
            # simplified path: storage round-trip first, one compute stage
            middle = yield ctl.call(self.resolver(query))
            stages.update(middle)
            t_start = env.now
            self._edge("compute_finalize", "processing", query)
            yield ctl.wait_compute(query, complexity * self.overhead)
            t_end = env.now
            self._edge("compute_finalize", "completed", query, t=t_end)
            stages["compute_finalize"] = StageTiming(
                component=self.full_path,
                stage="compute_finalize",
                start=t_start,
                end=t_end,
                service_time=t_end - t_start,
            )
            return stages

        # three-phase (default): issue-compute, storage round-trip, finalize
        issue = complexity * self.issue_ratio * self.overhead
        t_issue_start = env.now
        self._edge("compute_issue", "processing", query, t=t_issue_start)
        yield ctl.wait_compute(query, issue)
        t_issue_end = env.now
        self._edge("compute_issue", "completed", query, t=t_issue_end)
        stages["compute_issue"] = StageTiming(
            component=self.full_path,
            stage="compute_issue",
            start=t_issue_start,
            end=t_issue_end,
            service_time=t_issue_end - t_issue_start,
        )

        middle = yield ctl.call(self.resolver(query))
        stages.update(middle)

        t_fin_start = env.now
        self._edge("compute_finalize", "processing", query, t=t_fin_start)
        finalize = complexity * (1.0 - self.issue_ratio) * self.overhead
        yield ctl.wait_compute(query, finalize)
        t_fin_end = env.now
        self._edge("compute_finalize", "completed", query, t=t_fin_end)
        stages["compute_finalize"] = StageTiming(
            component=self.full_path,
            stage="compute_finalize",
            start=t_fin_start,
            end=t_fin_end,
            service_time=t_fin_end - t_fin_start,
        )
        return stages
