"""LLMEndpoint: the simulated inference server built on the simkit kernel.

The wired request contract is attributes ``{input_tokens, output_tokens,
agent_id, request_id}``; for convenience a simkit :class:`Request` wrapping
those values in its ``params`` mapping is accepted too (attribute lookup
falls back to the ``params`` mapping).

Config (``llm`` namespace; a nested ``llm:`` section is preferred, but the
keys are also read directly from the top level when the section is absent)::

    llm:
      slots: 4                  # slot capacity (concurrent requests)
      input_tokens_per_s: 1000  # input-phase processing rate
      output_tokens_per_s: 500  # output-phase generation rate
      ingress: fifo             # any simkit ingress spec (default "fifo")
      quantum: 0.01             # token-cap pacing granularity (optional)

Semantics (the settled architectural shape):

- Requests arrive with the wired contract attributes
  ``{input_tokens, output_tokens, agent_id, request_id}`` and are served on a
  :class:`~agent_db_sim.simkit.processing.SlotServer`: queue-when-saturated
  comes from the slot pool, one slot per in-flight request.
- Within one slot hold the two phases run **sequentially**: input tokens are
  processed first at ``input_tokens_per_s``, then output tokens are generated
  at ``output_tokens_per_s`` — ``cost_fn`` returns the total
  ``input/in_rate + output/out_rate``.
- On top of the phase floor, the aggregate tokens/s cap
  (``input_tokens_per_s + output_tokens_per_s``) paces the request's tokens
  with fair share across all in-flight token-consuming requests: a solo
  request takes exactly its phase floor, and when several requests share the
  endpoint each earns ``cap / |in-flight|`` so per-request time stretches
  until the aggregate throughput cannot exceed the cap.
- Per-request ``queue_time`` / ``service_time`` split comes free from the
  SlotServer records; occupancy/throughput probes go to the probe sink
  (counters ``tokens_input``/``tokens_output``, levels ``slot_busy`` and
  ``queue_depth`` from the underlying primitives).
- Lifecycle edge samples: one per state transition, named
  ``{component}.{stage}`` with ``stage`` in
  ``submitted | queued | processing | completed`` and tags/attrs
  ``{agent_id, request_id, state}``.

Calling contract (for components that use the endpoint, e.g. AgentClient)::

    ev = endpoint.submit(request)   # request: the contract-shaped object
    yield ev                        # admission handoff (blocks for ingress `none`)
    rec = yield endpoint.wait(request)   # completion; see response_of(request)

The ``requests`` input port declares the inbound contract and the
``responses`` output port the outbound one; the underlying simkit ports are
validation/introspection only, so transport is this submit/wait API and the
payload handed back per request is the :class:`LLMResponse` built by
:meth:`response_of`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Iterator, List, Optional, Tuple

from agent_db_sim.simkit.component import Component
from agent_db_sim.simkit.errors import ComponentError
from agent_db_sim.simkit.ingress import Request
from agent_db_sim.simkit.processing import ServiceRecord, SlotServer
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

__all__ = ["LLMEndpoint", "LLMResponse", "CFG_SECTION"]

CFG_SECTION = "llm"

_REQUEST_ATTRS = ("input_tokens", "output_tokens", "agent_id", "request_id")
_LIFECYCLE_STATES = ("submitted", "queued", "processing", "completed")

_MISSING = object()


def _lookup(node: Any, dotted: str) -> Tuple[bool, Any]:
    """Resolve a dotted config path across mappings and attribute nodes."""
    cur = node
    for part in dotted.split("."):
        if hasattr(cur, "__contains__"):
            try:
                if part in cur:
                    cur = cur[part]
                    continue
            except TypeError:  # non-mapping __contains__
                pass
        try:
            cur = getattr(cur, part)
        except AttributeError:
            return False, None
    return True, cur


@dataclass(frozen=True)
class LLMResponse:
    """The payload delivered on the ``responses`` port for one request."""

    request_id: Any
    agent_id: Any
    input_tokens: float
    output_tokens: float
    queue_time: float
    service_time: float
    start: float
    end: float


class LLMEndpoint(Component):
    """Serving component: slot pool + sequential token phases + aggregate cap."""

    input_ports: ClassVar[Dict[str, Any]] = {"requests": "req"}
    output_ports: ClassVar[Dict[str, Any]] = {"responses": "response"}

    def __init__(
        self,
        name: str = "llm",
        sink: Optional[ProbeSink] = None,
        cfg_section: Optional[Any] = None,
    ) -> None:
        """``cfg_section`` optionally carries the ``llm:`` mapping directly
        (the ``build_model``-style convention); otherwise the ``llm``
        namespace is read from the master config at ``construct``."""
        super().__init__(name)
        self._sink_explicit = sink
        self._sink: ProbeSink = NULL_SINK
        self._cfg_section = cfg_section
        self.server: Optional[SlotServer] = None

        # build_model-facing config (validated at elaborate, fail fast)
        self._slots = 4
        self._in_rate = 0.0
        self._out_rate = 0.0
        self._ingress_spec = "fifo"
        self._quantum: Optional[float] = None

        # rid-keyed request bookkeeping (bounded by requests served, like
        # the SlotServer's own records list)
        self._by_rid: Dict[int, Any] = {}          # rid -> contract request
        self._rid_of: Dict[int, int] = {}          # id(contract req) -> rid
        self._sreq_by_rid: Dict[int, Request] = {}  # rid -> shaped simkit request
        self._rec_by_rid: Dict[int, ServiceRecord] = {}

    # ------------------------------------------------------------------ #
    # lifecycle                                                           #
    # ------------------------------------------------------------------ #
    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)
        if self._sink_explicit is not None:
            self._sink = self._sink_explicit
        else:
            found, injected = _lookup(cfg, "probes.sink")
            self._sink = (
                injected if found and isinstance(injected, ProbeSink) else NULL_SINK
            )

        found, section = (
            (True, self._cfg_section)
            if self._cfg_section is not None
            else _lookup(cfg, CFG_SECTION)
        )
        base = section if found and section is not None else cfg

        self._slots = self._get(base, "slots", default=4, cast=int)
        self._in_rate = self._get(base, "input_tokens_per_s", default=None, required=True, cast=float)
        self._out_rate = self._get(base, "output_tokens_per_s", default=None, required=True, cast=float)
        self._ingress_spec = str(self._get(base, "ingress", default="fifo", cast=str))
        if _lookup(base, "quantum")[0]:
            self._quantum = self._get(base, "quantum", default=None, cast=float)

    def elaborate(self) -> None:
        super().elaborate()  # wiring validation first (fail fast)
        if self._slots < 1:
            raise ComponentError(
                f"LLMEndpoint {self.full_path!r}: slots must be >= 1, "
                f"got {self._slots}"
            )
        for label, rate in (
            ("input_tokens_per_s", self._in_rate),
            ("output_tokens_per_s", self._out_rate),
        ):
            if rate <= 0:
                raise ComponentError(
                    f"LLMEndpoint {self.full_path!r}: {label} must be > 0, got {rate}"
                )
        self.server = SlotServer(
            self.env,
            f"{self.name}.server",
            slots=self._slots,
            cost_fn=self._service_time,
            ingress=self._ingress_spec,
            token_cap=self._in_rate + self._out_rate,
            quantum=self._quantum if self._quantum is not None else 0.01,
            sink=self._sink,
        )

    def start(self) -> None:
        super().start()
        if self.server is None:
            raise ComponentError(
                f"LLMEndpoint {self.full_path!r} started before elaborate()"
            )
        self.server.start()

    def collect(self) -> Dict[str, Any]:
        assert self.server is not None
        requests: List[Dict[str, Any]] = []
        total_in = 0.0
        total_out = 0.0
        for rec in self.server.records:
            sreq = self._sreq_by_rid.get(rec.rid)
            params = sreq.params if sreq is not None else {}
            in_tok = float(params.get("input_tokens", rec.tokens))
            out_tok = float(params.get("output_tokens", 0.0))
            total_in += in_tok
            total_out += out_tok
            requests.append(
                {
                    "agent_id": params.get("agent_id"),
                    "request_id": params.get("request_id"),
                    "input_tokens": in_tok,
                    "output_tokens": out_tok,
                    "submitted_at": rec.arrival,
                    "started_at": rec.start,
                    "finished_at": rec.end,
                    "queue_time": rec.queue_time,
                    "service_time": rec.service_time,
                }
            )
        return {
            "requests": requests,
            "served": len(self.server.records),
            "input_tokens": total_in,
            "output_tokens": total_out,
            "slots": self._slots,
            "input_tokens_per_s": self._in_rate,
            "output_tokens_per_s": self._out_rate,
        }

    # ------------------------------------------------------------------ #
    # caller-facing serving API                                           #
    # ------------------------------------------------------------------ #
    def submit(self, request: Any) -> Any:
        """Admit one contract request; returns the event to ``yield``.

        The request must carry the wired contract attributes
        ``{input_tokens, output_tokens, agent_id, request_id}``.
        """
        if self.server is None:
            raise ComponentError(
                f"LLMEndpoint {self.full_path!r} used before elaborate()"
            )
        in_tok = self._token_field(request, "input_tokens")
        out_tok = self._token_field(request, "output_tokens")
        agent_id = self._attr(request, "agent_id")
        request_id = self._attr(request, "request_id")

        shaped = Request(
            params={
                "input_tokens": in_tok,
                "output_tokens": out_tok,
                "agent_id": agent_id,
                "request_id": request_id,
            },
            tokens=in_tok + out_tok,  # total tokens against the aggregate cap
        )
        self._by_rid[shaped.rid] = request
        self._rid_of[id(request)] = shaped.rid
        self._sreq_by_rid[shaped.rid] = shaped

        self._emit(request_id, agent_id, "submitted")
        ev = self.server.submit(shaped)
        # queued = admitted and awaiting a slot (duration may be zero when a
        # slot is free at the same instant)
        self._emit(request_id, agent_id, "queued")
        self.env.process(self._completion(shaped))
        return ev

    def wait(self, request: Any) -> Any:
        """The event fulfilled with the request's ServiceRecord."""
        rid = self._rid_of.get(id(request))
        if rid is None or rid not in self._sreq_by_rid:
            raise ComponentError(
                f"LLMEndpoint {self.full_path!r}: wait() on an untracked request"
            )
        return self._sreq_by_rid[rid].done

    def response_of(self, request: Any) -> LLMResponse:
        """The :class:`LLMResponse` for a completed request."""
        rid = self._rid_of.get(id(request))
        rec = self._rec_by_rid.get(rid) if rid is not None else None
        if rec is None:
            raise ComponentError(
                f"LLMEndpoint {self.full_path!r}: request has not completed; "
                f"yield wait(request) first"
            )
        params = self._sreq_by_rid[rid].params
        return LLMResponse(
            request_id=params["request_id"],
            agent_id=params["agent_id"],
            input_tokens=params["input_tokens"],
            output_tokens=params["output_tokens"],
            queue_time=rec.queue_time,
            service_time=rec.service_time,
            start=rec.start,
            end=rec.end,
        )

    @property
    def records(self) -> List[ServiceRecord]:
        """Raw per-request ServiceRecords (proxy to the SlotServer)."""
        assert self.server is not None
        return self.server.records

    # ------------------------------------------------------------------ #
    # internals                                                           #
    # ------------------------------------------------------------------ #
    def _service_time(self, shaped: Request) -> float:
        """Phase floor cost_fn: called exactly when a slot is acquired."""
        params = shaped.params
        in_tok = float(params["input_tokens"])
        out_tok = float(params["output_tokens"])
        self._emit(params["request_id"], params["agent_id"], "processing")
        self._sink.count(
            f"{self.name}.tokens_input",
            in_tok,
            agent_id=params["agent_id"],
            request_id=params["request_id"],
        )
        return in_tok / self._in_rate + out_tok / self._out_rate

    def _completion(self, shaped: Request) -> Iterator[Any]:
        rec = yield shaped.done  # fulfilled with the ServiceRecord
        params = shaped.params
        self._emit(params["request_id"], params["agent_id"], "completed")
        self._sink.count(
            f"{self.name}.tokens_output",
            float(params["output_tokens"]),
            agent_id=params["agent_id"],
            request_id=params["request_id"],
        )
        self._rec_by_rid[shaped.rid] = rec

    def _emit(self, request_id: Any, agent_id: Any, state: str) -> None:
        self._sink.edge(
            f"{self.name}.{state}",
            state,
            agent_id=agent_id,
            request_id=request_id,
            state=state,
        )

    def _attr(self, request: Any, name: str) -> Any:
        value = getattr(request, name, _MISSING)
        if value is _MISSING:
            params = getattr(request, "params", None)
            if isinstance(params, Mapping) and name in params:
                value = params[name]
        if value is _MISSING:
            missing = self._missing_attrs(request)
            raise ComponentError(
                f"LLMEndpoint {self.full_path!r}: request is missing required "
                f"attribute(s) {missing}; contract attrs are {list(_REQUEST_ATTRS)}"
            )
        return value

    def _missing_attrs(self, request: Any) -> List[str]:
        missing = []
        for attr in _REQUEST_ATTRS:
            found = getattr(request, attr, _MISSING)
            if found is not _MISSING:
                continue
            params = getattr(request, "params", None)
            if isinstance(params, Mapping) and attr in params:
                continue
            missing.append(attr)
        return missing

    def _token_field(self, request: Any, name: str) -> float:
        value = self._attr(request, name)
        try:
            tokens = float(value)
        except (TypeError, ValueError):
            raise ComponentError(
                f"LLMEndpoint {self.full_path!r}: {name} must be a number, got "
                f"{value!r}"
            ) from None
        if tokens < 0:
            raise ComponentError(
                f"LLMEndpoint {self.full_path!r}: {name} must be >= 0, got {tokens}"
            )
        return tokens

    def _get(
        self,
        base: Any,
        key: str,
        default: Any,
        *,
        required: bool = False,
        cast: Any = None,
    ) -> Any:
        found, value = _lookup(base, key)
        if not found or value is None:
            if required:
                raise ComponentError(
                    f"LLMEndpoint {self.full_path!r}: missing required config "
                    f"{CFG_SECTION}.{key}"
                )
            return default
        if cast is not None:
            try:
                value = cast(value)
            except (TypeError, ValueError):
                raise ComponentError(
                    f"LLMEndpoint {self.full_path!r}: bad value for "
                    f"{CFG_SECTION}.{key}: {value!r}"
                ) from None
        return value
