"""AgentClient: N closed-loop agents on the simkit kernel.

Each agent walks one *workflow template* — an ordered sequence of
``inference``/``db`` steps — continuously for the whole simulated duration,
looping back to step 0 after the last entry (closed loop over the sequence).
Without ``workflow:`` config every agent uses the legacy alternating template
``[inference, db]``, which reproduces exactly the v1 loop::

    submit inference request (input_tokens, output_tokens)
      -> wait for the inference response
      -> token generator derives the DB hit (QueryProfile draw)
      -> submit DB query (complexity, volume)
      -> wait for the DB response
      -> (repeat, or continue with the next template step)

Ports (typed per the ticket-03 contract; kinds ``req`` / ``response``):

- outputs ``inference_requests``, ``db_queries``
- inputs  ``inference_responses``, ``db_responses``

Wiring contract (what a model-assembly ticket does):

- declarative: ``graph.connect(agent_client, "inference_requests", llm, <in>)``
  and ``graph.connect(llm, <out>, agent_client, "inference_responses")`` (same
  for the db side). Every serving side exposes the uniform
  ``submit(req) -> event`` admission shape (simkit Ingress/SlotServer).
- runtime tx: ``agent_client.bind_output(port, endpoint.submit)`` — the agent
  yields the returned admission event (an endpoint whose ingress is ``none``
  rendezvous back; ``fifo``/``priority`` admit immediately).
- runtime rx: the serving component delivers a completed request with
  ``agent_client.deliver(port, req)`` (alias ``submit_response``); it returns
  an event to yield. Delivery routes ``req.params["agent_id"]`` into that
  agent's per-port rendezvous buffer — an :class:`IngressNone` per
  (agent, input port); the agent observes the response from
  ``buffer.get()`` and that observation is the ``received`` transition.
  Delivery order is exact even with all agents sharing one client, because
  each (agent, port) pair buffers separately.

Lifecycle trace events: every send/observation is one probe *edge* sample on
the probe sink, signal name ``{component}.{stage}`` with component = the
agent's unique id (``{client.full_path}.agent-{i}``) and stage ``inference``
or ``db``; tags are ``{agent_id, request_id, parent_id, state,
workflow_template, workflow_step}`` and the sample value is ``state``
(``submitted`` at send, ``received`` when the agent observes the result).
``workflow_template`` is the assigned template's name (``inference_db`` for
the legacy default), ``workflow_step`` the 0-based index of the step inside
the template's sequence (it resets to 0 on each wrap-around), and
``initial_step`` the agent's first-request kind (``llm``|``db``: the config
draw when ``initial_step:`` is configured, else the template's step-0 kind).
Simulated
time is passed explicitly (``t=``), so emission is correct whatever the
sink's clock. ``parent_id`` links a DB query to the inference request whose
output produced it (None for inference requests). Requests carry their ids
in ``req.params`` (plus ``workflow_template``/``workflow_step``) so every
downstream hop inherits them; ``req.tokens`` is ``input_tokens +
output_tokens`` for inference requests (what an aggregate token/s cap
throttles) and ``0`` for DB queries (volume is bandwidth, not tokens).

Config: the component reads its subtree ``cfg[cfg_key]`` (``cfg_key`` defaults
to the component name; OmegaConf layers work like plain mappings)::

    agents:
      count: 4
      input_tokens:      # distribution-spec (see TOKEN_DISTRIBUTIONS)
        distribution: fixed
        value: 256
      output_tokens:
        distribution: gamma
        shape: 2.0
        scale: 120.0
      query_profiles:    # weighted complexity x volume classes
        - {name: point, complexity: 0.25, volume: 0.05, weight: 0.8}
        - {name: scan,  complexity: 4.0,  volume: 32.0, weight: 0.2}
      workflow:          # optional; omitted = legacy [inference, db] loop
        templates:       # ordered steps, drawn from {inference, db}
          - {name: simple_qa, steps: [inference, db]}
          - {name: multi_hop, steps: [inference, db, inference, db]}
        mix: [0.5, 0.5]  # per-agent draw weights; default uniform
      initial_step:      # optional; absent = every agent starts at step 0
        llm: 0.6         # probability the agent's first request is an LLM call
        db: 0.4          # probability the agent's first request is a DB query

Each agent draws ONE template from the mix at construct time (its own named
RNG child stream, see below) and runs it closed-loop. With ``initial_step:``
each agent additionally draws one first-request kind (``llm``/``db``) at
construct time, also from its own named child stream; that draw picks the
STARTING INDEX of the template (the first step whose kind matches), so the
first request is submitted at t=0 (no long common LLM head on every agent).
Weights are ``>= 0``, must sum to 1 (tolerance 1e-9), and either key may be
omitted only if the other is 1.0; both missing is the same as the key absent.

RNG: no global random anywhere. Every agent gets four named child streams
derived deterministically from ``seed/full_path/agent-{i}/{tokens,queries,
workflow,initial_step}`` (the same derivation scheme as the simkit Component
base), so per-agent token, profile, template and first-step draws are
reproducible and mutually independent.
"""

from __future__ import annotations

import itertools
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from agent_db_sim.simkit.component import Component, master_seed
from agent_db_sim.simkit.errors import ComponentError, WiringError
from agent_db_sim.simkit.ingress import IngressNone, Request
from agent_db_sim.simkit.probes import NULL_SINK, ProbeSink

#: rendezvous buffers are per-agent plumbing; their transient depth is not a
#: metric anyone consumes, so they do not pollute the shared probe sink
_NO_INGRESS_PROBE = NULL_SINK

__all__ = [
    "AgentClient",
    "TokenGenerator",
    "TokenSampler",
    "TOKEN_DISTRIBUTIONS",
    "QueryProfile",
    "ProfileMix",
    "build_mix",
    "parse_token_generator",
    "parse_query_profiles",
    "WorkflowTemplate",
    "WorkflowMix",
    "parse_workflow",
    "DEFAULT_WORKFLOW_NAME",
    "InitialStepWeights",
    "parse_initial_step",
]

TokenSampler = Callable[[random.Random, Mapping[str, Any], str], int]

_TX_PORTS = ("inference_requests", "db_queries")
_RX_PORTS = ("inference_responses", "db_responses")


# --------------------------------------------------------------------------- #
# token distributions (named, seeded; no global random)                       #
# --------------------------------------------------------------------------- #

def _finite(value: Any, what: str, param: str) -> float:
    try:
        x = float(value)
    except (TypeError, ValueError):
        raise ComponentError(f"{what}: parameter {param!r} must be a number, got {value!r}")
    if not math.isfinite(x):
        raise ComponentError(f"{what}: parameter {param!r} must be finite, got {value!r}")
    return x


def _positive(x: float, what: str, param: str) -> float:
    if x <= 0:
        raise ComponentError(f"{what}: parameter {param!r} must be > 0, got {x}")
    return x


def _tokens_at_least_one(x: float, what: str) -> int:
    """Token counts are integers >= 1 for inference requests (0 would opt a
    request out of an aggregate token cap, which inference never does)."""
    if x < 0:
        raise ComponentError(f"{what}: sampled token value {x} < 0")
    return max(1, int(round(x)))


def _sample_fixed(rng: random.Random, spec: Mapping[str, Any], what: str) -> int:
    value = _finite(spec["value"], what, "value")
    # fixed draws follow the same >= 1 integer clamp as every other distribution
    return _tokens_at_least_one(value, what)


def _sample_uniform(rng: random.Random, spec: Mapping[str, Any], what: str) -> int:
    low = _finite(spec["low"], what, "low")
    high = _finite(spec["high"], what, "high")
    if low < 1 or high < 1 or high < low:
        raise ComponentError(
            f"{what}: uniform needs 1 <= low <= high, got low={low} high={high}"
        )
    return rng.randint(int(round(low)), int(round(high)))


def _sample_geometric(rng: random.Random, spec: Mapping[str, Any], what: str) -> int:
    p = _finite(spec["p"], what, "p")
    if not 0.0 < p <= 1.0:
        raise ComponentError(f"{what}: geometric needs 0 < p <= 1, got p={p}")
    if p == 1.0:
        return 1
    u = 1.0 - rng.random()  # support: trials until first success, i.e. >= 1
    return max(1, int(math.ceil(math.log(u) / math.log1p(-p))))


def _sample_exponential(rng: random.Random, spec: Mapping[str, Any], what: str) -> int:
    rate = _positive(_finite(spec["rate"], what, "rate"), what, "rate")
    return _tokens_at_least_one(rng.expovariate(rate), what)


def _sample_gamma(rng: random.Random, spec: Mapping[str, Any], what: str) -> int:
    shape = _positive(_finite(spec["shape"], what, "shape"), what, "shape")
    scale = _positive(_finite(spec["scale"], what, "scale"), what, "scale")
    return _tokens_at_least_one(rng.gammavariate(shape, scale), what)


def _sample_normal(rng: random.Random, spec: Mapping[str, Any], what: str) -> int:
    mean = _finite(spec["mean"], what, "mean")
    sigma = _positive(_finite(spec["sigma"], what, "sigma"), what, "sigma")
    return _tokens_at_least_one(rng.gauss(mean, sigma), what)


def _sample_lognormal(rng: random.Random, spec: Mapping[str, Any], what: str) -> int:
    mean = _finite(spec["mean"], what, "mean")
    sigma = _positive(_finite(spec["sigma"], what, "sigma"), what, "sigma")
    return _tokens_at_least_one(rng.lognormvariate(mean, sigma), what)


#: name -> (sampler, required spec keys); bounds are checked in the samplers.
#: Keys are the YAML ``distribution:`` values; unknown names fail fast at
#: construct.
TOKEN_DISTRIBUTIONS: Dict[str, Tuple[TokenSampler, Sequence[str]]] = {
    "fixed": (_sample_fixed, ("value",)),
    "uniform": (_sample_uniform, ("low", "high")),
    "geometric": (_sample_geometric, ("p",)),
    "exponential": (_sample_exponential, ("rate",)),
    "gamma": (_sample_gamma, ("shape", "scale")),
    "normal": (_sample_normal, ("mean", "sigma")),
    "lognormal": (_sample_lognormal, ("mean", "sigma")),
}


@dataclass(frozen=True)
class TokenGenerator:
    """One draw of a token count: a named distribution + its parameters.

    Configuration forms: ``{"distribution": <name>, <params...>}`` or a bare
    number (shorthand for ``fixed``).
    """

    name: str
    spec: Mapping[str, Any]
    sampler: TokenSampler
    what: str = "token generator"

    def sample(self, rng: random.Random) -> int:
        """Draw one integer token count (>= 1) from the named distribution."""
        return int(self.sampler(rng, self.spec, self.what))


def parse_token_generator(spec: Any, *, what: str) -> TokenGenerator:
    """Config -> TokenGenerator (fail fast on unknown names/parameters)."""
    if isinstance(spec, (int, float)):  # shorthand: `output_tokens: 128`
        spec = {"distribution": "fixed", "value": spec}
    name = spec.get("distribution") if isinstance(spec, Mapping) else None
    if not isinstance(name, str):
        raise ComponentError(
            f"{what}: needs a 'distribution:' key naming one of "
            f"{sorted(TOKEN_DISTRIBUTIONS)}, got {spec!r}"
        )
    entry = TOKEN_DISTRIBUTIONS.get(name)
    if entry is None:
        available = ", ".join(sorted(TOKEN_DISTRIBUTIONS))
        raise ComponentError(
            f"{what}: unknown token distribution {name!r}; available: {available}"
        )
    sampler, required_params = entry
    frozen = dict(spec)
    missing = [p for p in required_params if p not in frozen]
    if missing:
        raise ComponentError(
            f"{what}: distribution {name!r} missing parameter(s) {', '.join(missing)}"
        )
    return TokenGenerator(name=name, spec=frozen, sampler=sampler, what=what)


# --------------------------------------------------------------------------- #
# QueryProfile: complexity x volume classes and their weighted mix            #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class QueryProfile:
    """One (complexity, volume) class with its mix weight.

    ``complexity`` is the compute cost, ``volume`` the data touched (DB
    compute/storage/network parameterizations read them from
    ``req.params``); ``weight`` is the draw probability within the mix (all
    weights are normalized to 1).
    """

    name: str
    complexity: float
    volume: float
    weight: float


def parse_query_profiles(spec: Any, *, what: str = "query_profiles") -> List[QueryProfile]:
    """Config list -> validated QueryProfiles (fail fast)."""
    if not isinstance(spec, Sequence) or isinstance(spec, (str, bytes)):
        raise ComponentError(f"{what}: must be a list of profile entries, got {spec!r}")
    if len(spec) == 0:
        raise ComponentError(f"{what}: needs at least one profile entry")

    profiles: List[QueryProfile] = []
    seen: set = set()
    for i, entry in enumerate(spec):
        where = f"{what}[{i}]"
        if not isinstance(entry, Mapping):
            raise ComponentError(f"{where}: must be a mapping of name/complexity/volume/weight")
        missing = [k for k in ("name", "complexity", "volume", "weight") if k not in entry]
        if missing:
            raise ComponentError(f"{where}: missing key(s) {', '.join(missing)}")
        name = entry["name"]
        if not isinstance(name, str) or not name:
            raise ComponentError(f"{where}: 'name' must be a non-empty string, got {name!r}")
        if name in seen:
            raise ComponentError(f"{what}: duplicate profile name {name!r}")
        seen.add(name)
        complexity = _finite(entry["complexity"], where, "complexity")
        volume = _finite(entry["volume"], where, "volume")
        weight = _finite(entry["weight"], where, "weight")
        if complexity < 0 or volume < 0:
            raise ComponentError(
                f"{where}: complexity and volume must be >= 0 "
                f"(complexity={complexity}, volume={volume})"
            )
        if weight < 0:
            raise ComponentError(f"{where}: weight must be >= 0, got {weight}")
        profiles.append(
            QueryProfile(name=name, complexity=complexity, volume=volume, weight=weight)
        )

    total = sum(p.weight for p in profiles)
    if total <= 0:
        raise ComponentError(f"{what}: at least one weight must be > 0 (sum={total})")
    return profiles


@dataclass(frozen=True)
class ProfileMix:
    """Normalized weighted mix over QueryProfiles; ``draw`` is inverse-CDF."""

    profiles: Tuple[QueryProfile, ...]
    _edges: Tuple[Tuple[float, QueryProfile], ...] = field(repr=False)

    def draw(self, rng: random.Random) -> QueryProfile:
        """Draw one profile class (deterministic per the passed RNG stream)."""
        x = rng.random()
        for threshold, profile in self._edges:
            if x < threshold:
                return profile
        return self._edges[-1][1]  # x is in [0, 1); defensive fallthrough

    def names(self) -> Tuple[str, ...]:
        return tuple(p.name for p in self.profiles)


def build_mix(spec: Any, *, what: str = "query_profiles") -> ProfileMix:
    profiles = parse_query_profiles(spec, what=what)
    total = sum(p.weight for p in profiles)
    acc = 0.0
    edges: List[Tuple[float, QueryProfile]] = []
    for profile in profiles:
        acc += profile.weight / total
        edges.append((acc, profile))
    edges[-1] = (1.0, edges[-1][1])  # guard against FP drift at the tail
    return ProfileMix(profiles=tuple(profiles), _edges=tuple(edges))


# --------------------------------------------------------------------------- #
# WorkflowTemplate: named per-agent loop shapes and their weighted mix         #
# --------------------------------------------------------------------------- #

_WORKFLOW_STEPS = ("inference", "db")

#: the implicit template every agent gets without ``workflow:`` config —
#: the legacy v1 alternating loop, as a round-trip through the generic machinery
DEFAULT_WORKFLOW_NAME = "inference_db"
_DEFAULT_WORKFLOW_STEPS: Tuple[str, ...] = ("inference", "db")


@dataclass(frozen=True)
class WorkflowTemplate:
    """One named agent loop shape: an ordered sequence of steps.

    Steps are drawn from exactly ``inference | db``; the agent walks the
    sequence from the top and loops back to step 0 after the last entry
    (closed loop over the sequence).
    """

    name: str
    steps: Tuple[str, ...]


@dataclass(frozen=True)
class WorkflowMix:
    """Normalized weighted mix over WorkflowTemplates; ``draw`` is inverse-CDF."""

    templates: Tuple[WorkflowTemplate, ...]
    _edges: Tuple[Tuple[float, WorkflowTemplate], ...] = field(repr=False)

    def draw(self, rng: random.Random) -> WorkflowTemplate:
        """Draw one template (deterministic per the passed RNG stream)."""
        x = rng.random()
        for threshold, template in self._edges:
            if x < threshold:
                return template
        return self._edges[-1][1]  # x is in [0, 1); defensive fallthrough

    def names(self) -> Tuple[str, ...]:
        return tuple(t.name for t in self.templates)


def _parse_workflow_template(entry: Any, where: str) -> WorkflowTemplate:
    if not isinstance(entry, Mapping):
        raise ComponentError(f"{where}: must be a mapping of name/steps, got {entry!r}")
    missing = [k for k in ("name", "steps") if k not in entry]
    if missing:
        raise ComponentError(f"{where}: missing key(s) {', '.join(missing)}")
    name = entry["name"]
    if not isinstance(name, str) or not name:
        raise ComponentError(f"{where}: 'name' must be a non-empty string, got {name!r}")
    steps = entry["steps"]
    if (
        not isinstance(steps, Sequence)
        or isinstance(steps, (str, bytes))
        or len(steps) == 0
    ):
        raise ComponentError(f"{where}: 'steps' must be a non-empty list of steps")
    for i, step in enumerate(steps):
        if step not in _WORKFLOW_STEPS:
            raise ComponentError(
                f"{where}.steps[{i}]: must be one of {', '.join(_WORKFLOW_STEPS)}, "
                f"got {step!r}"
            )
    return WorkflowTemplate(name=name, steps=tuple(steps))


def parse_workflow(spec: Any, *, what: str = "workflow") -> WorkflowMix:
    """Config ``agents.workflow`` (or ``None``) -> validated WorkflowMix.

    ``None`` (no ``workflow:`` key at all) yields the single legacy template
    ``[inference, db]`` with weight 1, i.e. exactly the v1 alternating loop.
    ``mix`` is optional; omitted weights default to uniform.
    """
    if spec is None:
        default = WorkflowTemplate(
            name=DEFAULT_WORKFLOW_NAME, steps=_DEFAULT_WORKFLOW_STEPS
        )
        return WorkflowMix(templates=(default,), _edges=((1.0, default),))
    if not isinstance(spec, Mapping):
        raise ComponentError(
            f"{what}: must be a mapping with a 'templates:' list, got {spec!r}"
        )
    templates_spec = spec.get("templates")
    if (
        not isinstance(templates_spec, Sequence)
        or isinstance(templates_spec, (str, bytes))
        or len(templates_spec) == 0
    ):
        raise ComponentError(f"{what}: 'templates' must be a non-empty list of templates")
    templates: List[WorkflowTemplate] = []
    seen: set = set()
    for i, entry in enumerate(templates_spec):
        template = _parse_workflow_template(entry, f"{what}.templates[{i}]")
        if template.name in seen:
            raise ComponentError(f"{what}: duplicate template name {template.name!r}")
        seen.add(template.name)
        templates.append(template)

    mix_spec = spec.get("mix")
    weights: List[float]
    if mix_spec is None:
        weights = [1.0 / len(templates)] * len(templates)
    else:
        if (
            not isinstance(mix_spec, Sequence)
            or isinstance(mix_spec, (str, bytes))
            or len(mix_spec) != len(templates)
        ):
            raise ComponentError(
                f"{what}: 'mix' must be a list of {len(templates)} weights "
                f"(one per template), got {mix_spec!r}"
            )
        weights = []
        for i, weight in enumerate(mix_spec):
            try:
                w = float(weight)
            except (TypeError, ValueError):
                raise ComponentError(
                    f"{what}: mix[{i}] must be a number, got {weight!r}"
                )
            if not math.isfinite(w) or w < 0:
                raise ComponentError(
                    f"{what}: mix[{i}] must be a finite weight >= 0, got {w}"
                )
            weights.append(w)
    total = sum(weights)
    if total <= 0:
        raise ComponentError(
            f"{what}: at least one mix weight must be > 0 (sum={total})"
        )

    acc = 0.0
    edges: List[Tuple[float, WorkflowTemplate]] = []
    for template, weight in zip(templates, weights):
        acc += weight / total
        edges.append((acc, template))
    edges[-1] = (1.0, edges[-1][1])  # guard against FP drift at the tail
    return WorkflowMix(templates=tuple(templates), _edges=tuple(edges))


# --------------------------------------------------------------------------- #
# initial_step: weights for the per-agent first-request kind draw              #
# --------------------------------------------------------------------------- #

_INITIAL_STEP_KINDS = {"llm": "inference", "db": "db"}


@dataclass(frozen=True)
class InitialStepWeights:
    """Draw weights for an agent's first-request kind.

    ``llm``/``db`` are probabilities (``>= 0``, summing to 1 within 1e-9);
    the draw selects the STARTING INDEX of the agent's workflow template
    (the first step whose kind matches: ``llm`` -> ``inference``, ``db`` ->
    ``db``), so the first request goes out at t=0.
    """

    llm: float
    db: float


def parse_initial_step(
    spec: Any, *, what: str = "initial_step"
) -> Optional[InitialStepWeights]:
    """Config ``agents.initial_step`` (or ``None``) -> validated weights.

    ``None`` (key absent) yields ``None``: every agent starts at template
    step 0. An empty mapping is the same as the key absent. Either key may
    be omitted only if the other is 1.0 (it then defaults to 0.0).
    """
    if spec is None:
        return None
    if not isinstance(spec, Mapping):
        raise ComponentError(
            f"{what}: must be a mapping with 'llm'/'db' weights, got {spec!r}"
        )
    unknown = [k for k in spec.keys() if k not in _INITIAL_STEP_KINDS]
    if unknown:
        raise ComponentError(
            f"{what}: unknown key(s) {', '.join(str(k) for k in unknown)}; "
            f"only 'llm' and 'db' are allowed"
        )
    present = {k: spec[k] for k in ("llm", "db") if k in spec}
    if not present:
        return None  # both missing == the key absent
    weights: Dict[str, float] = {}
    for key, value in present.items():
        w = _finite(value, what, key)
        if w < 0:
            raise ComponentError(f"{what}: {key!r} weight must be >= 0, got {w}")
        weights[key] = w
    total = sum(weights.values())
    if abs(total - 1.0) > 1e-9:
        raise ComponentError(
            f"{what}: weights must sum to 1 (tolerance 1e-9), "
            f"got sum={total} from {present}"
        )
    return InitialStepWeights(llm=weights.get("llm", 0.0), db=weights.get("db", 0.0))


# --------------------------------------------------------------------------- #
# AgentClient component                                                       #
# --------------------------------------------------------------------------- #

@dataclass
class _AgentWorker:
    """Per-agent loop state (one closed-loop process in the AgentClient)."""

    index: int
    agent_id: str
    template: WorkflowTemplate
    tokens_rng: random.Random
    profile_rng: random.Random
    workflow_rng: random.Random  # draws the template once, at construct
    initial_step: str = "llm"  # first-request kind ('llm'|'db'), drawn at construct
    start_index: int = 0  # template index the closed loop starts at
    boxes: Dict[str, IngressNone] = field(default_factory=dict)
    seq: Any = None  # itertools.count incremented lazily in start()
    counters: Dict[str, int] = field(
        default_factory=lambda: {
            "inference_submitted": 0,
            "inference_received": 0,
            "db_submitted": 0,
            "db_received": 0,
        }
    )


class AgentClient(Component):
    """N closed-loop agents over the four typed workload ports.

    See the module docstring for the wiring contract and config schema.
    """

    input_ports = {
        "inference_responses": "response",
        "db_responses": "response",
    }
    output_ports = {
        "inference_requests": "req",
        "db_queries": "req",
    }

    def __init__(self, name: str, *, cfg_key: Optional[str] = None) -> None:
        super().__init__(name)
        self._cfg_key = cfg_key if cfg_key is not None else str(name)
        self.sink: ProbeSink = NULL_SINK
        self._tx: Dict[str, Callable[[Request], Any]] = {}
        self._workers: List[_AgentWorker] = []
        self._boxes_by_agent: Dict[str, Dict[str, IngressNone]] = {}
        self._count: int = 0
        self._input_gen: Optional[TokenGenerator] = None
        self._output_gen: Optional[TokenGenerator] = None
        self._mix: Optional[ProfileMix] = None
        self._workflow: Optional[WorkflowMix] = None
        self._initial_step: Optional[InitialStepWeights] = None

    # ------------------------------------------------------------------ #
    # wiring helpers (called between construct and elaborate)             #
    # ------------------------------------------------------------------ #
    def attach_sink(self, sink: ProbeSink) -> "AgentClient":
        """Point the lifecycle trace events at ``sink`` (call before construct)."""
        self.sink = sink
        return self

    def bind_output(self, port: str, handler: Callable[[Request], Any]) -> "AgentClient":
        """Runtime transport for one output port.

        ``handler(req)`` must return the event to yield — the uniform
        ``server.submit(req)`` admission shape (simkit Ingress/SlotServer).
        Full fail-fast binding checks run at elaboration.
        """
        if port not in _TX_PORTS:
            raise WiringError(
                f"{self.name}: no output port {port!r} "
                f"(declared outputs: {', '.join(_TX_PORTS)})"
            )
        if not callable(handler):
            raise WiringError(
                f"{self.name}: output port {port!r} transport must be callable"
            )
        self._tx[port] = handler
        return self

    # ------------------------------------------------------------------ #
    # response-side API (called by serving components in simulated time)  #
    # ------------------------------------------------------------------ #
    def deliver(self, port: str, req: Request) -> Any:
        """Deliver a completed response; returns the event the sender yields.

        Routes by ``req.params['agent_id']`` into that agent's per-port
        rendezvous buffer (``IngressNone``): the submit blocks through the
        event until the waiting agent takes the response — the agent's
        ``received`` trace transition is exactly that handoff.
        """
        if port not in in_ports(self):
            raise WiringError(
                f"{self.name}: no input port {port!r} "
                f"(declared inputs: {', '.join(in_ports(self))})"
            )
        params = req.params if isinstance(req.params, Mapping) else {}
        agent_id = params.get("agent_id")
        if agent_id is None:
            raise ComponentError(
                f"{self.name}: response on {port!r} without 'agent_id' in "
                f"request params (request {getattr(req, 'rid', '?')})"
            )
        boxes = self._boxes_by_agent.get(str(agent_id))
        if boxes is None:
            known = ", ".join(sorted(self._boxes_by_agent)) or "<none>"
            raise ComponentError(
                f"{self.name}: response on {port!r} for unknown agent_id "
                f"{agent_id!r} (agents: {known})"
            )
        return boxes[port].submit(req)

    #: serving-side alias for ``deliver``
    submit_response = deliver

    # ------------------------------------------------------------------ #
    # introspection                                                       #
    # ------------------------------------------------------------------ #
    @property
    def agent_ids(self) -> Tuple[str, ...]:
        """Unique ids of the closed-loop agents (in construction order)."""
        return tuple(w.agent_id for w in self._workers)

    @property
    def count(self) -> int:
        return self._count

    @property
    def query_profiles(self) -> Tuple[QueryProfile, ...]:
        """The configured complexity x volume classes (validated at construct)."""
        return self._mix.profiles if self._mix is not None else ()

    @property
    def workflow_templates(self) -> Tuple[str, ...]:
        """Names of the configured workflow templates (``inference_db`` for
        the legacy default when ``workflow:`` is omitted)."""
        return self._workflow.names() if self._workflow is not None else ()

    @property
    def agent_templates(self) -> Tuple[str, ...]:
        """The workflow template drawn per agent (in construction order).

        One draw per agent at construct time, from the named-mix RNG stream.
        """
        return tuple(w.template.name for w in self._workers)

    @property
    def agent_initial_steps(self) -> Tuple[str, ...]:
        """The first-request kind per agent (``'llm'``|``'db'``, in
        construction order).

        Drawn once per agent at construct time, from the named
        ``initial_step`` RNG stream. Without an ``initial_step:`` config
        this is the kind of the template's step 0 (``'llm'`` for an
        ``inference`` step, ``'db'`` otherwise).
        """
        return tuple(w.initial_step for w in self._workers)

    # ------------------------------------------------------------------ #
    # lifecycle                                                           #
    # ------------------------------------------------------------------ #
    def construct(self, env: Any, cfg: Any) -> None:
        super().construct(env, cfg)
        section = _read_section(self.cfg, self._cfg_key, self.full_path)
        self._configure(section)
        seed = master_seed(cfg)
        for index in range(self._count):
            agent_id = f"{self.full_path}.agent-{index}"
            # the template draw is deterministic per agent and independent of
            # the token/profile streams (separate named child stream)
            workflow_rng = random.Random(f"{seed}/{agent_id}/workflow")
            template = self._workflow.draw(workflow_rng)  # type: ignore[union-attr]
            # the first-request kind draw is likewise a separate named child
            # stream (independent of workflow/tokens/queries); it selects the
            # STARTING INDEX of the template so the first submit lands at t=0
            if self._initial_step is None:
                initial_step = "llm" if template.steps[0] == "inference" else "db"
                start_index = 0
            else:
                initial_step_rng = random.Random(f"{seed}/{agent_id}/initial_step")
                initial_step = (
                    "llm"
                    if initial_step_rng.random() < self._initial_step.llm
                    else "db"
                )
                step_kind = _INITIAL_STEP_KINDS[initial_step]
                matches = [
                    i for i, step in enumerate(template.steps) if step == step_kind
                ]
                if not matches:
                    raise ComponentError(
                        f"{self.full_path}: agent {agent_id!r} initial_step is "
                        f"{initial_step!r} but workflow template {template.name!r} "
                        f"has no '{step_kind}' step "
                        f"(steps: {', '.join(template.steps)})"
                    )
                start_index = matches[0]
            boxes = {
                port: IngressNone(env, name=f"{agent_id}.{port}", sink=_NO_INGRESS_PROBE)
                for port in _RX_PORTS
            }
            worker = _AgentWorker(
                index=index,
                agent_id=agent_id,
                template=template,
                tokens_rng=random.Random(f"{seed}/{agent_id}/tokens"),
                profile_rng=random.Random(f"{seed}/{agent_id}/queries"),
                workflow_rng=workflow_rng,
                initial_step=initial_step,
                start_index=start_index,
                boxes=boxes,
            )
            self._workers.append(worker)
            self._boxes_by_agent[agent_id] = boxes

    def elaborate(self) -> None:
        super().elaborate()
        for port in _TX_PORTS:
            if port not in self._tx:
                raise WiringError(
                    f"{self.full_path}: output port {port!r} has no transport "
                    f"handler; call bind_output({port!r}, endpoint.submit) "
                    f"before elaboration"
                )

    def start(self) -> None:
        super().start()
        for worker in self._workers:
            worker.seq = itertools.count(1)
            self.env.process(self._closed_loop(worker))

    def collect(self) -> Dict[str, Any]:
        """Per-agent loop counters (submitted/received per the two ports).

        Keyed by agent id; a wrapping :class:`Composite` merges this under
        the AgentClient's own name, i.e. ``collected["agents"][agent_id]``.
        """
        return {
            worker.agent_id: dict(worker.counters) for worker in self._workers
        }

    # ------------------------------------------------------------------ #
    # internals                                                           #
    # ------------------------------------------------------------------ #
    def _configure(self, section: Any) -> None:
        what = f"agent client {self.name!r} config"
        if not isinstance(section, Mapping):
            raise ComponentError(f"{what}: must be a mapping, got {type(section)}")
        missing = [k for k in ("count", "input_tokens", "output_tokens", "query_profiles") if k not in section]
        if missing:
            raise ComponentError(f"{what}: missing key(s) {', '.join(missing)}")
        count = section["count"]
        try:
            count_int = int(count)
        except (TypeError, ValueError):
            raise ComponentError(f"{what}: 'count' must be an integer, got {count!r}")
        if count_int < 0:
            raise ComponentError(f"{what}: 'count' must be >= 0, got {count_int}")
        self._count = count_int
        self._input_gen = parse_token_generator(section["input_tokens"], what=f"{what} input_tokens")
        self._output_gen = parse_token_generator(section["output_tokens"], what=f"{what} output_tokens")
        self._mix = build_mix(section["query_profiles"], what=f"{what} query_profiles")
        # optional; None (key absent) = legacy [inference, db] loop for everyone
        workflow_spec = None
        if "workflow" in section:
            workflow_spec = section["workflow"]
        self._workflow = parse_workflow(workflow_spec, what=f"{what} workflow")
        # optional; None (key absent) = every agent starts at template step 0
        initial_step_spec = None
        if "initial_step" in section:
            initial_step_spec = section["initial_step"]
        self._initial_step = parse_initial_step(
            initial_step_spec, what=f"{what} initial_step"
        )

    def _closed_loop(self, worker: _AgentWorker) -> Any:
        """One agent's template-driven loop; runs until the sim ends.

        Walks ``worker.template.steps`` in order, looping back to step 0 after
        the last entry, for the whole simulated duration. A ``db`` step links
        to the most recent inference request (the one whose output produced
        it; ``None`` before the first inference of the run).
        """
        steps = worker.template.steps
        n_steps = len(steps)
        step_pos = worker.start_index  # first submit at t=0 from this step on
        last_inference_id: Optional[str] = None
        template_name = worker.template.name
        while True:
            step = steps[step_pos % n_steps]
            if step == "inference":
                n = next(worker.seq)
                request_id = f"{worker.agent_id}#{n}"
                input_tokens = self._input_gen.sample(worker.tokens_rng)
                output_tokens = self._output_gen.sample(worker.tokens_rng)
                request = Request(
                    params={
                        "kind": "inference",
                        "agent_id": worker.agent_id,
                        "request_id": request_id,
                        "parent_id": None,
                        "input_tokens": int(input_tokens),
                        "output_tokens": int(output_tokens),
                        "workflow_template": template_name,
                        "workflow_step": step_pos % n_steps,
                        "initial_step": worker.initial_step,
                    },
                    tokens=float(int(input_tokens) + int(output_tokens)),
                )
                worker.counters["inference_submitted"] += 1
                self._emit(
                    worker, "inference", "submitted", request_id,
                    parent_id=None, step=step_pos % n_steps,
                )
                yield self._tx["inference_requests"](request)

                yield worker.boxes["inference_responses"].get()
                worker.counters["inference_received"] += 1
                self._emit(
                    worker, "inference", "received", request_id,
                    parent_id=None, step=step_pos % n_steps,
                )
                last_inference_id = request_id
            else:  # "db"
                # ---- token generator derives the DB hit ----------------- #
                profile = self._mix.draw(worker.profile_rng)
                n = next(worker.seq)
                db_request_id = f"{worker.agent_id}#{n}"
                db_request = Request(
                    params={
                        "kind": "db_query",
                        "agent_id": worker.agent_id,
                        "request_id": db_request_id,
                        "parent_id": last_inference_id,
                        "complexity": profile.complexity,
                        "volume": profile.volume,
                        "profile": profile.name,
                        "workflow_template": template_name,
                        "workflow_step": step_pos % n_steps,
                        "initial_step": worker.initial_step,
                    },
                    tokens=0.0,  # DB volume is bandwidth, not LLM tokens
                )
                worker.counters["db_submitted"] += 1
                self._emit(
                    worker, "db", "submitted", db_request_id,
                    parent_id=last_inference_id, step=step_pos % n_steps,
                )
                yield self._tx["db_queries"](db_request)

                yield worker.boxes["db_responses"].get()
                worker.counters["db_received"] += 1
                self._emit(
                    worker, "db", "received", db_request_id,
                    parent_id=last_inference_id, step=step_pos % n_steps,
                )
            step_pos += 1

    def _emit(
        self,
        worker: _AgentWorker,
        stage: str,
        state: str,
        request_id: str,
        parent_id: Optional[str],
        step: int,
    ) -> None:
        """One lifecycle edge sample: {component}.{stage}, typed attrs, explicit t."""
        self.sink.edge(
            f"{worker.agent_id}.{stage}",
            state,
            t=float(self.env.now),
            agent_id=worker.agent_id,
            request_id=request_id,
            parent_id=parent_id,
            state=state,
            workflow_template=worker.template.name,
            workflow_step=step,
            initial_step=worker.initial_step,
        )


# --------------------------------------------------------------------------- #
# small local helpers                                                          #
# --------------------------------------------------------------------------- #

def in_ports(component: Component) -> Tuple[str, ...]:
    """Declared input port names of ``component`` (for error messages)."""
    return tuple(component.input_ports.keys())


def _read_section(cfg: Any, key: str, component: str) -> Any:
    """Read ``cfg[key]`` (works for OmegaConf layers and plain mappings)."""
    try:
        if key in cfg:
            return cfg[key]
    except TypeError:
        pass
    available: Any = "<unknown>"
    try:
        available = sorted(cfg.keys())
    except Exception:  # pragma: no cover - exotic config objects
        pass
    raise ComponentError(
        f"agent client {component!r}: missing config section '{key}: ' "
        f"(available keys: {available})"
    )
