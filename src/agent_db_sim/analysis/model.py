"""The analytical workload model: run config -> closed-chain stations.

Shape of our exact workload (from ``AgentClient``/``LLMEndpoint``/``DBEndpoint``):
a closed loop of N agents, each cycle = one LLM inference then one DB query,
no think time. The analyzer decomposes the cycle into M/M/c chain stations
(exponential service, means only) whose parameters come *only* from the
scenario config (configured distribution means / profile classes — never
trace data), plus the storage preset/override params on the DB side.

Model decisions (documented invariants):

- **DB compute slot held through the whole round-trip** (``PhaseSlotServer``
  semantics, ticket 05 option b): a query occupies its compute slot from
  admission through issue -> middle (net + storage legs) -> finalize. The DB
  is therefore modeled as ONE monolithic M/M/c station per entry-node slot
  pool (``c`` = total slots over the coordinators/entry nodes), with mean
  service ``tau_db = issue + middle_residence + finalize``. The middle
  residence carries the storage under shared-ceiling contention: a
  mean-field stretch factor ``s = max(1, k_bar)`` multiplies the pacing
  work (in-flight ops share the node budget; at concurrency ``k`` a
  lone-op time stretches to ``k x`` — exactly the earn-rate loop in
  ``StorageNode``; Little's law gives ``k_bar = X * mid / M``, iterated to
  a fixed point with the chain throughput). Net transfer service is fixed
  (``latency + volume/bandwidth``) and needs no stretch.

- **Aggregate LLM token cap**: modeled as a second bottleneck *inside the
  LLM station* (not as a separate chain node): pacing shares the
  aggregate cap across slot holders only (``SlotServer._pacing``), so when
  the LLM queue is chronically nonempty all ``c_l`` slots hold requests
  paced at ``cap / c_l`` tokens/s and a request's slot hold stretches to
  ``c_l * E[T] / cap``. The analyzer solves the unconstrained chain; if
  its token demand ``X * E[T]`` exceeds the cap, it re-solves with the
  stretched LLM service, reproducing the pinned cap goodput
  ``X = cap / E[T]``. This is the cap regime the sim's queue-based
  attribution is blind to (saturation shows on token rates, not queues).

- **Distributed topology**: coordinators front every query (each holds its
  slot for the whole service); the serving peer's slot is held while its
  local storage serves the middle. The peer wait is folded into
  ``tau_db`` via an Erlang-C wait term on the peer slot pool (mean-field,
  iterated with the same fixed point).

- Per-stage analytic means are *deterministic decompositions* of the
  monolithic residence (issue/middle/finalize legs by profile class mix);
  the sim's per-stage ``latency`` columns map onto them, and the station
  queue wait maps onto the sim's ``queue`` columns of the DB entry stage.

Supported scope: the legacy ``[inference, db]`` loop exactly; with
``agents.workflow`` templates, per-cycle visit counts are the mix-weighted
counts (exact only when every template alternates 1:1 — a note is issued
otherwise). ``phase_split: two_phase`` is approximated as issue folded
into the post-storage compute stage. Connections-bounded network pools
(``db.network.connections``) are not modeled (queue terms ignored).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from omegaconf import OmegaConf

from agent_db_sim.analysis.chain import (
    ChainSolution,
    Station,
    StationResult,
    erlang_c_wait,
    solve_chain,
)

__all__ = [
    "StageSpec",
    "StationModel",
    "TokenSpec",
    "build_model",
    "expected_tokens",
    "service_scv",
]


# --------------------------------------------------------------------------- #
# config shorthands                                                           #
# --------------------------------------------------------------------------- #

def _f(cfg: Any, key: str, default: float) -> float:
    try:
        value = OmegaConf.select(cfg, key)
    except Exception:  # pragma: no cover - exotic config objects
        value = None
    try:
        return float(default if value is None else value)
    except (TypeError, ValueError):
        return default


def _i(cfg: Any, key: str, default: int) -> int:
    return max(1, int(_f(cfg, key, float(default))))


def _s(cfg: Any, key: str, default: str) -> str:
    try:
        value = OmegaConf.select(cfg, key)
    except Exception:  # pragma: no cover
        value = None
    return default if value is None else str(value)


# --------------------------------------------------------------------------- #
# token means (configured distributions — same families as AgentClient)       #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class TokenSpec:
    """Configured mean/variance of a token-count draw (means only)."""

    name: str      # distribution name ('fixed' for bare numbers)
    mean: float
    variance: float


def _as_mapping(spec: Any) -> Any:
    """Normalize an OmegaConf node or dict into a plain dict-like."""
    if isinstance(spec, dict):
        return spec
    try:
        container = OmegaConf.to_container(spec, resolve=True)
        if isinstance(container, dict):
            return container
    except Exception:  # pragma: no cover
        pass
    return spec


def expected_tokens(spec: Any, what: str = "tokens") -> TokenSpec:
    """Config token spec -> (mean, variance) of the configured draw.

    Same distribution families as ``agent_client.TOKEN_DISTRIBUTIONS``.
    The simulator's ``>= 1`` integer clamp is ignored (real parameters are
    orders of magnitude above 1).
    """
    if isinstance(spec, (int, float)):
        value = float(spec)
        return TokenSpec(name="fixed", mean=value, variance=0.0)
    d = _as_mapping(spec)
    name = str(d.get("distribution") or "") if hasattr(d, "get") else ""
    if name == "fixed":
        return TokenSpec(name="fixed", mean=float(d.get("value")), variance=0.0)
    if name == "uniform":
        low = float(d.get("low"))
        high = float(d.get("high"))
        return TokenSpec(name="uniform", mean=(low + high) / 2.0,
                         variance=(high - low) ** 2 / 12.0)
    if name == "geometric":
        p = float(d.get("p"))
        return TokenSpec(name="geometric", mean=1.0 / p, variance=(1.0 - p) / p**2)
    if name == "exponential":
        rate = float(d.get("rate"))
        return TokenSpec(name="exponential", mean=1.0 / rate, variance=1.0 / rate**2)
    if name == "gamma":
        shape = float(d.get("shape"))
        scale = float(d.get("scale"))
        return TokenSpec(name="gamma", mean=shape * scale, variance=shape * scale**2)
    if name == "normal":
        mean_v = float(d.get("mean"))
        sigma = float(d.get("sigma"))
        return TokenSpec(name="normal", mean=mean_v, variance=sigma * sigma)
    if name == "lognormal":
        mean_v = float(d.get("mean"))
        sigma = float(d.get("sigma"))
        e_s2 = math.exp(sigma * sigma)
        mean_l = math.exp(mean_v + sigma * sigma / 2.0)
        var = (e_s2 - 1.0) * math.exp(2.0 * mean_v + sigma * sigma)
        return TokenSpec(name="lognormal", mean=mean_l, variance=var)
    raise ValueError(f"{what}: unsupported token distribution {name!r}")


def service_scv(parts: Sequence[Tuple[float, float]]) -> float:
    """SCV of a weighted mixture of deterministic per-class durations.

    ``parts`` is [(duration, weight), ...]; each profile class contributes a
    deterministic duration (fixed complexity/volume constants), so the DB
    service time is a mixture random variable: Var/E^2 of the mixture.
    """
    total_w = sum(w for _, w in parts)
    if total_w <= 0:
        return 0.0
    mean = sum(t * w for t, w in parts) / total_w
    if mean <= 0:
        return 0.0
    second = sum(t * t * w for t, w in parts) / total_w
    var = max(0.0, second - mean * mean)
    return var / (mean * mean)


# --------------------------------------------------------------------------- #
# structures                                                                  #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ProfileClass:
    """One QueryProfile class (deterministic complexity x volume)."""

    name: str
    complexity: float
    volume: float
    weight: float


@dataclass
class StageSpec:
    """One decomposed deterministic stage of the DB residence mean."""

    name: str
    tau: float
    kind: str  # 'compute' | 'storage' | 'net' | 'wait'


@dataclass
class StationModel:
    """The built analytical model for one run (config-derived only)."""

    n_agents: int
    z_think: float                      # 0.0: agents loop straight back
    stations: List[Station]             # chain: llm visits then db visits
    visits: Tuple[int, int]             # (llm visits/cycle, db visits/cycle)
    token_cap_rate: Optional[float]     # aggregate tokens/s cap
    e_tokens: float                     # E[in + out] per inference request
    cap_bound: bool                     # the token-cap regime was entered
    solution: ChainSolution
    stages: List[StageSpec]             # DB residence decomposition (means)
    x_cap: Optional[float]              # cap-implied throughput bound
    # decomposed deterministic means of the DB residence (per query)
    tau_issue: float = 0.0
    tau_finalize: float = 0.0
    tau_middle: float = 0.0
    wait_terms: List[StageSpec] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def x(self) -> float:
        return self.solution.X

    @property
    def e2e_mean(self) -> float:
        """Interactive law with Z=0: R = N / X (equals sum of station R's)."""
        if self.solution.X <= 0:
            return float("inf")
        return self.n_agents / self.solution.X

    def unique_stations(self) -> List[StationResult]:
        """Per-station marginals collapsed over repeated visit instances."""
        out: Dict[str, StationResult] = {}
        for res in self.solution.stations:
            out[res.name] = res
        return list(out.values())

    @property
    def llm(self) -> StationResult:
        return self.solution.stations[0]

    @property
    def db(self) -> StationResult:
        return [s for s in self.solution.stations if s.name == "db"][0]

    @property
    def bottleneck(self) -> Optional[StationResult]:
        if not self.solution.stations:
            return None
        return max(self.solution.stations, key=lambda s: s.R)

    def total_queue_length(self) -> float:
        """Sum of per-station mean queue lengths (Lq, excluding service)."""
        return sum(res.Lq for res in self.solution.stations)

    def stages_dict(self) -> Dict[str, StageSpec]:
        """Decomposed stages by name (deterministic means)."""
        return {st.name: st for st in self.stages}


# --------------------------------------------------------------------------- #
# DB-side decomposition                                                       #
# --------------------------------------------------------------------------- #

_MAX_ITER = 80


class DbDecomposition:
    """Config-only decomposition of one DB query's residence, per topology."""

    def __init__(self, cfg: Any, profiles: List[ProfileClass]) -> None:
        self.profiles = profiles
        self.topology = _s(cfg, "db.topology", "centralized")
        self.compute_c = _i(cfg, "db.compute.slots", 1)
        self.nodes = _i(cfg, "db.compute_nodes", 1)
        self.storage_nodes = _i(cfg, "db.storage_nodes", 1)
        self.coordinators = _i(cfg, "db.coordinators", 1)
        self.issue_ratio = min(1.0, max(0.0, _f(cfg, "db.compute.issue_ratio", 0.1)))
        self.phase_split = _s(cfg, "db.compute.phase_split", "three_phase")
        self.overhead = max(1.0, _f(cfg, "db.network.coordinator_overhead", 1.0))
        from agent_db_sim.components.db.backend import resolve_backend_params

        self.backend: Dict[str, float] = resolve_backend_params(
            OmegaConf.select(cfg, "db.storage")
        ).as_dict()
        self.net = {
            "latency": _f(cfg, "db.network.latency", 0.01),
            "bandwidth": _f(cfg, "db.network.bandwidth", 1000.0),
            "capacity": _f(cfg, "db.network.capacity", 8.0),
            "request_size": _f(cfg, "db.network.request_size", 0.01),
        }

    # -- geometry ---------------------------------------------------------- #
    @property
    def entry_slots(self) -> int:
        """Slots of the pool a DB query is admitted against (entry nodes)."""
        if self.topology == "disaggregated":
            return self.compute_c * self.nodes
        if self.topology == "distributed":
            return self.compute_c * self.coordinators
        return self.compute_c

    @property
    def m_storage(self) -> float:
        """Storage budgets the concurrent middle phases spread over."""
        if self.topology == "disaggregated":
            return float(max(1, self.storage_nodes))
        return 1.0

    def notes(self) -> List[str]:
        out: List[str] = []
        if self.phase_split != "three_phase":
            out.append(
                f"phase_split={self.phase_split!r}: approximated by folding "
                "the issue compute into the post-storage compute stage"
            )
        if self.topology == "distributed":
            out.append(
                "distributed: coordinator slot pools + peer hold folded "
                "(Erlang-C mean field) into the db residence"
            )
        return out

    # -- per-class deterministic stages ------------------------------------ #
    def _storage_work(self, volume: float) -> float:
        """Lone-op storage pacing work (per ``StorageNode``): the max() window."""
        b = self.backend
        return max(
            b["per_gb_time"] * volume,
            volume / b["bandwidth_ceiling"],
            1.0 / b["iops_ceiling"],
        )

    def _class_stages(
        self,
        profile: ProfileClass,
        stretch: float,
        x: float,
    ) -> Tuple[float, float, List[StageSpec]]:
        """(issue, finalize, stages) for one profile class at this stretch."""
        c_over = self.overhead if self.topology == "distributed" else 1.0
        if self.phase_split == "three_phase":
            issue = profile.complexity * self.issue_ratio * c_over
            finalize = profile.complexity * (1.0 - self.issue_ratio) * c_over
        else:
            issue = 0.0
            finalize = profile.complexity * c_over
        stages: List[StageSpec] = []
        if issue > 0:
            stages.append(StageSpec("db issue", issue, "compute"))
        if self.topology == "centralized":
            stages.extend(self._mid_colocated(profile, stretch))
        elif self.topology == "disaggregated":
            stages.extend(self._mid_segment(profile, stretch, x))
        else:
            stages.extend(self._mid_distributed(profile, stretch, x))
        if finalize > 0:
            stages.append(StageSpec("db finalize", finalize, "compute"))
        return issue, finalize, stages

    def _mid_colocated(
        self, profile: ProfileClass, stretch: float
    ) -> List[StageSpec]:
        base = self.backend["base_latency"]
        stages: List[StageSpec] = []
        if base > 0:
            stages.append(StageSpec("db storage base", base, "storage"))
        stages.append(
            StageSpec("db storage pace", stretch * self._storage_work(profile.volume),
                      "storage")
        )
        return stages

    def _mid_segment(
        self, profile: ProfileClass, stretch: float, x: float
    ) -> List[StageSpec]:
        stages: List[StageSpec] = [StageSpec(
            "db net req",
            self.net["latency"] + self.net["request_size"] / self.net["bandwidth"],
            "net",
        )]
        stages.extend(self._mid_colocated(profile, stretch))
        vol = profile.volume
        stages.append(StageSpec(
            "db net resp", self.net["latency"] + vol / self.net["bandwidth"], "net",
        ))
        # the shared staging level bounds concurrent volume transfers at
        # ~capacity/volume; fold an Erlang-C wait when the bound is tight
        cap = self.net["capacity"] / max(vol, 1e-12)
        if cap < 4.0 and self.net["bandwidth"] > 0:
            service = vol / self.net["bandwidth"]
            wait = erlang_c_wait(max(x, 1e-9), service, max(1, int(cap)))
            if wait > 0 and math.isfinite(wait):
                stages.append(StageSpec("db net staging wait", min(wait, 1e6), "wait"))
        return stages

    def _mid_distributed(
        self, profile: ProfileClass, stretch: float, x: float
    ) -> List[StageSpec]:
        stages: List[StageSpec] = [StageSpec(
            "db net req",
            self.net["latency"] + self.net["request_size"] / self.net["bandwidth"],
            "net",
        )]
        # the peer server holds one of its slots while its local storage
        # serves the middle hop; the peer-side wait is folded with Erlang-C
        n_peers = max(1, self.nodes - self.coordinators)
        peer_tau = self.backend["base_latency"] + stretch * self._storage_work(
            profile.volume
        )
        if n_peers * self.compute_c > 0 and x > 0:
            wait = erlang_c_wait(x, max(peer_tau, 1e-12), n_peers * self.compute_c)
            if wait > 0 and math.isfinite(wait):
                stages.append(StageSpec(
                    "db peer wait", min(wait, 1e6), "wait"
                ))
        stages.append(StageSpec("db peer hold", peer_tau, "storage"))
        stages.append(StageSpec(
            "db net resp", self.net["latency"] + profile.volume / self.net["bandwidth"],
            "net",
        ))
        return stages

    # -- aggregate composition --------------------------------------------- #
    def compose(
        self, stretch: float, x: float
    ) -> Tuple[float, float, List[StageSpec]]:
        """(mean residence, service SCV, decomposed stage list) at a given
        storage stretch and throughput ``x`` (for the Erlang fold-ins)."""
        total_w = sum(p.weight for p in self.profiles)
        parts: List[Tuple[float, float]] = []
        stages_acc: Dict[str, float] = {}
        kinds: Dict[str, str] = {}
        issue_acc = 0.0
        finalize_acc = 0.0
        for p in self.profiles:
            issue, finalize, stages = self._class_stages(p, stretch, x)
            total = sum(st.tau for st in stages)
            parts.append((total, p.weight))
            issue_acc += issue * p.weight
            finalize_acc += finalize * p.weight
            for st in stages:
                stages_acc[st.name] = stages_acc.get(st.name, 0.0) + st.tau * p.weight
                kinds[st.name] = st.kind
        stages = [
            StageSpec(name, tau / total_w, kinds[name])
            for name, tau in sorted(stages_acc.items())
        ]
        mean_tau = sum(t * w for t, w in parts) / total_w
        return mean_tau, service_scv(parts), stages


# --------------------------------------------------------------------------- #
# visit counts (workflow templates or the legacy loop)                        #
# --------------------------------------------------------------------------- #

def _cycle_visits(cfg: Any) -> Tuple[int, int, List[str]]:
    """Per-cycle (llm, db) visit counts + notes from agents.workflow."""
    spec = OmegaConf.select(cfg, "agents.workflow")
    if spec is None:
        return (1, 1, [])
    templates = OmegaConf.select(spec, "templates")
    if templates is None:
        return (1, 1, [])
    mix = OmegaConf.select(spec, "mix")
    weights: List[float] = []
    counts: List[Tuple[int, int]] = []
    for t in templates:
        steps = [str(s) for s in (t.get("steps") or [])]
        counts.append((steps.count("inference"), steps.count("db")))
    if mix is not None:
        weights = [float(w) for w in mix]
    if len(weights) != len(counts) or not weights:
        weights = [1.0] * len(counts)
    total = sum(weights) or 1.0
    v_l = sum(w * c[0] for w, c in zip(weights, counts)) / total
    v_d = sum(w * c[1] for w, c in zip(weights, counts)) / total
    notes: List[str] = []
    if any(c != counts[0] for c in counts):
        notes.append(
            "mixed workflow templates: visit counts are mix-weighted "
            "averages (exact only when every template alternates 1:1)"
        )
    elif not all(c == (1, 1) for c in counts):
        notes.append(
            "non-alternating workflow steps: visits folded into repeated "
            "chain instances (exact single-class theory)"
        )
    return max(1, int(round(v_l))), max(1, int(round(v_d))), notes


# --------------------------------------------------------------------------- #
# build                                                                       #
# --------------------------------------------------------------------------- #

def build_model(cfg: Any) -> StationModel:
    """Build the closed-chain model from a scenario / run ``config.yaml``.

    Config only (grounded means, never trace data). Raises ``ValueError``
    on unsupported/missing config pieces.
    """
    n = _i(cfg, "agents.count", 1)
    llm_c = _i(cfg, "llm.slots", 1)
    in_rate = _f(cfg, "llm.input_tokens_per_s", 0.0)
    out_rate = _f(cfg, "llm.output_tokens_per_s", 0.0)
    if in_rate <= 0 or out_rate <= 0:
        raise ValueError(
            "build_model: llm.input_tokens_per_s / llm.output_tokens_per_s "
            "must both be > 0"
        )
    in_tok = expected_tokens(OmegaConf.select(cfg, "agents.input_tokens"),
                             what="agents.input_tokens")
    out_tok = expected_tokens(OmegaConf.select(cfg, "agents.output_tokens"),
                              what="agents.output_tokens")
    profiles = _profiles(cfg)

    tau_llm = in_tok.mean / in_rate + out_tok.mean / out_rate
    scv_llm = _llm_scv(in_tok, out_tok, in_rate, out_rate)

    db = DbDecomposition(cfg, profiles)
    v_l, v_d, visit_notes = _cycle_visits(cfg)

    cap_rate = in_rate + out_rate     # the aggregate tokens/s cap
    e_tokens = in_tok.mean + out_tok.mean

    notes: List[str] = list(visit_notes) + db.notes()

    def _chain(t_llm: float) -> List[Station]:
        return [
            Station("llm", max(1e-9, t_llm), llm_c, scv_llm) for _ in range(v_l)
        ] + [
            Station("db", max(1e-9, db_tau), db.entry_slots, db_scv)
            for _ in range(v_d)
        ]

    # -- solve -------------------------------------------------------------
    # Stage 1: floor chain (LLM at the phase floor) with the storage
    # middle-stretch fixed point: k_bar = X * mid / M (Little at the
    # storage budgets), damped iteration.
    stretch = 1.0
    db_tau = 0.0
    db_scv = 0.0
    stages: List[StageSpec] = []
    X0 = 0.0
    for _ in range(_MAX_ITER):
        db_tau, db_scv, stages = db.compose(stretch, 0.0)
        chain = _chain(tau_llm)
        sol = solve_chain(n, 0.0, chain)
        if not sol.ok:
            raise ValueError("closed chain degenerate (zero normalizing constant)")
        X0 = sol.X
        new_stretch = _next_stretch(db, sol, stretch)
        if _close(new_stretch, stretch):
            stretch = new_stretch
            break
        stretch = 0.5 * stretch + 0.5 * new_stretch

    # Stage 2: the aggregate token cap as an explicit throughput bound.
    # X_cap = cap / E[tokens] is a hard constraint on goodput; when the
    # floor chain exceeds it, the effective X is X_cap and the LLM slot
    # hold stretches somewhere in [tau_floor, tau_max] (all min(c_L, N)
    # paced slots share the cap). tau* is bisected so the final chain
    # reproduces the pinned throughput, keeping station means consistent.
    tau_floor = tau_llm
    tau_max = min(llm_c, n) * e_tokens / cap_rate
    cap_bound = X0 * e_tokens > cap_rate * (1.0 + 1e-9) and tau_max > tau_floor
    x_cap = cap_rate / e_tokens
    if cap_bound:
        lo, hi = tau_floor, min(tau_max, 1e9)
        target = x_cap
        for _ in range(60):

            def _x(t: float) -> float:
                return solve_chain(n, 0.0, _chain(t)).X

            mid = 0.5 * (lo + hi)
            if _x(mid) > target:
                lo = mid
            else:
                hi = mid
        tau_llm_eff = 0.5 * (lo + hi)
        solution = solve_chain(n, 0.0, _chain(tau_llm_eff))
    else:
        tau_llm_eff = tau_floor
        solution = solve_chain(n, 0.0, _chain(tau_llm_eff))

    issue_mean = sum(st.tau for st in stages if st.name == "db issue")
    finalize_mean = sum(st.tau for st in stages if st.name == "db finalize")
    middle_mean = sum(
        st.tau for st in stages if st.kind in ("net", "storage", "wait")
    )
    if cap_bound:
        notes.append(_cap_note(cap_rate, e_tokens, x_cap, tau_llm_eff))
    if stretch > 1.0 + 1e-6:
        notes.append(
            f"storage contention mean-field stretch s={stretch:.3f} "
            "(shared-ceiling pacing at the observed middle concurrency)"
        )
    notes.append(
        "MVA/exact-chain assumptions: exponential service & means only "
        "(SCV from the configured draws); percentiles have no analytical "
        "counterpart."
    )

    chain = [
        Station("llm", tau_llm_eff, llm_c, scv_llm) for _ in range(v_l)
    ] + [
        Station("db", max(db_tau, 1e-9), db.entry_slots, db_scv)
        for _ in range(v_d)
    ]
    if cap_bound and tau_llm_eff > tau_floor:
        # the bisected tau keeps the chain at the pinned throughput; keep the
        # final solution consistent with the effective chain
        solution = solve_chain(n, 0.0, chain)

    return StationModel(
        n_agents=n,
        z_think=0.0,
        stations=chain,
        visits=(v_l, v_d),
        token_cap_rate=cap_rate,
        e_tokens=e_tokens,
        cap_bound=cap_bound,
        solution=solution,
        stages=stages,
        x_cap=x_cap,
        tau_issue=issue_mean,
        tau_finalize=finalize_mean,
        tau_middle=middle_mean,
        wait_terms=[st for st in stages if st.kind == "wait"],
        notes=notes,
    )


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-6 * max(1.0, b)


def _next_stretch(
    db: DbDecomposition, sol: ChainSolution, current: float
) -> float:
    """Mean-field storage stretch from Little's law at the current X."""
    mid = sum(
        st.tau for st in db.compose(current, sol.X)[2]
        if st.kind in ("net", "storage", "wait")
    )
    k_bar = sol.X * mid / max(1.0, db.m_storage)
    return max(1.0, k_bar)


def _cap_note(
    cap_rate: float, e_tokens: float, x_cap: Optional[float], tau_eff: float
) -> str:
    return (
        "token-cap regime: chain demand X*E[tokens] exceeds the aggregate "
        f"cap ({cap_rate:.0f} tok/s); LLM slot hold stretched to "
        f"{tau_eff:.4g} so throughput pins at cap/E[T] = {x_cap:.4g} req/s. "
        "Queue-share attribution is blind here (saturation lives on the "
        "token rate, not on queues)."
    )


def _llm_scv(
    in_tok: TokenSpec, out_tok: TokenSpec, in_rate: float, out_rate: float
) -> float:
    """Service SCV of the LLM slot hold In/r_i + Out/r_o (independent draws)."""
    e = in_tok.mean / in_rate + out_tok.mean / out_rate
    var = in_tok.variance / in_rate**2 + out_tok.variance / out_rate**2
    return var / (e * e) if e > 0 else 0.0


def _profiles(cfg: Any) -> List[ProfileClass]:
    raw = OmegaConf.select(cfg, "agents.query_profiles")
    if raw is None or not raw:
        raise ValueError("build_model: agents.query_profiles missing/empty")
    out: List[ProfileClass] = []
    for entry in raw:
        out.append(ProfileClass(
            name=str(entry.get("name", "profile")),
            complexity=float(entry.get("complexity", 0.0)),
            volume=float(entry.get("volume", 0.0)),
            weight=float(entry.get("weight", 1.0)),
        ))
    if sum(p.weight for p in out) <= 0:
        raise ValueError("build_model: query_profiles need positive weights")
    return out
