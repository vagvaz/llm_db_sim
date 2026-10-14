"""Exact closed-chain solver — faithful Python port of ``core2.js``.

``core2.js`` (repo root, user-authored, the mathematical spec and validation
oracle for this package) implements the exact Gordon–Newell / Buzen product-form
closed chain over an arbitrary list of stations::

    chain of K M/G/c service stations (+ optional IS think node), closed
    loop of N agents visiting every station once per cycle.

Its per-station marginals come from prefix/suffix convolutions of the
normalizing-constant arrays (O(K·N²)), so we port it line-for-line:

- :func:`logfact` / :func:`mmcf_log` / :func:`isf_log` — log-space
  per-station generating-function PMFs (FCFS M/M/c and infinite-server);
- :func:`norm_exp`, :func:`conv` — rescaled exponentiation & exact
  convolution used for the normalizing constant;
- :func:`solve_chain` — the exact chain solution (throughput via flow
  balance, per-station queue length ``L``, utilization, mean response).
- the Allen–Cunneen per-station wait inflation from ``core2.js solve()``
  (``corr = (1 + scv) / 2``) for non-exponential service (SCV ≠ 1).

Everything here assumes **exponential service and means only** — the exact
chain is exact for the means of the product-form model; realistic service
distributions enter only through the SCV wait inflation. Percentiles have no
analytical counterpart in this model.

One port deviation (documented in compare.py / model.py): multi-station
chains with per-cycle **visit counts** V_i > 1 (workflow templates) are
solved by duplicating each station instance V_i times in the chain list,
which is exact for a single-class chain.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

__all__ = [
    "Station",
    "ChainSolution",
    "logfact",
    "mmcf_log",
    "is_log",
    "norm_exp",
    "conv",
    "solve_chain",
]


@dataclass
class Station:
    """One chain station: name, mean service ``tau``, ``c`` servers, SCV.

    ``tau`` is the mean slot-hold (service) time, ``c`` the number of
    parallel servers (slots), ``scv`` the service-time squared coefficient of
    variation (1 = exponential, 0 = deterministic). Only means enter the
    exact chain; ``scv`` feeds the Allen–Cunneen wait correction.
    """

    name: str
    tau: float
    c: int
    scv: float = 1.0

    def validated(self) -> "Station":
        tau = max(1e-9, float(self.tau)) if math.isfinite(self.tau) else 1e-9
        c = max(1, int(self.c))
        scv = max(0.0, float(self.scv))
        # keep extreme SCVs from producing negative/infinite corrections
        scv = min(scv, 1e6)
        return Station(name=self.name, tau=tau, c=c, scv=scv)


@dataclass
class StationResult:
    """Per-station marginals of the closed chain (plus SCV-adjusted waits)."""

    name: str
    tau: float
    c: int
    scv: float
    #: mean number of customers in the station (in service + queueing)
    L: float
    #: station departure rate = system throughput (flow balance)
    X: float
    #: utilization per server
    rho: float
    #: mean response time per visit to the station (Little, R = L / X)
    R: float
    #: mean wait against the exact chain mean (Allen–Cunneen inflation)
    Wq: float
    #: mean queue length (excludes customers in service)
    Lq: float


@dataclass
class ChainSolution:
    """Exact closed-chain solve for N agents over the station list."""

    ok: bool
    N: int
    #: system throughput (cycles per time unit)
    X: float
    stations: List[StationResult]
    #: mean number of agents outside the chain (think time Z = 0 here)
    LT: float

    @property
    def bottleneck(self) -> Optional[StationResult]:
        """The station with the highest mean response time (or None)."""
        if not self.stations:
            return None
        return max(self.stations, key=lambda s: s.R)


# --------------------------------------------------------------------------- #
# pure math port (names kept close to core2.js)                                #
# --------------------------------------------------------------------------- #

def logfact(k: int) -> float:
    """log(k!) for k >= 0 (core2.logfact)."""
    s = 0.0
    for i in range(2, k + 1):
        s += math.log(i)
    return s


def mmcf_log(c: int, tau: float, n_max: int) -> List[float]:
    """Log PMF weights of an FCFS M/M/c station with n = 0..n_max customers."""
    lf = [0.0] * (n_max + 1)
    lt = math.log(tau)
    cur = 0.0
    for n in range(1, n_max + 1):
        if n <= c:
            cur = n * lt - logfact(n)
        else:
            cur = lf[n - 1] + (lt - math.log(c))
        lf[n] = cur
    return lf


def is_log(tau: float, n_max: int) -> List[float]:
    """Log PMF weights of an IS (infinite-server) station."""
    lf = [0.0] * (n_max + 1)
    lt = math.log(tau)
    for n in range(1, n_max + 1):
        lf[n] = lf[n - 1] + lt - math.log(n)
    return lf


def norm_exp(lf: Sequence[float]) -> List[float]:
    """exp(lf[i] - max(lf)) — anchor the log weights at their maximum."""
    m = -math.inf
    for v in lf:
        if v > m:
            m = v
    return [math.exp(v - m) for v in lf]


def conv(a: Sequence[float], b: Sequence[float], n_max: int) -> List[float]:
    """Truncated convolution of two n_max-supporting arrays (core2.convArr)."""
    g = [0.0] * (n_max + 1)
    na = min(len(a) - 1, n_max)
    nb = min(len(b) - 1, n_max)
    for i in range(na + 1):
        ai = a[i]
        if not ai:  # falsy: 0
            continue
        for j in range(min(nb, n_max - i) + 1):
            g[i + j] += ai * b[j]
    return g


def _erlang_b(c: int, a: float) -> float:
    """Erlang B blocking at offered load a over c servers (core2.erlangB)."""
    b = 1.0
    for k in range(1, c + 1):
        b = a * b / (k + a * b)
    return b


def erlang_c(c: int, a: float) -> float:
    """Erlang C probability of waiting (core2.erlangC) — open M/M/c."""
    b = _erlang_b(c, a)
    return b / (1.0 - (a / c) * (1.0 - b))


def erlang_c_wait(lambda_rate: float, tau: float, c: int) -> float:
    """Mean wait (s) in an open M/M/c with arrival rate λ, service mean τ.

    Returns the Erlang-C mean queueing delay (1/rate form); useful as a
    mean-field fold-in for secondary pools, not part of the exact chain.
    """
    a = lambda_rate * tau
    if a >= c:
        return float("inf")
    const = erlang_c(c, a)
    # Wq = C / (c/τ − λ)
    return const / max(c / tau - lambda_rate, 1e-12)


def solve_chain(
    n_agents: int,
    tau_think: float,
    stations: Sequence[Station],
) -> ChainSolution:
    """Exact Gordon–Newell closed chain (faithful port of core2.solveChain).

    ``tau_think`` is the mean IS think-node time (0 = no think node, our
    workload shape: agents loop straight back). Every station is one visit
    per cycle; multi-visit stations are represented by repeated instances
    upstream (see module docstring).
    """
    station_list = [s.validated() for s in stations]
    n = max(1, int(n_agents))
    if not station_list:
        raise ValueError("solve_chain needs at least one station")

    fs: List[List[float]] = []
    has_think = tau_think > 1e-9
    if has_think:
        fs.append(norm_exp(is_log(tau_think, n)))
    svc_idx: List[int] = []
    for st in station_list:
        fs.append(norm_exp(mmcf_log(st.c, st.tau, n)))
        svc_idx.append((len(fs) - 1))
    kt = len(fs)

    unit = [0.0] * (n + 1)
    unit[0] = 1.0
    pref: List[List[float]] = [unit]
    for k in range(1, kt + 1):
        pref.append(conv(pref[k - 1], fs[k - 1], n))
    suf: List[Optional[List[float]]] = [None] * (kt + 1)
    suf[kt] = unit
    for k in range(kt - 1, -1, -1):
        suf[k] = conv(fs[k], suf[k + 1] if suf[k + 1] is not None else [], n)  # type: ignore[arg-type]

    gn = pref[kt][n]
    if not gn > 0:
        return ChainSolution(ok=False, N=n, X=0.0, stations=[], LT=n)

    results: List[StationResult] = []
    # first pass: per-station queue length L and departure rate Xs, one loop
    l_by_station: List[float] = []
    x_sum = 0.0
    for sd, idx in zip(station_list, svc_idx):
        # core2.rest = convArr(pref[idx], suf[idx + 1]): the suffix must
        # EXCLUDE this station's own PMF (suf[idx] already includes it)
        rest = conv(pref[idx], suf[idx + 1], n)  # type: ignore[arg-type]
        big_l = 0.0
        xs = 0.0
        for k in range(1, n + 1):
            p = fs[idx][k] * rest[n - k] / gn
            big_l += k * p
            xs += min(k, sd.c) / sd.tau * p
        l_by_station.append(big_l)
        x_sum += xs

    # flow balance: every station's departure rate must equal X (exact)
    x = x_sum / len(station_list)

    l_sum = 0.0
    for sd, big_l in zip(station_list, l_by_station):
        rho = x * sd.tau / sd.c
        big_r = big_l / x if x > 0 else float("inf")
        # Allen–Cunneen inflation (core2.solve): non-exponential service
        corr = (1.0 + sd.scv) / 2.0
        wq = max(0.0, big_r - sd.tau) * corr
        lq = x * wq
        results.append(
            StationResult(
                name=sd.name, tau=sd.tau, c=sd.c, scv=sd.scv,
                L=big_l, X=x, rho=rho, R=big_r, Wq=wq, Lq=lq,
            )
        )
        l_sum += big_l

    lt = max(0.0, n - l_sum)
    return ChainSolution(ok=True, N=n, X=x, stations=results, LT=lt)


def solve_capped(
    n_agents: int,
    stations: Sequence[Station],
) -> Tuple[ChainSolution, float]:
    """A convenience wrapper with no think node (this workload's shape)."""
    return solve_chain(n_agents, 0.0, stations), 0.0
