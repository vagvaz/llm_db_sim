"""Analytical cross-check lane: the ported closed-chain math.

Validates the faithful Python port of the user-authored ``core2.js``
mathematical core (repo root, the spec + oracle) three ways:

1. **Node oracle**: require ``core2.js`` under ``node`` and compare the
   ported :func:`solve_chain` against ``core2.solveChain`` for a multi-
   station chain (and the sanity-family parameters) to 1e-9. Skipped when
   ``node`` is unavailable.
2. **Classic open-form references**: Erlang C against hand-known
   M/M/1 and M/M/c values (e.g. M/M/1 at rho=0.8 -> Lq = 3.2).
3. **Closed-form degenerate cases**: single-station chains where the exact
   answer is on paper (N<=c: X = N/tau; N>=c: X = c/tau), and the
   Allen–Cunneen wait inflation for deterministic service (SCV 0).
"""

from __future__ import annotations

import json
import math
import shutil
import subprocess
from pathlib import Path

import pytest

from agent_db_sim.analysis.chain import (
    Station,
    erlang_c,
    logfact,
    solve_chain,
)

REPO = Path(__file__).resolve().parents[1]

# --------------------------------------------------------------------------- #
# node oracle                                                                  #
# --------------------------------------------------------------------------- #

NODE = shutil.which("node")


def _core2_solve(n_agents: int, stations: list[tuple]) -> dict:
    """Run core2.js's solveChain under node; return its raw solve() output."""
    script = f"""
const core = require({str(REPO / 'core2.js')!r});
const stations = {json.dumps(
        [{"name": n, "tau": t, "c": c, "scv": s} for n, t, c, s in stations]
)};
const core2 = core.solveChain({n_agents}, 0, stations);
console.log(JSON.stringify({{X: core2.X, st: core2.st.map(s =>
    ({{name: s.name, L: s.L, Xs: s.Xs, rho: s.rho, R: s.R}}))}}));
"""
    out = subprocess.run(
        [NODE, "-e", script], capture_output=True, text=True, check=True,
        timeout=30,
    )
    return json.loads(out.stdout)


@pytest.mark.skipif(NODE is None, reason="node not available for the oracle")
def test_solve_chain_matches_node_oracle() -> None:
    stations = [("llm", 1.0, 1, 0.0), ("db", 0.6001, 2, 0.0)]  # sanity_queue
    node = _core2_solve(2, stations)
    sol = solve_chain(2, 0.0, [Station(n, t, c) for n, t, c, _ in stations])
    assert sol.ok
    assert math.isclose(sol.X, node["X"], rel_tol=1e-12)
    for res, ns in zip(sol.stations, node["st"]):
        assert res.name == ns["name"]
        assert math.isclose(res.L, ns["L"], rel_tol=1e-12)
        assert math.isclose(res.X, ns["Xs"], rel_tol=1e-12)
        assert math.isclose(res.R, ns["R"], rel_tol=1e-12)
        assert math.isclose(res.rho, ns["rho"], rel_tol=1e-12)


@pytest.mark.skipif(NODE is None, reason="node not available for the oracle")
def test_solve_chain_matches_node_oracle_wider_chain() -> None:
    # 3-station chain with fractional capacities, N=9, exponential service
    stations = [("a", 0.7, 3, 1.0), ("b", 1.3, 2, 1.0), ("c", 0.4, 1, 1.0)]
    node = _core2_solve(9, stations)
    sol = solve_chain(9, 0.0, [Station(n, t, c, s) for n, t, c, s in stations])
    assert sol.ok
    assert math.isclose(sol.X, node["X"], rel_tol=1e-12)
    for res, ns in zip(sol.stations, node["st"]):
        assert math.isclose(res.L, ns["L"], rel_tol=1e-12)
        assert math.isclose(res.R, ns["R"], rel_tol=1e-12)


@pytest.mark.skipif(NODE is None, reason="node not available for the oracle")
def test_node_oracle_erlang_c_classic_table_values() -> None:
    """M/M/1 rho=0.8 -> Lq = 3.2 (classic table); M/M/c hand-form."""
    script = f"""
const core = require({str(REPO / 'core2.js')!r});
console.log(JSON.stringify({{
  c1: core.erlangC(1, 0.8),      // M/M/1 at a=0.8 (rho=0.8), mu=1
  c2: core.erlangC(2, 1.6),      // M/M/2 at a=1.6 (rho=0.8)
  b2: core.erlangB(2, 1.6),
}}));
"""
    out = json.loads(
        subprocess.run(
            [NODE, "-e", script], capture_output=True, text=True, check=True,
            timeout=30,
        ).stdout
    )
    # hand-form Erlang B/C (independent recursion) as the oracle reference
    b1_16 = 1.6 / (1.0 + 1.6)                      # B(1, 1.6)
    b2 = 1.6 * b1_16 / (2.0 + 1.6 * b1_16)         # B(2, 1.6) = a*B1/(2 + a*B1)
    c2_expected = b2 / (1.0 - (1.6 / 2.0) * (1.0 - b2))
    assert math.isclose(out["c1"], 0.8, abs_tol=1e-12)
    # M/M/1 at rho=0.8: Wq = C/(mu - lambda) = 4, Lq = lambda*Wq = 3.2
    lq = 0.8 * out["c1"] / (1.0 - 0.8)
    assert math.isclose(lq, 3.2, abs_tol=1e-12)
    assert math.isclose(out["b2"], b2, abs_tol=1e-12)
    assert math.isclose(out["c2"], c2_expected, abs_tol=1e-12)
    assert math.isclose(erlang_c(2, 1.6), c2_expected, abs_tol=1e-12)
    # python port agrees on both stations
    assert math.isclose(erlang_c(1, 0.8), 0.8, abs_tol=1e-12)
    assert math.isclose(erlang_c(2, 1.6), c2_expected, abs_tol=1e-12)


# --------------------------------------------------------------------------- #
# closed-form degenerate cases                                                 #
# --------------------------------------------------------------------------- #

def _single(tau: float, c: int, n_agents: int):
    return solve_chain(n_agents, 0.0, [Station("s", tau, c)])


def test_single_station_unsaturation_is_no_queue() -> None:
    # N <= c: every agent always in service; X = N/tau, R = tau, Lq ~ 0
    sol = _single(2.0, 4, 3)
    assert sol.ok
    assert math.isclose(sol.X, 3.0 / 2.0, abs_tol=1e-9)
    assert math.isclose(sol.stations[0].R, 2.0, abs_tol=1e-9)
    assert sol.stations[0].Lq <= 1e-9


def test_single_station_saturated_scales_linearly() -> None:
    # N > c: station saturated; X = c/tau (c=1: M/M/1-flow)
    sol = _single(0.5, 2, 7)
    assert sol.ok
    assert math.isclose(sol.X, 2.0 / 0.5, abs_tol=1e-9)
    assert sol.stations[0].rho > 0.999


def test_think_node_breaks_even_with_known_cycle() -> None:
    # N=1, one M/M/1 (tau=2) plus think tau=3: X = 1/5 exactly, L = X*2
    sol = solve_chain(1, 3.0, [Station("s", 2.0, 1)])
    assert sol.ok
    assert math.isclose(sol.X, 1.0 / 5.0, abs_tol=1e-12)
    assert math.isclose(sol.stations[0].L, 0.4, abs_tol=1e-12)


def test_allen_cunneen_deflate_for_deterministic_service() -> None:
    # SCV=0: Wq halves the exact-chain wait ((1+0)/2)
    sol = solve_chain(6, 0.0, [Station("a", 1.0, 1, 1.0), Station("b", 1.0, 1, 0.0)])
    a, b = sol.stations
    exact_wait_a = a.R - 1.0
    exact_wait_b = b.R - 1.0
    assert math.isclose(a.Wq, exact_wait_a * 1.0, abs_tol=1e-12)
    assert math.isclose(b.Wq, exact_wait_b * 0.5, abs_tol=1e-12)


def test_logfact_sanity() -> None:
    assert logfact(0) == logfact(1) == 0.0
    assert math.isclose(logfact(5), math.log(120.0), abs_tol=1e-12)
