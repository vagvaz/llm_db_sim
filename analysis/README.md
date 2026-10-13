# Agent–DB Workload Analyzer — user guide

An independent **analytical** (queueing-theoretic) model of the closed-loop agent workload:
**N agents**, each cycling *think → LLM call → DB query*, with a queueing
(many-server) LLM endpoint and DB endpoint. You set the parameters, it computes
throughput, utilizations, queue depths, and the **latency distribution**
(mean ± sd, p50/p90/p95/p99). It also ships an in-page **Monte Carlo** cross-check
(discrete-event simulation of the same stochastic model).

> This is deliberately separate from the repo's **DES simulator** (SimPy) — that one is
> documented in the root `README.md`. The analyzer here does not execute the DES
> simulator; it models the same workload mathematics, and the DES run outputs can be
> compared against these predictions in later tickets.


## Files

| File | Purpose |
|---|---|
| `agent_db_sim_analyzer.html` | **The webpage** — single self-contained file (analytic + Monte Carlo + compare). Open it in any browser; no server, no network. |
| `core.js` | The math engine (dependency-free JS, browser *and* node): multi-station closed chains, arrival theorem, sojourn percentiles. Embedded in the webpage between `MATHCORE` markers. |
| `math_primer.html` | **Detailed HTML presentation of the math** — acronym/symbol glossary, layer-by-layer explanations with worked examples, and references with verified digital links (free/paywalled tagged). |
| `verify_core.js` | Verification suite (`node analysis/verify_core.js`) — 27 checks against independent ground truth. |
| `smoke_page.js` | Headless regression test (`node analysis/smoke_page.js`) — runs the page's scripts in a DOM shim. |
| `README.md` | This guide. |

## Quick start

- Run the checks: `node analysis/verify_core.js` — must print `30 passed, 0 failed`-style summary.
- Webpage: open `analysis/agent_db_sim_analyzer.html` in a browser (no server, no network; 100% local).

## Parameters

| Parameter | Meaning |
|---|---|
| `N` | Concurrent agents in the closed loop. |
| `τ_think` | Mean think time per cycle (exponential). |
| `τ_LLM`, `c_LLM` | LLM endpoint: per-call mean service time, number of parallel servers. |
| `c²_s,LLM` | Service-time squared CoV (SCV): 1 = exponential, 0 = deterministic. |
| `τ_DB`, `c_DB`, `c²_s,DB` | Same three knobs for the DB endpoint. |
| Numerics | Grid resolution / horizon for the percentile engine; MC cycles kept, warm-up fraction, RNG seed; sweep range. |

Defaults match the repo's smoke scenario heavy case: N=8 (2 in sim), τ_think=2 s,
τ_LLM=1.5 s (c=4), τ_DB=0.3 s (c=8).

## Topology: multiple nodes (compute / storage / inference)

The **Topology — station chain** card lists the M/G/c endpoints every agent visits **once per
cycle, in order**. Each row is a node: name, τ, servers c, SCV. Presets: *LLM+DB* and
*LLM+DB (compute + storage)* — the decomposed-DB chain (all DB demand split over chained
nodes); add more stages (e.g. a second inference step) with **+ station**. Multiple agents is
the closed loop itself (N). Means are exact via Buzen for any chain length (verified against
independent CTMC solves at K=3 with N up to 8).

## The three headline metrics

1. **DB arrival distribution** — exact PMF of how many agents are already at the last station
   when a fresh query arrives (arrival theorem: the arriving job sees the N−1 chain; exact
   product-form marginals). The DES histogram should match this same shape.
2. **Agent-facing DB latency** — per-station sojourn percentiles (p50/p90/p95/p99 of τ + wait)
   computed in closed form from the calibrated wait mixture (exact mean; exact-τ collapse when
   P(wait=0) > the percentile).
3. **Bottleneck detection** — the argmax-ρ station is named in the banner; tiles show its load
   and P(wait); the station table adds the **queue-delay share** (% of loop time spent queueing
   at each node) and the saturation sweep sharpens where the knee is.

## Modes

- **Analytic** — exact closed-chain solution (Gordon–Newell/Buzen, product form, exact
  for SCV=1) for throughput/queues/sojourns, plus a calibrated wait-mixture model for the
  latency CDF. Recomputes live on every input; instant.
- **Monte Carlo** — exact FCFS discrete-event simulation of the same model
  (binary event ordering, FIFO queues, general service distributions via Gamma sampling).
  Empirical percentiles; error O(1/√samples); runs on Recompute / tab entry (seeded, reproducible).
- **Compare** — overlays empirical MC CDF with the analytic CDF and shows both readings per tile.

## Outputs

Stat tiles (p50/p90/p95/p99, mean ± sd, throughput X in cycles/s, per-server loads ρ,
P(wait>0) per station); latency PDF/CDF chart with percentile markers;
saturation sweep (mean latency vs. N with knee marker); per-station table
(λ, ρ, P(wait), W_q, R=τ+W_q, L_q).

## Model & math references

- **A.K. Erlang (1917)** — multiserver loss/wait formulas; used as M/M/c (Erlang C) per-station formulas.
- **J.D.C. Little (1961), *Operations Research* 9(3)** — Little's Law L = λW; sojourn times R = L/X.
- **W.J. Gordon & G.F. Newell (1967), *Operations Research* 15(2)** — product-form closed queueing
  networks; the steady state of the closed agent chain is exactly of this form.
- **J.P. Buzen (1973), *Comm. ACM* 16(9)** — convolution algorithm; how the exact means are computed.
- **F. Baskett, K. Chandy, R. Muntz, F. Palacios (1975), *JACM* 22(2) (BCMP)** — why the multiserver
  FCFS M/M/c stations and the infinite-server think node qualify for the product form.
- **J.F.C. Kingman (1961)** heavy-traffic formula; **A.O. Allen / E. Cunneen** approximation —
  the (c²_a + c²_s)/2 wait inflation used when SCV ≠ 1; engineering form in **W. Whitt (1983),
  "The Queueing Network Analyzer", *Bell System Tech. J.* 62(9)**.
- **L. Kleinrock (1975), *Queueing Systems Vol. I*, ch. 3** — M/M/c waiting-time structure used for
  the latency percentile mixture (wait = 0 w.p. (1−π), else Exponential tail).
- **G. Marsaglia & W. Tsang (2000), *ACM TOMS* 26(3)** — Gamma sampler for M/G/c service times (MC engine).
- Texts: **Bolch et al. (2006), *Queueing Networks and Markov Chains*, Wiley**;
  **Lazowska et al. (1984), *Quantitative System Performance***.

## Caveats — what is verified and what is NOT (please read)

**Verified** (node suite, 49 checks + headless page smoke test):
- Exact means cross-checked against a brute-force CTMC stationary solve (independent method)
  to 1e-6 on several configs (N=1..8, servers 1..4, with/without think node).
- Closed M/M/1//2 chain against the textbook hand-analysis: X = 2/3, L = 1 per node.
- MC engine vs. exact chain: <1% agreement on means and <1% on throughput at SCV=1 (seeded, reproducible).
- Gamma/Exp samplers: mean/variance within statistical tolerance at SCV ∈ {0.5, 1, 2.5}.
- Percentile engine internal consistency: grid mean ≈ chain mean (≤1%).
- Headless smoke test (`node analysis/smoke_page.js`): page scripts run end-to-end
  (analytic tiles, MC tiles, table build) via the extracted core in a DOM shim.

**Known approximations / remaining caveats:**
1. **Percentile calibration** (chosen empirically, documented): the wait mixture reproduces the
   exact closed-chain mean wait; its tail rate is μ̂ = c/τ (a waiting agent sees all c servers
   busy — closed-chain behaviour), validated vs MC in a bake-off (defaults and near-saturation
   and zero-think regimes all improve over the open-chain alternative). E.g. at defaults
   p50/p95/p99: analytic 3.36/7.97/11.19 s vs MC 3.42/8.87/12.25 s. Expect ~±10–15% on upper
   percentiles; use the MC tab for empirical values. P(wait>0) remains an approximation
   (analytic 0.47 vs MC 0.30 at defaults).
2. **Deterministic think time**: the engine assumes exponential think; a deterministic-think
   toggle is not implemented yet (MC could support it; analytic would use a delay-node approximation).
3. **Token-rate cap** (roadmap: DB service extended by tokens/rate) not yet exposed as a parameter.
4. **Saturation**: closed chains are stable for finite N, so ρ < 1 always; ρ ≥ 0.995 is flagged
   as near-saturation with steep latency growth rather than a hard cap.
5. **Sweep chart scope**: the saturation sweep uses the analytic engine only; MC percentiles
   appear in tiles, the latency chart and the station table.
