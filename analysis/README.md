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
| `core.js` | The math engine (dependency-free JS, runs in browser *and* node). |
| `verify_core.js` | Verification suite (`node verify_core.js`) — asserts against independent ground truth. |
| `agent_db_sim_analyzer.html` | The self-contained webpage **(being regenerated — see caveats)**. |

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

**Verified** (node suite, 30 checks):
- Exact means cross-checked against a brute-force CTMC stationary solve (independent method)
  to 1e-6 on several configs (N=1..8, servers 1..4, with/without think node).
- Closed M/M/1//2 chain against the textbook hand-analysis: X = 2/3, L = 1 per node.
- MC engine vs. exact chain: <1% agreement on means at SCV=1 (3-seed average).
- Gamma/Exp samplers: mean/variance within statistical tolerance at SCV ∈ {0.5, 1, 2.5}.
- Percentile engine internal consistency: grid mean ≈ chain mean (≤1%).

**Known approximations / open issues:**
1. **Percentile calibration** (open): the wait mixture matches the *mean* wait exactly, but its
   P(wait>0) is an open-chain (Poisson) imputation — MC shows the closed-chain empirical value is
   higher (0.30 vs 0.11 at defaults). Percentiles from the analytic engine should be read as
   ±10–15% vs the MC empirical values; a better closed-chain calibration is being fitted
   (calibration `μ = c/τ` currently wins the empirical bake-off but is anti-conservative at high load).
2. **Percentile engine bug (open)**: in the `τ_think = 0` regime the analytic p50 can come out
   below the theoretical floor T0 = τ_LLM + τ_DB (impossible) — the grid engine mishandles the
   delta-think path. Fix in progress; until then treat zero-think percentiles as invalid
   (MC tab is correct for that regime).
3. **Deterministic think time**: the engine assumes exponential think; a deterministic-think
   toggle is not implemented yet (MC could do it; analytic would use a delay-node approximation).
4. **Token-rate cap** (README roadmap: DB service extended by tokens/rate) not yet exposed as a parameter.
5. **Saturation**: closed chains are stable for finite N, so ρ < 1 always; "saturation" is reported
   as near-saturation (ρ ≥ 0.995) with diverging latency growth instead of a hard cap.
6. The webpage itself is being reassembled from the verified `core.js` in the next commit
   (an earlier broken intermediate was removed rather than committed).
