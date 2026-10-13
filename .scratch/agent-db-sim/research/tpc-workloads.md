# Research: TPC-H / TPC-C / TPC-DS → query workload profiles

Source lane: lib-5 (librarian), 2026-10-13. Official spec PDFs text-verified (TPC-H v3.0.1, TPC-C v5.11.0, TPC-DS v4.0.0) + Dreseler VLDB'20 choke-point study + Doris/ClickHouse measurements. Feeds `query_profiles` + workload mixes.

## Key structural findings

- **TPC-H/TPC-DS have NO think-time model** (back-to-back per stream); only TPC-C models keying/think delays → TPC-C is the source for agent inter-call latency (negative-exponential, truncated at 10× mean).
- TPC-H SF unit ≈ GB; LINEITEM = 64% of bytes (0.64 GB per SF row-storage); columnar engines read 5–10× less (Q6 skips up to 85% of lineitem).
- TPC-H per-query spread ~30×; Q13/Q18/Q9 hardest (33/31/18 CSE @SF100/1core); set ≈ 197 core-seconds @SF100.
- TPC-DS = 2.6× TPC-H CPU per pass at SF1000 (Doris: 261.3 s vs 101.5 s), longer tail (q23 38 s), highly skewed data.
- TPC-C: 10 terminals/warehouse, ~0.08 GB/warehouse; weighted keying+think ≈ 22 s/txn ⇒ **tpmC ceiling ≈ 12.5–12.9/warehouse** (Yugabyte measured 12,590/1000 WH = 97% of theoretical).

## TPC-C transaction table (spec p.72)

| Tx | mix | keying s | think s | RT budget p90 |
|---|---|---|---|---|
| New-Order | 45% | 18 | 12 | 5 s |
| Payment | 43% | 3 | 12 | 5 s |
| Order-Status | 4% | 2 | 10 | 5 s |
| Delivery | 4% | 2 | 5 | 5 s (+≥90% within 80 s) |
| Stock-Level | 4% | 2 | 5 | 20 s |

Think time: `Tt = −ln(r)·μ` truncated at 10×μ. ~1% New-Order rollbacks (fault-injection toggle).

## Anchor conventions (for profile comments)

- `complexity` = single-core CPU-seconds-equivalent (CSE) at SF100/100 GB scale; from Dreseler SF10/1-core ×10, cross-checked ~2× against Doris SF1000 wall-times.
- `volume` = logical row-storage GB touched; sim applies per-engine `io_efficiency` (columnar 0.1–0.2 of row basis).
- TPC-H/DS: ~12–20 MB scan-relevant bytes per CPU-second couples complexity×volume for storage modeling.

## YAML-ready query profiles (ranges = [typical, hard tail])

```yaml
query_profiles:
  point_lookup:          # TPC-C Order-Status/Stock-Level, agent KV fetches
    complexity_cse: [0.001, 0.02]
    volume_gb:      [0.00001, 0.001]
    spec_examples: [tpc-h Q2 47ms@SF10/1core]
  oltp_tx:               # TPC-C New-Order/Payment
    complexity_cse: [0.002, 0.05]
    volume_gb:      [0.00005, 0.0005]
    think_s: {new_order: 12, payment: 12, order_status: 10, delivery: 5, stock_level: 5}
    keying_s: {new_order: 18, payment: 3, order_status: 2, delivery: 2, stock_level: 2}
  small_analytic:        # TPC-H Q6/Q11/Q12/Q14/Q15/Q22
    complexity_cse: [0.5, 3]
    volume_gb:      [0.1, 2]        # up to 85% prunable (VLDB'20)
  join_heavy:            # TPC-H Q2-Q10,Q19,Q21 + DS joins
    complexity_cse: [3, 25]         # Q9 17.8 CSE
    volume_gb:      [0.5, 8]
  wide_scan_aggregate:   # TPC-H Q1,Q13,Q17,Q18,Q20
    complexity_cse: [5, 35]         # Q13 33 CSE
    volume_gb:      [0.6, 0.9]
  refresh_maintenance:   # TPC-H RF1/RF2, TPC-C Delivery
    complexity_cse: [0.1, 5]
    volume_gb:      [0.01, 2]

workload_mixes:
  analytical_agent_tpch_like:   # 22-template bucket counts
    {small_analytic: 0.32, join_heavy: 0.45, wide_scan_aggregate: 0.23}
    think_time_s: 0             # spec: back-to-back
    concurrency: "2..11 streams (SF1..SF100000 spec table)"
  oltp_agent_tpcc_like:
    {oltp_tx: 0.88, point_lookup: 0.12}
    think_time_s: {dist: neg_exp, mean: [5, 12], trunc: 10.0}
  mixed_agent_retrieval_analytics:   # COMPOSED, not spec-derived
    {point_lookup: 0.45, small_analytic: 0.20, join_heavy: 0.10,
     wide_scan_aggregate: 0.10, oltp_tx: 0.15}
    think_time_s: {dist: neg_exp, mean: [5, 12], trunc: 10.0}
```

**Caveat:** `mixed_agent` is the only composed (not spec-derived) class — no public spec exists for LLM-agent DB workloads; closest official anchors are TPCx-AI/TPCx-BB. Volume scales linearly in SF; complexity ~linear (validated: Q9 1.78 s@SF10/1core → 15.9 s@SF1000/96C).

## Sources

- Specs: tpc.org TPC-H v3.0.1, TPC-C v5.11.0, TPC-DS v4.0.0 PDFs
- Dreseler et al., *Quantifying TPC-H Choke Points*, PVLDB 13(8), 2020
- Apache Doris TPC-H/TPC-DS SF1000 benchmark tables; ClickHouse "TPC-H for less than a cent"
- Nambiar & Poess, *The Making of TPC-DS* (VLDB 2007); *Why You Should Run TPC-DS* (VLDB 2009)
- YugabyteDB 10k-warehouse TPC-C scaling blog
