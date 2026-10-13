# Research: LLM inference serving parameters (2025–2026)

Source lane: lib-4 (librarian), 2026-10-13. Feeds `llm.*` scenario config (input/output_tokens_per_s, slots) and `agents` token distributions.

## 1. Serving throughput: single-stream vs aggregate

**Independent measurements (highest trust):** InferenceX (SemiAnalysis), Llama 3.3 70B FP8 8k-in/1k-out:
- 78 tok/s/user → 877 (H100) / 1,864 (H200) tok/s/GPU → ~11–24 slots
- 58 tok/s/user → 1,171 / 2,516 → ~20–43 slots
- 38 tok/s/user → 1,848 / 3,310 → ~49–87 slots
- DeepSeek R1 671B FP8 H200: 1,235 tok/s/GPU @50 t/s/user (~25 slots), peak 2,965; interactivity band 19–97 t/s/user.

**MLPerf v5.0/v5.1** (submitter-reported): Llama 3.1 405B 224 tok/s/GPU offline (Blackwell Ultra); Llama 2 70B ~12.7k tok/s/GPU offline, 7,856 interactive; CoreWeave 8×H200 Llama-70B 33k TPS/system → ~4,125/GPU.

**Community/production:** SGLang DeepSeek-V3 22.3k out tok/s/node (2.8k/GPU, PD-disagg); vLLM wide-EP 2.2k tok/s/H200; SGLang R1 cookbook conc-64 prefill anchor ≈ **11k input tok/s aggregate**.

**Single-stream decode (≈ per-request rate):** Llama 70B-class 30–51 t/s (TRT-LLM 51.1 on H200; vLLM 30–40); Llama 8B H100 150–200; DeepSeek 671B 8×H200 30–50; hosted APIs: Claude Sonnet 4.5 = 39, Gemini 3 Flash = 170 (Artificial Analysis, independent) → hosted range ~40–170.

**Physics crosscheck:** 70B FP8 weights ≈35 GB/step → H100 3.35 TB/s → ~100 tok/s single-stream ceiling (H200 ~140). Aggregate comes from batching only.

**Vendor marketing to ignore:** NVIDIA InferenceMAX 60k t/s claims, Simplismart 501 t/s, AMD MI355X cost claims.

## 2. Prefill vs decode

- Sarathi-Serve OSDI'24 (arXiv 2403.02310): prefill compute-bound, decode memory-bound; chunked prefill.
- Per-stream: 256-tok prompt TTFT 123 ms → ≥2–3k tok/s prefill vs 30–51 t/s decode ≈ **50–100× per-stream ratio**; agents send ~8× more input than output → aggregate input should run **2–10× aggregate output**.

## 3. Agent workload token statistics

| Statistic | Value | Source |
|---|---|---|
| Input tokens/call (coding agents) | ~50K (2025-05) → **~84K (2026-04)** | Requesty *Coding Agent Economy* (production data) |
| LLM calls per session | 50–200 | same |
| Agentic vs chat token burn | 1200× multi-turn chat | arXiv 2604.22750 (Stanford/MIT) |
| Per-task totals | 0.5M–5M+, tail 7M; 30× run variance | same |
| In:Out ratio per task | ~2:1 (53.9% in / 24.4% out / 21.6% reasoning) | arXiv 2601.14470 (Tokenomics) |
| Output/benchmark task | ~27K–119K incl. reasoning | Artificial Analysis |

## 4. Recommended simulator defaults

```yaml
llm:                          # one endpoint = one modern GPU, 70B-class FP8
  output_tokens_per_s:        # aggregate decode across in-flight requests
    low: 400                  # 1×H100 modest batching
    mid: 2000                 # well-tuned 70B H100/H200
    high: 12000               # 8B-class or FP4 Blackwell
  input_tokens_per_s:         # aggregate prefill ≈ 2–10× decode
    low: 2000
    mid: 8000
    high: 40000
  single_request_decode: 30–200   # 70B: 30–50; 8B: 150–200; hosted: 40–170
  slots:
    low: 8
    mid: 32                   # InferenceX @50–78 t/s/user ⇒ 12–90
    high: 128                 # KV-cache bound; vLLM default 256
agent_request:
  input_tokens:
    shape: lognormal / uniform-with-outliers   # heavy tail, 30× run variance
    base_system_and_tools: 5_000–10_000
    median_per_call: 30_000–64_000
    mean_per_call: 60_000–84_000
    outliers_p95: 150_000–300_000
  output_tokens:              # bimodal
    short_mode: {weight: 0.6, range: 100–500}    # tool-call turns (derived — weakest evidence)
    long_mode:  {weight: 0.4, range: 2_000–10_000} # answer/planning turns
  calls_per_task: 10–200 (median ~50)
  prefill_decode_ratio: aggregate input ≈ 2–10× output
```

**Trust flags:** InferenceX/Artificial Analysis/arXiv = independent; MLPerf = standardized but self-run; SGLang/vLLM benches = reproducible vendor-technical; InferenceMAX/Simplismart/AMD claims = marketing (upper bounds only).

**Known gap:** per-call output-token split for tool-calling turns (60/40, ranges) is derived (AA per-task ÷ Requesty call counts), not directly published — a real agent trace should override it.
