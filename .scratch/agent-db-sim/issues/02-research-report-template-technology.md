# 02 — Template report technology (expandable, served to browser)

Type: research
Status: resolved
Blocked by:

## Question

The platform must produce a standard report from simulation results — latency percentiles, utilizations, bottleneck views, scenario comparisons — served to a browser by a dedicated server, built from templates that a user can expand (start from a standard template, add their own analysis).

What technology should own the "standard report, user-expandable" layer?

- Jupyter-based: papermill/nbclient parameterized notebooks + nbconvert to HTML; Quarto; jupyter-server based live view — how does each fit "templates served by our visualization server"?
- Plain Python templating: Jinja2 + plotting (matplotlib/plotly) rendered server-side — is that simpler for this scope?
- Anything else credible in 2026 for template-driven, browser-served, expandable reports from Python simulation results (plotly-dash? panel? — only if genuinely lighter).

Requirements: Python 3.10, offline/local-first (no cloud), reads our metrics outputs (tables/parquet/CSV — open question), integrates with a visualization server that also serves VCD waveform views, user-expandable templates. Deliver a compact comparison with sources and a single recommendation.

## Answer

**Recommendation: Jinja2 + Plotly server-side rendering, owned by the viz server.** Report layer = a Jinja2 template directory + small figure-builder modules; `metrics-dir → HTML` as one route on the same server that serves VCD views. `plotly.min.js` served once from the server's static dir (offline, no CDN). Users expand by dropping a new `.html` template (+ optional figure builder) — server auto-indexes templates.

- Chosen over: papermill/nbconvert (kernel spin-up per render, papermill org dormant — nbclient active), Quarto (standalone CLI toolchain outside pip, wants to own rendering), jupyter-server views (full Jupyter stack, converges to Jinja anyway), Dash/Panel/streamlit/marimo (app frameworks, heavier than the problem).
- Optional later: `.ipynb` template kind via `nbclient` + `nbconvert.HTMLExporter` — same serving route, no architecture change.
- Metrics input: pandas/pyarrow over ticket-11 output tables (parquet preferred, CSV fallback), same loader the sweep runner uses.

Full findings (comparison table, sources): branch `research/report-templates`, file `report-templates-findings.md` (commit fbcf2b0, worktree /tmp/opencode/ads-research-02).
