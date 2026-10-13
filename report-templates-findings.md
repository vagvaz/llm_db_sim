# 02 — Research: template report technology (expandable, served to browser)

Date: 2026-10-13 · Scope: "standard report, user-expandable" layer for `agent-db-sim`
Requirements recap: Python 3.10+, local/offline-first, reads metrics tables on disk (parquet/CSV), served as HTML by the same visualization server that serves VCD waveforms, templates a user can extend.

---

## TL;DR Recommendation

**Jinja2 + Plotly server-side rendering, owned by the viz server.** The report layer is a Jinja2 template directory + a small renderer that builds Plotly figures from metrics tables and embeds them into the page; the viz server serves the rendered HTML as just another route. Fully pip-installable, tiny dependency footprint, trivially offline, plain-HTML templates users can extend, and zero conflict with the VCD serving stack (see ticket 01).

Keep the door open (no work now) for a second, optional template kind — `.ipynb` executed with `nbclient` + `nbconvert.HTMLExporter` — because it composes with the same serving model; skip papermill, Quarto, jupyter-server live view, Dash/Panel/streamlit/marimo (rationale below).

---

## Option comparison

### 1. Jupyter-based

#### 1a. papermill / nbclient → nbconvert → HTML

The canonical "notebooks as templates" pipeline: notebook with a `parameters`-tagged cell → `papermill.execute_notebook(in, out, parameters={...})` → `jupyter nbconvert --to html` ([papermill docs — parameterize](https://papermill.readthedocs.io/en/latest/usage-parameterize.html); [wrighters.io walkthrough of exactly the report loop](https://www.wrighters.io/parameters-jupyter-notebooks-with-papermill)). Papermill supports Python 3.10+ ([README](https://github.com/nteract/papermill)).

- **Pros:** templates are runnable notebooks — a user "expands the template" by adding cells; nbconvert `HTMLExporter` also produces fully standalone offline HTML (though embedding Plotly in offline HTML often needs the third-party `nb_offline_convert` hack, [plotly forum thread](https://community.plotly.com/t/jupyter-notebook-html-using-nbconvert-not-showing-the-tables-in-the-html-after-a-while/79846)).
- **Cons:** each render spawns a kernel (startup latency per report); templates are `.ipynb` files (JSON — awkward to author/version for non-Jupyter users); papermill (nteract org) is in quiet maintenance — docs still at 2.7.0 with the changelog ending at 2.7.0/2.6.0 ([papermill docs](https://papermill.readthedocs.io)). Active successor is nbclient, maintained by the Jupyter org ([nbclient repo](https://github.com/jupyter/nbclient)).
- **Fit for "templates served by our viz server":** workable but heavyweight — the server must shell out to (or drive) a kernel execution for every view of a report.

#### 1b. Quarto

Quarto renders `.qmd`/`.ipynb` to production-quality HTML by executing code with Jupyter, then converting with Pandoc ([quarto: using Python](https://quarto.org/docs/computations/python.html), [Jumping Rivers: Quarto for the Python user](https://www.jumpingrivers.com/blog/quarto-for-python-users)).

- **Pros:** best-document-quality output of all options; offline rendering is real; user-expandable templates are plain text (`.qmd`).
- **Cons:** **not a pip package** — it is a standalone CLI whose installers bundle Pandoc, Typst, Deno, Mermaid, esbuild, Dart Sass ([Quarto FAQ on dependencies](https://quarto.org/docs/faq)) — a separate ~hundreds-of-MB toolchain outside the Python dependency manager, integrating only via `subprocess` from the viz server; slower per-render; version coupling between Quarto's Jupyter engine and your env. 2026 status: very actively maintained (v1.10 current, 1.11 pre-release; [download page](https://quarto.org/docs/download.html)).
- **Fit:** poor match for "our own lightweight server renders a report route" — Quarto wants to own the render pipeline, and the toolchain weight contradicts the sim platform's lean local-first design.

#### 1c. jupyter-server based live view

Serve reports via a jupyter-server extension: subclass `JupyterHandler`, register tornado routes, optionally use `ExtensionAppJinjaMixin`/`ExtensionHandlerJinjaMixin`, which add a **Jinja environment** for templates ([jupyter-server docs — writing server extensions](https://jupyter-server.readthedocs.io/en/latest/developers/extensions.html)).

- **Pros:** real server model (matches the requirement); discoverable that even Jupyter's own report-serving layer resolves to Jinja templates + handlers — which validates the lighter option below.
- **Cons:** you pull in the entire Jupyter server stack (ServerApp, extensions, auth, config) to serve what is a static report; live interactive views additionally need kernels and JupyterLab machinery. You end up writing Jinja templates anyway.
- **Fit:** heavy for the job — the only deliverable you'd actually use from it is Jinja templating.

### 2. Plain Python templating: Jinja2 + Plotly/matplotlib

- **Pattern (documented by Plotly themselves):** build figures server-side, render each to an HTML div string, drop it into a Jinja template — [plotly.com/python/interactive-html-export](https://plotly.com/python/interactive-html-export/) shows precisely `jinja2.Template` + `fig.to_html(full_html=False)` as the recommended way to make "templates" page-level reports.
- **Offline-first:** `include_plotlyjs=True` embeds the ~3MB plotly.js into the HTML so files are self-contained ([plotly.io.to_html docs](https://plotly.com/python-api-reference/generated/plotly.io.to_html.html)); better, use `include_plotlyjs='directory'` / serve `plotly.min.js` **once from the viz server's static dir** (same JS bundle can also serve the VCD view; identical interactive-chart infrastructure).
- **Matplotlib** for static, JS-free figures (SVG/PNG inline) or PDF export — [nbconvert library docs](https://nbconvert.readthedocs.io/en/latest/nbconvert_library.html) show the complementary "render HTML programmatically" mindset; tables render via pandas `DataFrame.to_html()` from the parquet/CSV metrics.
- **Expansion model:** templates are plain `.html` files in a search-path directory (Jinja `FileSystemLoader`); a user adds a template file + optionally a small Python "figure builder" module, and the viz server lists/renders it. No toolchain, no kernel, no external process.
- **Coexistence with VCD server:** the report renderer is a pure function `metrics-dir → HTML`; it plugs into whatever ASGI/tornado server ticket 01 chose as one extra route.
- **Cons:** page interactivity is chart-level only (no reactive dash callbacks); styling consistency is on you (solvable with one base template).

### 3. Other "lighter" 2026 contenders — none lighter

- **Plotly Dash:** app-server + React bundle; applications are Python *code*, not templates; static per-route HTML export isn't a first-class feature — heavier and a worse "expandable template" model than plain Jinja.
- **Panel (HoloViz):** lighter than Dash and has FastTemplate/templated layouts + `panel.save('report.html')` for offline static export, but the boilerplate still requires bokeh-server machinery or bokeh-JS embedding, and the "template" model is HoloViz-specific ([panel docs](https://panel.holoviz.org/user_guide/Templates.html)) — doesn't beat "Jinja2 + plotly.to_html".
- **Streamlit:** runtime-driven interactive app model (whole-script rerun semantics), not template-driven reports; wrong shape for this job.
- **marimo:** genuinely modern pip-only reactive notebooks with `marimo export html` static export and app serving ([marimo docs](https://docs.marimo.io/guides/coming_from/papermill)); marimo notebooks-as-reports is attractive for *analysts*, but reports become marimo app definitions that run in marimo's runtime — orthogonal to, and additively separate from, the viz server's plain-HTML route model.

---

## Comparison table

| Criterion | papermill/nbclient + nbconvert | Quarto | jupyter-server view | **Jinja2 + Plotly** | Dash | Panel |
|---|---|---|---|---|---|---|
| Deps (pip-managed) |✔ (papermill dormant, nbclient active) | ✘ standalone CLI + Pandoc/Deno/Sass | ✔ full Jupyter stack | ✔ | ✔ + React | ✔ + bokeh |
| Python 3.10 | ✔ | ✔ (via Jupyter) | ✔ | ✔ | ✔ | ✔ |
| Offline/local-first | ✔ (kernel + occasional offline-HTML hack) | ✔ | ✔ | ✔ (embed or serve plotly.js once) | offline-able but heavy | offline-able but heavy |
| Reads disk metrics tables | ✔ in template code | ✔ | ✔ | ✔ pandas/pyarrow | awkward | ✔ |
| Template = user-expandable artifact | `.ipynb` (JSON, clunky) | `.qmd` (nice) | async handler code | **`.html` file** | app code | `.formattable` partials |
| Render cost | kernel spin-up per render | full render pass (subprocess CLI) | kernel (live) | in-process, ms | server session | server session |
| Coexists with VCD viz server | subprocess or second server | subprocess | second server | **one route on the same server** | parallel app server | parallel app server |

## Recommendation (one)

**Standard template layer: Jinja2 templates + Plotly figures rendered server-side inside the visualization server.**

- Metrics reading: pandas/pyarrow over the ticket-11 output tables (parquet preferred; CSV fallback), same loader used by the sweep runner.
- Serving: one route per report (`/reports/<template>/<run>`); `plotly.min.js` served once from the server's static directory and referenced by every figure (`to_html` div mode) — same bundle strategy as the VCD waveform viewer, so no CDN and no per-file 3MB duplication.
- User expansion: drop a new `.html` template in the templates dir (+ optional small Python builder for its figures); server indexes available templates automatically.
- Optional later extension (not now): a `.ipynb` template kind executed with `nbclient` + `nbconvert.HTMLExporter` (skip papermill, whose host org is dormant) — it reuses the exact same serving route, so no architecture change if the team wants notebook-based analysis as reports.
- Explicitly not chosen: Quarto (external CLI toolchain outside pip, overkill for report views served by our own server), jupyter-server live views (full Jupyter stack for static reports — and it converges to Jinja anyway), Dash/Panel/streamlit (app frameworks, heavier than the problem; no lighter "other" option found that beats the plain-templating path in 2026).

## Sources

- https://papermill.readthedocs.io/en/latest/usage-parameterize.html — parameterized notebook execution (docs v2.7.0, Python 3.10+)
- https://github.com/nteract/papermill — README; last documented release 2.7.0
- https://github.com/jupyter/nbclient — maintained notebook-execution library (papermill's execution dependency)
- https://nbconvert.readthedocs.io/en/latest/nbconvert_library.html — HTMLExporter as a library, custom templates
- https://quarto.org/docs/computations/python.html and https://www.jumpingrivers.com/blog/quarto-for-python-users — Jupyter + Pandoc render pipeline
- https://quarto.org/docs/faq — bundled dependencies (Pandoc, Typst, Deno, Mermaid, esbuild, Dart Sass); standalone CLI, not pip
- https://quarto.org/docs/download.html — 2026 release activity (1.10 current, 1.11 pre-release)
- https://jupyter-server.readthedocs.io/en/latest/developers/extensions.html — custom routes, `ExtensionAppJinjaMixin`/`ExtensionHandlerJinjaMixin` (Jinja under the hood)
- https://plotly.com/python/interactive-html-export/ — official Jinja2 + `fig.to_html(full_html=False)` templated-report pattern
- https://plotly.com/python-api-reference/generated/plotly.io.to_html.html — offline modes: embed (~3MB) vs `'directory'` (serve plotly.js locally once)
- https://community.plotly.com/t/jupyter-notebook-html-using-nbconvert-not-showing-the-tables-in-the-html-after-a-while/79846 — offline-HTML friction of the notebook-export path
- https://docs.marimo.io/guides/coming_from/papermill and https://marimo.io — marimo's static HTML export / app model (checked as "other")
- https://panel.holoviz.org/user_guide/Templates.html — Panel templating model (checked as "other")
