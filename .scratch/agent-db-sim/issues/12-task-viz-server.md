# 12 — Live VCD emission + visualization server + report templates

Type: task
Status: resolved
Blocked by: 01, 02, 11

## Question

Build the visualization stack per the research recommendations from tickets 01 and 02:

- VCD writer fed by simkit signal hooks: real VCD files, written **live** during a run (tail-able), covering the agreed signal catalog (queue depths, slot utilization, request states, token counters).
- The dedicated visualization server: browser view of VCD/waveform-style signals over the running or finished simulation, per ticket 01's serving approach.
- The report layer: standard template(s) rendered from metrics outputs (ticket 11 result shape), user-expandable, served by the same server per ticket 02's technology.

Validated end-to-end: run a scenario → open the server → waveform view shows live signals, report renders percentiles/utilizations.

## Answer

Implemented under `src/agent_db_sim/viz/` + CLI additions per ticket-01/02 recommendations:

**Modules**
- `viz/vcd.py` — live VCD emission from probe samples via **pyvcd**. Catalog policy: `level` → `integer`/`real` vars, `counter` → cumulative `integer` vars, lifecycle edges (samples with `agent_id`+`request_id` tags) → `string` state vars (`<signal>_state`, values are the canonical states; scope = `component` tag, else dotted-signal decomposition, all under a root `agent_db_sim` scope). Timestamps quantized onto the tick grid (`$timescale 1 ns`, 1000 quanta per sim time unit; aggregates 0.001 sim-unit ticks). Incremental: completed `#timestamp` blocks are written through; `flush()` makes the file tailable at any moment. Safe-tail: `VcdTailer` consumes only complete `#timestamp` blocks (the newest `#` block is held back until a later `#` or the writer's closing `#ts` marker arrives). pyvcd only registers vars before the first t>0 change; signals first seen afterwards (writers that never `seal()`) are buffered and spilled into a trailing re-registration section by a second pyvcd writer sharing the file at `close()` — round-trip parseable by our own reader; `preregister()`/batch `write_vcd()` guarantee a single clean header for fixture/CI use. Ship shape: always ends with a `#ts` marker so a tailing reader never loses the last block.
- `viz/server.py` — FastAPI app factory: `GET /` (HTML shell embedding **d3-wave**, vendored locally in `viz/static/` — no CDN), `GET /signals` (catalog JSON), `GET /stream` (SSE: `init` snapshot with signal catalog + full per-signal series; ~1 Hz `delta` events with committed samples + newly-declared signals; keepalives while live; a finished/aged run gets `done` — liveness = VCD mtime window; delta discipline = same safe-tail boundary, tailer offset pinned at the snapshot byte), `GET /vcd` (raw file), `GET /reports` (auto-indexed templates), `GET /reports/<name>` (rendered, with Surfer deep-dive link). `serve` CLI runs it under **uvicorn**.
- `viz/report.py` — Jinja2 templates dir (`viz/templates/`, auto-indexed: any `*.html` not starting with `_` and not a scaffold) + Plotly figure builders: per-stage latency percentiles (p50/p95/p99 grouped bars), utilization series (time-weighted, from `utilization_series`), token-rate series, bottleneck attribution (hybrid queue-share/saturation bars); standard report renders from the run's `metrics.db` + metadata (meta table, `config.yaml` snapshot, VCD signal count/t-end). `plotly.min.js` (~4.8 MB) vendored from the plotly package into `viz/static/` (served at `/static/plotly.min.js`); standalone renders ship it next to the HTML.
- `viz/fixture.py` — synthetic run factory (`synthesize_samples` deterministic closed-loop agents → `make_fixture_run` writes `run.vcd` + `metrics.db` without any component integration — the whole viz stack validated standalone).
- `cli.py` — `agent-db-sim serve <run-dir> [--host --port]` (default 8060) and `agent-db-sim render <run-dir> [--name standard] [--out]` producing standalone report HTML for CI.
- Client glue `viz/static/app.js`: consumes `/signals` + SSE `/stream` (`init`/`delta`/`done`), folds samples into d3-wave `WaveGraph` row shape; `viz/templates/{base,wave,standard,reports}.html`; vendored `d3.min.js`, `d3-wave.js`(+css v1.1.5 UMD), minimal `fa-icons.js` shim (4 icons the bundle uses), `plotly.min.js`.

**Deps added**: `pyvcd`, `fastapi`, `uvicorn`, `jinja2`, `plotly`; dev: `httpx` (TestClient).

**Verification**: 23 new tests (`tests/test_viz_vcd.py` 9, `tests/test_viz_server_report.py` 14): VCD round-trip parse (catalog + counter cumulation + string states + quantum grid), late-signal spill, out-of-order clamp, safe-tail on partially-written file + closing-marker commit, fixture synthesis; FastAPI TestClient `/signals`, `/vcd` byte equality, `/` shell (no CDN refs, local vendored JS, Surfer link), SSE `init`→`delta`(held last block)→`done`, missing-VCD error event, `/reports` auto-index with user drop-in template, `/reports/standard` contains fixture db numbers, `render` CLI (exit codes + plotly copy + numbers), `serve` guard. Full suite: **124 passed** (101 prior + 23). uvicorn booted manually and endpoints smoke-tested.
