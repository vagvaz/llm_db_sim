# 01 — Live VCD viewing in a browser via a server

Type: research
Status: resolved
Blocked by:

## Question

The simulation must emit real VCD files live (while a run progresses), and a dedicated visualization server must let a user watch those signals as VCD/waveform-style traces in a browser (server-generated HTML over a browser, not a static HTML file).

What are the viable options, and which should we pick?

- Existing browser-capable waveform/VCD viewers or libraries (e.g. Surfer and its web story, GTKWave alternatives, JS waveform/VCD renderers such as dvt/waveform libraries — find what actually exists and is maintained in 2026): can any of them tail/stream a VCD file that is being written during simulation, or load it incrementally?
- Server options for "live": tailing VCD + websocket/SSE push vs periodic reload of a VCD the client fetches; simple static-server + polling vs a small framework (FastAPI/Starlette) — what is the lightest credible stack?
- VCD practicalities for our signals: signal catalog size (queue depths, slot utilization, request states, token counters, in-flight counts), sampling strategy while a sim runs (event-driven writes vs time-grid sampling), VCD file size at scale.

Deliver a compact comparison with sources and a single recommendation (viewer library + serving approach + how live tailing works), suitable for a grilling ticket to react to.

## Answer

**Split live vs deep-dive; build the delta pipeline ourselves (no browser viewer streams VCD today).**

- **Live view:** small **FastAPI/uvicorn** server — `GET /` (HTML shell embedding **d3-wave**, BSD-3, active 2026 — it consumes JSON signal rows, ideal for appending deltas), `GET /stream` (**SSE** push: `init` snapshot + ~1 Hz `delta` events), `GET /vcd` (full file), `GET /signals` (catalog from VCD header). Server **tails the VCD**, consuming only up to the last complete `#timestamp` block (half-written tails skipped server-side — browsers choke on incomplete VCD, so safe-tail logic must live in the server). SSE auto-reconnect re-emits init snapshot.
- **Deep-dive:** post-run link to **Surfer** (actively maintained, WASM web build accepts a VCD URL; self-host later; identical file to `/vcd`).
- **VCD practicalities:** event-driven writes (VCD-native), timestamps quantized to sim tick (defines the safe tail boundary); ~50–300 integer/enum signals; request states as enums/strings (d3-wave renders them); change-frequency drives size (~tens of MB worst case, gzip on demand). Writer: **pyvcd** (active, 2026-08).
- Rejected: websocket (needless), static-server+refetch (no safe boundary, client must parse raw VCD), VCDrom (static-only), VSC/desktop viewers (not plain-browser).
- Sources: surfer-project.org + GitLab activity, d3-wave GitHub, pyvcd PyPI, Verilator FST FAQ — full list in findings file.

Full findings: branch `research/browser-vcd-viewing`, file `findings-browser-vcd-live-viewing.md` (commit fbbce81, worktree /tmp/opencode/ads-research-01).
