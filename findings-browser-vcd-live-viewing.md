# Ticket 01 — Research: Live VCD viewing in a browser via a server

Status: complete · Date: 2026-10-13 · Sources verified live (repo API timestamps as of today)

## TL;DR — Recommendation

**Split the problem in two: a "live" view during the run, and a "deep dive" view afterwards.**

1. **Live view (watching while the sim runs):** Small **FastAPI/uvicorn** server exposing:
   - `GET /` — server-generated HTML page bundling **d3-wave** (BSD-3-Clause, TypeScript, last push **2026-06-09** — active) as the waveform renderer.
   - `GET /vcd` — serves the on-disk VCD (and a gzipped variant).
   - `GET /stream` — **SSE** push (server-sent events, one-way, auto-reconnect, needs no websocket library).
   The server **tails the VCD file** incrementally, parses only up to the *last complete newline / last complete `#timestamp`* (this is what makes "tail a file being written" safe — a half-written tail line is simply ignored until it is complete), converts new changes to d3-wave's JSON row format (`["time", "value"]` per signal), and pushes ~1 Hz deltas over SSE. The client appends rows to the d3-wave data model — the browser never parses VCD itself.
   - VCD parsing on the server side can start with hand-rolled ~100-line logic or Python packages (`pyvcd` v0.5.0, released **2026-08-11**, actively maintained, writes and reads VCD; or `vcdvcd`).
2. **Deep-dive view (after a run / anytime):** link out to **Surfer** (EUPL-1.2) — the clear leader in actively-maintained open waveform viewers (last commit **2026-10-12**, five days ago). Its WASM web build lives at `app.surfer-project.org` and accepts a VCD URL; it also has a headless `surver` server mode. We can self-host the WASM build later; for now the CI/URL workflow is enough.
3. **Serving approach:** FastAPI ≫ plain static server, because the static server cannot do SSE/streaming. FastAPI/Starlette remains "light" (single file, one `uvicorn` process).

---

## 1. Browser-capable waveform/VCD viewers — what actually exists (2026)

| Project | What it is | Formats | Browser story | Activity (verified) | License | Live/tailing? |
|---|---|---|---|---|---|---|
| **Surfer** (surfer-project) | Rust waveform viewer + WASM web build + `surver` headless server | VCD, FST, GHW | Web build at `app.surfer-project.org`, VS Code extension (23k installs), downloadable source web build | **2026-10-12** (last_activity), v0.7.0 released ~2026-05 | EUPL-1.2 | "Wave file reloading" is a shipped feature (desktop-hit auto-reload of changed files exists — a CHANGELOG bugfix "crash when file was changed multiple times on disk" confirms the mechanism). **No incremental/streaming append**; reload = full re-parse. Web/WASM version is slower (no multithreading) and feature-reduced. |
| **VCDrom** (wavedrom org) | Pure-JS VCD drag-and-drop viewer, vc.drom.io | VCD | Runs fully in browser after file drop | pushed **2025-09-01** (~1 yr ago), 113★, 41 open issues | MIT | No streaming; static load of complete file. Maintained-ish, not actively. |
| **d3-wave** (Nic30) | D3.js-based SVG waveform renderer ("GTKWave for javascript") | **Not VCD** — consumes JSON signal dumps `{name, type{name,width}, data[[t,v]], children}` | Rendered by any server-side-generated page | pushed **2026-06-09**, 68★ | BSD-3-Clause | **Ideal for live deltas**: appending data rows is just adding entries to the JSON model. No VCD parsing client-side means the server owns ownership of time/chunks. |
| **VaporView** (Lramseyer) | VS Code waveform viewer | VCD, FST, GHW | VS Code only (webview), not a plain-web page | pushed **2026-10-06** (very active), 361★ | GPL-3.0 | Uses wellen (WASM); even implements the **surfer-surver protocol** — proof the surver protocol is reusable — but not a standalone browser app. |
| **wellen** (ekiwi) | Rust VCD/FST/GHW parser (backend used by Surfer/VaporView) | VCD, FST, GHW | Ships a WASM binding | pushed **2026-08-23**, 146★ | BSD-3-Clause | Threaded/multi-signal-subset optimized; no public incremental-read API for a growing file. |
| **nanjo712/Waveform-Viewer** | C++→WASM parser + React frontend, Tauri desktop | VCD, FST | GitHub Pages demo | created 2026-02, pushed **2026-03-01**, 1★ | MIT | Too new/single-author to bet on. |
| **DigitalJS (Online)** | Web logic-circuit simulator w/ waveform view | VCD + Yosys JSON netlist | Web app | semi-active | unclear-CLA-ish | Needs Yosys netlist — overkill for a sim dashboard. |
| **eightolives WaveformViewer** | Legacy browser JS waveform tool | VCD (creates & reads) | Browser | stale | mixed | Dated, low activity. |
| **WaveCrux** | Commercial-ish cross-platform viewer | VCD/FST/GHW | Web-capable | closed-source | commercial | Not our model. |
| **wavedash** (donn) | SVG waveform viewer | (undocumented) | Browser | Explicitly "🚧 not stable / not really supported (yet)" | Apache-2 | Rejected by own README. |
| **GTKWave / Dinotrace / Impulse / DVT-IDE viewers** | Desktop/IDE viewers | VCD/FST | Not browser-embeddable (DVT is commercial IDE; note its changelog even logs "Exception thrown when parsing an *incomplete* .vcd file" — incomplete-VCD handling is a real, universal hazard) | various | various | Out of scope. |

**Key takeaway:** **No browser viewer today streams or incrementally appends a VCD being written.** Surfer is the only one with any "reload-from-disk" behavior, and it is full-file-reload. Anything truly live ⇒ we build the delta pipeline (server-tail + JSON push) and put a client-side renderer underneath it — d3-wave is precisely shaped for that (it consumes pre-chewed JSON deltas).

## 2. Server approach for "live"

| Mechanism | Pros | Cons | Verdict |
|---|---|---|---|
| Static file server + browser-periodic refetch of whole VCD | Simplest; ~zero deps | Re-downloads whole (possibly growing) file each poll; no server code ⇒ no safe-tail boundary; client still needs a VCD parser (VCDrom/surfer-web only handle complete files, weak with truncated tails) | Fallback only |
| **SSE (Server-Sent Events) over FastAPI/Starlette** | One-way push; automatic browser reconnect; HTTP-native; ~30 lines server code; isomorphic with Python declarative sim model | One-way only (that's all we need for a dashboard) | **Pick this** |
| Websocket | Bidirectional | Needs a websocket layer (extra deps/complexity) for nothing; reconnection is DIY | Skip |
| Surfer `surver` as "the server" | Best post-run UX, remote load of file from a URL, remote control via SUCL | Definitely not live-tail; parses a complete file then compresses in-memory; separate dependency stack (Rust) from our Python sim | Use for deep-dive hyperlink only, not as the live server |

**Sizing check on "lightweight":** FastAPI + uvicorn is one dependency group, no DB, no JS build step. It's lighter than pulling node/npm tooling into the project. If we truly wanted zero deps, we could implement SSE with `http.server` by hand — but it's error-prone and manual; go FastAPI.

## 3. VCD practicalities for our signals

Signal catalog (queue depths, slot occupancy, request state enums, token counters, in-flight counts) implies **~50–300 `integer`/`wire` vars** with modest widths (counts fit in 8–16 bits). Two properties of our data matter more than signal count:

- **Change frequency, not signal count, drives size.** VCD records actual value changes only (never unchanged lines — `$dumpall`-buried values excepted). Our command/queue signals change hundreds of times per tick at most, usually far fewer. With event-driven writing and *timestamps quantized to simulation ticks*, per-1000-tick estimate for 200 signals at ~15–25 bytes per change line ≈ **1–5 MB of VCD per long multi-1000-tick run** — fine to gzip-stream for the browser on demand and trivially fine for SSE JSON deltas of "new tick X → tick X+N rows".
- **Frame the "state-change" signals as enums/strings.** d3-wave has renderers for `str` and `enum` types per its README — mapping "request state" to named enum strings (like Surfer's enum translation) makes traces readable in the live view.

**Sampling strategy:**
- **Event-driven (write every actual change) is correct** and is native VCD semantics — don't force a uniform time grid; it inflates the file with redundant values and dilutes the visualization.
- Quantize timestamps to the simulation tick (one `#t` line per tick boundary, not per sub-event), so the file has a well-defined safe boundary for the tailer to stop at.
- Optional watch-list exclusion: skip writing stable-forever counters (e.g. a token counter that rarely moves) or write them via periodic re-dump at intervals (VCD allows re-asserting values at any timestamp; that "heartbeat" pattern keeps a trace alive over watch periods without per-event spam).
- Escape hatch: if a run proves too chatty, post-run conversion to **FST** shrinks it to "a fraction of the size of the equivalent VCD" (official Verilator FAQ language) — but Surfer/d3-wave both handle VCD fine and the requirement is "real VCD", so keep VCD primary.

**Parse-robustness for live tailing:** VCD's ASCII format makes a half-written tail a *recoverable* anomaly on the read side for our server (stop at last complete newline). Browsers are worse at this — surfer release notes literally include "Loading VCD files with errors more rarely crashes Surfer", and DVT's changelog has "Exception thrown when parsing an incomplete .vcd file". ⇒ **The safe-tail-boundary logic must live in the server**, not the browser. Another reason the delta-push design wins over "browser reloads the raw VCD periodically".

## 4. Concrete architecture (recommended)

```
simulator (writes run.vcd, flushes per tick)      /tmp/run.vcd
         │
         ▼
FastAPI app (uvicorn, single process)
  ├─ GET /            → HTML shell embedding d3-wave (served from vendor dir)
  ├─ GET /signals     → {name, type{name,width}} catalog (from VCD header)
  ├─ GET /stream      → SSE:
  │      event: init  {signals:[...], initial:[rows up to now]}
  │      event: delta {t_min, rows:{...}} every ~0.5–1 s
  ├─ GET /vcd         → full file (for download / Surfer deep-dive link)
  └─ on-run-complete  → emit "done", client stops listening
         │
         ▼
Browser: d3-wave component keeps appending `data` arrays; live "follow
cursor" scrolls to newest tick; end of run → offer "open in Surfer" link.
```

- **Live tail detail:** server opens the file, tracks byte offset, reads appended bytes each interval, splits lines, only consumes up to the last `#t` block whose lines all end cleanly (i.e., next `#` seen or EOF-right-after-newline). id-chars → values from VCD header.
- **Restart safety:** SSE auto-reconnects in the browser; the server can re-emit init-snapshot with current rows on reconnect, meaning the client never loses sync.
- **Post-run:** endpoint that builds a URL to self-hosted Surfer wasm (or `app.surfer-project.org/?file=<URL>` link) — the identical file that we streamed.

## Sources (all fetched/verified 2026-10-13)

- Surfer main site / features: https://surfer-project.org · docs https://docs.surfer-project.org/book/ (CLI: `surfer server`, `--command`, `surver` variant)
- Surfer repo/activity: https://gitlab.com/surfer-project/surfer (`last_activity_at: 2026-10-12`), releases v0.6.0 / v0.7.0; README features list explicitly includes "Wave file reloading" and "Remote support: Serving waveforms from remote server / Remote control of Surfer"
- Surfer WASM story + URL-load demo: YosysHQ Community Spotlight, http://blog.yosyshq.com/p/community-spotlight-surfer ("works almost flawlessly in web assembly… append that file to the URL along with some commands")
- Surfer talk (OSDA2024) — surver = load file, compress in-memory, connect-from-browser; on-demand signal decompression: https://www.youtube.com/watch?v=z3RqzdBz7j4
- Surfer VS Code extension (web-port limitations, multithreading): https://marketplace.visualstudio.com/items?itemName=surfer-project.surfer
- d3-wave: https://github.com/Nic30/d3-wave · npm https://www.jsdelivr.com/package/npm/d3-wave (BSD-3-Clause) · JSON input format & feature list in README
- VCDrom: https://github.com/wavedrom/vcdrom (MIT, 113★, pushed 2025-09-01), demo https://vc.drom.io
- VaporView (wellen-WASM, surfer-surver-protocol compatible): https://github.com/Lramseyer/vaporview (GPL-3, pushed 2026-10-06)
- wellen parser: https://github.com/ekiwi/wellen (BSD-3, pushed 2026-08-23)
- nanjo712/Waveform-Viewer (C++/WASM+React): https://github.com/nanjo712/Waveform-Viewer (MIT, pushed 2026-03-01)
- wavedash: https://github.com/donn/wavedash (Apache-2, "Under Construction / not supported")
- DigitalJS Online: http://digitaljs.tilk.eu
- pyvcd (writes/reads VCD per IEEE 1800-2023 §21.7): https://pypi.org/project/pyvcd/ (v0.5.0, 2026-08-11)
- VCD vs FST size/"fraction of the size": Verilator FAQ https://veripool.org/guide/latest/faq.html ("FST dumps are a fraction of the size of the equivalent VCD"), cocotb docs https://docs.cocotb.org/en/stable/simulator_support.html ("FST traces are much smaller and more efficient to write")
- Evidence of the incomplete-VCD hazard in other viewers: surfer RELEASE notes ("Loading VCD files with errors more rarely crashes Surfer") — https://gitlab.com/surfer-project/surfer/-/releases · DVT viz changelog "Exception thrown when parsing an incomplete .vcd file" (DVT-24791) — https://eda.amiq.com/documentation/vscode-readme-changelog/latest
