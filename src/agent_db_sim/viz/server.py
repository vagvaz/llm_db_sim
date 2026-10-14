"""Visualization server (ticket 01 serving approach): FastAPI + uvicorn.

- ``GET /``             HTML shell embedding d3-wave (locally vendored JS)
- ``GET /signals``      signal catalog JSON parsed from the run's VCD
- ``GET /stream``       SSE: ``init`` snapshot, then ~1 Hz ``delta`` events
                        while the run is live, ``done`` when it finishes
- ``GET /vcd``          the raw VCD file (Surfer deep-dive link, post-run)
- ``GET /reports``      auto-indexed report templates
- ``GET /reports/<k>``  rendered report from metrics.db (Jinja2 + Plotly)
- ``GET /sweeps``       list sweep roots found under ``sweeps_root`` (JSON)
- ``GET /sweeps/<s>``   sweep-comparison report over that sweep root
                        (default template ``sweep_compare``)
- ``GET /sweeps/<s>/reports/<t>``  sweep report with an explicit template
- ``GET /live``         animated component view + parameter form
- ``GET /live/scenarios`` scenario picker entries (JSON)
- ``POST /live/run``    launch one live run (rejects concurrent runs, 409)
- ``GET /live/state``   live-run slot status (JSON)
- ``GET /live/stream``  SSE: live ``metrics.db`` sample rows -> ``delta`` events
- ``GET /live/run/<id>/report`` / ``.../vcd``  artifacts of a finished live run

SSE tail discipline per ticket 01: the server holds back the newest
``#timestamp`` block (it may still be mid-write); clients only ever see
catalog/sample data derived from complete blocks. A finished run emits
``done`` right after ``init`` and never again.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from agent_db_sim.viz.live import (
    LiveBusy,
    LiveError,
    LiveManager,
    SamplesTail,
)
from agent_db_sim.viz.report import (
    render_report,
    render_reports_index,
    render_sweep_report,
    static_dir,
    templates_dir,
)
from agent_db_sim.viz.vcd import QUANTA, VcdTailer, parse_vcd_bytes

__all__ = ["create_app", "sse_events", "live_sse_events", "VCD_NAME", "METRICS_NAME"]

VCD_NAME = "run.vcd"
METRICS_NAME = "metrics.db"

# stream poll cadence: ~1 Hz delta events per ticket-01 answer
POLL_INTERVAL = 1.0
# a run counts as live while its VCD was touched within this window
LIVE_WINDOW = 10.0
# optional idle cap (None = unbounded keepalives while the run stays live)
MAX_IDLE_POLLS = None


def _sse(event: str, payload: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, default=str)}\n\n"


def create_app(
    run_dir: Any,
    sweeps_root: Optional[str] = None,
    live_root: Optional[str] = None,
    scenarios_root: Optional[str] = None,
    live_poll: float = POLL_INTERVAL,
) -> FastAPI:
    """Build the visualization app around one run directory.

    ``sweeps_root`` scopes ``GET /sweeps`` discovery; it defaults to the
    run dir's parent (the usual ``runs/`` layout, where sweep roots —
    directories holding ``sweep_summary.json`` and per-run ``metrics.db``
    dirs — are siblings of the served run).

    ``live_root`` / ``scenarios_root`` scope the ``/live`` animated view:
    where launched runs land (``runs/live/<run_id>/`` by default) and the
    scenario picker's directory (the repo-root ``scenarios/`` by default).
    """
    run_dir = Path(run_dir)
    vcd_path = run_dir / VCD_NAME
    sweeps_root = Path(sweeps_root) if sweeps_root is not None else run_dir.parent
    if live_root is None:
        live_root = run_dir.parent / "live"
    if scenarios_root is None:
        guess = Path(__file__).resolve().parents[3] / "scenarios"
        scenarios_root = str(guess) if guess.is_dir() else None
    live = LiveManager(
        Path(live_root), Path(scenarios_root) if scenarios_root else None
    )

    app = FastAPI(title="agent-db-sim viz", version="0.1.0")
    app.state.live = live
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    templates = Jinja2Templates(directory=str(templates_dir))

    def _snapshot(include_tail: bool = True) -> "Any":
        data = parse_vcd_bytes(vcd_path.read_bytes(), include_tail=include_tail)
        catalog = data.catalog()
        series = {
            name: [[t, val] for t, val in rows]
            for name, rows in data.signals.items()
        }
        return {
            "t_min": data.t_min,
            "t_max": data.t_max / QUANTA,
            "signals": catalog,
            "series": series,
        }

    def _not_found(detail: str) -> HTTPException:
        return HTTPException(status_code=404, detail=detail)

    # -- routes --------------------------------------------------------- #
    @app.get("/")
    def home(request: Request):
        if not vcd_path.exists():
            raise _not_found(f"no {VCD_NAME} under {run_dir}")
        surfer_url: Optional[str] = None
        try:
            vcd_url = str(request.url_for("get_vcd"))
            if vcd_url.startswith("http"):
                surfer_url = f"https://surfer-project.org/?file={vcd_url}"
        except Exception:
            pass
        return templates.TemplateResponse(
            request,
            "wave.html",
            context={
                "meta": {"run_name": run_dir.name},
                "surfer_url": surfer_url,
            },
        )

    @app.get("/signals")
    def signal_catalog() -> JSONResponse:
        if not vcd_path.exists():
            raise _not_found(f"no {VCD_NAME} under {run_dir}")
        snap = _snapshot()
        return JSONResponse({"signals": snap["signals"], "t_max": snap["t_max"]})

    @app.get("/stream")
    def stream() -> Response:
        if not vcd_path.exists():
            body = (
                _sse("error", {"detail": f"no {VCD_NAME} under {run_dir}"})
                + _sse("done", {"reason": "missing"})
            )
            return Response(content=body, media_type="text/event-stream")
        return StreamingResponse(
            sse_events(vcd_path),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/vcd", name="get_vcd")
    def get_vcd() -> FileResponse:
        if not vcd_path.exists():
            raise _not_found(f"no {VCD_NAME} under {run_dir}")
        return FileResponse(
            vcd_path,
            media_type="application/octet-stream",
            filename=vcd_path.name,
        )

    @app.get("/reports")
    def reports_root() -> Response:
        return Response(render_reports_index(run_dir), media_type="text/html")

    @app.get("/reports/{name}")
    def report(name: str, request: Request) -> Response:
        extra: Dict[str, Any] = {}
        try:  # deep-dive link for the identical /vcd file
            extra["surfer_url"] = (
                f"https://surfer-project.org/?file={request.url_for('get_vcd')}"
            )
        except Exception:
            pass
        try:
            html = render_report(run_dir, name=name, standalone=False, extra_context=extra)
        except (FileNotFoundError, ValueError) as exc:
            # FileNotFoundError: unknown template; ValueError: e.g. a sweep-only
            # template was requested against a single run (404 + guidance).
            raise _not_found(str(exc)) from exc
        return Response(html, media_type="text/html")

    # -- sweeps (stage-6 lane B) ---------------------------------------- #
    def _is_sweep_dir(directory: Path) -> bool:
        if not (directory / "sweep_summary.json").exists():
            return any(
                (child / "metrics.db").exists()
                for child in directory.iterdir() if child.is_dir()
            )
        return True

    def _list_sweeps() -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if not sweeps_root.exists():
            return out
        for child in sorted(sweeps_root.iterdir()):
            if not child.is_dir() or not _is_sweep_dir(child):
                continue
            n_runs = sum(
                1 for c in child.iterdir()
                if c.is_dir() and (c / "metrics.db").exists()
            )
            out.append({"name": child.name, "path": str(child), "n_runs": n_runs})
        return out

    def _resolve_sweep(sweep: str) -> Path:
        resolved_root = sweeps_root.resolve()
        candidate = (sweeps_root / sweep).resolve()
        if candidate.parent != resolved_root:
            raise _not_found(f"no sweep {sweep!r} under {sweeps_root}")
        if not candidate.is_dir():
            raise _not_found(f"no sweep {sweep!r} under {sweeps_root}")
        return candidate

    def _render_sweep(sweep: str, template: str) -> Response:
        try:
            sweep_dir = _resolve_sweep(sweep)
            html = render_sweep_report(
                sweep_dir, name=template, standalone=False,
            )
        except (FileNotFoundError, ValueError) as exc:
            raise _not_found(str(exc)) from exc
        return Response(html, media_type="text/html")

    @app.get("/sweeps")
    def sweeps() -> JSONResponse:
        return JSONResponse({"sweeps": _list_sweeps()})

    @app.get("/sweeps/{sweep}")
    def sweep_default(sweep: str) -> Response:
        return _render_sweep(sweep, "sweep_compare")

    @app.get("/sweeps/{sweep}/reports/{template}")
    def sweep_report(sweep: str, template: str) -> Response:
        return _render_sweep(sweep, template)

    # -- live animated view ------------------------------------------------ #
    @app.get("/live")
    def live_page(request: Request):
        return templates.TemplateResponse(request, "live.html")

    @app.get("/live/scenarios")
    def live_scenarios() -> JSONResponse:
        return JSONResponse({"scenarios": live.scenarios()})

    @app.get("/live/state")
    def live_state() -> JSONResponse:
        return JSONResponse(live.refresh())

    @app.post("/live/run")
    async def live_run(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        scenario = payload.get("scenario")
        overrides = payload.get("overrides") or {}
        if not isinstance(overrides, dict):
            raise HTTPException(status_code=422, detail="overrides must be an object")
        try:
            st = live.start(scenario, overrides)
        except LiveBusy as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except LiveError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse(st.public(), status_code=202)

    @app.get("/live/stream")
    def live_stream() -> Response:
        return StreamingResponse(
            live_sse_events(live, poll_interval=live_poll),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    def _live_run_dir(run_id: str) -> Path:
        candidate = (live.live_root / run_id).resolve()
        if candidate.parent != live.live_root.resolve():
            raise _not_found(f"no live run {run_id!r}")
        if not candidate.is_dir():
            raise _not_found(f"no live run {run_id!r}")
        return candidate

    @app.get("/live/run/{run_id}/report")
    def live_report(run_id: str) -> FileResponse:
        report = _live_run_dir(run_id) / "report.html"
        if not report.exists():
            raise _not_found(f"no report for live run {run_id!r} yet")
        return FileResponse(report, media_type="text/html")

    @app.get("/live/run/{run_id}/vcd")
    def live_vcd(run_id: str) -> FileResponse:
        vcd = _live_run_dir(run_id) / VCD_NAME
        if not vcd.exists():
            raise _not_found(f"no {VCD_NAME} for live run {run_id!r} yet")
        return FileResponse(
            vcd, media_type="application/octet-stream", filename=vcd.name
        )

    return app


# --------------------------------------------------------------------- #
# SSE event pipeline (sync generator; testable per unit)                 #
# --------------------------------------------------------------------- #


def tailer_read(
    tailer: VcdTailer,
) -> Tuple[List[List[Any]], List[Dict[str, Any]]]:
    """One ``tailer.read()`` pass with JSON-safe conversion."""
    samples, defs = tailer.read()
    rows = [
        [float(t), name, val if isinstance(val, (int, float, str)) else str(val)]
        for t, name, val in samples
    ]
    return rows, defs


def _is_live(vcd_path: Path, live_window: float) -> bool:
    try:
        age = time.time() - vcd_path.stat().st_mtime
    except OSError:
        return False
    return age <= live_window


def sse_events(
    vcd_path: Any,
    poll_interval: float = POLL_INTERVAL,
    live_window: float = LIVE_WINDOW,
    max_idle_polls: int = MAX_IDLE_POLLS,
    fast: bool = False,
) -> Iterator[str]:
    """SSE pipeline: ``init`` snapshot, ``delta`` events, then ``done``.

    The run is treated as live while its VCD mtime is within ``live_window``;
    a finished run gets exactly one ``done`` event. ``fast=True`` shrinks the
    poll sleeps for tests.
    """
    vcd_path = Path(vcd_path)
    wait = 0.01 if fast else poll_interval
    if not vcd_path.exists():
        yield _sse("error", {"detail": f"no VCD at {vcd_path}"})
        yield _sse("done", {"reason": "missing"})
        return

    def _still_live() -> bool:
        return _is_live(vcd_path, live_window)

    snap_bytes = vcd_path.stat().st_size   # snapshot offset: deltas flow after
    snap = parse_vcd_bytes(vcd_path.read_bytes(), include_tail=True)
    init = {
        "t_min": snap.t_min,
        "t_max": snap.t_max / QUANTA,
        "signals": snap.catalog(),
        "series": {
            name: [[float(t), val] for t, val in rows]
            for name, rows in snap.signals.items()
        },
    }
    yield _sse("init", init)
    end_t = snap.t_max / QUANTA

    # tail: safe view only — deltas flow once a block is committed
    tailer = VcdTailer(vcd_path, start_offset=snap_bytes)
    tailer.seed_engine(snap.vars)
    try:
        idle = 0
        while True:
            time.sleep(wait)
            samples, new_defs = tailer_read(tailer)
            if samples or new_defs:
                yield _sse("delta", {"samples": samples, "new_signals": new_defs})
                idle = 0
                continue
            if not _still_live():
                yield _sse("done", {"reason": "finished", "t_max": end_t})
                return
            if max_idle_polls is not None and idle >= max_idle_polls:
                yield _sse("done", {"reason": "idle", "t_max": end_t})
                return
            idle += 1
            yield ": keepalive\n\n"
    finally:
        tailer.close()


def live_sse_events(
    live: LiveManager,
    poll_interval: float = POLL_INTERVAL,
    fast: bool = False,
) -> Iterator[str]:
    """SSE pipeline for the live view: ``init`` state, per-connection ``delta``
    batches of ``metrics.db`` sample rows (``[t, signal, kind, value, tags]``),
    then ``done`` once the run process finishes.

    Each connection tails the samples table from row 0, so a tab that joins
    mid-run catches up first and then follows the stream; a finished run is
    drained in full and ends immediately (replay).
    """
    wait = 0.02 if fast else poll_interval
    live._sync()  # noqa: SLF001 - same-package state refresh
    st = live.state
    if st is None:
        yield _sse("error", {"detail": "no live run launched yet"})
        yield _sse("done", {"reason": "idle"})
        return

    yield _sse("init", st.public())
    tail = SamplesTail(st.run_dir / METRICS_NAME)  # per-connection cursor
    try:
        while True:
            drained = False
            while True:
                rows = tail.since()
                if not rows:
                    break
                drained = True
                st.latest_t = max(st.latest_t, max(float(r[1]) for r in rows))
                yield _sse(
                    "delta",
                    {"rows": [[r[1], r[2], r[3], r[4], r[5]] for r in rows]},
                )
            live._sync()  # noqa: SLF001
            if st.status in ("done", "error"):
                # final drain: the process is gone, all batches are committed
                final = tail.since()
                if final:
                    st.latest_t = max(st.latest_t, max(float(r[1]) for r in final))
                    yield _sse(
                        "delta",
                        {
                            "rows": [
                                [r[1], r[2], r[3], r[4], r[5]] for r in final
                            ]
                        },
                    )
                payload = {
                    "reason": st.status,
                    "run_id": st.run_id,
                    "t_max": st.latest_t,
                }
                if st.status == "done":
                    payload["report"] = f"/live/run/{st.run_id}/report"
                    payload["vcd"] = f"/live/run/{st.run_id}/vcd"
                if st.error:
                    payload["error"] = st.error
                yield _sse("done", payload)
                return
            if not drained:
                yield ": keepalive\n\n"
            time.sleep(wait)
    finally:
        tail.close()
