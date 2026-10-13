"""Visualization server (ticket 01 serving approach): FastAPI + uvicorn.

- ``GET /``             HTML shell embedding d3-wave (locally vendored JS)
- ``GET /signals``      signal catalog JSON parsed from the run's VCD
- ``GET /stream``       SSE: ``init`` snapshot, then ~1 Hz ``delta`` events
                        while the run is live, ``done`` when it finishes
- ``GET /vcd``          the raw VCD file (Surfer deep-dive link, post-run)
- ``GET /reports``      auto-indexed report templates
- ``GET /reports/<k>``  rendered report from metrics.db (Jinja2 + Plotly)

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

from agent_db_sim.viz.report import (
    render_report,
    render_reports_index,
    static_dir,
    templates_dir,
)
from agent_db_sim.viz.vcd import QUANTA, VcdTailer, parse_vcd_bytes

__all__ = ["create_app", "sse_events", "VCD_NAME", "METRICS_NAME"]

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


def create_app(run_dir: Any) -> FastAPI:
    """Build the visualization app around one run directory."""
    run_dir = Path(run_dir)
    vcd_path = run_dir / VCD_NAME

    app = FastAPI(title="agent-db-sim viz", version="0.1.0")
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
        except FileNotFoundError as exc:
            raise _not_found(str(exc)) from exc
        return Response(html, media_type="text/html")

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
