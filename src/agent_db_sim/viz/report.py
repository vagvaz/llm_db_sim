"""Report layer (ticket 02 recommendation): Jinja2 + Plotly, server-rendered.

The viz server renders reports from a run's ``metrics.db``; the ``render``
CLI command produces the same HTML standalone for CI. Templates live in
``viz/templates/`` (auto-indexed: any ``*.html`` file not starting with an
underscore and not a scaffold file is listed at ``GET /reports``); users
extend the layer by dropping in a new template.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from agent_db_sim.viz.vcd import QUANTA, parse_vcd_bytes

__all__ = [
    "templates_dir",
    "static_dir",
    "list_templates",
    "load_metrics_db",
    "percentile_rows",
    "utilization_rows",
    "run_metadata",
    "build_figures",
    "render_report",
    "render_sweep_report",
    "render_reports_index",
    "PLOTTY_SRC",
]

PACKAGE_DIR = Path(__file__).resolve().parent
templates_dir = PACKAGE_DIR / "templates"
static_dir = PACKAGE_DIR / "static"

# Per ticket 02: plotly.min.js is served once from the server's static dir
# (offline/local-first, never a CDN); standalone renders ship it next to the
# HTML instead.
PLOTTY_SRC = "plotly.min.js"
PLOTTY_SRC_SERVER = "/static/plotly.min.js"

_SCAFFOLDS = ("base.html", "reports.html", "wave.html", "blocks.html")


# --------------------------------------------------------------------- #
# template index                                                         #
# --------------------------------------------------------------------- #

def list_templates() -> List[Dict[str, str]]:
    """Auto-index report templates.

    Two flavors, both indexed:

    - blocks-driven: a ``<name>.blocks.yaml`` block list (stage-6 composable
      reports; rendered through the ``blocks.html`` scaffold);
    - plain Jinja2: a hand-written ``<name>.html`` file.
    """
    entries: List[Dict[str, str]] = []
    seen: set = set()
    for path in sorted(templates_dir.glob("*.blocks.yaml")):
        name = path.name[: -len(".blocks.yaml")]
        if name.startswith("_"):
            continue
        entries.append({"name": name, "file": path.name, "mode": "blocks"})
        seen.add(name)
    for path in sorted(templates_dir.glob("*.html")):
        if path.name.startswith("_") or path.name in _SCAFFOLDS:
            continue
        if path.stem in seen:  # a blocks file drives an html twin's content
            continue
        entries.append({"name": path.stem, "file": path.name, "mode": "html"})
    return entries


# --------------------------------------------------------------------- #
# metrics db reads                                                       #
# --------------------------------------------------------------------- #

def load_metrics_db(run_dir: Any) -> sqlite3.Connection:
    """Read-only connection to a run's metrics.db."""
    path = Path(run_dir) / "metrics.db"
    if not path.exists():
        raise FileNotFoundError(f"no metrics.db under {run_dir}")
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def percentile_rows(
    conn: sqlite3.Connection,
) -> List[Tuple[str, str, Any, Any, Any, int]]:
    """``latency_percentiles`` rows: (source, kind, p50, p95, p99, n)."""
    return [
        (src, kind, p50, p95, p99, n)
        for src, kind, p50, p95, p99, n in conn.execute(
            "SELECT source, kind, p50, p95, p99, n FROM latency_percentiles "
            "ORDER BY source, kind"
        )
    ]


def utilization_rows(conn: sqlite3.Connection) -> List[Tuple[str, str, Any, Any, Any]]:
    """``utilization_stats`` rows: (signal, component, mean, peak, mean_util)."""
    return [
        (sig, comp, mean, peak, mean_util)
        for sig, comp, mean, peak, mean_util in conn.execute(
            "SELECT signal, component, mean, peak, mean_util FROM utilization_stats"
        )
    ]


def run_metadata(run_dir: Any) -> Dict[str, Any]:
    """Run metadata for templates: meta table + config snapshot + VCD size."""
    run_dir = Path(run_dir)
    meta: Dict[str, Any] = {"run_name": run_dir.name, "run_dir": str(run_dir)}
    try:
        conn = load_metrics_db(run_dir)
        try:
            for key, value in conn.execute("SELECT key, value FROM meta"):
                meta[key] = value
        finally:
            conn.close()
    except FileNotFoundError:
        pass
    config_path = run_dir / "config.yaml"
    if config_path.exists():
        try:
            cfg = yaml.safe_load(config_path.read_text())
            if isinstance(cfg, dict):
                meta["config"] = cfg
        except Exception:
            pass
    vcd_path = run_dir / "run.vcd"
    if vcd_path.exists():
        data = parse_vcd_bytes(vcd_path.read_bytes(), include_tail=True)
        meta["vcd_signals"] = len(data.catalog())
        meta["vcd_t_end"] = data.t_max / QUANTA
    return meta


# --------------------------------------------------------------------- #
# figures (plotly)                                                       #
# --------------------------------------------------------------------- #

def fig_latency_percentiles(conn: sqlite3.Connection) -> Any:
    """Grouped p50/p95/p99 per source, latency kind (bar chart)."""
    import plotly.graph_objs as go

    x: List[str] = []
    p50s: List[Any] = []
    p95s: List[Any] = []
    p99s: List[Any] = []
    for src, kind, p50, p95, p99, _n in percentile_rows(conn):
        if kind != "latency":
            continue
        x.append(src)
        p50s.append(p50)
        p95s.append(p95)
        p99s.append(p99)
    traces = [
        go.Bar(name=kind, x=x, y=vals)
        for kind, vals in (("p50", p50s), ("p95", p95s), ("p99", p99s))
    ]
    return go.Figure(
        data=traces,
        layout=go.Layout(
            barmode="group",
            title="Per-stage latency percentiles",
            xaxis_title="stage",
            yaxis_title="latency (sim time units)",
        ),
    )


def fig_utilization(conn: sqlite3.Connection) -> Any:
    """Time-weighted utilization series per (signal, component)."""
    import plotly.graph_objs as go

    rows = conn.execute(
        "SELECT signal, component, t, value FROM utilization_series ORDER BY t"
    ).fetchall()
    grouped: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    for sig, comp, t, val in rows:
        grouped.setdefault((str(sig), str(comp)), []).append((float(t), float(val)))
    traces = [
        go.Scatter(
            name=f"{sig}@{comp}" if comp else sig,
            x=[t for t, _ in pts],
            y=[v for _, v in pts],
            mode="lines+markers",
            line={"shape": "hv"},
        )
        for (sig, comp), pts in sorted(grouped.items())
    ]
    return go.Figure(
        data=traces,
        layout=go.Layout(
            title="Utilization series (time-weighted)",
            xaxis_title="sim time",
            yaxis_title="level",
        ),
    )


def fig_token_rate(conn: sqlite3.Connection) -> Any:
    """Token throughput rate series per component."""
    import plotly.graph_objs as go

    rows = conn.execute(
        "SELECT component, t, rate FROM token_rate_series ORDER BY t"
    ).fetchall()
    grouped: Dict[str, List[Tuple[float, float]]] = {}
    for comp, t, rate in rows:
        grouped.setdefault(str(comp), []).append((float(t), float(rate)))
    traces = [
        go.Scatter(
            name=f"tokens@{comp}",
            x=[t for t, _ in pts],
            y=[v for _, v in pts],
            mode="lines",
            line={"shape": "hv"},
        )
        for comp, pts in sorted(grouped.items())
    ]
    return go.Figure(
        data=traces,
        layout=go.Layout(
            title="Token throughput (per sample tick)",
            xaxis_title="sim time",
            yaxis_title="tokens / tick",
        ),
    )


def fig_bottleneck(conn: sqlite3.Connection) -> Any:
    """Hybrid bottleneck attribution (queue-delay share + saturation)."""
    import plotly.graph_objs as go

    rows = conn.execute(
        "SELECT component, stage, queue_delay, queue_share, sat_score, score "
        "FROM bottleneck ORDER BY score DESC"
    ).fetchall()
    y = [f"{c}.{s}" for c, s, *_ in rows]
    return go.Figure(
        data=[
            go.Bar(
                name="queue-delay share",
                x=[r[3] for r in rows], y=y, orientation="h",
            ),
            go.Bar(
                name="saturation score",
                x=[r[4] for r in rows], y=y, orientation="h",
                marker={"opacity": 0.5},
            ),
        ],
        layout=go.Layout(
            barmode="overlay",
            title="Bottleneck attribution (hybrid)",
            xaxis_title="share / score",
            yaxis={"autorange": "reversed"},
        ),
    )


def build_figures(
    db_path: Any,
    kinds: Tuple[str, ...] = ("latency", "utilization", "tokens", "bottleneck"),
) -> Dict[str, str]:
    """Build the standard figure set; returns {div id: plotly json string}."""
    builders = {
        "latency": (fig_latency_percentiles, "fig-latency-percentiles"),
        "utilization": (fig_utilization, "fig-utilization"),
        "tokens": (fig_token_rate, "fig-token-rate"),
        "bottleneck": (fig_bottleneck, "fig-bottleneck"),
    }
    out: Dict[str, str] = {}
    conn = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    try:
        for key in kinds:
            builder, div = builders[key]
            fig = builder(conn)
            out[div] = fig.to_json()
    finally:
        conn.close()
    return out


# --------------------------------------------------------------------- #
# render                                                                 #
# --------------------------------------------------------------------- #

def jinja_env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=True,
        undefined=StrictUndefined,
        trim_blocks=True,
    )


def render_report(
    run_dir: Any,
    name: str = "standard",
    standalone: bool = False,
    extra_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render report template ``name`` from a run directory.

    ``standalone=True`` ships a relative plotly.min.js reference (CLI
    ``render``); server mode serves ``/static/plotly.min.js``.

    If a ``<name>.blocks.yaml`` block list exists next to the templates, the
    report is block-driven (stage-6): the ordered block list produces the
    sections (see ``viz/blocks.py``) rendered through the ``blocks.html``
    scaffold. Otherwise the plain ``<name>.html`` Jinja2 template renders
    exactly as before.
    """
    from agent_db_sim.viz.blocks import compose_run_report, load_blocks

    blocks = load_blocks(name)
    if blocks is None and not (templates_dir / f"{name}.html").exists():
        raise FileNotFoundError(f"no report template {name!r} in {templates_dir}")
    if blocks is not None:
        return compose_run_report(
            blocks, run_dir, name=name, standalone=standalone,
            extra_context=extra_context,
        )

    db_path = Path(run_dir) / "metrics.db"
    if not db_path.exists():
        raise FileNotFoundError(f"no metrics.db under {run_dir}")

    conn = load_metrics_db(run_dir)
    ctx: Dict[str, Any] = {
        "meta": run_metadata(run_dir),
        "figures": build_figures(db_path),
        "plotly_src": PLOTTY_SRC if standalone else PLOTTY_SRC_SERVER,
        "rows_percentiles": percentile_rows(conn),
        "rows_utilization": utilization_rows(conn),
        "template_name": name,
    }
    conn.close()
    if extra_context:
        ctx.update(extra_context)
    return jinja_env().get_template(f"{name}.html").render(**ctx)


def render_sweep_report(
    sweep_dir: Any,
    name: str = "sweep_compare",
    standalone: bool = False,
    extra_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render a sweep-comparison report (e.g. ``sweep_compare``) over a
    sweep root directory containing ``sweep_summary.json`` and per-run
    ``metrics.db`` files (see ``viz/blocks.py::load_sweep``)."""
    from agent_db_sim.viz.blocks import compose_sweep_report, load_blocks

    has_html = (templates_dir / f"{name}.html").exists()
    blocks = load_blocks(name)
    if not has_html and blocks is None:
        raise FileNotFoundError(f"no report template {name!r} in {templates_dir}")
    return compose_sweep_report(
        blocks or [], sweep_dir, name=name, standalone=standalone,
        extra_context=extra_context,
    )


def render_reports_index(run_dir: Any) -> str:
    """Render the ``/reports`` index page (auto-indexed templates)."""
    return jinja_env().get_template("reports.html").render(
        templates=list_templates(),
        meta={"run_name": Path(run_dir).name},
    )
