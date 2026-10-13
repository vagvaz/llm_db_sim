"""Block-composable reports (stage-6, lane B).

A report — either per-run or sweep-level — is an **ordered list of named
blocks** declared in a simple ``<name>.blocks.yaml`` file that lives next to
the template scaffolds in ``viz/templates/``. Users add, remove, or reorder
sections by editing that file; no code is involved.

Block kinds
-----------
Run-level blocks (reads one run's ``metrics.db``):

- ``metadata``         — run metadata key/value table (``t_end``, ``warmup_end``, VCD stats);
- ``metric_table``     — a derived table (``latency_percentiles`` /
                         ``utilization_stats`` / ``bottleneck``), filterable by column;
- ``plot_series``      — Plotly time series from ``utilization_series``
                         (``series: utilization``) or ``token_rate_series``
                         (``series: tokens``);
- ``plot_percentiles`` — grouped per-stage p50/p95/p99 bar chart;
- ``plot_bottleneck``  — hybrid bottleneck attribution bar chart;
- ``plot_distribution`` — latency histogram rebuilt from the ``trace`` table
                         (``source: e2e`` or a component name);
- ``markdown``         — free-form prose (``text:``).

Sweep-level blocks (reads a sweep root: ``sweep_summary.json`` + every
run's ``metrics.db``):

- ``run_table``         — per-run factors + e2e percentiles + bottleneck winner;
- ``factor_group_plot`` — an e2e metric (p50/p95/p99) grouped/colored by one factor;
- ``factor_delta``      — paired deltas between runs equal on every other factor.

A block spec is a mapping with a mandatory ``type`` plus optional ``name``
(stable section id), ``title`` (heading text), ``inline`` (glue under the
previous section instead of starting a new one), and per-kind parameters.
The single-run ``standard`` report is block-driven with identical content
(see ``standard.blocks.yaml``); ``sweep_compare`` renders over a sweep root.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import yaml
from jinja2 import DictLoader, Environment

from agent_db_sim.metrics.derive import e2e_latencies, stage_durations
from agent_db_sim.viz.report import (
    PLOTTY_SRC,
    load_metrics_db,
    fig_bottleneck,
    fig_latency_percentiles,
    fig_token_rate,
    fig_utilization,
    jinja_env,
    run_metadata,
    templates_dir,
)

__all__ = [
    "load_blocks",
    "blocks_file",
    "compose_run_report",
    "compose_sweep_report",
    "load_sweep",
]

# --------------------------------------------------------------------- #
# blocks file                                                            #
# --------------------------------------------------------------------- #

def blocks_file(name: str) -> Optional[Path]:
    """Path of the blocks declaration for template ``name``, or ``None``."""
    path = templates_dir / f"{name}.blocks.yaml"
    return path if path.exists() else None


def load_blocks(name: str) -> Optional[List[Dict[str, Any]]]:
    """Parse ``<name>.blocks.yaml`` into an ordered list of block specs.

    Returns ``None`` when the template has no blocks file (plain Jinja2
    template, legacy path). Directory-like specs also accept strings
    (``- metadata`` short form) and a top-level map with ``blocks:``.
    """
    path = blocks_file(name)
    if path is None:
        return None
    data = yaml.safe_load(path.read_text())
    if data is None:
        return []
    if isinstance(data, dict):
        data = data.get("blocks", data.get("sections", []))
    if not isinstance(data, list):
        raise ValueError(
            f"{path}: blocks file must be a YAML list of blocks "
            "(or a map with a 'blocks:' key)"
        )
    blocks: List[Dict[str, Any]] = []
    for index, item in enumerate(data):
        if isinstance(item, str):
            item = {"type": item}
        if not isinstance(item, dict) or "type" not in item:
            raise ValueError(
                f"{path}: block #{index} must be a mapping with a \"type:\" key"
            )
        blocks.append(item)
    return blocks


# --------------------------------------------------------------------- #
# data views (run mode vs sweep mode)                                    #
# --------------------------------------------------------------------- #

class _View:
    """Block renderer data access: run mode (a metrics.db) or sweep mode."""

    def __init__(
        self,
        run_dir: Optional[Path] = None,
        sweep: Optional[Dict[str, Any]] = None,
        standalone: bool = False,
    ) -> None:
        self.run_dir = run_dir
        self.sweep = sweep
        self.standalone = standalone
        self._conn: Optional[sqlite3.Connection] = None
        self._meta: Optional[Dict[str, Any]] = None

    @property
    def plotly_src(self) -> str:
        from agent_db_sim.viz.report import PLOTTY_SRC_SERVER

        return PLOTTY_SRC if self.standalone else PLOTTY_SRC_SERVER

    @property
    def meta(self) -> Dict[str, Any]:
        if self.run_dir is None:
            raise ValueError("no run directory in this context")
        if self._meta is None:
            self._meta = run_metadata(self.run_dir)
        return self._meta

    @property
    def conn(self) -> sqlite3.Connection:
        if self.run_dir is None:
            raise ValueError("no run directory in this context")
        if self._conn is None:
            self._conn = load_metrics_db(self.run_dir)
        return self._conn


def _warmup_end(meta: Dict[str, Any]) -> float:
    try:
        return float(meta.get("warmup_end", 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


# --------------------------------------------------------------------- #
# sweep loading                                                          #
# --------------------------------------------------------------------- #

def _values_from_config(run_dir: Path) -> Dict[str, Any]:
    """Dotted factor values for a summary-missing run, from its config.yaml."""
    out: Dict[str, Any] = {}
    path = run_dir / "config.yaml"
    if not path.exists():
        return out
    try:
        cfg = yaml.safe_load(path.read_text())
    except Exception:
        return out
    if not isinstance(cfg, dict):
        return out
    for section, keys in (
        ("db", ("topology", "compute_nodes", "storage_nodes", "coordinators")),
        ("agents", ("count",)),
        ("llm", ("slots",)),
    ):
        sub = cfg.get(section)
        if isinstance(sub, dict):
            for key in keys:
                if sub.get(key) is not None:
                    out[f"{section}.{key}"] = sub[key]
    return out


def _augment_run(sweep_dir: Path, run: Dict[str, Any]) -> Dict[str, Any]:
    """Fill e2e percentiles / bottleneck winner from the run's metrics.db."""
    run = dict(run)
    run_id = str(run.get("run_id", ""))
    run["run_id"] = run_id
    run_dir = sweep_dir / run_id
    run.setdefault("values", {})
    run.setdefault("bottleneck_winner", None)
    run.setdefault("run_dir", str(run_dir))
    if not run.get("e2e_percentiles") and (run_dir / "metrics.db").exists():
        conn = load_metrics_db(run_dir)
        try:
            row = conn.execute(
                "SELECT p50, p95, p99, n FROM latency_percentiles "
                "WHERE source='e2e' AND kind='latency'"
            ).fetchone()
            if row:
                run["e2e_percentiles"] = {
                    "p50": row[0], "p95": row[1], "p99": row[2], "n": row[3],
                }
            win = conn.execute(
                "SELECT component, stage, score, queue_share FROM bottleneck "
                "ORDER BY score DESC LIMIT 1"
            ).fetchone()
            if win:
                run["bottleneck_winner"] = {
                    "component": win[0], "stage": win[1],
                    "score": win[2], "queue_share": win[3],
                }
        finally:
            conn.close()
    return run


def load_sweep(sweep_dir: Any) -> Dict[str, Any]:
    """Read a sweep root: ``sweep_summary.json`` + each run's ``metrics.db``.

    Runs listed in the summary are kept as-is (augmented from their
    ``metrics.db`` when fields are missing); run directories with a
    ``metrics.db`` but no summary entry are appended as fallback runs with
    config-derived factor values.
    """
    sweep_dir = Path(sweep_dir)
    data: Dict[str, Any] = {}
    summary_path = sweep_dir / "sweep_summary.json"
    if summary_path.exists():
        try:
            loaded = json.loads(summary_path.read_text())
        except json.JSONDecodeError as exc:
            raise ValueError(f"{summary_path}: invalid JSON ({exc})") from exc
        if isinstance(loaded, dict):
            data = loaded

    runs_in: List[Dict[str, Any]] = [
        r for r in (data.get("runs") or []) if isinstance(r, dict)
    ]
    known = {str(r.get("run_id")) for r in runs_in}
    if sweep_dir.exists():
        for child in sorted(sweep_dir.iterdir()):
            if not child.is_dir() or not (child / "metrics.db").exists():
                continue
            if child.name in known:
                continue
            known.add(child.name)
            runs_in.append(
                {"run_id": child.name, "values": _values_from_config(child)}
            )

    factors_data = data.get("factors")
    if isinstance(factors_data, dict):
        factors = list(factors_data.keys())
    elif isinstance(factors_data, list):
        factors = [str(k) for k in factors_data]
    else:
        factors = []
    if not factors:
        seen: List[str] = []
        for r in runs_in:
            for key in (r.get("values") or {}):
                if key not in seen:
                    seen.append(key)
        factors = seen

    return {
        "name": data.get("name") or sweep_dir.name,
        "factors": factors,
        "runs": [_augment_run(sweep_dir, r) for r in runs_in],
    }


# --------------------------------------------------------------------- #
# formatting + fragments                                                 #
# --------------------------------------------------------------------- #

def _fmt(value: Any) -> str:
    """Human formatting mirroring the legacy standard template ('—' for NULL)."""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    try:
        return "%.4g" % float(value)
    except (TypeError, ValueError):
        return str(value)


def _slug(text: Any) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(text).lower()).strip("-")
    return slug or "block"


_PALETTE = [
    "#636efa", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
    "#19d3f3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52",
]

_FRAGMENTS = {
    "table.html": (
        '<table class="latency">\n'
        "  <thead><tr>{% for h in head %}<th>{{ h }}</th>{% endfor %}</tr></thead>\n"
        "  <tbody>\n"
        "  {% for row in rows %}\n"
        "    <tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>\n"
        "  {% endfor %}\n"
        "  </tbody>\n"
        "</table>"
    ),
    "key_value.html": (
        '<table class="latency">\n'
        "  {% for label, value in pairs %}\n"
        "  <tr><th>{{ label }}</th><td>{{ value }}</td></tr>\n"
        "  {% endfor %}\n"
        "</table>"
    ),
    "figure.html": (
        '<div id="{{ div }}" class="figure">\n'
        '  <script>Plotly.newPlot("{{ div }}", {{ json | safe }}, {responsive: true});</script>\n'
        "</div>"
    ),
}

_FRAG_ENV: Optional[Environment] = None


def _frag_env() -> Environment:
    global _FRAG_ENV
    if _FRAG_ENV is None:
        _FRAG_ENV = Environment(
            loader=DictLoader(_FRAGMENTS), autoescape=True, trim_blocks=True,
        )
    return _FRAG_ENV


def _table_html(head: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    return _frag_env().get_template("table.html").render(
        head=list(head), rows=[list(r) for r in rows]
    )


def _key_value_html(pairs: Sequence[Tuple[str, Any]]) -> str:
    return _frag_env().get_template("key_value.html").render(pairs=list(pairs))


def _figure_html(div: str, fig_json: str) -> str:
    return _frag_env().get_template("figure.html").render(
        div=div, json=fig_json
    )


# --------------------------------------------------------------------- #
# run-level blocks                                                       #
# --------------------------------------------------------------------- #

def _block_metadata(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    meta = view.meta
    pairs: List[Tuple[str, Any]] = []
    fields = spec.get("fields")
    if fields:
        for key in fields:
            pairs.append((str(key), str(meta.get(str(key), "—"))))
    else:
        for key in ("t_end", "warmup_end"):
            pairs.append((key, str(meta.get(key, "—"))))
        for key in ("vcd_signals", "vcd_t_end"):
            if meta.get(key) is not None:
                pairs.append((key, str(meta[key])))
    return "Run metadata", _key_value_html(pairs)


_TABLES: Dict[str, Dict[str, Any]] = {
    "latency_percentiles": {
        "columns": ["source", "kind", "p50", "p95", "p99", "n"],
        "order": "ORDER BY source, kind",
    },
    "utilization_stats": {
        "columns": ["signal", "component", "mean", "peak", "mean_util"],
        "order": "ORDER BY signal, component",
    },
    "bottleneck": {
        "columns": [
            "component", "stage", "queue_delay", "queue_share", "sat_score", "score",
        ],
        "order": "ORDER BY score DESC",
    },
}


def _block_metric_table(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    table = str(spec.get("table", "latency_percentiles"))
    info = _TABLES.get(table)
    if info is None:
        raise ValueError(
            f"unknown metric_table {table!r} "
            f"(known: {', '.join(sorted(_TABLES))})"
        )
    if "columns" in spec:
        cols = [str(c) for c in (spec["columns"] or [])]
        bad = [c for c in cols if c not in info["columns"]]
        if bad:
            raise ValueError(
                f"metric_table {table!r}: unknown columns {', '.join(bad)}"
            )
    else:
        cols = info["columns"]

    conds: List[str] = []
    params: List[Any] = []
    for key, value in (spec.get("filter") or {}).items():
        key = str(key)
        if key not in info["columns"]:
            raise ValueError(
                f"metric_table {table!r}: filter column {key!r} not in {info['columns']}"
            )
        conds.append(f"{key} = ?")
        params.append(str(value))

    sql = f"SELECT {', '.join(cols)} FROM {table}"
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += f" {info['order']}"
    rows = [[_fmt(v) for v in row] for row in view.conn.execute(sql, params)]
    return f"table: {table}", _table_html(cols, rows)


def _block_plot_series(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    which = str(spec.get("series", "utilization"))
    default_div = {
        "utilization": "fig-utilization",
        "tokens": "fig-token-rate",
    }
    builder = {
        "utilization": fig_utilization,
        "tokens": fig_token_rate,
    }.get(which)
    if builder is None:
        raise ValueError("plot_series: 'series' must be 'utilization' or 'tokens'")
    fig = builder(view.conn)
    div = str(spec.get("div") or default_div[which])
    return f"plot_series: {which}", _figure_html(div, fig.to_json())


def _block_plot_percentiles(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    fig = fig_latency_percentiles(view.conn)
    div = str(spec.get("div") or "fig-latency-percentiles")
    return "Latency percentiles (per stage)", _figure_html(div, fig.to_json())


def _block_plot_bottleneck(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    fig = fig_bottleneck(view.conn)
    div = str(spec.get("div") or "fig-bottleneck")
    title = "Bottleneck attribution (hybrid: queue share 70% + saturation 30%)"
    return title, _figure_html(div, fig.to_json())


def _block_plot_distribution(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    import plotly.graph_objs as go

    source = str(spec.get("source", "e2e"))
    conn = view.conn
    warm = _warmup_end(view.meta)
    if source == "e2e":
        values = [lat for _, _, lat in e2e_latencies(conn, warm)]
    else:
        values = [
            lat
            for comp, _stage, lat, _q in stage_durations(conn, warm)
            if comp == source
        ]
    div = str(spec.get("div") or f"fig-dist-{_slug(source)}")
    fig = go.Figure(
        data=[go.Histogram(x=values)],
        layout=go.Layout(
            title=f"{source} latency distribution (traced requests)",
            xaxis_title="latency (sim time units)",
            yaxis_title="count",
        ),
    )
    return f"plot_distribution: {source}", _figure_html(div, fig.to_json())


def _markdown_html(text: str) -> str:
    """Tiny markdown-ish conversion: headings, bullet lists, paragraphs, and
    inline ``**bold**`` / ``*em*`` / ``back-ticks``."""
    text = (text or "").strip()
    if not text:
        return ""

    def inline(s: str) -> str:
        s = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', s)
        s = re.sub(r"`([^`]+)`", r"<code>\1</code>", s)
        s = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", s)
        s = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", s)
        return s

    out: List[str] = []
    bullets: List[str] = []

    def flush_bullets() -> None:
        if bullets:
            out.append(
                "<ul>" +
                "".join(f"<li>{inline(b)}</li>" for b in bullets) +
                "</ul>"
            )
            bullets.clear()

    for line in text.splitlines():
        heading = re.match(r"^(#{1,6})\s+(.*)$", line)
        item = re.match(r"^\s*[-*]\s+(.*)$", line)
        if heading is not None:
            flush_bullets()
            level = len(heading.group(1))
            out.append(f"<h{level}>{inline(heading.group(2))}</h{level}>")
        elif item is not None:
            bullets.append(item.group(1))
        elif line.strip() == "":
            flush_bullets()
        else:
            out.append(f"<p>{inline(line.strip())}</p>")
    flush_bullets()
    return "\n".join(out)


def _block_markdown(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    return "Notes", _markdown_html(str(spec.get("text", "")))


# --------------------------------------------------------------------- #
# sweep-level blocks                                                     #
# --------------------------------------------------------------------- #

_SWEEP_METRICS = ("p50", "p95", "p99")


def _sweep_runs(view: _View) -> List[Dict[str, Any]]:
    return sorted(view.sweep["runs"], key=lambda r: str(r.get("run_id")))


def _sweep_factor(view: _View, spec: Dict[str, Any]) -> str:
    factor = spec.get("factor")
    if factor:
        return str(factor)
    factors: List[str] = [str(k) for k in view.sweep["factors"]]
    runs = view.sweep["runs"]
    # adaptive default: the first factor that actually splits the runs
    for key in factors:
        values = {
            str((r.get("values") or {}).get(key, "—")) for r in runs
        }
        if len(values) > 1:
            return key
    return str(factors[0]) if factors else ""


def _e2e(run: Dict[str, Any]) -> Dict[str, Any]:
    p = run.get("e2e_percentiles")
    return p if isinstance(p, dict) else {}


def _block_run_table(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    factors: List[str] = [str(k) for k in view.sweep["factors"]]
    head = ["run_id", *factors, "e2e p50", "e2e p95", "e2e p99", "n",
            "bottleneck", "score", "queue share"]
    rows: List[List[str]] = []
    for run in _sweep_runs(view):
        values = run.get("values") or {}
        pct = _e2e(run)
        win = run.get("bottleneck_winner") or {}
        rows.append(
            [str(run.get("run_id", ""))]
            + [str(values.get(k, "—")) for k in factors]
            + [
                _fmt(pct.get("p50")), _fmt(pct.get("p95")), _fmt(pct.get("p99")),
                str(pct.get("n", "—")),
                (
                    f"{win.get('component')}.{win.get('stage')}"
                    if win.get("component") is not None and win.get("stage") is not None
                    else "—"
                ),
                _fmt(win.get("score")), _fmt(win.get("queue_share")),
            ]
        )
    return "Runs", _table_html(head, rows)


def _block_factor_group_plot(
    view: _View, spec: Dict[str, Any]
) -> Tuple[str, str]:
    import plotly.graph_objs as go

    metric = str(spec.get("metric", "p95"))
    if metric not in _SWEEP_METRICS:
        raise ValueError(
            f"factor_group_plot: 'metric' must be one of {', '.join(_SWEEP_METRICS)}"
        )
    factor = _sweep_factor(view, spec)
    color_map: Dict[str, str] = {}
    x: List[str] = []
    y: List[Any] = []
    colors: List[str] = []
    for run in _sweep_runs(view):
        value = str((run.get("values") or {}).get(factor, "—"))
        color_map.setdefault(value, _PALETTE[len(color_map) % len(_PALETTE)])
        x.append(str(run.get("run_id", "")))
        y.append(_e2e(run).get(metric))
        colors.append(color_map[value])
    fig = go.Figure(
        data=[go.Bar(x=x, y=y, marker={"color": colors}, showlegend=False)],
        layout=go.Layout(
            title=f"e2e {metric} by {factor}",
            xaxis_title="run",
            yaxis_title=f"e2e {metric}",
        ),
    )
    div = str(spec.get("div") or f"fig-factor-{_slug(metric)}-{_slug(factor)}")
    legend = " ".join(
        f'<span style="color:{color};font-weight:bold">■</span> {value}'
        for value, color in color_map.items()
    )
    body = _figure_html(div, fig.to_json()) + f'<p class="factor-legend">{legend}</p>'
    return f"e2e {metric} by {factor}", body


def _block_factor_delta(view: _View, spec: Dict[str, Any]) -> Tuple[str, str]:
    import plotly.graph_objs as go

    metric = str(spec.get("metric", "p95"))
    if metric not in _SWEEP_METRICS:
        raise ValueError(
            f"factor_delta: 'metric' must be one of {', '.join(_SWEEP_METRICS)}"
        )
    factor = _sweep_factor(view, spec)
    factors = [str(k) for k in view.sweep["factors"]]
    others = [k for k in factors if k != factor]

    # group runs that agree on every factor except `factor`
    groups: Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]] = {}
    for run in _sweep_runs(view):
        signature = tuple((run.get("values") or {}).get(k) for k in others)
        value = str((run.get("values") or {}).get(factor, "—"))
        groups.setdefault(signature, {}).setdefault(value, run)

    rows: List[List[str]] = []
    traces: List[go.Bar] = []
    for signature, by_value in sorted(groups.items(), key=lambda kv: str(kv[0])):
        usable = {
            v: r for v, r in by_value.items()
            if _e2e(r).get(metric) is not None
        }
        if len(usable) < 2:
            continue
        values = sorted(usable)
        base_value, base_run = values[0], usable[values[0]]
        base_metric = float(_e2e(base_run)[metric])
        label = (
            ", ".join(f"{k}={v}" for k, v in zip(others, signature)) or "(all runs)"
        )
        deltas: List[Tuple[str, float]] = []
        for value in values[1:]:
            run = usable[value]
            this_metric = float(_e2e(run)[metric])
            delta = this_metric - base_metric
            deltas.append((value, delta))
            rows.append(
                [
                    label,
                    f"{factor}={base_value}", f"{base_metric:.4g}",
                    f"{factor}={value}", f"{this_metric:.4g}",
                    f"{delta:+.4g}",
                ]
            )
        color = _PALETTE[len(traces) % len(_PALETTE)]
        traces.append(
            go.Bar(
                name=label,
                x=[d[0] for d in deltas],
                y=[d[1] for d in deltas],
                marker={"color": color},
            )
        )
    head = ["other factors", "base pair", f"base e2e {metric}",
            "other pair", f"other e2e {metric}", f"Δ {metric} (other − base)"]
    body = _table_html(head, rows)
    if traces:
        fig = go.Figure(
            data=traces,
            layout=go.Layout(
                barmode="group",
                title=f"e2e {metric} delta by {factor} (other − base)",
                xaxis_title=str(factor),
                yaxis_title=f"Δ {metric}",
            ),
        )
        div = str(spec.get("div") or f"fig-delta-{_slug(metric)}-{_slug(factor)}")
        body += _figure_html(div, fig.to_json())
    return f"e2e {metric} delta by {factor}", body


# --------------------------------------------------------------------- #
# block registry + composition                                           #
# --------------------------------------------------------------------- #

_RUN = "run"
_SWEEP = "sweep"
_ANY = "any"

_RENDERERS: Dict[str, Tuple[Callable[[_View, Dict[str, Any]], Tuple[str, str]], str]] = {
    # run-level blocks
    "metadata": (_block_metadata, _RUN),
    "metric_table": (_block_metric_table, _RUN),
    "plot_series": (_block_plot_series, _RUN),
    "plot_percentiles": (_block_plot_percentiles, _RUN),
    "plot_bottleneck": (_block_plot_bottleneck, _RUN),
    "plot_distribution": (_block_plot_distribution, _RUN),
    "markdown": (_block_markdown, _ANY),
    # sweep-level blocks
    "run_table": (_block_run_table, _SWEEP),
    "factor_group_plot": (_block_factor_group_plot, _SWEEP),
    "factor_delta": (_block_factor_delta, _SWEEP),
}

KNOWN_BLOCK_TYPES = sorted(_RENDERERS)


def _validate_blocks(specs: Sequence[Dict[str, Any]], view: _View) -> None:
    for spec in specs:
        btype = str(spec.get("type", ""))
        entry = _RENDERERS.get(btype)
        if entry is None:
            raise ValueError(
                f"unknown block type {btype!r} "
                f"(known: {', '.join(KNOWN_BLOCK_TYPES)})"
            )
        _, needs = entry
        if needs == _RUN and not view.run_dir:
            raise ValueError(
                f"block type {btype!r} needs a run directory — it is not valid "
                f"in a sweep report (render it over a run dir)"
            )
        if needs == _SWEEP and view.sweep is None:
            raise ValueError(
                f"block type {btype!r} is sweep-only — render this template "
                f"over a sweep root (e.g. render --sweep <sweep-dir>, or "
                f"GET /sweeps/<name>/reports/<template>)"
            )


def _sections(specs: Sequence[Dict[str, Any]], view: _View) -> List[Dict[str, str]]:
    """Render every block into ordered sections (``inline`` glues to the last)."""
    out: List[Dict[str, str]] = []
    for index, spec in enumerate(specs):
        btype = str(spec.get("type"))
        renderer = _RENDERERS[btype][0]
        default_title, body = renderer(view, spec)
        title = str(spec.get("title") or default_title)
        if spec.get("inline") and out:
            out[-1]["body"] += "\n" + body
            continue
        out.append(
            {
                "slug": _slug(spec.get("name") or f"{index}-{btype}"),
                "title": title,
                "body": body,
            }
        )
    return out


def _compose_html(
    specs: Sequence[Dict[str, Any]],
    view: _View,
    *,
    name: str,
    meta: Dict[str, Any],
    extra_context: Optional[Dict[str, Any]] = None,
) -> str:
    _validate_blocks(specs, view)
    ctx: Dict[str, Any] = {
        "meta": meta,
        "blocks": _sections(specs, view),
        "template_name": name,
        "plotly_src": view.plotly_src,
    }
    if extra_context:
        ctx.update(extra_context)
    return jinja_env().get_template("blocks.html").render(**ctx)


def compose_run_report(
    blocks: Sequence[Dict[str, Any]],
    run_dir: Any,
    name: str,
    standalone: bool = False,
    extra_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render a blocks-driven report over one run directory."""
    run_dir = Path(run_dir)
    view = _View(run_dir=run_dir, standalone=standalone)
    return _compose_html(
        blocks, view, name=name, meta=view.meta, extra_context=extra_context
    )


def compose_sweep_report(
    blocks: Sequence[Dict[str, Any]],
    sweep_dir: Any,
    name: str,
    standalone: bool = False,
    extra_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render a blocks-driven sweep-comparison report over a sweep root."""
    sweep = load_sweep(sweep_dir)
    view = _View(sweep=sweep, standalone=standalone)
    return _compose_html(
        blocks, view, name=name, meta={"run_name": sweep["name"]},
        extra_context=extra_context,
    )
