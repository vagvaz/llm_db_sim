"""Derived metric tables computed at finalize (ticket 04 spec).

Three passes over the trace + raw sink samples, all writing into the same
per-run ``metrics.db``:

- **Percentiles** (per-request distributions): stage latencies and
  end-to-end agent latency; p50/p95/p99 via linear interpolation.
- **Utilization** (time-weighted): ``level`` samples become piecewise
  step functions sampled at ``sample_interval`` ticks; ``counter`` samples
  named ``*token*`` become per-tick throughput rates.
- **Bottleneck attribution** (hybrid): primary = queueing-delay share of
  agent latency per ``(component, stage)``; supporting = saturation score
  (time-weighted utilization fraction of the component's capacity, with a
  queue-nonempty fraction fallback when capacity is not observable).

Warm-up: a ``warmup`` fraction (default 0.1) of the run duration is excluded
from every statistic; requests/stages starting at or before the warm-up
boundary are dropped, and utilization integrates only over the post-warm-up
window.
"""

from __future__ import annotations

import math
import sqlite3
from typing import Dict, List, Optional, Sequence, Tuple

__all__ = [
    "percentile",
    "DERIVED_DDL",
    "create_derived",
    "stage_durations",
    "e2e_latencies",
    "derive_percentiles",
    "step_mean",
    "derive_utilization",
    "derive_bottleneck",
]

# --------------------------------------------------------------------- #
# schema                                                                 #
# --------------------------------------------------------------------- #

DERIVED_DDL = [
    """
    CREATE TABLE IF NOT EXISTS latency_percentiles (
        source TEXT NOT NULL,      -- 'e2e' or '<component>.<stage>'
        kind TEXT NOT NULL,        -- 'latency' | 'queue'
        p50 REAL, p95 REAL, p99 REAL,
        n INTEGER NOT NULL,
        PRIMARY KEY (source, kind)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS utilization_series (
        signal TEXT NOT NULL,
        component TEXT NOT NULL,
        t REAL NOT NULL,
        value REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS utilization_stats (
        signal TEXT NOT NULL,
        component TEXT NOT NULL,
        mean REAL,                 -- time-weighted mean over post-warmup window
        peak REAL,                 -- max observed post-warmup
        mean_util REAL,            -- capacity-normalized mean (or NULL)
        PRIMARY KEY (signal, component)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS token_rate_series (
        component TEXT NOT NULL,
        t REAL NOT NULL,
        rate REAL NOT NULL,        -- tokens per time unit over the last tick
        PRIMARY KEY (component, t)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bottleneck (
        component TEXT NOT NULL,
        stage TEXT NOT NULL,
        queue_delay REAL NOT NULL,   -- summed queueing delay post-warmup
        queue_share REAL NOT NULL,   -- share of agent-facing latency
        sat_score REAL NOT NULL,     -- component saturation (0..1 scale)
        score REAL NOT NULL,         -- 0.7 * queue_share + 0.3 * sat_score
        PRIMARY KEY (component, stage)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
]

DERIVED_INDEXES = [
    "CREATE INDEX IF NOT EXISTS utilization_series_time_idx ON utilization_series (t)",
    "CREATE INDEX IF NOT EXISTS token_rate_time_idx ON token_rate_series (t)",
]


def create_derived(conn: sqlite3.Connection) -> None:
    for ddl in DERIVED_DDL:
        conn.execute(ddl)
    for ddl in DERIVED_INDEXES:
        conn.execute(ddl)


# --------------------------------------------------------------------- #
# percentiles                                                            #
# --------------------------------------------------------------------- #


def percentile(values: Sequence[float], p: float) -> float:
    """Percentile with linear interpolation (numpy-style rank).

    rank = (n - 1) * p; interpolates between the floor/ceil neighbors.
    """
    if not values:
        raise ValueError("percentile of empty sequence")
    if not 0.0 <= p <= 1.0:
        raise ValueError(f"p must be in [0, 1], got {p}")
    xs = sorted(float(v) for v in values)
    n = len(xs)
    k = (n - 1) * p
    f, c = math.floor(k), math.ceil(k)
    if f == c:
        return xs[int(k)]
    return float(xs[f] * (c - k) + xs[c] * (k - f))


def _write_pctl(
    conn: sqlite3.Connection, rows: Dict[Tuple[str, str], List[float]]
) -> None:
    for (source, kind), vals in rows.items():
        conn.execute(
            "INSERT OR REPLACE INTO latency_percentiles VALUES (?, ?, ?, ?, ?, ?)",
            (
                source,
                kind,
                percentile(vals, 0.50),
                percentile(vals, 0.95),
                percentile(vals, 0.99),
                len(vals),
            ),
        )


_STAGE_DURATIONS_SQL = """
SELECT agent_id, request_id, component, stage,
       MAX(CASE WHEN state = 'submitted'  THEN t END) AS t_sub,
       MAX(CASE WHEN state = 'queued'     THEN t END) AS t_queued,
       MAX(CASE WHEN state = 'processing' THEN t END) AS t_proc,
       MAX(CASE WHEN state = 'completed'  THEN t END) AS t_comp
FROM trace
GROUP BY agent_id, request_id, component, stage
"""

_E2E_SQL = """
SELECT agent_id, request_id,
       MAX(CASE WHEN state = 'received'  THEN t END) AS t_received,
       MIN(CASE WHEN state = 'submitted' THEN t END) AS t_submitted
FROM trace
GROUP BY agent_id, request_id
"""

# component-level state-machine recovery: used only when no single stage of
# the component hop carries a complete submitted..completed machine
_COMPONENT_FALLBACK_SQL = """
SELECT agent_id, request_id, component,
       MIN(CASE WHEN state = 'submitted'  THEN t END) AS t_sub,
       MIN(CASE WHEN state = 'queued'     THEN t END) AS t_queued,
       MIN(CASE WHEN state = 'processing' THEN t END) AS t_proc,
       MAX(CASE WHEN state = 'completed'  THEN t END) AS t_comp
FROM trace
GROUP BY agent_id, request_id, component
"""


def stage_durations(
    conn: sqlite3.Connection, warmup_end: float
) -> List[Tuple[str, str, float, float]]:
    """Per-request ``(component, stage, latency, queue_delay)`` post-warmup.

    Primary rule: group by (agent, request, component, stage) and run the
    state machine inside the stage (starts at ``submitted``, fallback
    ``queued``; ``latency`` = completed - start; ``queue_delay`` =
    processing - start, 0.0 when no processing row).

    Fallback: some emitters split one hop's state machine across sibling
    stages of the same component (e.g. ``storage_queue`` holds
    submitted/queued while ``storage_service`` holds processing/completed).
    When no stage of a component hop yields a complete machine, the hop is
    recovered at component level and reported under stage ``"*"`` (no
    double counting: full stage machines suppress the fallback).
    """
    rows: List[Tuple[str, str, float, float]] = []
    covered: set = set()
    for agent, rid, comp, stage, t_sub, t_q, t_proc, t_comp in conn.execute(
        _STAGE_DURATIONS_SQL,
    ):
        start = t_sub if t_sub is not None else t_q
        if start is None or t_comp is None:
            continue
        if start < warmup_end:
            continue
        covered.add((agent, rid, comp))
        queue = t_proc - start if t_proc is not None and t_proc >= start else 0.0
        rows.append((comp, stage, float(t_comp - start), float(queue)))

    for agent, rid, comp, t_sub, t_q, t_proc, t_comp in conn.execute(
        _COMPONENT_FALLBACK_SQL,
    ):
        if (agent, rid, comp) in covered:
            continue
        start = t_sub if t_sub is not None else t_q
        if start is None or t_comp is None:
            continue
        if start < warmup_end:
            continue
        queue = t_proc - start if t_proc is not None and t_proc >= start else 0.0
        rows.append((comp, "*", float(t_comp - start), float(queue)))
    return rows


def e2e_latencies(
    conn: sqlite3.Connection, warmup_end: float
) -> List[Tuple[str, str, float]]:
    """Per-request ``(agent_id, request_id, e2e_latency)``.

    e2e = agent-observed ``received`` - earliest ``submitted`` for the
    request, post-warmup only (submit before the warm-up boundary is
    dropped).
    """
    out: List[Tuple[str, str, float]] = []
    for agent, rid, t_rec, t_sub in conn.execute(_E2E_SQL):
        if t_rec is None or t_sub is None:
            continue
        if t_sub < warmup_end:
            continue
        out.append((str(agent), str(rid), float(t_rec) - float(t_sub)))
    return out


def derive_percentiles(conn: sqlite3.Connection, warmup_end: float) -> None:
    """Fill ``latency_percentiles`` for every stage + end-to-end."""
    stage_rows: Dict[Tuple[str, str], List[float]] = {}
    queue_rows: Dict[Tuple[str, str], List[float]] = {}
    for comp, stage, latency, queue in stage_durations(conn, warmup_end):
        source = f"{comp}.{stage}" if comp else stage
        stage_rows.setdefault((source, "latency"), []).append(latency)
        queue_rows.setdefault((source, "queue"), []).append(queue)
    e2es = [lat for _, _, lat in e2e_latencies(conn, warmup_end)]
    if e2es:
        stage_rows[("e2e", "latency")] = e2es
    _write_pctl(conn, stage_rows)
    _write_pctl(conn, queue_rows)


# --------------------------------------------------------------------- #
# utilization (time-weighted)                                            #
# --------------------------------------------------------------------- #


def _step_value(points: Sequence[Tuple[float, float]], t: float) -> Optional[float]:
    """Last-value-hold lookup: value of the latest point at or before ``t``."""
    v: Optional[float] = None
    for pt, val in points:  # points are pre-sorted
        if pt <= t:
            v = val
        else:
            break
    return v


def step_mean(
    points: Sequence[Tuple[float, float]], a: float, b: float
) -> Optional[float]:
    """Time-weighted mean of a last-value-hold step series over ``[a, b]``.

    Returns None when the series carries no value anywhere in the window.
    """
    if b <= a:
        return None
    val: Optional[float] = None
    area = 0.0
    covered = 0.0
    prev_t = a
    for pt, v in points:  # sorted
        if pt <= a:
            val = v
            continue
        if pt >= b:
            break
        if val is not None:
            area += val * (pt - prev_t)
            covered += pt - prev_t
        val = v
        prev_t = pt
    if val is not None:
        area += val * (b - prev_t)
        covered += b - prev_t
    if covered <= 0.0:
        return None
    return area / covered


def derive_utilization(
    conn: sqlite3.Connection,
    *,
    level_points: Dict[Tuple[str, str], List[Tuple[float, float]]],
    token_points: Dict[Tuple[str, str], List[Tuple[float, float]]],
    capacities: Dict[Tuple[str, str], float],
    warmup_end: float,
    t_end: float,
    sample_interval: float,
) -> None:
    """Write ``utilization_series`` + ``utilization_stats`` + ``token_rate_series``.

    - Series rows: evaluated at each ``sample_interval`` tick multiple in
      ``(warmup_end, t_end]`` (last-value-hold per (signal, component)).
    - Stats mean/peak: per (signal, component), mean integrated time-weighted
      over ``[warmup_end, t_end]``; peak = max observed post-warmup sample.
    - ``mean_util``: value/capacity when the samples carry a capacity
      (``capacity`` or ``slots`` tag) — for queue-depth signals without a
      capacity it falls back to the nonempty-queue time fraction.
    - Token signals (``*token*`` counters): cumulative sums -> per-tick
      throughput rates.
    """
    ticks = _ticks(warmup_end, t_end, sample_interval)

    for (signal, comp), points in level_points.items():
        pts = sorted(points)
        if not pts:
            continue
        for t in ticks:
            v = _step_value(pts, t)
            if v is not None:
                conn.execute(
                    "INSERT INTO utilization_series VALUES (?, ?, ?, ?)",
                    (signal, comp, float(t), float(v)),
                )
        mean = step_mean(pts, warmup_end, t_end)
        post = [v for (pt, v) in pts if pt > warmup_end]
        peak = max(post) if post else _step_value(pts, warmup_end) or 0.0
        mean_util: Optional[float] = None
        cap = capacities.get((signal, comp))
        if cap is not None and cap > 0:
            mean_util = mean / cap if mean is not None else None
        elif "queue" in signal:
            frac = step_mean([(pt, 1.0 if v > 0 else 0.0) for pt, v in pts],
                             warmup_end, t_end)
            mean_util = frac
        conn.execute(
            "INSERT INTO utilization_stats VALUES (?, ?, ?, ?, ?)",
            (signal, comp, mean, float(peak), mean_util),
        )

    # token throughput vs cap: counters are deltas -> cumulative -> per-tick rate
    for (signal, comp), points in token_points.items():
        pts = sorted(points)
        if not pts:
            continue
        cum: List[Tuple[float, float]] = []
        total = 0.0
        for pt, delta in pts:
            total += float(delta)
            cum.append((pt, total))
        prev_t: float = warmup_end
        prev_cum = _step_value(cum, warmup_end) or 0.0
        local_rates: List[float] = []
        for t in ticks:
            cur = _step_value(cum, t)
            if cur is not None:
                rate = (cur - prev_cum) / (t - prev_t)
                conn.execute(
                    "INSERT OR REPLACE INTO token_rate_series VALUES (?, ?, ?)",
                    (comp, float(t), float(rate)),
                )
                local_rates.append(rate)
                prev_t = t
                prev_cum = cur
        vals = local_rates
        conn.execute(
            "INSERT INTO utilization_stats VALUES (?, ?, ?, ?, ?)",
            (
                signal + ".rate",
                comp,
                sum(vals) / len(vals) if vals else None,
                max(vals) if vals else None,
                None,
            ),
        )


def _ticks(warmup_end: float, t_end: float, interval: float) -> List[float]:
    if interval <= 0:
        raise ValueError(f"sample_interval must be > 0, got {interval}")
    if t_end <= warmup_end:
        return []
    start = math.ceil(warmup_end / interval) * interval
    if start <= warmup_end:
        start += interval
    out: List[float] = []
    t = start
    while t <= t_end + 1e-12:
        out.append(t)
        t += interval
    return out


# --------------------------------------------------------------------- #
# bottleneck attribution (hybrid)                                        #
# --------------------------------------------------------------------- #

QUEUE_SHARE_WEIGHT = 0.7
SATURATION_WEIGHT = 0.3


def derive_bottleneck(
    conn: sqlite3.Connection,
    *,
    warmup_end: float,
    sat_by_component: Dict[str, float],
) -> None:
    """Hybrid attribution into ``bottleneck``.

    Primary rule: queueing-delay share — the summed queueing delay per
    ``(component, stage)`` divided by total agent-facing end-to-end latency
    (fallback denominator when no ``received`` rows exist: total stage
    latency). Supporting: the component's saturation score from
    ``utilization_stats``. Final ``score`` is the weighted mix.
    """
    queue_sums: Dict[Tuple[str, str], float] = {}
    stage_latency_sum = 0.0
    for comp, stage, latency, queue in stage_durations(conn, warmup_end):
        queue_sums[(comp, stage)] = queue_sums.get((comp, stage), 0.0) + queue
        stage_latency_sum += latency

    e2e_total = sum(lat for _, _, lat in e2e_latencies(conn, warmup_end))
    if e2e_total <= 0.0:
        e2e_total = stage_latency_sum
    if e2e_total <= 0.0:
        return

    rows = []
    for (comp, stage), qsum in sorted(queue_sums.items()):
        sat = float(sat_by_component.get(comp, 0.0))
        share = qsum / e2e_total
        rows.append(
            (
                comp,
                stage,
                float(qsum),
                float(share),
                sat,
                QUEUE_SHARE_WEIGHT * share + SATURATION_WEIGHT * sat,
            )
        )
    rows.sort(key=lambda r: r[-1], reverse=True)
    conn.executemany("INSERT INTO bottleneck VALUES (?, ?, ?, ?, ?, ?)", rows)


def saturation_scores(conn: sqlite3.Connection) -> Dict[str, float]:
    """Component-level saturation score from ``utilization_stats``.

    ``mean_util`` is the utilization fraction signal; a component's score is
    the max across its sampled signals (0.0 when nothing utilizable was
    sampled). This is the supporting signal of the hybrid attribution.
    """
    out: Dict[str, float] = {}
    for comp, mean_util in conn.execute(
        "SELECT component, mean_util FROM utilization_stats WHERE mean_util IS NOT NULL"
    ):
        out[str(comp)] = max(out.get(comp, 0.0), float(mean_util))
    return out
