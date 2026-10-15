"""Live animated-run support for the ``/live`` view (viz package only).

A live run is :func:`agent_db_sim.pipeline.execute_run` launched into its own
process against ``runs/live/<run_id>/``. The pipeline already streams every
probe sample into ``metrics.db`` **while the simulation runs** (batched,
committed :class:`TraceWriter` writes), so the page's delta source is the
``samples`` table tailed incrementally — row -> JSON conversion happens
server-side (see :meth:`SamplesTail.since`).

Guards: exactly one live run at a time per server (a second ``POST /live/run``
while one is in flight gets rejected as 409). Completed runs stay listed in
``/live/state`` so the page can replay their tail.
"""

from __future__ import annotations

import json
import multiprocessing
import shutil
import sqlite3
import tempfile
import time
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from omegaconf import OmegaConf

__all__ = [
    "LiveManager",
    "LiveBusy",
    "LiveError",
    "SamplesTail",
    "zip_run_dir",
    "editor_schema",
    "suggest_replay_rate",
]

#: form- and component-editor-reachable config keys (dotted OmegaConf paths)
#: with coercion kinds: int > 0, float > 0, ratio/weight in [0, 1],
#: choice (non-empty string), ingress (none | fifo | priority:<key>), str.
OVERRIDABLE: Dict[str, str] = {
    "agents.count": "int",
    "agents.initial_step.llm": "weight",
    "agents.initial_step.db": "weight",
    "llm.slots": "int",
    "llm.input_tokens_per_s": "float",
    "llm.output_tokens_per_s": "float",
    "llm.ingress": "ingress",
    "db.topology": "choice",
    "db.storage.backend": "str",
    "db.storage.overrides.base_latency": "float",
    "db.storage.overrides.per_gb_time": "float",
    "db.storage.overrides.iops_ceiling": "float",
    "db.storage.overrides.bandwidth_ceiling": "float",
    "db.compute_nodes": "int",
    "db.storage_nodes": "int",
    "db.compute.slots": "int",
    "db.compute.ingress": "ingress",
    "db.compute.issue_ratio": "ratio",
    "sim.until": "float",
}
TOPOLOGIES = ("centralized", "disaggregated", "distributed")
BACKENDS = ("ram", "nvme", "ssd", "disk")  # static floor; registry extends it
INGRESS_KINDS = ("none", "fifo")  # "priority" needs a request-key path

#: AUTO replay pace: the page aims to render the whole run in roughly this
#: many wall seconds (a ~120u run then plays at ~4x instead of crawling at
#: 1x through prefill dead zones). Speed buttons stay manual overrides.
PACE_TARGET_SECONDS = 30.0


def backend_names() -> List[str]:
    """Known storage backends: the static list plus everything the actual
    preset registry ships (``ssd_enterprise`` & friends are valid scenario
    values the override path must accept)."""
    out = list(BACKENDS)
    try:
        from agent_db_sim.components.db.backend import STORAGE_BACKEND_REGISTRY

        for name in sorted(STORAGE_BACKEND_REGISTRY):
            if name not in out:
                out.append(name)
    except Exception:
        pass
    return out


def suggest_replay_rate(until: float) -> float:
    """AUTO pace for a run of ``until`` sim-seconds: a rate so playback
    lasts ~PACE_TARGET_SECONDS wall (never below 1x, so short runs are not
    artificially slowed)."""
    until = float(until or 0.0)
    if until <= 0:
        return 1.0
    return max(1.0, round(until / PACE_TARGET_SECONDS, 2))


def _field(path: str, label: str, kind: str, **extra: Any) -> Dict[str, Any]:
    """One editor field; the dotted ``path`` doubles as the override key the
    RE-LAUNCH button sends to ``POST /live/run`` (validated by OVERRIDABLE).
    ``where`` tells the front end to read the field's CURRENT value from the
    resolved display config instead of the flat path (compute/storage fields
    live under renamed sections there). ``readonly`` renders the value as a
    label only (storage ceilings derive from the backend preset, so the
    editor exposes the preset and keeps them informational)."""
    out: Dict[str, Any] = {"path": path, "label": label, "kind": kind}
    out.update(extra)
    return out


def editor_schema() -> List[Dict[str, Any]]:
    """Per-component edit-form definitions: one entry per editable component
    (agent pool / llm / compute nodes / storage nodes) listing its fields.
    The front end renders the inline editor from this payload, so the
    editor -> override mapping has exactly one source of truth server-side
    (and every emitted override key is validated against ``OVERRIDABLE``)."""
    return [
        {
            "component": "agents",
            "title": "agent pool",
            "fields": [
                _field("agents.count", "agents", "int"),
                _field("agents.initial_step.llm", "first step · llm weight",
                       "weight"),
                _field("agents.initial_step.db", "first step · db weight",
                       "weight"),
            ],
        },
        {
            "component": "llm",
            "title": "llm.server",
            "fields": [
                _field("llm.slots", "slots", "int"),
                _field("llm.input_tokens_per_s", "input tok/s", "float"),
                _field("llm.output_tokens_per_s", "output tok/s", "float"),
                _field("llm.ingress", "ingress", "choice",
                       options=list(INGRESS_KINDS)),
            ],
        },
        {
            "component": "compute",
            "title": "db compute nodes (shared config)",
            "fields": [
                _field("db.compute.slots", "slots / node", "int",
                       where="db.compute.slots"),
                _field("db.compute.ingress", "ingress", "choice",
                       options=list(INGRESS_KINDS),
                       where="db.compute.ingress"),
                _field("db.compute.issue_ratio", "issue ratio", "ratio",
                       where="db.compute.issue_ratio"),
            ],
        },
        {
            "component": "storage",
            "title": "db storage nodes",
            "fields": [
                # ONE editable param: the preset. Ceilings follow from it via
                # resolve_backend_params() server-side and show as read-only
                # labels (they still ride OVERRIDABLE for API power users).
                _field("db.storage.backend", "backend preset", "choice",
                       options=backend_names()),
                _field("db.storage.overrides.base_latency",
                       "base latency s", "float",
                       where="db.storage.base_latency", readonly=True),
                _field("db.storage.overrides.per_gb_time",
                       "per-GB time s", "float",
                       where="db.storage.per_gb_time", readonly=True),
                _field("db.storage.overrides.iops_ceiling",
                       "IOPS ceiling", "float",
                       where="db.storage.iops_ceiling", readonly=True),
                _field("db.storage.overrides.bandwidth_ceiling",
                       "bandwidth GB/s", "float",
                       where="db.storage.bandwidth_ceiling", readonly=True),
            ],
        },
    ]


LIVE_DIR_NAME = "live"
#: safety cap on rows delivered by one delta poll
MAX_ROWS_PER_POLL = 40000


class LiveBusy(Exception):
    """A live run is already in flight."""


class LiveError(Exception):
    """A live run could not be launched (bad scenario / overrides)."""


def _coerce(kind: str, key: str, raw: Any) -> Any:
    try:
        if kind == "int":
            v = int(raw)
            if v <= 0:
                raise ValueError
            return v
        if kind == "float":
            v = float(raw)
            if v <= 0:
                raise ValueError
            return v
        if kind in ("weight", "ratio"):
            v = float(raw)
            if not 0.0 <= v <= 1.0:
                raise ValueError
            return v
        v = str(raw)
        if kind == "ingress":
            _validate_ingress(key, v)
            return v
        if kind == "choice":
            if "." not in key or not v:
                raise ValueError
            return v
        return v
    except (TypeError, ValueError) as exc:
        raise LiveError(f"invalid value for {key!r}: {raw!r}") from exc


def _validate_ingress(key: str, v: str) -> None:
    """Ingress overrides accept ``none | fifo | priority:<dotted-key>`` (the
    editor form offers none/fifo; a priority spec passed by API is validated
    structurally, not against request attributes)."""
    kind, sep, sub = v.partition(":")
    if (kind in INGRESS_KINDS and not sep) or (kind == "priority" and sep and sub):
        return
    raise LiveError(
        f"unknown ingress {v!r} for {key!r}; pick one of {INGRESS_KINDS}"
    )


def _worker(cfg: Any, run_dir: str) -> None:  # pragma: no cover - subprocess
    """Child-process entry: full pipeline, then a result marker file."""
    from agent_db_sim.pipeline import execute_run

    marker = Path(run_dir) / "live_result.json"
    try:
        summary = execute_run(cfg, run_dir)
        marker.write_text(json.dumps({"ok": True, "summary": summary}, default=str))
    except BaseException as exc:  # noqa: BLE001 - child reports, parent polls
        marker.write_text(
            json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        )


class SamplesTail:
    """Incremental reader of a live run's ``metrics.db`` samples table.

    Reads only committed rows through a read-only connection; lock errors are
    swallowed (the writer batches synchronously — a retry on the next poll
    picks the rows up). Rows come back as ``[id, t, signal, kind, value, tags]``
    with ``value``/``tags`` JSON-decoded server-side.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.last_id = 0
        self._conn: Optional[sqlite3.Connection] = None

    def _open(self) -> Optional[sqlite3.Connection]:
        if self._conn is not None:
            return self._conn
        if not self.db_path.exists():
            return None
        try:
            conn = sqlite3.connect(
                f"file:{self.db_path}?mode=ro", uri=True, timeout=0.5
            )
        except sqlite3.Error:
            return None
        self._conn = conn
        return conn

    def since(self, limit: int = 20000) -> List[List[Any]]:
        conn = self._open()
        if conn is None:
            return []
        try:
            cur = conn.execute(
                "SELECT id, t, signal, kind, value, tags FROM samples "
                "WHERE id > ? ORDER BY id LIMIT ?",
                (self.last_id, limit),
            )
            rows = cur.fetchall()
        except sqlite3.Error:  # locked / table not there yet
            return []
        out: List[List[Any]] = []
        for rid, t, signal, kind, value, tags in rows:
            self.last_id = max(self.last_id, rid)
            try:
                decoded_value = json.loads(value)
            except (TypeError, ValueError):
                decoded_value = value
            try:
                decoded_tags = json.loads(tags)
            except (TypeError, ValueError):
                decoded_tags = {}
            out.append([rid, float(t), str(signal), str(kind), decoded_value, decoded_tags])
        return out

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
            self._conn = None

    def reset(self) -> None:
        self.close()
        self.last_id = 0


@dataclass
class LiveState:
    """One live-run slot (only one active at a time)."""

    run_id: str
    run_dir: Path
    status: str = "starting"        # starting | running | done | error
    scenario: Optional[str] = None
    overrides: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    until: float = 0.0
    error: Optional[str] = None
    started_wall: float = field(default_factory=time.time)
    finished_wall: Optional[float] = None
    latest_t: float = 0.0
    process: Optional[multiprocessing.process.BaseProcess] = None
    tail: Optional[SamplesTail] = None

    def public(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "scenario": self.scenario,
            "overrides": self.overrides,
            "config": self.config,
            "until": self.until,
            # AUTO replay pace + per-component editor definitions: the page
            # renders ✎-editors and its default replay rate from these, so
            # the editor -> override-key mapping lives server-side.
            "pace": suggest_replay_rate(self.until),
            "pace_target_s": PACE_TARGET_SECONDS,
            "editors": editor_schema(),
            "error": self.error,
            "t_last": round(self.latest_t, 3),
            "elapsed": round(
                (self.finished_wall or time.time()) - self.started_wall, 1
            ),
        }


class LiveManager:
    """Launch/guard/tail for the single live run served by one viz app."""

    def __init__(self, live_root: Any, scenarios_root: Optional[Path] = None) -> None:
        self.live_root = Path(live_root)
        self.scenarios_root = Path(scenarios_root) if scenarios_root else None
        self.state: Optional[LiveState] = None
        # default platform context (fork on Linux — same context the sweep
        # executor uses); the child only ever touches its own run dir.
        self._ctx = multiprocessing.get_context()

    # -- discovery -------------------------------------------------------- #
    def scenarios(self) -> List[Dict[str, Any]]:
        if self.scenarios_root is None or not self.scenarios_root.is_dir():
            return []
        out: List[Dict[str, Any]] = []
        for path in sorted(self.scenarios_root.rglob("*.yaml")):
            rel = path.relative_to(self.scenarios_root).as_posix()
            try:
                cfg = OmegaConf.load(path)
            except Exception:
                continue
            # only component scenarios the live pipeline can actually build
            # (agents + llm namespaces); skips legacy dummy-loop files
            if _safe_select(cfg, "agents.count") is None:
                continue
            if _safe_select(cfg, "llm.slots") is None:
                continue
            out.append(
                {
                    "path": rel,
                    "until": _safe_select(cfg, "sim.until"),
                    "agents": _safe_select(cfg, "agents.count"),
                    "topology": _safe_select(cfg, "db.topology"),
                }
            )
        return out

    # -- lifecycle ---------------------------------------------------------- #
    def refresh(self) -> Optional[Dict[str, Any]]:
        """Sync worker status, then return the public state snapshot."""
        self._sync()
        return self.state.public() if self.state else {"status": "idle"}

    def _sync(self) -> None:
        st = self.state
        if st is None or st.status not in ("starting", "running"):
            return
        proc = st.process
        if proc is not None and proc.is_alive():
            st.status = "running"
            return
        # process is gone (or never started): read the result marker
        marker = st.run_dir / "live_result.json"
        if proc is not None:
            proc.join(timeout=0)
        if marker.exists():
            try:
                result = json.loads(marker.read_text())
            except (ValueError, OSError):
                result = {"ok": False, "error": "unreadable live_result.json"}
            if result.get("ok"):
                st.status = "done"
            else:
                st.status = "error"
                st.error = result.get("error", "unknown failure")
        else:
            exitcode = proc.exitcode if proc is not None else None
            if exitcode == 0:
                st.status = "done"
            else:
                st.status = "error"
                st.error = (
                    f"live run worker exited with code {exitcode} "
                    "without a result marker"
                )
        st.finished_wall = time.time()
        # final drain so /live/state reports the run's last sample time
        if st.tail is not None:
            for row in st.tail.since(MAX_ROWS_PER_POLL):
                st.latest_t = max(st.latest_t, float(row[1]))

    def start(self, scenario: Optional[str], overrides: Dict[str, Any]) -> LiveState:
        """Launch one live run; :class:`LiveBusy` when one is in flight."""
        self._sync()
        if self.state is not None and self.state.status in ("starting", "running"):
            raise LiveBusy(f"live run {self.state.run_id!r} already in flight")

        cfg = self._resolve_cfg(scenario, overrides)
        run_id = time.strftime("%Y%m%d-%H%M%S") + f"-{int(time.time() * 1000) % 1000:03d}"
        run_dir = self.live_root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        OmegaConf.save(cfg, run_dir / "live_request.yaml")

        st = LiveState(
            run_id=run_id,
            run_dir=run_dir,
            scenario=scenario,
            overrides=dict(overrides or {}),
            config=_display_cfg(cfg),
            until=float(OmegaConf.select(cfg, "sim.until", default=0.0) or 0.0),
            tail=SamplesTail(run_dir / "metrics.db"),
        )
        plain = OmegaConf.create(OmegaConf.to_container(cfg, resolve=True))
        proc = self._ctx.Process(target=_worker, args=(plain, str(run_dir)), daemon=True)
        try:
            proc.start()
        except OSError as exc:  # pragma: no cover - resource exhaustion
            raise LiveError(f"could not launch run: {exc}") from exc
        st.process = proc
        st.status = "running"
        self.state = st
        return st

    def _resolve_cfg(
        self, scenario: Optional[str], overrides: Optional[Dict[str, Any]]
    ) -> Any:
        if not scenario:
            raise LiveError("scenario is required")
        base = self._scenario_path(scenario)
        cfg = OmegaConf.load(base)
        OmegaConf.set_struct(cfg, False)
        for key, raw in (overrides or {}).items():
            if key not in OVERRIDABLE:
                raise LiveError(f"override not allowed: {key!r}")
            if raw is None or raw == "":
                continue
            kind = OVERRIDABLE[key]
            value = _coerce(kind, key, raw)
            if key == "db.topology":
                if value not in TOPOLOGIES:
                    raise LiveError(f"unknown topology {value!r}; pick one of {TOPOLOGIES}")
            if key == "db.storage.backend":
                names = backend_names()
                if value not in names:
                    raise LiveError(f"unknown backend {value!r}; pick one of {names}")
            OmegaConf.update(cfg, key, value, merge=True)
        self._check_initial_step(cfg)
        return cfg

    @staticmethod
    def _check_initial_step(cfg: Any) -> None:
        """`agents.initial_step` weights must sum to 1 (the same rule the
        agent client enforces) — reject at LAUNCH time, not mid-run."""
        spec = _safe_select(cfg, "agents.initial_step")
        if spec is None:
            return
        try:
            from agent_db_sim.components.agent_client import parse_initial_step

            parse_initial_step(OmegaConf.to_container(spec, resolve=True))
        except Exception as exc:
            raise LiveError(f"agents.initial_step: {exc}") from exc

    def _scenario_path(self, scenario: str) -> Path:
        if self.scenarios_root is None:
            raise LiveError("no scenarios directory configured")
        candidate = (self.scenarios_root / scenario).resolve()
        root = self.scenarios_root.resolve()
        if root not in candidate.parents and candidate != root:
            raise LiveError(f"scenario escapes scenarios root: {scenario!r}")
        if not candidate.is_file():
            raise LiveError(f"no scenario file: {scenario}")
        return candidate

    # -- streaming ------------------------------------------------------------ #
    def rows_since(self, limit: int = MAX_ROWS_PER_POLL) -> List[List[Any]]:
        self._sync()
        st = self.state
        if st is None or st.tail is None:
            return []
        rows = [row[1:] for row in st.tail.since(limit)]  # [t, signal, kind, value, tags]
        if rows:
            st.latest_t = max(st.latest_t, max(float(r[0]) for r in rows))
        return rows


def _safe_select(cfg: Any, key: str) -> Any:
    try:
        return OmegaConf.select(cfg, key)
    except Exception:
        return None


def _expected_tokens(spec: Any) -> Optional[float]:
    """Mean of a token-count draw (any TOKEN_DISTRIBUTIONS form, or a bare
    number); ``None`` when the spec isn't a recognisable distribution."""
    if spec is None:
        return None
    try:
        if isinstance(spec, (int, float)) and not isinstance(spec, bool):
            return float(spec)
        try:
            d = OmegaConf.to_container(spec, resolve=True)
        except Exception:
            d = spec
        if not isinstance(d, Mapping):
            return None
        dist = str(d.get("distribution", "fixed"))
        if dist == "fixed":
            return float(d["value"])
        if dist == "uniform":
            return (float(d["low"]) + float(d["high"])) / 2.0
        if dist == "geometric":
            p = float(d["p"])
            return 1.0 / p if p > 0 else None
        if dist == "exponential":
            return 1.0 / float(d["rate"])
        if dist == "gamma":
            return float(d["shape"]) * float(d["scale"])
        if dist == "normal":
            return float(d["mean"])
        if dist == "lognormal":
            import math

            return math.exp(float(d["mean"]) + float(d["sigma"]) ** 2 / 2.0)
    except (KeyError, TypeError, ValueError):
        return None
    return None


def _expected_sigma(cfg: Any, key: str) -> float:
    """sigma of a lognormal token spec (0 for fixed/other distributions)."""
    spec = _safe_select(cfg, key)
    try:
        d = OmegaConf.to_container(spec, resolve=True)
        if isinstance(d, Mapping) and str(d.get("distribution")) == "lognormal":
            return float(d.get("sigma", 0.0) or 0.0)
    except Exception:
        pass
    return 0.0


def _first_completion_eta(cfg: Any, sel: Any) -> Optional[float]:
    """Expected sim-time of the FIRST LLM completion, from config alone —
    the same cost model the endpoint runs: ``k = min(agents, slots)``
    requests hold slots from t=0 and share the aggregate token cap
    ``in_rate + out_rate`` (fair share: each earns cap/k), on top of the
    per-request phase floor ``in/in_rate + out/out_rate``. The *first*
    completion is the fastest of the k, so the draws are order-statistic
    adjusted (lognormal mean shifted by ``exp(-sigma*sqrt(2 ln k))``).
    Cross-checked against observed runs: grounded_70b at 4 slots / 24
    agents predicts ~11 (observed ~11); at 32 slots it predicts ~40
    (observed 48). The view shows this during a heavy-prefill phase so a
    quiet scene reads as honest queueing, not a frozen page."""
    import math

    e_in = _expected_tokens(_safe_select(cfg, "agents.input_tokens"))
    e_out = _expected_tokens(_safe_select(cfg, "agents.output_tokens"))
    in_rate = float(sel("llm.input_tokens_per_s", 0.0) or 0.0)
    out_rate = float(sel("llm.output_tokens_per_s", 0.0) or 0.0)
    if e_in is None or e_out is None or in_rate <= 0 or out_rate <= 0:
        return None
    k = max(1, min(int(sel("agents.count", 1)), int(sel("llm.slots", 1))))
    z = math.sqrt(2.0 * math.log(k))
    adj_in = math.exp(-z * _expected_sigma(cfg, "agents.input_tokens"))
    adj_out = math.exp(-z * _expected_sigma(cfg, "agents.output_tokens"))
    head_in, head_out = e_in * adj_in, e_out * adj_out
    floor = head_in / in_rate + head_out / out_rate       # phase floor
    cap = (head_in + head_out) * k / (in_rate + out_rate)  # fair share
    eta = max(floor, cap)
    return round(eta, 1) if eta > 0 else None


def _display_cfg(cfg: Any) -> Dict[str, Any]:
    """The subset of the resolved config the live view needs to draw.

    Carries every component's *processing capabilities* (LLM slot count and
    token-rate ceilings, DB compute slots-per-node and node counts, the
    resolved storage backend preset with its IOPS/bandwidth ceilings, and the
    internal-network link bandwidth + connection-pool size) so the scene can
    label them straight from the run's resolved config. The storage/network
    values are resolved through the very code the simulation itself uses
    (``resolve_backend_params`` / ``NetworkConfig``), so what the page shows
    is exactly what the run models.
    """

    def sel(key: str, default: Any = None) -> Any:
        v = OmegaConf.select(cfg, key)
        return default if v is None else v

    def section(key: str) -> Dict[str, Any]:
        v = OmegaConf.select(cfg, key)
        try:
            if v is None:
                return {}
            if isinstance(v, Mapping):
                return dict(v)
            return OmegaConf.to_container(v, resolve=True) or {}
        except Exception:
            return {}

    compute_cfg = section("db.compute")
    storage_cfg = section("db.storage")
    network_cfg = section("db.network")

    backend = str(sel("db.storage.backend", "ssd"))
    storage: Dict[str, Any] = {"backend": backend}
    try:  # same resolution path the DB endpoint builds its nodes from
        from agent_db_sim.components.db.backend import resolve_backend_params

        storage.update(resolve_backend_params(storage_cfg or storage).as_dict())
    except Exception:
        pass  # unknown preset etc: the run itself will fail; show bare name

    network: Dict[str, Any] = {}
    try:
        from agent_db_sim.components.db.network import NetworkConfig

        nc = NetworkConfig(network_cfg)
        network = {
            "latency": nc.latency,
            "bandwidth": nc.bandwidth,
            "capacity": nc.capacity,
            "connections": nc.connections,
            "request_size": nc.request_size,
            "coordinator_overhead": nc.coordinator_overhead,
        }
    except Exception:
        pass

    topology = str(sel("db.topology", "centralized"))
    init_step = _safe_select(cfg, "agents.initial_step")
    try:
        init_step_c = (
            OmegaConf.to_container(init_step, resolve=True)
            if init_step is not None else None
        )
    except Exception:
        init_step_c = None
    return {
        "agents": {
            "count": int(sel("agents.count", 1)),
            "initial_step": init_step_c if isinstance(init_step_c, Mapping) else None,
        },
        "llm": {
            "slots": int(sel("llm.slots", 1)),
            "input_tokens_per_s": float(sel("llm.input_tokens_per_s", 0.0) or 0.0),
            "output_tokens_per_s": float(sel("llm.output_tokens_per_s", 0.0) or 0.0),
            "ingress": str(sel("llm.ingress", "fifo")),
            "first_completion_eta": _first_completion_eta(cfg, sel),
        },
        "db": {
            "topology": topology,
            "compute_nodes": int(sel("db.compute_nodes", 1)),
            "storage_nodes": int(sel("db.storage_nodes", 1)),
            "coordinators": int(sel("db.coordinators", 1)),
            "backend": backend,
            "compute": {
                "slots": int(compute_cfg.get("slots", 1) or 1),
                "ingress": str(compute_cfg.get("ingress", "fifo")),
                "issue_ratio": float(compute_cfg.get("issue_ratio", 0.1) or 0.1),
                "phase_split": str(compute_cfg.get("phase_split", "three_phase")),
            },
            "storage": storage,
            "network": network,
        },
    }


# --------------------------------------------------------------------- #
# run-directory export (the /live EXPORT button)                        #
# --------------------------------------------------------------------- #

#: name of the metrics database inside a run dir (same file the tailer reads)
METRICS_DB_NAME = "metrics.db"


def _snapshot_metrics_db(src: Path, dest_dir: Path) -> Optional[Path]:
    """A transactionally consistent copy of a live ``metrics.db``.

    The run process commits batched writes to metrics.db while it runs, so
    a plain file copy can catch a half-written page and the archived
    database could open torn. ``sqlite3``'s online-backup API reads through
    the locking protocol instead and always yields a queryable snapshot —
    that is the chosen safe path for exporting mid-run. Returns the
    snapshot path, or ``None`` when the db could not be backed up (missing,
    locked past the timeout, not a SQLite file); the caller then falls back
    to a raw byte copy.
    """
    dest = dest_dir / METRICS_DB_NAME
    try:
        src_conn = sqlite3.connect(f"file:{src}?mode=ro", uri=True, timeout=5.0)
        try:
            dst_conn = sqlite3.connect(str(dest))
            try:
                src_conn.backup(dst_conn)
            finally:
                dst_conn.close()
        finally:
            src_conn.close()
    except sqlite3.Error:
        dest.unlink(missing_ok=True)
        return None
    return dest


def zip_run_dir(run_dir: Path, archive: Path) -> List[str]:
    """Pack a run directory into the zip file at ``archive``; list members.

    Walks the whole run dir — every file therein is archived (config.yaml,
    metrics.db, run.vcd, report.html, summary.json, the live_request /
    live_result markers, anything a future pipeline adds), paths relative
    to the run dir. ``metrics.db`` goes through the sqlite online-backup
    API (see :func:`_snapshot_metrics_db`) so an export DURING a live run
    cannot tear the database; if the backup is impossible the raw bytes are
    archived instead (documented fallback: the zip stays valid, the db copy
    may lag one commit).
    """
    run_dir = Path(run_dir)
    archive = Path(archive)
    names: List[str] = []
    staging = Path(tempfile.mkdtemp(prefix="agent-db-sim-export-"))
    try:
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(run_dir.rglob("*")):
                if not path.is_file():
                    continue
                rel = path.relative_to(run_dir).as_posix()
                if path.name == METRICS_DB_NAME and path.parent == run_dir:
                    snap = _snapshot_metrics_db(path, staging)
                    if snap is not None:
                        zf.write(snap, rel)
                        snap.unlink(missing_ok=True)
                        names.append(rel)
                        continue
                zf.write(path, rel)
                names.append(rel)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return names
