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
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from omegaconf import OmegaConf

__all__ = ["LiveManager", "LiveBusy", "LiveError", "SamplesTail"]

#: form-reachable config keys (dotted OmegaConf paths) with coercion
OVERRIDABLE: Dict[str, str] = {
    "agents.count": "int",
    "llm.slots": "int",
    "llm.input_tokens_per_s": "float",
    "llm.output_tokens_per_s": "float",
    "db.topology": "choice",
    "db.storage.backend": "str",
    "db.compute_nodes": "int",
    "db.storage_nodes": "int",
    "sim.until": "float",
}
TOPOLOGIES = ("centralized", "disaggregated", "distributed")
BACKENDS = ("ram", "nvme", "ssd", "disk")

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
        v = str(raw)
        if kind == "choice":
            if "." not in key or not v:
                raise ValueError
            return v
        return v
    except (TypeError, ValueError) as exc:
        raise LiveError(f"invalid value for {key!r}: {raw!r}") from exc


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
                if value not in BACKENDS:
                    raise LiveError(f"unknown backend {value!r}; pick one of {BACKENDS}")
            OmegaConf.update(cfg, key, value, merge=True)
        return cfg

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


def _display_cfg(cfg: Any) -> Dict[str, Any]:
    """The subset of the resolved config the live view needs to draw."""

    def sel(key: str, default: Any = None) -> Any:
        v = OmegaConf.select(cfg, key)
        return default if v is None else v

    return {
        "agents": {"count": int(sel("agents.count", 1))},
        "llm": {
            "slots": int(sel("llm.slots", 1)),
            "input_tokens_per_s": float(sel("llm.input_tokens_per_s", 0.0) or 0.0),
            "output_tokens_per_s": float(sel("llm.output_tokens_per_s", 0.0) or 0.0),
        },
        "db": {
            "topology": str(sel("db.topology", "centralized")),
            "compute_nodes": int(sel("db.compute_nodes", 1)),
            "storage_nodes": int(sel("db.storage_nodes", 1)),
            "backend": str(sel("db.storage.backend", "ssd")),
        },
    }
