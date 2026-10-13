"""Live VCD emission from probe samples (ticket 12, per ticket-01 research).

The writer consumes :class:`~agent_db_sim.simkit.probes.Sample` objects pushed
by serving primitives (queue depths, slot occupancy, token counters,
request-state enumerations) and produces a real VCD file incrementally so a
separate process can tail it while the simulation still runs.

Signal catalog policy
---------------------

- ``level`` samples become ``integer`` (whole values) or ``real`` (fractional)
  variables, named after the sample's ``component`` tag and dotted signal.
- ``counter`` samples become ``integer`` variables holding the *cumulative*
  count (the sample value is a delta).
- ``edge`` samples carrying ``agent_id``/``request_id`` tags (request
  lifecycle transitions) become ``string`` variables whose value is the
  canonical state (``submitted | queued | processing | completed | received``).
  The variable name gets a ``_state`` suffix to distinguish it from any
  numeric variable of the same stage.

Quantization / safe-tail
------------------------

All timestamps are quantized onto the sample grid: :data:`QUANTA` integer
quanta per simulated time unit (VCD ``$timescale 1 ns``, i.e. one quantum =
0.001 sim time units). pyvcd emits the VCD header plus a ``$dumpvars`` section
when the first value change past t=0 arrives; from then on every value block
is written straight to the file, so a concurrent tailer always sees VCD data
up to the last complete ``#timestamp`` block (the newest block may be
mid-write; readers must hold it back — see :class:`VcdTailer`).

pyvcd only accepts variable registrations *before* the first past-t0 change,
so signals first observed after the header was sealed cannot join the main
stream: they are buffered and spilled into a trailing re-registration section
(``$scope``/``$var``/``$enddefinitions``/``#ts``) by a second pyvcd writer
sharing the same file handle at close. Mainstream VCD tools (Surfer, GTKWave)
tolerate that pattern and this module's own reader merges it back into a
single catalog. Run drivers should call :meth:`VcdSampleWriter.seal` early
(right after elaboration, when the component catalog is known) so that
typically nothing ever spills.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from vcd.writer import VCDWriter

from agent_db_sim.simkit.probes import Sample

__all__ = [
    "QUANTA",
    "TIMESCALE",
    "VcdError",
    "SplitSignal",
    "signal_key",
    "VcdSampleWriter",
    "write_vcd",
    "VcdVar",
    "VcdData",
    "parse_vcd_bytes",
    "VcdTailer",
]

# VCD timescale: 1 quantum = 1 ns of simulated time -> resolution
# 0.001 sim-time-units (the tick grid the ticket-04 spec quantizes to).
QUANTA = 1000
TIMESCALE = "1 ns"

_ROOT_SCOPE = "agent_db_sim"

# Identifiers the spill writer may use must never collide with the main
# writer's (which restarts at '!'); offset its counter far past any routine
# catalog size.
_SPILL_IDENT_OFFSET = 5000


class VcdError(Exception):
    """Raised when VCD emission cannot proceed."""


@dataclass(frozen=True)
class SplitSignal:
    """Scope/variable placement derived from one sample."""

    scope: Tuple[str, ...]
    name: str
    is_lifecycle: bool

    @property
    def full_name(self) -> str:
        return ".".join(self.scope + (self.name,))


def signal_key(sample: Sample) -> SplitSignal:
    """Derive the (scope, name) VCD placement for a sample.

    A ``component`` tag wins as the scope; otherwise dotted signal segments
    decompose into scope + leaf. Lifecycle edges (``agent_id``/``request_id``
    tags) get a ``_state`` suffix on the variable name and map to ``string``.
    """
    tags = sample.tags
    lifecycle = sample.kind == "edge" and "request_id" in tags and "agent_id" in tags
    comp = str(tags.get("component", "")) if tags else ""
    signal = str(sample.signal)
    if not comp and "." in signal:
        comp, signal = signal.rsplit(".", 1)
    scope = tuple(seg for seg in comp.split(".") if seg)
    if lifecycle:
        signal = f"{signal}_state"
    return SplitSignal(scope=scope, name=signal, is_lifecycle=lifecycle)


def _quantize(t: float) -> int:
    """Quantize a sim-time float onto the integer tick grid."""
    return max(int(round(float(t) * QUANTA)), 0)


def _value_type(sample: Sample) -> str:
    """Var kind for a sample: ``int`` | ``real`` | ``string``."""
    if sample.kind == "counter":
        return "int"
    if sample.kind == "level":
        v = sample.value
        if isinstance(v, float) and not float(v).is_integer():
            return "real"
        return "int"
    # edge samples (lifecycle + generic state transitions): string enum
    return "string"


_VAR_TYPE_STR = {"int": "integer", "real": "real", "string": "string"}


def _value_for(
    sample: Sample, vtype: str, counters: Dict[str, float], ident: str
) -> Any:
    """Convert a sample into the value that gets written for ``ident``."""
    kind = sample.kind
    if kind == "counter":
        cur = counters.get(ident, 0.0) + float(sample.value)
        counters[ident] = cur
        return int(cur) if cur.is_integer() else cur
    if vtype == "string":
        state = sample.tags.get("state", sample.value) if kind == "edge" else sample.value
        return str(state) if state is not None else ""
    if vtype == "real":
        return float(sample.value)
    return int(sample.value)


# --------------------------------------------------------------------- #
# writer                                                                 #
# --------------------------------------------------------------------- #


class VcdSampleWriter:
    """Incremental sample -> VCD writer for one run file.

    Feed samples with :meth:`feed`; completed ``#timestamp`` blocks are
    written through, so a concurrent tailer always sees VCD up to the last
    complete timestamp (only the newest block may be mid-write). Call
    :meth:`seal` once the fed catalog is complete (usually right after
    elaboration); signals discovered after pyvcd's implied header are
    buffered and spilled into a trailing section at :meth:`close`.
    """

    def __init__(self, path: Any, comment: str = "agent-db-sim probe signals") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = io.open(self.path, "w", encoding="utf-8", newline="")
        self._writer = VCDWriter(
            self.handle,
            timescale=TIMESCALE,
            comment=comment,
            version="agent-db-sim/viz",
            date="",
            init_timestamp=0,
        )
        self._vars: Dict[str, Tuple[Any, str]] = {}   # ident -> (var, vtype)
        self._counters: Dict[str, float] = {}
        self._ts = 0
        self._sealed = False
        self._header_open = True   # pyvcd still accepts registrations
        self._late_samples: List[Sample] = []          # post-header buffered
        self._closed = False

    # -- ingestion ----------------------------------------------------- #
    def feed(self, sample: Sample) -> bool:
        """Route one probe sample into the VCD stream.

        Returns True when the sample produced a value change; the header was
        already implied (False signals "handed to the spill buffer").
        """
        if self._closed:
            raise VcdError("writer is closed; cannot feed samples")
        ident = signal_key(sample).full_name
        ts = max(_quantize(sample.t), self._ts)
        if ident not in self._vars:
            # pyvcd registers only before time 0; its header is implied by
            # the first t>0 value change even if seal() was never called.
            if self._sealed or not self._header_open:
                self._late_samples.append(sample)
                return False
            self._register(ident, sample)
        self._ts = ts
        return self._emit(ident, sample, ts)

    def _emit(self, ident: str, sample: Sample, ts: int) -> bool:
        out = self._change(ident, sample, ts)
        if ts > 0 and self._header_open:
            self._header_open = False  # pyvcd finalized the header here
        return out

    def _register(self, ident: str, sample: Sample) -> None:
        key = signal_key(sample)
        vtype = _value_type(sample)
        var = self._writer.register_var(
            (_ROOT_SCOPE,) + key.scope, key.name, _VAR_TYPE_STR[vtype], init=None
        )
        self._vars[ident] = (var, vtype)

    def _change(self, ident: str, sample: Sample, ts: int) -> bool:
        var, vtype = self._vars[ident]
        value = _value_for(sample, vtype, self._counters, ident)
        self._writer.change(var, ts, value)
        return True

    def flush(self) -> None:
        """Push every completed timestamp block out to the OS."""
        if self._closed:
            return
        self._writer.flush()
        self.handle.flush()

    # -- catalog freeze ------------------------------------------------ #
    def sealed(self) -> bool:
        """True once the declared catalog is frozen."""
        return self._sealed

    def preregister(self, samples: Iterable[Sample]) -> None:
        """Register every distinct signal in ``samples`` up front.

        Batch (fixture/CI) use only, before any :meth:`feed`: guarantees the
        complete catalog lands in the main VCD header even for signals whose
        first sample is later than t>0.
        """
        if self._vars or self._ts:
            raise VcdError("preregister() must run before any feed")
        for sample in sorted(samples, key=lambda s: _quantize(s.t)):
            ident = signal_key(sample).full_name
            if ident not in self._vars:
                self._register(ident, sample)

    def seal(self) -> None:
        """Freeze the catalog of variables that live in the main header.

        Later :meth:`feed` calls with unseen signals are then buffered for
        the trailing spill section (pyvcd cannot add vars after time 0).
        """
        self._sealed = True

    # -- finalization --------------------------------------------------- #
    def close(self, final_t: Optional[float] = None) -> None:
        """Finish the run: spill late signals, then mark the end.

        ``final_t`` (sim time) makes the file end with a ``#ts`` marker so a
        tailing reader's last data block counts as complete — without a
        marker, a tailer stops one timestamp block short.
        """
        if self._closed:
            return
        self._sealed = True
        if self._late_samples:
            self._spill()
        marker_ts = _quantize(final_t) if final_t is not None else None
        self._writer.close(marker_ts)
        if marker_ts is not None:
            # always end with a complete '#' line: pyvcd skips its own
            # marker when the internal timestamp already equals it.
            self.handle.write(f"#{marker_ts}\n")
        self.handle.flush()
        self.handle.close()
        self._closed = True

    def _spill(self) -> None:
        """Write buffered late signals as a trailing re-registration section."""
        first_ts = max(min(_quantize(s.t) for s in self._late_samples), 0)
        last_ts = max(max(_quantize(s.t) for s in self._late_samples), self._ts)
        w2 = VCDWriter(
            self.handle,
            timescale=TIMESCALE,
            comment="signals discovered after the header was sealed",
            version="agent-db-sim/viz",
            date="",
            init_timestamp=first_ts,
        )
        w2._next_var_id = _SPILL_IDENT_OFFSET  # noqa: SLF001 - avoid ident reuse
        vars2: Dict[str, Tuple[Any, str]] = {}
        # pyvcd registers vars only before its first change: two passes.
        for sample in self._late_samples:
            ident = signal_key(sample).full_name
            if ident not in vars2:
                key = signal_key(sample)
                vtype = _value_type(sample)
                vars2[ident] = (
                    w2.register_var(
                        (_ROOT_SCOPE,) + key.scope,
                        key.name,
                        _VAR_TYPE_STR[vtype],
                        init=None,
                    ),
                    vtype,
                )
        counters2: Dict[str, float] = {}
        for sample in self._late_samples:
            ident = signal_key(sample).full_name
            ts = max(_quantize(sample.t), self._ts, first_ts)
            var, vtype = vars2[ident]
            value = _value_for(sample, vtype, counters2, ident)
            w2.change(var, ts, value)
        w2.close(last_ts)
        for ident, entry in vars2.items():
            self._vars.setdefault(ident, entry)

    def __enter__(self) -> "VcdSampleWriter":
        return self

    def __exit__(self, exc_t: Any, exc_v: Any, tb: Any) -> None:
        if not self._closed:
            self.close()


def write_vcd(
    path: Any,
    samples: Iterable[Sample],
    comment: str = "agent-db-sim probe signals",
) -> Path:
    """One-shot write of a sample sequence into ``path`` (batch/fixture use).

    Samples are stably sorted by quantized time first; the writer is sealed
    before feeding so the whole catalog lands in the VCD header.
    """
    ordered = sorted(samples, key=lambda s: _quantize(s.t))
    writer = VcdSampleWriter(path, comment=comment)
    writer.preregister(ordered)
    writer.seal()
    for s in ordered:
        writer.feed(s)
    end_t = max((_quantize(s.t) for s in ordered), default=None)
    writer.close(final_t=None if end_t is None else end_t / QUANTA)
    return writer.path


# --------------------------------------------------------------------- #
# reader (round-trip, catalog, tail support)                             #
# --------------------------------------------------------------------- #


@dataclass
class VcdVar:
    """One declared variable: scope path + reference name + VCD type."""

    scope: str
    name: str
    vtype: str
    width: int

    @property
    def full_name(self) -> str:
        return f"{self.scope}.{self.name}" if self.scope else self.name


def var_json(var: VcdVar) -> Dict[str, Any]:
    """JSON-ready catalog entry."""
    return {
        "scope": var.scope,
        "name": var.name,
        "type": var.vtype,
        "width": var.width,
    }


@dataclass
class VcdData:
    """Parsed VCD: catalog + per-signal value-change rows (ascending t)."""

    vars: Dict[str, VcdVar] = field(default_factory=dict)  # ident-code -> meta
    signals: Dict[str, List[Tuple[float, Any]]] = field(default_factory=dict)
    t_min: int = 0
    t_max: int = 0

    def catalog(self) -> List[Dict[str, Any]]:
        """JSON-ready signal catalog, sorted by full name."""
        seen: Dict[str, Dict[str, Any]] = {}
        for var in self.vars.values():
            seen[var.full_name] = var_json(var)
        return [seen[name] for name in sorted(seen)]


def _convert(vtype: str, raw: str) -> Any:
    if vtype in ("integer", "int"):
        try:
            return int(raw, 2)
        except ValueError:
            try:
                return int(raw)
            except ValueError:
                return raw
    if vtype in ("real", "realtime"):
        try:
            return float(raw)
        except ValueError:
            return raw
    return raw  # string / wire scalars stay textual


class _LineEngine:
    """Shared line-by-line VCD semantics used by the full parser and tailer."""

    def __init__(self) -> None:
        self._scope_stack: List[str] = []
        self._idents: Dict[str, VcdVar] = {}
        self._definitions_seen = False
        self._ts = 0
        self._ts_min: Optional[int] = None
        self._ts_max: Optional[int] = None

    def line(self, text: str) -> Tuple[List[Tuple[int, str, Any]], List[VcdVar]]:
        """Interpret one line; returns (samples, newly declared vars)."""
        line = text.strip()
        if not line:
            return [], []
        if line.startswith("#"):
            body = line[1:]
            try:
                ts = int(body)
            except ValueError:
                try:
                    ts = int(body.rstrip("ns "))
                except ValueError:
                    return [], []
            self._ts = max(self._ts, ts)
            if self._ts_min is None:
                self._ts_min = ts
            self._ts_max = ts
            return [], []
        if line.startswith("$"):
            return self._keyword(line)
        if not self._definitions_seen:
            return [], []
        if self._ts_min is None:
            return [], []  # tailer joined mid-block: wait for a timestamp
        return self._value_lines(line)

    # -- header keywords ------------------------------------------------ #
    def _keyword(self, line: str) -> Tuple[List[Tuple[int, str, Any]], List[VcdVar]]:
        if line.startswith("$scope"):
            parts = line.split()
            if len(parts) >= 3:
                self._scope_stack.append(parts[2])
            return [], []
        if line.startswith("$upscope"):
            if self._scope_stack:
                self._scope_stack.pop()
            return [], []
        if line.startswith("$var"):
            parts = line.split()
            if len(parts) >= 6:
                vtype = parts[1]
                width = int(parts[2])
                ident = parts[3]
                name = " ".join(parts[4:-1]) or parts[4]
                scope = ".".join(self._scope_stack)
                meta = VcdVar(scope=scope, name=name, vtype=vtype, width=width)
                self._idents[ident] = meta
                return [], [meta]
            return [], []
        if line.startswith("$enddefinitions"):
            self._definitions_seen = True
            self._scope_stack.clear()
        return [], []

    # -- value changes --------------------------------------------------- #
    def _value_lines(
        self, line: str
    ) -> Tuple[List[Tuple[int, str, Any]], List[VcdVar]]:
        if line.startswith("s"):
            # pyvcd StringVariable: 's<value> <ident>'  (ident = last token)
            if " " not in line:
                return [], []
            value, ident = line[1:].rsplit(" ", 1)
            return self._emit(ident, value), []
        if line.startswith(("b", "B")):
            parts = line.split()
            if len(parts) != 2:
                return [], []
            return self._emit(parts[1], parts[0][1:]), []
        if line.startswith(("r", "R")):
            parts = line.split()
            if len(parts) != 2:
                return [], []
            return self._emit(parts[1], parts[0][1:]), []
        # scalar change: '<char><idents>' e.g. '1$'
        if line and line[0] in "01xzXZ" and len(line) >= 2:
            return self._emit(line[1:], line[0]), []
        return [], []

    def _emit(self, ident: str, raw: Any) -> List[Tuple[int, str, Any]]:
        meta = self._idents.get(ident)
        if meta is None:
            return []
        full = meta.full_name
        sim_t = self._ts / QUANTA
        return [(sim_t, full, _convert(meta.vtype, str(raw)))]


def _drop_unsafe_tail(lines: List[str]) -> List[str]:
    """Hold back everything after the newest ``#`` line (block may grow)."""
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].lstrip().startswith("#"):
            return lines[:i]
    return lines


def parse_vcd_bytes(data: Union[bytes, str], include_tail: bool = False) -> VcdData:
    """Parse VCD bytes into :class:`VcdData` (main + spill sections merged).

    By default lenient for a still-live file: the trailing newest ``#`` block
    and any partial line are skipped. ``include_tail=True`` additionally
    parses the newest block (for finished files / snapshot endpoints - a
    long-but-partial last block simply parses best-effort).
    """
    if isinstance(data, (bytes, bytearray)):
        text = bytes(data).decode("utf-8", errors="replace")
    else:
        text = str(data)
    engine = _LineEngine()
    out = VcdData()
    lines = text.splitlines()
    if not include_tail:
        lines = _drop_unsafe_tail(lines)
    for text_line in lines:
        samples, new_vars = engine.line(text_line)
        for ident, var in engine._idents.items():
            out.vars[ident] = var
        for ts, full, val in samples:
            out.signals.setdefault(full, []).append((float(ts), val))
    if engine._ts_min is not None:
        out.t_min = engine._ts_min
    if engine._ts_max is not None:
        out.t_max = engine._ts_max
    return out


class VcdTailer:
    """Incremental VCD reader that consumes only complete ``#timestamp`` blocks.

    ``read()`` yields everything appended since the previous successful read,
    holding back the trailing block that starts with the newest ``#`` line
    (it may still be mid-write); the next ``read()`` picks it up once another
    ``#`` line - or the writer's closing marker - commits it. With
    ``start_offset`` the tailer skips existing content entirely (for SSE
    streams that already delivered an ``init`` snapshot).
    """

    def __init__(self, path: Any, start_offset: Optional[int] = None) -> None:
        self.path = Path(path)
        self._fh = io.open(self.path, "r", encoding="utf-8", errors="replace")
        if start_offset is not None:
            self._fh.seek(start_offset)
        self._engine = _LineEngine()
        self._known: set = set()         # full signal names already reported
        self._buffered: List[str] = []   # unconsumed complete lines
        self._partial = ""               # tail without its ending newline yet

    def is_open(self) -> bool:
        return not self._fh.closed

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass

    def _retrieve(self) -> bool:
        """Pull new complete lines out of the file; True when any appeared."""
        if self._fh.closed:
            return False
        try:
            new_bytes = self._fh.read()
        except OSError:
            return False
        if not new_bytes:
            return False
        chunk = self._partial + new_bytes
        lines = chunk.splitlines()
        self._partial = lines.pop() if (chunk and chunk[-1] != "\n") else ""
        self._buffered.extend(lines)
        return True

    def read(self) -> Tuple[List[Tuple[float, str, Any]], List[Dict[str, Any]]]:
        """Consume the safe portion of the stream.

        Returns ``(samples, new_vars)``: samples are
        ``(sim_t, full_signal_name, value)`` triples in stream order;
        ``new_vars`` lists signal-catalog entries not returned by any earlier
        call (mid-stream spill sections included).
        """
        self._retrieve()
        lines = self._buffered
        self._buffered = []
        samples: List[Tuple[float, str, Any]] = []
        safe_lines: List[str] = []
        if lines:
            safe_lines = _drop_unsafe_tail(lines)
            held = len(lines) - len(safe_lines)
            if held:
                # keep the newest (still-growing) block for the next read
                self._buffered = lines[len(safe_lines):]
        new_defs: List[Dict[str, Any]] = []
        for text_line in safe_lines:
            row_samples, declared = self._engine.line(text_line)
            samples.extend(row_samples)
            for var in declared:
                if var.full_name not in self._known:
                    self._known.add(var.full_name)
                    new_defs.append(var_json(var))
        return samples, new_defs

    # helpers ------------------------------------------------------------- #
    def seed_engine(self, idents: Dict[str, VcdVar]) -> None:
        """Pre-load a parsed catalog (for ``start_at_end`` tailing).

        The stream's ``init`` snapshot parsed the header already; without
        seeding, the tailer cannot map identifier codes to signal names.
        """
        self._engine._idents.update(idents)  # noqa: SLF001
        self._engine._definitions_seen = True
        for var in idents.values():
            self._known.add(var.full_name)

    @property
    def known_full_names(self) -> set:
        return set(self._known)

    def got_definitions(self) -> bool:
        """True once ``$enddefinitions`` passed (first stage tailable)."""
        return self._engine._definitions_seen  # noqa: SLF001
