"""VCD writer/reader/tailer tests (ticket 12).

All inputs are synthetic probe samples — no dependency on the model
components. The fixture path mirrors the metrics collector tests
(test_metrics_collect.py) so the same samples feed both.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from agent_db_sim.simkit.probes import InMemorySink, Sample
from agent_db_sim.viz.vcd import (
    QUANTA,
    VcdError,
    VcdSampleWriter,
    VcdTailer,
    parse_vcd_bytes,
    signal_key,
    write_vcd,
)
from agent_db_sim.viz.fixture import synthesize_samples


# --------------------------------------------------------------------- #
# signal naming                                                          #
# --------------------------------------------------------------------- #

def test_signal_key_scope_and_lifecycle() -> None:
    s_level = Sample(0.0, "queue_depth", "level", 1, {"component": "llm"})
    assert signal_key(s_level).full_name == "llm.queue_depth"
    # dotted signal without a component tag decomposes into scope + leaf
    s_dot = Sample(0.0, "top.llm.ingress.queue_depth", "level", 1, {})
    assert signal_key(s_dot).full_name == "top.llm.ingress.queue_depth"
    # lifecycle edges become string state vars with _state suffix
    s_edge = Sample(
        1.0, "llm.request", "edge", "queued",
        {"agent_id": "a", "request_id": "r1", "state": "queued"},
    )
    key = signal_key(s_edge)
    assert key.full_name == "llm.request_state"
    assert key.is_lifecycle


# --------------------------------------------------------------------- #
# writer round-trip                                                      #
# --------------------------------------------------------------------- #

@pytest.fixture()
def sink() -> InMemorySink:
    s = InMemorySink()
    s.sample("slot_busy", 1, t=0.0, component="llm", slots=4)
    s.sample("queue_depth", 0, t=0.0, component="llm")
    s.count("tokens", 3, t=1.0, component="llm")        # +3
    s.edge(
        "llm.request", "submitted", t=1.5,
        agent_id="a", request_id="r1", state="submitted",
    )
    s.edge(
        "llm.request", "queued", t=2.0,
        agent_id="a", request_id="r1", state="queued",
    )
    s.sample("slot_busy", 3, t=5.0, component="db", slots=8)
    s.count("tokens", 2, t=6.0, component="llm")        # -> cumulative 5
    return s


def test_write_vcd_round_trip(tmp_path: Path, sink: InMemorySink) -> None:
    path = write_vcd(tmp_path / "run.vcd", sink.samples)
    data = parse_vcd_bytes(path.read_bytes(), include_tail=True)

    # catalog: every fed signal appears with the agreed kind
    catalog = {f"{v['scope']}.{v['name']}": v["type"] for v in data.catalog()}
    assert catalog["agent_db_sim.llm.slot_busy"] == "integer"
    assert catalog["agent_db_sim.llm.queue_depth"] == "integer"
    assert catalog["agent_db_sim.llm.tokens"] == "integer"
    assert catalog["agent_db_sim.llm.request_state"] == "string"
    assert catalog["agent_db_sim.db.slot_busy"] == "integer"
    assert set(catalog) == set(sink_signals()) | {"agent_db_sim.llm.queue_depth"}

    # counters accumulate: token deltas 3 + 2 -> values [.., 3, 5]
    tokens = data.signals["agent_db_sim.llm.tokens"]
    values = [v for _, v in tokens if isinstance(v, int)]
    assert values[-1] == 5

    # lifecycle states emerge as strings, in arrival order
    states = [v for _, v in data.signals["agent_db_sim.llm.request_state"]]
    assert states == ["", "submitted", "queued"]

    # tick quantization: all row times are multiples of the quantum
    for rows in data.signals.values():
        for t, _v in rows:
            assert t * QUANTA == pytest.approx(round(t * QUANTA))


def sink_signals() -> set:
    return {
        "agent_db_sim.llm.slot_busy",
        "agent_db_sim.llm.tokens",
        "agent_db_sim.llm.request_state",
        "agent_db_sim.db.slot_busy",
    }


def test_parse_is_reflective_of_raw_values(tmp_path: Path, sink: InMemorySink) -> None:
    path = write_vcd(tmp_path / "r.vcd", sink.samples)
    raw = path.read_text()
    assert "$timescale 1 ns" in raw
    assert "$enddefinitions $end" in raw
    assert "squeued" in raw
    # scope hierarchy: root then component scopes
    assert "$scope module agent_db_sim $end" in raw
    assert "$scope module llm $end" in raw
    data = parse_vcd_bytes(raw, include_tail=True)
    assert max(data.signals["agent_db_sim.db.slot_busy"], key=lambda r: r[0])[1] == 3


def test_feed_out_of_order_timestamps_clamped(tmp_path: Path) -> None:
    path = tmp_path / "o.vcd"
    w = VcdSampleWriter(path)
    w.preregister(
        [Sample(0.0, "x", "level", 0, {}), Sample(5.0, "x", "level", 9, {})]
    )
    w.seal()
    w.feed(Sample(5.0, "x", "level", 9, {}))
    w.feed(Sample(1.0, "x", "level", 7, {}))  # past sample: clamped forward
    w.close()
    data = parse_vcd_bytes(path.read_bytes(), include_tail=True)
    ts = [t for t, _v in data.signals.get("agent_db_sim.x", [])]
    assert ts == sorted(ts)  # never decreasing


# --------------------------------------------------------------------- #
# spill (post-header late signals)                                       #
# --------------------------------------------------------------------- #

def test_late_signals_spilled_and_parseable(tmp_path: Path, sink: InMemorySink) -> None:
    path = tmp_path / "late.vcd"
    w = VcdSampleWriter(path)
    written = 0
    for s in sink.samples:
        if w.feed(s):
            written += 1
    assert written > 0
    # a signal that first shows up after the header was implied:
    w.feed(Sample(10.0, "inflight", "level", 4, {"component": "db"}))
    w.seal()
    w.close(final_t=10.0)
    data = parse_vcd_bytes(path.read_bytes(), include_tail=True)
    catalog = {f"{v['scope']}.{v['name']}" for v in data.catalog()}
    assert "agent_db_sim.db.inflight" in catalog
    # the late signal carries its late values, not dropped
    rows = data.signals.get("agent_db_sim.db.inflight", [])
    assert any(t == 10.0 and v == 4 for t, v in rows)


# --------------------------------------------------------------------- #
# safe-tail                                                              #
# --------------------------------------------------------------------- #

def test_safe_tail_skips_incomplete_final_block(tmp_path: Path, sink: InMemorySink) -> None:
    full_path = write_vcd(tmp_path / "full.vcd", sink.samples)
    full_text = full_path.read_text()
    lines = full_text.splitlines(keepends=True)

    # emulate a partially-written file: drop the final block's tail lines
    cut = len(lines) - 2
    partial = tmp_path / "part.vcd"
    partial.write_text("".join(lines[:cut]))

    tailer = VcdTailer(partial)
    samples1, _defs1 = tailer.read()
    # the newest # block is held back: parse(safe) == tailer read
    held_data = parse_vcd_bytes(partial.read_bytes())  # include_tail=False
    expected = held_data.signals["agent_db_sim.llm.tokens"]
    got = [(t, v) for t, name, v in samples1 if name == "agent_db_sim.llm.tokens"]
    assert got == expected
    n1 = len(samples1)

    # complete the file the way a live writer would: append the missing tail
    with partial.open("a", encoding="utf-8") as fh:
        fh.write("".join(lines[cut:]))
    samples2, _ = tailer.read()
    assert len(samples2) > 0
    merged = samples1 + samples2
    full_data = parse_vcd_bytes(full_text, include_tail=True)
    for name in ("agent_db_sim.llm.tokens", "agent_db_sim.llm.request_state"):
        # every value in full parse is covered by the (trimmed) tail reads
        full_vals = [v for _t, v in full_data.signals[name] if v not in (0, "x", "")]
        got_vals = [v for _t, _n, v in merged if _n == name and v not in (0, "x", "")]
        assert got_vals == full_vals
    tailer.close()


def test_final_marker_makes_last_block_complete(tmp_path: Path) -> None:
    path = tmp_path / "m.vcd"
    w = VcdSampleWriter(path)
    w.preregister([Sample(0.0, "x", "level", 0, {}), Sample(3.0, "x", "level", 5, {})])
    w.seal()
    w.feed(Sample(0.0, "x", "level", 0, {}))
    w.feed(Sample(3.0, "x", "level", 5, {}))
    w.flush()          # block "#3000" written; tailer holds it (newest #)
    t = VcdTailer(path)
    # only the committed $dumpvars (t=0) block is visible so far
    assert [(n, v) for _s, n, v in t.read()[0]] == [("agent_db_sim.x", 0)]
    w.close(final_t=3.0)   # trailing # marker commits the last block
    names = [n for _s, n, v in t.read()[0]]
    assert names == ["agent_db_sim.x"]  # the value under the closing marker
    t.close()


# --------------------------------------------------------------------- #
# fixture path                                                           #
# --------------------------------------------------------------------- #

def test_fixture_synthesis_round_trips(tmp_path: Path) -> None:
    sink = synthesize_samples(agents=2, until=25.0, seed=3)
    path = write_vcd(tmp_path / "f.vcd", sink.samples)
    data = parse_vcd_bytes(path.read_bytes(), include_tail=True)
    catalog = {f"{v['scope']}.{v['name']}" for v in data.catalog()}
    assert "agent_db_sim.llm.queue_depth" in catalog
    assert "agent_db_sim.db.slot_busy" in catalog
    assert "agent_db_sim.llm.request_state" in catalog
    assert "agent_db_sim.llm.tokens" in catalog
    # counters monotonic strictly increasing
    vals = [v for _, v in data.signals["agent_db_sim.llm.tokens"] if isinstance(v, int)]
    assert vals == sorted(vals)
    assert len(vals) >= 2


def test_writer_rejects_feed_after_close(tmp_path: Path) -> None:
    w = VcdSampleWriter(tmp_path / "c.vcd")
    w.close()
    with pytest.raises(VcdError):
        w.feed(Sample(0.0, "x", "level", 1, {}))
