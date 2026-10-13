"""Visualization stack (ticket 12): live VCD emission, viz server, reports.

Modules:
- ``vcd``     live VCD writer from probe samples + lenient reader/tailer
- ``server``  FastAPI app factory (waveforms + SSE stream + reports)
- ``report``  Jinja2 + Plotly report rendering from a run's metrics.db
- ``fixture`` synthetic run factory for standalone testing/validation

The run driver wires the writer by feeding a live one with samples
(``VcdSampleWriter``); batch/fixture code uses the one-shot ``write_vcd``.
"""
