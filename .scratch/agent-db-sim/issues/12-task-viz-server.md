# 12 — Live VCD emission + visualization server + report templates

Type: task
Status: open
Blocked by: 01, 02, 11

## Question

Build the visualization stack per the research recommendations from tickets 01 and 02:

- VCD writer fed by simkit signal hooks: real VCD files, written **live** during a run (tail-able), covering the agreed signal catalog (queue depths, slot utilization, request states, token counters).
- The dedicated visualization server: browser view of VCD/waveform-style signals over the running or finished simulation, per ticket 01's serving approach.
- The report layer: standard template(s) rendered from metrics outputs (ticket 11 result shape), user-expandable, served by the same server per ticket 02's technology.

Validated end-to-end: run a scenario → open the server → waveform view shows live signals, report renders percentiles/utilizations.
