# 10 — DBEndpoint: compute, storage, network, topologies, scale-out

Type: task
Status: open
Blocked by: 05, 07

## Question

Implement the DBEndpoint per the v1 DB model spec from ticket 05:

- Sub-components DBCompute (slot capacity, complexity → compute time), DBStorage (pluggable StorageBackend + latency profiles, volume → data touched), internal network (links per spec).
- The three topologies as configuration wiring: centralized, disaggregated (N × M), distributed; **scale-out knobs** grow N (compute) or M (storage) on the disaggregated topology.
- Query service path emits per-hop records (compute wait/service, storage wait/service, network transfer) and signals.

Validated by tests: one scenario per topology runs end-to-end with an agent; scale-out config changes only N/M as expected; disaggregated path exercises the network model.
