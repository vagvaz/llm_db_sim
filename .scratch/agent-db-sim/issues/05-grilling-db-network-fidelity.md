# 05 — DB network & storage backend fidelity for v1

Type: grilling
Status: resolved
Blocked by:

## Question

What does the internal DB network model contain at v1 fidelity, and how do the topologies and StorageBackend profiles wire up?

- Network: links between compute nodes and storage (bandwidth, latency, contention — per-link queueing?), and for the distributed topology the compute↔compute exchange — v1 scope: single shared network resource vs per-link models? Which of these become VCD signals?
- StorageBackend: pluggable backend with latency profiles (the user wants to change StorageBackend and its latency profiles) — what is the v1 interface (IOPS/bandwidth ceilings + latency profile per operation class?) and how do the low/high volume profiles interact with it?
- Topology wiring: centralized / disaggregated / distributed / scale-out expressed as configuration — how does config describe N compute × M storage × links, and what is shared vs per-node (slots, backends)?
- Query model: complexity → compute time, volume → data touched → network transfer + storage latency — confirm the mapping and where profile mixes enter.

Outcome: a v1 DB model spec the DB-endpoint ticket implements, explicitly deferring staged query workflows.

## Answer

**Network model — per-topology:**
- **Centralized**: colocated compute+storage, no internal network modeled.
- **Disaggregated**: shared storage-network segment — one bandwidth pool + fixed latency; all compute↔storage transfers contend on it (request transfer out, volume transfer back).
- **Distributed**: nodes are colocated compute+local-storage pairs (equal split); a configurable number of **Coordinator** nodes (a compute node that fronts peer queries, queries round-robined across coordinators); each coordinator has a **per-link** connection to every peer (own bandwidth/latency); coordinators have no links between them. Query path: coordinator compute → peer compute → peer local storage → peer compute → coordinator.

**Query compute path:** compute **wraps** storage (option b) — compute slot held through the storage round-trip. Standard three-phase breakdown: issue-compute (`issue_ratio`, default 0.1 of complexity) → storage wait → remaining compute (0.9); config parameter per topology. Simplified two-phase (storage → single compute) available as a knob.

**Coordinator overhead:** multiplier on the coordinator node's queries — `coordinator_overhead` (e.g. 1.1 → ×1.1 compute complexity).

**StorageBackend (a)+(c):** `base_latency + per_gb_time × volume` with shared `iops_ceiling`/`bandwidth_ceiling` across in-flight ops; named presets (SSD/NVMe/…) as parameter sets; low/high QueryProfiles = different parameter sets. Distributions deferred.

**Scale-out:** compute queries round-robin across compute nodes (each node has own slots); storage volume work striped across storage nodes (aggregate bandwidth/IOPS scales).

**Deferred to fog:** per-link network on disaggregated, full-mesh links, sharded (vs striped) storage, latency distributions, staged query workflows.
