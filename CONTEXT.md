# agent-db-sim

A discrete-event simulation platform that models LLM agents working against a database, to study bottlenecks and the behavior of different database topologies under agent workloads.

## Language

**AgentClient**:
A simulated worker that runs a closed loop of LLM inference calls alternating with database queries for the entire simulated duration.
_Avoid_: agent, user, client

**LLMEndpoint**:
A simulated inference server that serves AgentClient requests, limited by slot capacity and aggregate token throughput.
_Avoid_: LLM, model, inference server

**DBEndpoint**:
The simulated database under study, composed of DBCompute, DBStorage, and internal network links.
_Avoid_: database, DB

**DBCompute**:
The processing resource of DBEndpoint that spends query complexity as compute time.
_Avoid_: compute node, CPU

**DBStorage**:
The data resource of DBEndpoint that serves query volume through a swappable StorageBackend with latency profiles.
_Avoid_: storage, disk

**Slot**:
A unit of concurrent service capacity at a serving component; a request occupies one slot while processing.
_Avoid_: capacity, worker, channel

**Ingress**:
The configurable admission stage in front of a component's processing: nothing, a FIFO, or a priority queue.
_Avoid_: input queue, buffer (unless describing a specific queue)

**Processing**:
A wait-time event whose duration is computed from parameters by a cost function while a slot is held.
_Avoid_: execution, compute (as a generic verb)

**Topology**:
How DBEndpoint's compute, storage, and network are wired: centralized (colocated, no internal network), disaggregated (N compute × M storage over a shared storage network), or distributed (colocated compute+local-storage nodes with coordinator-fronted peer links).
_Avoid_: layout, architecture

**Coordinator**:
A distributed-topology compute node that fronts peer queries; queries round-robin across coordinators, and each coordinator links to every peer.
_Avoid_: leader, master, gateway

**Scale-out**:
Growing the number of compute or storage nodes on the disaggregated topology.
_Avoid_: horizontal scaling, scaling

**QueryProfile**:
A named mix of query complexity and volume classes that workloads draw queries from.
_Avoid_: workload profile (ambiguous with config overlays)

**Scenario**:
A complete simulation configuration, optionally modified by a named config overlay.
_Avoid_: config profile (ambiguous with QueryProfile)

**Sweep**:
A factorial run that enumerates a cross product of factor values over a base scenario, producing one run directory per combination.
_Avoid_: batch, experiment run
