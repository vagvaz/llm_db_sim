# 10 — DBEndpoint: compute, storage, network, topologies, scale-out

Type: task
Status: resolved
Blocked by: 05, 07

## Question

Implement the DBEndpoint per the v1 DB model spec from ticket 05:

- Sub-components DBCompute (slot capacity, complexity → compute time), DBStorage (pluggable StorageBackend + latency profiles, volume → data touched), internal network (links per spec).
- The three topologies as configuration wiring: centralized, disaggregated (N × M), distributed; **scale-out knobs** grow N (compute) or M (storage) on the disaggregated topology.
- Query service path emits per-hop records (compute wait/service, storage wait/service, network transfer) and signals.

Validated by tests: one scenario per topology runs end-to-end with an agent; scale-out config changes only N/M as expected; disaggregated path exercises the network model.

## Answer

### Modules (`src/agent_db_sim/components/db/`)

- `_cfg.py` — fail-fast `db.*` config accessors (OmegaConf-or-mapping; defaults so concurrent tickets can extend the schema).
- `query.py` — wired-contract shapes: `DBQuery` (attrs `complexity`, `volume`, `agent_id`, `request_id`; plus `priority` for `priority:<path>` ingress and simkit-compatible `rid`/`submitted_at`/`done`), `StageTiming` (per-hop queue/service + start/end + meta), `QueryResponse` (`db_latency`, `queue_time`, `service_time`, `stages` dict), `q_scalar`/`q_tag` (accept any object with the attrs or a `params` mapping — simkit `Request` bodies work).
- `backend.py` — ticket-05 StorageBackend: `BackendParams` (`base_latency + per_gb_time × volume`, shared `iops_ceiling`/`bandwidth_ceiling`); presets registry **`{"hdd", "ssd", "nvme"}`** merged into `simkit.registry.STORAGE_BACKEND_REGISTRY` (value type is `BackendParams`, not a class); `db.storage.overrides` patches any field; `StorageNode` is the per-node ceiling server (shared-rate pacing: each in-flight op earns IOPS and bandwidth at `ceiling / |in-flight|`; lone-op service = `max(base + per_gb·vol, vol/bw, 1/iops)`; contention stretches `storage_service`).
- `server.py` — **`PhaseSlotServer(SlotServer)`**: simkit SlotServer subclass (same ingress dispatcher, explicit slot pool, per-request `ServiceRecord`, shared in-flight token set, probe samples); only `_serve` is swapped for a scripted multi-phase body (issue → storage round-trip → finalize) so the **slot is held throughout** (option b). Token cap (`db.compute.token_cap`) is paced across the *compute* phases only, via `PhaseControl.wait_compute` (shared-rate pacing mirror of the base loop); `ctl.call(event)` yields foreign hops (network/storage/peer).
- `compute.py` — **`DBCompute(Component)`**: ports `queries`/`peer_queries` in, `responses` out (kinds `query`/`response`); ingress spec per node (`none|fifo|priority:<key>`), slot pool, `issue_ratio` (default 0.1), `phase_split: three_phase|two_phase` (two-phase knob = storage first, single compute stage), coordinator `overhead` multiplier folded into the coordinator's compute stages (ticket 04). Emits `submit → ingress → SlotServer-dispatcher → _serve → PhaseSlotServer._serve → record → succeed → pool.release → req.done(QueueResponse)` lifecycle edges `{path}.{stage}` states `submitted|queued|processing|completed` tagged `agent_id`/`request_id`.
- `network.py` — `NetworkConfig` parse + `NetLink` (fixed propagation latency → BandwidthPool transfer; emits `net_req`/`net_resp` edges; `StageTiming` carries `latency`/`volume` in meta; `request_size` = 0 controls a latency-only hop). Contains `NetPool(BandwidthPool)` with an fp-tolerant container (see verification note).
- `routing.py` — topology-injected middle-stage resolvers returning `{stage: StageTiming}`: `LocalResolver` (colocated, no network), `SegmentResolver` (disaggregated: net_req out → storage → net_resp back over the shared segment; storage ops striped round-robin across the M nodes), `CoordinatorResolver` (distributed: per-peer dedicated link; peer selection round-robins per coordinator; the **peer's compute slot is held during its local storage hop**, peer-slot wait folded into `storage_queue.queue_time`).
- `topology.py` — topologies as config wiring; **registry names `centralized | disaggregated | distributed`** in `TOPOLOGY_REGISTRY`. Scale-out = `db.compute_nodes` (N) / `db.storage_nodes` (M); growing them changes only the node counts (per-node params untouched). Distributed: `db.coordinators` subset of `compute_nodes` fronts peer queries (entry nodes = coordinators; every coordinator gets its own per-link conn to every peer; `coordinator_overhead` multiplies coordinator compute).
- `endpoint.py` — **`DBEndpoint(Composite)`**: input `queries`, output `responses`; `db.topology` registry lookup fails fast; `submit(query)` round-robins across entry nodes and returns one started process.

### Config schema (`db.*`)

```yaml
db:
  topology: centralized | disaggregated | distributed
  compute_nodes: 2            # N (disagg/dist); centralized fixed at 1
  storage_nodes: 2            # M (disagg only)
  coordinators: 1             # distributed only; 1 <= c < N
  compute: {slots: 2, ingress: fifo, issue_ratio: 0.1, phase_split: three_phase, token_cap: null}
  storage: {backend: nvme, overrides: {}}   # backend in {hdd, ssd, nvme}
  network: {latency: 0.001, bandwidth: 1000.0, capacity: 8.0, request_size: 0.01, coordinator_overhead: 1.1}
```

### `simkit/registry.py` (owned additions)

- `TOPOLOGY_REGISTRY` / `STORAGE_BACKEND_REGISTRY` placeholders now filled from the DB components (merged by value, fail-fast on unknown names at DBEndpoint/backend resolution).
- `build_model(cfg, sink=None) -> Composite` implemented for the DB portion: `top.db` = DBEndpoint; the shared probe sink is exposed as `top.probe_sink` (pass the same sink to `run_model`). **Agent/LLM wiring (tickets 08/09) is a clearly marked stub block inside** — no guarded import, so concurrent tickets can't break it; sink clock is attached lazily at `construct` (sim time unavailable earlier).

### Signal/records contract

- Probe edges: `{component full path}.{stage}` (`agent_id`, `request_id`, state) for stages `compute_issue` (+`compute_finalize`), `net_req`/`net_resp` (on net links), `storage_queue`/`storage_service` (on storage nodes); per-hop queue/service separation kept in `StageTiming`s and in every node's `collect()` (`responses`, `service_records`, backend params, link stats).
- Note: storage admission is ceiling-paced only — `storage_queue.queue_time` is ~0 by construction; contention shows up as stretched `storage_service` (per ticket 05's "(a)+(c)" backend contention spec). On distributed, the peer's compute-slot wait is folded into `storage_queue.queue_time` of the returned stage.

### Deviations / notes

- `tests/test_simkit_sweep.py::test_build_model_is_a_skeleton` (ticket 07) pinned `build_model` as an unimplemented stub raising `NotImplementedError`; ticket 10 fills that stub, so the test was updated to the new contract (`build_model` without a `db:` section raises `RegistryError`). Everything else in simkit is untouched.
- simpy quirk found: the ticket-07 `BandwidthPool` staging level wedges permanently when container volumes aren't float-representable w.r.t. capacity (`cap - level` falls 2e-15 short) — e.g. any default `request_size: 0.01`. Fixed DB-locally (simkit scope off-limits): `NetPool` subclass swaps in a tolerantly clamped container (1e-9 tolerance, level clamped to capacity). Flagging for tickets 08/09: their network/token primitives may want the same guard in simkit later.
- Per-query storage striping is round-robin across the M nodes (queries distribute; aggregate bandwidth/IOPS scale with M); true per-query chunked striping + per-gb distributions were deferred in ticket 05 and stay deferred.
- Connect graph: only composite-boundary links are validated (endpoint + distributed fan-out is structural because the graph forbids double-wired inputs; peer fan-in is direct object references).

### Verification

`uv run pytest`: **101 passed** (40 pre-existing + 61 new in `tests/test_db_endpoint.py`, incl. the ticket validation list: end-to-end stub-caller scenario per topology; scale-out changes only N or M; disaggregated/distributed exercise `net_req`/`net_resp` while centralized skips them; `hdd` vs `nvme` presets give distinctly different storage service times; slot held through the storage round-trip (next request's queue time == full envelope with 1 slot); coordinator overhead multiplier proportional on compute stages; plus registry/config fail-fast and distributed coordinator round-robin). All three scenario YAMLs (`db_centralized`, `db_disaggregated`, `db_distributed`) run end-to-end with a stub caller; db_latencies 0.700 / 0.703 / 0.753 show topology network+overhead additivity.
