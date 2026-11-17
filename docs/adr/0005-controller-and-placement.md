# ADR 0005: Controller: level-triggered heartbeats, rendezvous placement, all-replicas push gate

- Status: accepted
- Date: 2026-11-17

## Context
With many nodes, something has to decide which nodes host which partitions, notice dead nodes, and decide when a pushed version starts serving. That something must be simple enough to reason about under failures (lost messages, nodes restarting with empty disks).

## Decision
- **One controller process, state in memory** (`ControllerState`, pure logic with an injectable clock; `ControllerService` is the gRPC wrapper).
- **The heartbeat is the only node↔controller channel, and it's level-triggered.** Each node sends the full status of its replicas; the reply is the full desired state (partitions to host, serving version, target version). The node agent (`ClusterAgent`) reconciles the two every tick. Nothing depends on a message being delivered exactly once, so a lost heartbeat is repaired by the next one.
- **Liveness:** a node is live if it heartbeated within `nodeTimeoutMs`.
- **Placement by rendezvous (HRW) hashing:** for each (store, partition), score every live node with `hash(node, store, partition)` and take the top RF. There's no placement table to persist or keep consistent. When a node dies, only its partitions move (to each partition's next-best node); when it returns, they move back. Balance is statistical, so it needs many partitions per node: the unit test checks 240 partitions; with 12 partitions on 5 nodes we saw 2–9 replicas per node.
- **Push gate:** `startPush(v)` sets the target. Replicas prepare it (ADR 0004). The controller flips `serving = v` only when **every live assigned replica of every partition** reports v prepared. Dead nodes are dropped from assignment, so they don't block a push. Nodes swap on their next heartbeat after the flip.
- **Client view:** `GetClusterView` lists each partition's replicas and marks `ready` those serving the current serving version. The client routes to ready replicas first.

## Alternatives considered
- **Partition table with explicit moves (as in Helix):** gives fine control (throttled moves, rack-aware placement) but needs persisted state and a move protocol. HRW gives 90% of the value with zero state.
- **Quorum gate (swap when a majority is ready):** faster pushes, but the stragglers keep serving the old version longer. All-live-replicas is simple and makes "version N is serving" mean every replica.
- **Edge-triggered commands ("swap now"):** fewer bytes on the wire, but a lost or duplicated command leaves a node in the wrong state. Level-triggered reconciliation can't get stuck that way.
- **etcd/ZooKeeper for HA:** listed as a stretch goal. A single controller is a single point of failure for *changes* (pushes, rebalancing) but **not for reads**: nodes keep serving and clients keep their last view if the controller is down.

## Consequences
- Swaps across replicas aren't simultaneous: within one heartbeat interval, some replicas serve v and others v−1. A client can briefly see a version go backwards across replicas (non-monotonic reads), which is acceptable for derived data and visible because responses carry `version`.
- Rebalancing after a node death means the new replica bootstraps (download + ingest + replay). Meanwhile that partition runs with RF−1 ready replicas.
- Controller state is rebuilt from heartbeats after a restart, except store specs and serving versions, which would need persisting for a real deployment. The demo re-creates stores on startup.
