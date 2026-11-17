# Strata design

This document walks through how the system works end to end, with pointers into the code. For *why* each choice was made, follow the ADR links. For the order things were built in, see `JOURNAL.md`.

## 1. Data model
- A **store** is a named key→value table split into **P partitions** by `Partitioner` (Murmur3 of the key mod P). Every component uses this one function.
- Each partition has **RF replicas** on different storage nodes.
- Every stored value is a `StoredValue`: `[flags][ts][sourceId][value]`. `ts` is event time; tombstones are kept. Values are totally ordered by (ts, sourceId, tombstone, bytes), and the bigger one wins (ADR 0003).

## 2. Where data comes from

```
 batch:   dataset ──► push job ──► SSTs + manifest ──► object store ──► nodes ingest (prepare) ──► swap
 stream:  producer ──► update log (Kafka partition p) ──► every replica of partition p applies it
```

### Batch: the push job (`push-job/PushJob`)
1. For each partition: select its rows, sort by unsigned bytes, write one SST (`sst/SstWriter`) where each record is `StoredValue(ts = watermark, sourceId = BATCH)`.
2. **Delta mode:** merge-join the previous and the new snapshot; write upserts and RocksDB delete markers only.
3. Upload the SSTs to the `ObjectStore`, then publish `stores/<store>/v<N>/manifest.json` **last**. The manifest is the commit point.
4. Tell the controller: `StartPush(store, N)` (`strata-admin start-push`).

The **watermark** W is the snapshot's cutoff: the version reflects every update with event time ≤ W.

### Stream: the update log (`common/UpdateLog`)
A partitioned, offset-addressed log with broker append times: Kafka (`kafka-log/KafkaUpdateLog`) or in-memory (`InMemoryUpdateLog`, ADR 0001). Log partition p feeds store partition p. **Every replica consumes independently**, so the log is the replication stream (no leader/follower replication between nodes).

## 3. Inside a storage node (`storage-node`)

```
StorageNode
 ├─ gRPC server (Netty) on a fixed pool of platform threads ── StorageNodeService (Get/BatchGet)
 ├─ ClusterAgent ── heartbeat + reconcile with the controller
 ├─ RocksResources ── shared block cache, write-buffer budget, cleaner threads
 ├─ FileCache ── downloaded SSTs (immutable, reused across versions)
 └─ StoreEngine (per store)
     └─ PartitionReplica (per hosted partition)
          ├─ current: AtomicReference<PartitionVersion>   ← reads
          ├─ future:  PartitionVersion (prepared, not yet serving)
          └─ worker thread ── the ONLY writer: applies log records, replays, swaps
```

- **`PartitionVersion`** = one RocksDB instance in its own directory, plus a reference count (ADR 0002).
- **Read path:** `get(key)` → partition → `current.get()` → `tryAcquire()` (CAS) → `db.get` → check tombstone → `release()`. Zero-copy into the protobuf response. About 2.5 µs in-process, about 0.15 ms over loopback gRPC.
- **Stream apply (worker thread):** for each log record, apply to `current` and, if a version is prepared, to `future` (dual-write). Apply under FULL mode means: drop it if `ts ≤ version.watermark`; else read the existing value, and write only if the incoming one is bigger in the total order.
- **Version lifecycle (ADR 0004):**
  1. `prepare(v)`, off the worker: resolve the manifest's file chain (FULL + DELTAs), fetch the files into `FileCache`, create a RocksDB, hard-link and ingest the files in order.
  2. `install`, on the worker: replay log records from `offsetForTime(W − maxSkew)` up to the current position into the new version; from then on, dual-write.
  3. `swap(v)`, on the worker: `current.getAndSet(future)`, then retire the old version. It closes (on a cleaner thread) when its last reader releases.
- **Bootstrap** (new or restarted replica) is the same lifecycle with no current version: replay from `W − skew` to the end of the log, then consume live.
- **Restart is stateless**: a node wipes its data directory on start (ADR 0002).

## 4. The controller (`controller`)
`ControllerState` holds store specs, serving and target versions, and the last heartbeat of each node (ADR 0005).
- **Heartbeat (every 100–500 ms):** the node sends `{store, partition, current_version, prepared_version, lag}` for each replica. The reply lists, per store, the partitions this node should host, the `serving_version` and the `target_version`.
- **Placement:** for each (store, partition), the top-RF *live* nodes by rendezvous hash.
- **Push gate:** `serving := target` once every live assigned replica of every partition reports the target prepared.
- **`ClusterAgent.reconcile`** turns that answer into actions: host or drop partitions; get to the serving version (swap if prepared, else prepare); then prepare the target.
- **Cluster view** (for clients): partition → replicas with addresses and `ready = (current == serving)`.

## 5. The client (`client`)
`StrataClient` (ADR 0006):
1. Polls the cluster view every 200 ms.
2. `get(store, key)`: partition → ready replicas ranked by the `ReplicaSelector` (C3 by default: `R − S + (1 + os·w + q)³·S` from EWMAs of client latency and server-reported service time and queue depth), then the non-ready replicas.
3. Sends to the first. If still pending after the adaptive p95 delay and the token bucket allows, it **hedges** to the next. On `UNAVAILABLE`/deadline/connection errors it **fails over**.
4. The first success completes the future (on the client's executor); the other attempts are **cancelled**, and the server skips cancelled requests still in its queue.
5. `batchGet` = group by partition, one such call per group, in parallel.

## 6. Failure handling summary
| Failure | What happens |
|---|---|
| Node crash | Controller timeout (1–3 s) → its partitions move to the next HRW node, which bootstraps. Clients fail over immediately (the dead replica errors) and stop routing to it at the next view refresh. |
| Node returns | Same id, so the HRW partitions come back. It bootstraps them (`ClusterFailoverTest`, demo step 5). |
| Crash during a push | Dead replicas are dropped from assignment, so the push completes on the live ones. The rejoining node prepares the serving version directly. |
| Controller down | Reads continue (nodes keep serving, clients keep their last view). Pushes and rebalancing wait. |
| Late, duplicate or skewed events | Total-order LWW + watermark filter (ADR 0003). Skew beyond the configured bound causes rare stale reads (Q2: 0.05%). |
| Slow replica / GC pause / noisy neighbour | C3 ranking moves traffic away; hedging covers the rest (Q3). |
| Log retention shorter than cutoff→swap + skew | **Not handled**: replay would silently miss updates. Operational requirement (ADR 0004). |

## 7. Invariants (and where they're tested)
1. Every replica that has consumed the same log prefix on the same version holds identical values. *Test:* checker `DIVERGED` = 0; `ClusterFailoverTest`.
2. A served value is never older than the newest update the system could know (oracle definition). *Test:* checker in FULL mode, 0 of 501k wrong reads.
3. A delta chain serves exactly what a full push of the same snapshot serves. *Test:* `DeltaPushTest`.
4. A swap never fails a read or mixes versions within a read. *Test:* `VersionSwapTest` (concurrent readers across 6 swaps).

## 8. Code map
| Module | Main classes |
|---|---|
| `common` | `Partitioner`, `StoredValue`, `UpdateLog`/`InMemoryUpdateLog`, `ObjectStore`/`LocalObjectStore`, `Manifests`, protos |
| `sst` | `RocksFormat`, `SstWriter` |
| `push-job` | `PushJob` (full/delta), `SyntheticDataset`, `TsvDataset`, `PushJobMain` |
| `storage-node` | `engine/{PartitionVersion, PartitionReplica, StoreEngine, FileCache, RocksResources}`, `StorageNodeService`, `StorageNode`, `ClusterAgent`, `StorageNodeMain` |
| `kafka-log` | `KafkaUpdateLog` |
| `controller` | `ControllerState`, `ControllerService`, `ControllerServer`, `ControllerMain`, `AdminMain` |
| `client` | `StrataClient`, `ReplicaSelector` (incl. C3), `ReplicaStats`, `TokenBucket` |
| `fault` | `Faults` (slow disk, pauses, noisy neighbours) |
| `local-cluster` | `LocalCluster` (in-JVM controller + N nodes) |
| `checker` | `HistoryGenerator`, `Oracle`, `Checker`, `CheckerMain` |
| `bench` | `Demo`, `Phase1Baseline`, `Q1DeltaPush`, `Q2MergeCorrectness`, `Q3TailLatency`, `OpenLoopLoad`, `ZipfianGenerator`, JMH benchmarks |
