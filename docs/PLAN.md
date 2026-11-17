# Strata: project plan

> **Status (2026-11-17):** all phases are built (v0.1.0). What was built differs from this plan in a few places because of the dev machine (no Docker, small disk); see the "Deviations" table in `PROGRESS.md`. This file is kept as the original plan.

## Why this project
The user joins LinkedIn's online data infra team (Bangalore) in summer 2027. Their internship covered the *offline* path (Kafka → Flink → Iceberg → Spark). Strata covers the *online* path: serving derived and ML-feature data that is computed in bulk offline and also updated by streams. It's in Java, the role's language.

Goals, in priority order:
1. **Learn** modern Java, storage engines, replication and tail latency well enough to ramp up fast at LinkedIn.
2. **Answer real questions with numbers**, so it's an experiment and not a Venice clone.
3. **Make it good enough** for strangers to clone, run, trust and possibly use.

## Research questions (everything else exists only to support these)
- **Q1 – Delta vs full push:** when only a fraction *f* of keys changed since the last snapshot, at what *f* does shipping a delta beat a full re-push? Measure bytes shipped, push time, and read latency during the push.
- **Q2 – Batch/stream merge correctness:** how often does a naive merge serve stale or regressed values (late stream events, version swaps, replays) compared with timestamp-based resolution plus replay-on-swap, and what does correctness cost?
- **Q3 – Tail latency:** how much do adaptive replica selection and budgeted hedging cut p99/p999 under realistic slowness (GC pauses, slow disk, noisy neighbour), and at what extra load?

## Architecture

```
            OFFLINE                                  ONLINE
 Parquet/Iceberg ──► Spark push job ──► SST files ──► MinIO (object store)
                      (full or delta)        │
                                             ▼  "version N ready"
                                       ┌───────────┐  metadata, partition map,
 Kafka "updates" topic ───────────┐    │Controller │  version pointer, health
  (nearline writes, timestamps)   │    └─────┬─────┘
                                  ▼          │
                     ┌──────────────────────────────────┐
                     │ Storage nodes (RocksDB, P parts,  │  each replica consumes
                     │ RF=3, version dirs v(N), v(N+1)) │  Kafka independently
                     └────────────────┬─────────────────┘  (Kafka is the replication log)
                                      │ gRPC (get / batchGet), responses
                                      │ piggyback queue depth + service time
                              ┌───────┴────────┐
                              │  Smart client  │ routing, C3-style replica scoring,
                              │                │ budgeted hedging, cancellation
                              └────────────────┘
```

Key design decisions (write each up as an ADR in `docs/adr/` when it's built):
- **Storage:** RocksDB through `rocksdbjni`. Bulk versions are built offline as SST files (`SstFileWriter`) and loaded with `ingestExternalFile`, so the serving path never re-writes them.
- **Versioning:** each partition keeps `current` and `future` version directories. A swap is an atomic pointer flip in the controller, which nodes pick up through a watch/poll.
- **Merge semantics:** every value carries `(logicalTs, sourceId)`. Batch records get the snapshot's watermark timestamp, stream records carry event timestamps, and the higher timestamp wins (LWW), with a deterministic tie-break. Start per-record; per-field is a stretch.
- **The hard case, version swap:** while v(N+1) is being ingested, nodes **replay stream updates newer than v(N+1)'s watermark** onto it before swapping. Otherwise the swap wipes out recent writes, which is what Q2 measures. See `CONCEPTS.md`.
- **Replication:** every replica consumes the same Kafka partition. A replica that rejoins bootstraps from the latest SSTs plus a Kafka replay from the watermark offset.
- **Delta push:** the Spark job hashes rows per key, diffs snapshot N against N-1, and emits upsert and delete SSTs. Nodes hard-link version N's files and ingest the delta on top.
- **Controller:** starts as a single process holding metadata (partition map, versions, node heartbeats). Leader election with etcd (jetcd) is a stretch goal.

## Tech stack
Java 21 · Gradle (Kotlin DSL) multi-module · gRPC-java + Protobuf · rocksdbjni · Kafka client · Spark 3.5 (Java API) · MinIO · JUnit 5 + AssertJ + Testcontainers + jqwik · JMH · JFR + async-profiler · Docker Compose · GitHub Actions · HdrHistogram.

## Target repo layout
```
common/        protobufs, shared types, timestamp/merge logic
storage-node/  RocksDB partitions, version manager, Kafka ingestion, gRPC server
controller/    metadata, partition assignment, version lifecycle, heartbeats
client/        routing, replica selection, hedging
push-job/      Spark full and delta push → SST → MinIO
checker/       oracle model + randomized correctness harness
fault/         latency/GC/disk fault injection
bench/         workload generator, JMH, experiment scripts
deploy/        docker-compose (Kafka, MinIO, controller, 3–5 nodes)
docs/          PLAN, PROGRESS, CONCEPTS, BACKGROUND, adr/, exercises/, notes/, experiments/
```

## Phases (~15–20 h/week, about 24 weeks, plus 2–3 months of slack)

### Phase 0 – Java ramp-up and skeleton (weeks 1–2)
- Build: Gradle skeleton and CI *(done by Claude)*; an in-memory gRPC storage node; a JMH map benchmark.
- Learn: Java 17–21 features, `java.util.concurrent`, `CompletableFuture`, virtual threads, Java Memory Model basics. Read *Effective Java* items 1–20 and 78–84, plus *JCiP* chapters 1–5 and 16.
- Exit: the exercises in `docs/exercises/phase0/` are done. You can explain happens-before and when virtual threads help.

### Phase 1 – Single-node versioned store (weeks 3–6)
- Build: RocksDB partitions, a standalone SST builder, versioned ingestion with `ingestExternalFile`, atomic swap, gRPC `get`/`batchGet` on RocksDB, a workload generator with Zipfian keys, HdrHistogram latency.
- Learn: LSM trees, RocksDB block cache/bloom filters/compaction, JNI and off-heap memory, GC at p999 (JFR, async-profiler). Read the RocksDB wiki and the Voldemort read-only stores paper (FAST '12).
- Exit: 50M keys loaded, version swaps under read load, and a published baseline p50/p99/p999 with a swap-latency graph.

### Phase 2 – Streaming merge and correctness checker (weeks 7–10)
- Build: a Kafka consumer per partition, timestamped LWW merge, swap-with-replay from the watermark, and `checker/` (oracle plus jqwik histories with late events, duplicates, replays and mid-stream swaps). Include a **naive-merge mode** for comparison.
- Learn: Kafka consumer groups/offsets/rebalancing, idempotence, event time vs processing time, property-based testing.
- Exit: zero anomalies in timestamp mode, quantified anomalies in naive mode (the first half of Q2).

### Phase 3 – Spark push job and delta pushes (weeks 11–13)
- Build: a Spark job that reads Parquet, partitions by the store's hash, sorts, and writes SSTs to MinIO; a delta mode that diffs snapshots; hard-link clone plus delta ingest.
- Learn: Spark partitioning, sort/shuffle cost, and executor memory, all through the Java API.
- Exit: **Q1 experiment**: sweep *f* ∈ {0.1, 1, 5, 10, 25, 50}% at 50–100M keys and find the crossover. Write `docs/experiments/q1-delta-push.md`.

### Phase 4 – Distribution and replication (weeks 14–17)
- Build: a controller (assignment, heartbeats, version lifecycle), RF=3, node kill and rejoin (SST bootstrap plus Kafka replay), a basic routing client.
- Learn: failure detection, leases, rebalancing, replication lag. Read the Venice blog/paper and *The Tail at Scale*, and skim DDIA chapters 5 and 9.
- Exit: a 3–5 node compose cluster survives kill -9 during push and stream, and the checker stays clean. Second half of Q2: staleness and the cost of correct merging under faults.

### Phase 5 – Smart client and fault injection (weeks 18–21)
- Build: round-robin, least-outstanding, **C3-style scoring** (latency EWMA, plus piggybacked queue depth, with a cubic penalty), **budgeted hedging** (token bucket capping extra load at X%), cancellation, scatter-gather `batchGet`. In `fault/`: toxiproxy delay, simulated GC pauses, slow disk.
- Learn: queueing basics, gRPC/Netty internals (deadlines, cancellation, flow control), G1 vs ZGC. Read the C3 paper (NSDI '15).
- Exit: **Q3 experiment**: a strategies × faults matrix (p99/p999 and extra load), plus the GC comparison. Write `docs/experiments/q3-tail-latency.md`.

### Phase 6 – Hardening, write-up, users (weeks 22–24)
- Profile and fix the top 3 hotspots. One-command quickstart, a README with diagrams and headline numbers, and `design.md`.
- Blog post, then share on r/dataengineering, Hacker News and the Kafka/Spark communities.
- Release v0.1, with the client as a standalone Maven artifact.

### Stretch goals
Per-field merge · Iceberg input · controller HA on etcd · read-your-writes tokens · Feast online-store adapter · multi-region active-active.

## Definition of done ("good enough")
- `docker compose up` plus one script reproduces every graph in the README.
- CI runs unit tests, Testcontainers integration tests, and a 5-minute randomized checker run on every PR.
- Every non-obvious decision has an ADR, and every research question has a write-up with the method, raw data and limitations.
- Honest numbers: state the hardware and dataset size, and never extrapolate to "LinkedIn scale".
- Clean-room: open-source code only. Check the LinkedIn offer letter's outside-activities/IP clause before publicising.

## Verification (per phase)
- `./gradlew test` for unit tests, and Testcontainers for Kafka/MinIO.
- `./gradlew :checker:run --args="--histories 10000 --faults all"` must report 0 anomalies in timestamp mode.
- `deploy/chaos.sh` kills or restarts nodes during push + stream, then runs the checker.
- `bench/run-experiment.sh q1|q2|q3` writes CSVs and plots to `docs/experiments/`.
- At the end of each phase, the user writes a one-page note in `docs/notes/` on the hardest concept, from memory.
