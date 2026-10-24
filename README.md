# Strata

A serving store for derived data. A full batch snapshot (from Spark) forms the base layer; real-time updates (from Kafka) are layered on top, and the two are merged without serving stale values. A tail-latency-aware client routes reads across replicas.

> Status: Phase 0 (skeleton). Nothing here is production-ready.

## Research questions
1. **Delta vs full push.** When a fraction *f* of keys changed since the last snapshot, at what *f* does shipping a delta beat a full re-push?
2. **Batch/stream merge correctness.** How often does a naive merge serve stale or regressed values, compared with timestamp-based merging plus replay-on-swap, and what does correctness cost?
3. **Tail latency.** How much do adaptive replica selection and budgeted hedging reduce p99/p999 under realistic slowness, and at what extra load?

## Layout
| Module | Purpose |
|---|---|
| `common` | Protobuf/gRPC API and shared types |
| `storage-node` | Serves reads; later RocksDB, versioning, Kafka ingestion |
| `bench` | JMH micro-benchmarks and, later, experiment harnesses |

Coming in later phases: `controller`, `client`, `push-job`, `checker`, `fault`, `deploy`.

## Build
Requires JDK 21.

```sh
./gradlew build          # compile, test, format check
./gradlew spotlessApply  # auto-format
./gradlew :bench:jmh     # micro-benchmarks
```

## Roadmap
0. Java ramp-up and skeleton
1. Single-node versioned store (RocksDB, SST ingestion, atomic swap)
2. Streaming merge and correctness checker
3. Spark push job and delta pushes (Q1)
4. Controller, partitions, replication
5. Smart client and fault injection (Q3)
6. Hardening, write-up, v0.1

Design decisions live in [`docs/adr/`](docs/adr/).
