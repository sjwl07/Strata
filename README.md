# Strata

A serving store for **derived data**, such as ML features, recommendations and precomputed aggregates, that is rebuilt in bulk offline *and* updated in real time. Strata serves each key from a batch snapshot (SST files built by a push job) overlaid with a stream of updates (Kafka), merges the two **without ever serving a stale or resurrected value**, and ships with a client that keeps tail latency low when replicas misbehave.

Java 21 · RocksDB · gRPC · Kafka · about 6.3k lines of Java plus 1.3k of tests · three research questions answered with measurements.

## Headline results
All numbers are from one laptop (i3-1215U, 8 threads, 7 GB RAM); see each write-up for method and limitations.

| Question | Answer |
|---|---|
| **Q2: does a naive batch/stream merge serve wrong data?** ([write-up](docs/experiments/q2-merge-correctness.md)) | Under randomized histories with late, duplicate and clock-skewed events, version swaps and replica crashes: **naive merge 19% wrong reads, last-writer-wins alone 8.5%, Strata's watermark + replay-before-swap 0 of 501,456**. A wrong clock-skew bound brings back 0.05% stale reads. Correctness costs 4× slower stream apply (read-before-write), still about 430k updates/s per node. |
| **Q3: how much do adaptive replica selection and hedging cut tail latency?** ([write-up](docs/experiments/q3-tail-latency.md)) | With a slow node, GC-like pauses or noisy neighbours, **C3 replica ranking cut p99 17–63× vs round-robin (to about 0.6 ms) at zero extra load**. Budgeted hedging rescues load-blind routing for about 3% extra requests, but a small token bucket drains under bursty slowness, and hedging on top of C3 can slightly hurt. |
| **Q1: when does a delta push beat a full push?** ([write-up](docs/experiments/q1-delta-push.md)) | A delta ships about f × the bytes (1% changed → 100× smaller) but costs about 0.9 s extra to compute here. It wins end to end only when bandwidth per copy is below about (1 − f) × 57 MB/s (cross-region, or many replicas sharing one store). Stacked deltas don't slow point reads (Bloom filters). |

Plus: swapping versions under load costs a one-second p99 blip (0.38 → 0.76 ms) and zero errors ([Phase 1](docs/experiments/phase1-baseline.md)); a 5-node cluster that loses nodes mid-push and mid-stream converges to exactly the right value on every replica (`ClusterFailoverTest`).

## How it works
```
            OFFLINE                                   ONLINE
 dataset ──► push job ──► SST files + manifest ──► object store
             (full or delta;     │                     │ nodes download + hard-link ingest
              records stamped    ▼ StartPush(v)        ▼
              ts = watermark)  ┌────────────┐   ┌─────────────────────────────────────┐
                               │ controller │◄─►│ storage nodes (RocksDB per partition│
 producers ──► update log ─┐   │ HRW place- │hb │ per version; replay from watermark, │
   (event-time ts)  (Kafka)│   │ ment, push │   │ dual-write, atomic ref-counted swap)│
                           └──►│ gate, view │   └──────────────────┬──────────────────┘
          every replica consumes└────────────┘        gRPC Get/BatchGet + load feedback
          its partition                                            │
                                                     ┌─────────────┴─────────────┐
                                                     │ StrataClient: C3 ranking,  │
                                                     │ budgeted hedging, cancel,  │
                                                     │ failover, scatter-gather   │
                                                     └───────────────────────────┘
```
- **Merge rule:** every value carries (event time, source). The newest wins in a total order, so replicas converge in any delivery order. Updates at or below a version's snapshot cutoff are dropped, because the snapshot already reflects them, including deletes.
- **Swap rule:** before a new version serves, it replays the log from (cutoff − max clock skew) and then receives every live update, so the swap never loses an update.

Details: [`docs/design.md`](docs/design.md). The plain-language version with analogies: [`docs/CONCEPTS.md`](docs/CONCEPTS.md).

## Quickstart
Requires JDK 21. No Docker needed.
```sh
./gradlew :bench:installDist
bench/build/install/bench/bin/bench demo             # 3-node cluster, push, stream, swap, kill a node
bench/build/install/bench/bin/bench demo --mode=NAIVE  # same story; watch the swap lose an update
```

Separate processes (controller, nodes, push job), batch-only without Kafka:
```sh
./gradlew :controller:installDist :storage-node:installDist :push-job:installDist :bench:installDist
controller/build/install/controller/bin/controller --port=7000 &
for i in 1 2 3; do storage-node/build/install/storage-node/bin/storage-node --node-id=n$i \
  --port=710$i --data-dir=/tmp/strata/n$i --objects=/tmp/strata/objects --controller=localhost:7000 & done
controller/build/install/controller/bin/strata-admin create-store --name=demo --partitions=4 --rf=2
printf 'ravi\t111\nasha\t555\n' > /tmp/v1.tsv
push-job/build/install/push-job/bin/push-job --objects=/tmp/strata/objects --store=demo \
  --partitions=4 --version=1 --input=/tmp/v1.tsv
controller/build/install/controller/bin/strata-admin start-push --store=demo --version=1
bench/build/install/bench/bin/bench get --controller=localhost:7000 --store=demo --key=ravi
```
Add `--kafka=host:9092` to the nodes for streaming updates. `deploy/docker-compose.yml` sketches the full topology with Kafka; it wasn't run on the development machine (no Docker).

## Modules
| Module | What it is |
|---|---|
| `common` | Protobuf/gRPC APIs, partitioner, stored-value format and merge order, update-log and object-store interfaces |
| `sst` | SST format shared by writer and reader |
| `push-job` | Builds full and delta versions (partition, sort, SST, manifest) |
| `storage-node` | The versioned RocksDB engine, stream apply/replay/swap, gRPC service, cluster agent |
| `kafka-log` | Kafka implementation of the update log |
| `controller` | Membership, rendezvous placement, push lifecycle, cluster view, `strata-admin` |
| `client` | Smart client: C3, hedging, cancellation, failover (publishable on its own: `./gradlew :client:publishToMavenLocal`) |
| `fault` | Slow disk, stop-the-world pauses, noisy neighbours |
| `local-cluster` | Controller + N nodes in one JVM, with kill/restart |
| `checker` | Randomized correctness checker with an oracle |
| `bench` | Demo, experiments (`phase1`, `q1`, `q2`, `q3`), JMH micro-benchmarks |

## Build and test
```sh
./gradlew build                                            # compile, format check, all tests
./gradlew :checker:test -Pchecker.histories=40 --rerun     # longer randomized correctness run
./gradlew :bench:installDist && cd bench && build/install/bench/bin/bench q2   # rerun an experiment
./gradlew :bench:jmh -Pjmh.includes=ReadPathBenchmark -Pjmh.prof=gc
```
Kafka integration tests use Testcontainers and run wherever Docker is available (CI does); elsewhere they skip.

## Honest scope
- Experiments run on one machine; the multi-node cluster is in one JVM over loopback gRPC. Datasets are 0.1–1M keys, not production scale. Ratios and mechanisms are the result, not absolute numbers.
- Kafka, S3/MinIO and Spark are behind interfaces with local implementations (ADR 0001). The Kafka adapter is real and tested in CI; the object store is a shared directory; the push job is plain Java doing what a Spark job would.
- Not implemented: controller high availability, C3's rate control, per-field merge, Iceberg input. See `docs/PROGRESS.md`.

## Docs
- [`docs/JOURNAL.md`](docs/JOURNAL.md): how it was built, step by step, with the reasoning and the bugs found
- [`docs/design.md`](docs/design.md): how it works, with a code map
- [`docs/CONCEPTS.md`](docs/CONCEPTS.md): the ideas in plain language
- [`docs/adr/`](docs/adr/): design decisions (0001–0006)
- [`docs/experiments/`](docs/experiments/): Phase 1 baseline, Q1, Q2, Q3 write-ups and raw data
- [`docs/PLAN.md`](docs/PLAN.md) and [`docs/PROGRESS.md`](docs/PROGRESS.md): original plan and current status
