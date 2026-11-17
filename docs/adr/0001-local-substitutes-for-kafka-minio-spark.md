# ADR 0001: Local substitutes for Kafka, MinIO and Spark

- Status: accepted
- Date: 2026-11-17

## Context
The plan calls for Kafka (stream), MinIO (object store) and Spark (push job). The build machine has no running Docker daemon (starting it needs root), 7 GB of RAM and about 8.6 GB of free disk. The research questions are about the *store's* behaviour (merge correctness, delta vs full, tail latency), not about Kafka, MinIO or Spark themselves.

## Decision
Put each external system behind a narrow interface and ship a local implementation that keeps the semantics the experiments depend on:
- `UpdateLog`: a partitioned, offset-addressed append-only log with append timestamps. `InMemoryUpdateLog` is the default. `KafkaUpdateLog` is the production adapter, tested through Testcontainers when Docker is available.
- `ObjectStore`: put/get/list of immutable blobs. `LocalObjectStore` is filesystem-backed. Nodes still copy ("download") files into a local cache, and bytes are counted.
- Push job: plain Java, doing the same steps as the planned Spark job (hash-partition, sort per partition, write SSTs, diff two snapshots for a delta).

## Alternatives considered
- **Embedded Kafka broker (KRaft in-JVM):** pulls in Scala and the broker jars, is slow to start, and is flaky in CI. It doesn't change what we measure.
- **Spark 3.5 local mode:** not supported on Java 21 and about 300 MB of dependencies. Spark 4 supports Java 21, so it's a future option.
- **Waiting for Docker:** blocks the whole project on one machine setting.

## Consequences
- Experiments run in one JVM (in-process cluster on loopback gRPC), so absolute numbers are lower than a real deployment's, and cross-machine effects such as real network jitter are simulated by the fault injector.
- The interfaces keep Kafka and S3 swappable. `deploy/docker-compose.yml` shows the real topology but isn't exercised in CI.

## Update (2026-11-17, later the same day): Docker became available
The user started the Docker daemon, so the Kafka path was verified for real:
- Testcontainers 1.20.3 couldn't talk to Docker 29 ("client version 1.32 is too old"), so it was upgraded to 1.21.4. Both Kafka tests now run (0 skipped).
- **The real broker exposed a bug the in-memory log had hidden.** Replay treated an empty read as end-of-log. Kafka's `poll` returns empty while a fetch is in flight, so the replay silently skipped updates and a swap lost them. Fixed (replay polls until it reaches its target offset, or fails the prepare), and `InMemoryUpdateLog` can now imitate this (`emptyFirstRead`) so the merge tests catch it without Docker.
- `deploy/docker-compose.yml` (Kafka + controller + 3 nodes) was run end to end, including a SIGKILL of a node and its recovery (`deploy/README.md`).

**Lesson:** a substitute is only as good as the semantics it reproduces. The in-memory log matched Kafka on ordering, offsets and timestamps, but not on "a read may return nothing even when data exists", and that difference hid a correctness bug. The experiments (Q1–Q3) are unaffected: they used the in-memory log, which never returned empty reads while data existed.
