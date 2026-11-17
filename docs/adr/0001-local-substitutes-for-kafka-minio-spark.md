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
