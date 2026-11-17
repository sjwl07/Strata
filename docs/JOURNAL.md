# Strata: build journal

Every step, in order, with **what** was done and **why**. Read this from top to bottom to see how the project was built. The design decisions it mentions are written up properly as ADRs in `docs/adr/`.

Format of each entry:
- **Step**: what was built
- **Why**: the reasoning, including alternatives that were rejected
- **Learn**: the Java or systems idea worth studying in that step (pointers for your later review)

---

## 2026-11-17: Change of plan

**Step:** you asked me to build the whole project rather than giving hints. I updated `CLAUDE.md` with the new working mode and started this journal.

**Environment found on this machine** (it shapes several decisions below):
| Resource | Value | Consequence |
|---|---|---|
| CPU | 8 cores | Fine for an in-process multi-node cluster |
| RAM | 7 GB total, about 2 GB free | Experiments use 1–2M keys, not 50M. Small heaps |
| Disk | about 8.6 GB free | Datasets must stay at a few hundred MB |
| Docker | daemon not running, and starting it needs `sudo` | No Kafka, MinIO or Testcontainers runs here |
| JDK | 21 | As planned |

**Why this matters / decisions taken up front** (written up in ADR 0001):
1. **Kafka sits behind an interface (`UpdateLog`).** The default implementation is an in-memory log with Kafka semantics (partitions, offsets, append time, retention). A real `KafkaUpdateLog` is also included, with a Testcontainers test that runs only when Docker is available. Everything that matters for Q2 (ordering, offsets, replay) holds in both.
2. **MinIO sits behind an interface (`ObjectStore`)**, implemented on the local filesystem. The node still "downloads" files into its own cache and counts bytes, so Q1 measures transfer volume honestly.
3. **No Spark.** Spark 3.5 doesn't officially support Java 21, and its dependencies are around 300 MB. The push job is a plain-Java program that does the same steps Spark would (hash-partition, sort, write SSTs, diff snapshots for deltas). Running it on Spark 4 is listed as future work.
4. **The cluster runs in one JVM**: controller and N storage nodes, each with its own real Netty gRPC server on a loopback port, so requests really cross the network stack. "kill -9" is simulated by abruptly stopping a node and deleting its disk state.
5. **Experiment sizes are scaled down** (about 1M keys), and every write-up states the hardware.

---

## Phase 0: Java warm-up exercises

**Step 1: finish Exercise 1** (`StorageNodeService`, `StorageNodeMain`). I started from your draft of `get` and applied the six review points from last session:
1. Added the package and imports. `StorageNodeImplBase` is a nested class, so it's `StorageNodeGrpc.StorageNodeImplBase`.
2. Set `found`.
3. Return `Status.INVALID_ARGUMENT`.
4. Removed the redundant else branch.
5. `Map.copyOf` in the constructor.
6. Implemented `batchGet`, which validates all keys before answering.

The 5 acceptance tests pass.
- **Why:** this is the shape of the read API for the rest of the project. In Phase 1 the map is replaced by the versioned RocksDB engine behind the same gRPC methods.
- **Learn:** proto3 default values (why `found` exists); `StreamObserver` (`onNext` then `onCompleted`, or `onError` exactly once); why `Status...asRuntimeException()` matters (any other exception becomes `UNKNOWN` on the client).

**Step 2: Exercise 2, the JMH map benchmark.** Results and the explanation are in `docs/notes/phase0-jmh.md`. ConcurrentHashMap scales to about 2.3× at 8 threads, while every lock-based design gets *slower* with more threads. I added a `-Pjmh.threads=N` switch to `bench/build.gradle.kts`.
- **Why:** it sets the design rule for the hot read path: no shared writes per request.

**Step 3: Exercise 3, the concept note** (`docs/notes/phase0-concepts.md`): happens-before, virtual threads, and `CompletableFuture`. Each part ends with the Strata design decision it drives:
- an `AtomicReference` version pointer;
- a bounded platform-thread pool for reads, because RocksDB JNI calls pin virtual threads;
- client callbacks moved off the gRPC threads.

## Phase 1: Single-node versioned store

**Step 1: shared building blocks in `common`.**
- `Murmur3` + `Partitioner`: key → partition. The push job, nodes and client all call this one function. It's tested against the published MurmurHash3 vectors, because if two components ever disagree on a key's partition, reads silently miss.
- `StoredValue`: the value as stored, `[flags][ts][sourceId][value]`, with a **total order** (ts, then sourceId, then tombstone, then value bytes). Why total: replicas that receive the same updates in different orders must end with identical values, so ties need a deterministic winner.
- `ObjectStore` / `LocalObjectStore`: an S3-shaped blob store. Writes go to a temp file and are renamed, so readers never see half-written objects.
- `VersionManifest` (protobuf, stored as JSON): which SST files make up a version, its type (FULL/DELTA), its base version, and its **watermark**. The manifest is written *last*: a version exists only once its manifest does (the commit point).
- `UpdateLog` / `InMemoryUpdateLog`: the Kafka-shaped stream (ADR 0001). It has `offsetForTime`, the equivalent of Kafka `offsetsForTimes`, used later for replay.

**Step 2: the `sst` module.** SST table settings (Bloom filter at 10 bits/key, 16 KB blocks, LZ4) and an `SstWriter` that *checks* keys are strictly increasing.
- **Why a separate module:** the push job (writer) and the node (reader) must agree on the format, but the push job shouldn't depend on the server.

**Step 3: the push job (`push-job`).** For each partition (in parallel): scan the dataset keeping only that partition's rows, sort by unsigned bytes, write one SST with every record stamped `ts = watermark, sourceId = BATCH`, upload, and finally publish the manifest. `SyntheticDataset` generates any snapshot N deterministically from (seed, N), so experiments are reproducible and the delta push can diff N against N-1 without storing them. The delta code path is written too and is used in Phase 3.
- **Why scan per partition:** memory stays O(N/P) instead of O(N), standing in for Spark's shuffle.

**Step 4: the engine (`storage-node/.../engine`). This is the core.** See ADR 0002.
- `PartitionVersion`: one RocksDB per (partition, version), with a reference count so it's closed only after the last in-flight read.
- `PartitionReplica`: holds `current` (an `AtomicReference`) and a dedicated **single writer thread**. Every mutation (applying stream updates, replay, swap) runs on that thread, so last-writer-wins read-compare-write needs no locks.
- `StoreEngine`: turns a manifest into prepared partition versions (download the chain through `FileCache`, ingest), then swaps.
- `RocksResources`: one block cache and one write-buffer budget for all instances.
- **Learn:** `AtomicReference` + CAS loops (`tryAcquire`); why closing a JNI resource under a reader crashes the JVM; `ingestExternalFile` and hard links (`Files.createLink`); `CompletableFuture.supplyAsync(...).thenCompose(...)` to chain "load off-thread, then install on the worker".

**Step 5: the gRPC service on the engine.** `StorageNodeService` now takes a `KeyLookup` (the Phase 0 map constructor stays, and those tests still pass). It also:
- returns `version` and `ServerFeedback` (queue depth, service time) on every response;
- maps "partition not hosted / not loaded" to `UNAVAILABLE`, so clients fail over;
- skips work for already-cancelled calls.

`StorageNode` serves on a fixed platform-thread pool (see the Phase 0 note on virtual threads and JNI).

**Step 6: tests.**
- Unit tests for the codec order, Murmur vectors, partition balance and log offsets.
- Engine tests: every key of a pushed version is served correctly; and **swap stress**, where 4 reader threads verify every value while the store swaps versions 6 times (each swap closes a RocksDB under live readers). There were no errors or wrong values.

**Step 7: benchmark tooling (`bench`)**:
- `ZipfianGenerator` (Gray et al.);
- `OpenLoopLoad`, a fixed-rate sender that measures from the *intended* send time to avoid coordinated omission;
- the `phase1` experiment.

**Bug found while running it:** the first run showed 100% errors, because the experiment never called `store.host(p)`, so `load()` loaded nothing. I fixed it and made the experiment **abort if any request errors**, so a broken run can't produce a "result".

**Step 8: results** (`docs/experiments/phase1-baseline.md`):
- push of 1M keys: 3.4 s;
- **ingest: 0.17 s**, because hard-linked SSTs mean no rewrite;
- p50 about 0.15 ms and p99 about 0.4–1.2 ms up to 20k req/s, with saturation at 40k (client and server share 8 threads);
- the swap under load costs a p99 blip of 0.38 → 0.76 ms for one second, with zero errors.
