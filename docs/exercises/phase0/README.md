# Phase 0 — Java ramp-up

Goal: get comfortable with modern Java, gRPC and JMH before any storage code. Ask for a **hint**, an **approach**, or the **code** whenever you get stuck.

## Exercise 1 — An in-memory storage node over gRPC

Implement the `StorageNode` service from `common/src/main/proto/strata/v1/storage_node.proto`, backed by an in-memory map. In Phase 1 this map becomes RocksDB; the gRPC layer you write here stays.

**Write these files**
- `storage-node/src/main/java/io/strata/node/StorageNodeService.java`
  - `public StorageNodeService(Map<ByteString, ByteString> data)`
  - extends the generated `StorageNodeGrpc.StorageNodeImplBase`
  - `get` and `batchGet` behave as described in the proto comments
  - an empty key → `Status.INVALID_ARGUMENT`
- `storage-node/src/main/java/io/strata/node/StorageNodeMain.java`
  - starts a gRPC server on a port (`--port`, default `7070`), preloaded with a few keys
  - shuts down cleanly on Ctrl-C (JVM shutdown hook + `server.shutdown()` / `awaitTermination`)

**Done when**
1. `cp docs/exercises/phase0/StorageNodeServiceTest.java storage-node/src/test/java/io/strata/node/`
2. `./gradlew :storage-node:test` is green
3. `./gradlew :storage-node:run` starts a server, and `grpcurl -plaintext -d '{"key":"YWxpY2U="}' localhost:7070 strata.v1.StorageNode/Get` returns a value (keys are base64 in grpcurl JSON). Needs server reflection (`grpc-services`), or pass `-proto`. Your choice.

**Things to look up as you go:** generated code in `common/build/generated/source/proto`, `StreamObserver`, `ByteString`, `Status` vs `StatusRuntimeException`, why the map should be immutable or concurrent.

## Exercise 2 — Your first JMH benchmark

In `bench/src/jmh/java/io/strata/bench/MapBenchmark.java`, compare read-heavy (95% get / 5% put) throughput of:
- `HashMap` behind `synchronized`
- `Collections.synchronizedMap`
- `ConcurrentHashMap`
- `HashMap` behind a `ReentrantReadWriteLock`

at 1, 4 and 8 threads (`@Threads`, or `@Group` for mixed readers and writers) over 1M preloaded keys.

Run with `./gradlew :bench:jmh -Pjmh.includes=MapBenchmark`.

**Done when:** `docs/notes/phase0-jmh.md` has your results table and a paragraph on *why*. It should cover lock contention, cache-line effects, why the read-write lock disappoints, and what dead-code elimination is, plus how `Blackhole` prevents it.

## Exercise 3 — Concept note (no code)

In `docs/notes/phase0-concepts.md`, from memory, explain:
- the happens-before relation, and why `volatile` alone doesn't make `count++` safe
- when virtual threads help, and when they don't (CPU-bound work, `synchronized` pinning, JNI calls like RocksDB)
- `CompletableFuture`: `thenApply` vs `thenCompose`, and where callbacks run

## Reading
- *Effective Java* (3rd ed.) items 1–20 and 78–84
- *Java Concurrency in Practice* chapters 1–5 and 16
- gRPC Java basics tutorial: https://grpc.io/docs/languages/java/basics/
