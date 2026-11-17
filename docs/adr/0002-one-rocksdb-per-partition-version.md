# ADR 0002: One RocksDB instance per (partition, version); no WAL; stateless restart

- Status: accepted
- Date: 2026-11-17

## Context
A node must serve version N while it loads version N+1, then switch atomically, and it must never close a database while a read is running inside it (a JNI read on a closed handle crashes the JVM).

## Decision
- **One RocksDB instance per (store, partition, version)**, each in its own directory. A version is loaded by `ingestExternalFile` of pre-built SSTs, hard-linked from a local file cache.
- **An atomic pointer plus reference counting.** `PartitionReplica.current` is an `AtomicReference<PartitionVersion>`. A reader does `tryAcquire()` (a CAS increment that fails once the count has reached 0), reads, then `release()`. The owner holds one reference until the version is retired. Whoever drops the count to 0 schedules `close()` on a cleaner thread.
- **Shared native resources.** One LRU block cache and one `WriteBufferManager` per node bound off-heap memory however many instances are open.
- **No WAL.** All state is derived from immutable pushed SSTs plus the update log, so after a crash the node rebuilds instead of recovering.
- **Stateless restart.** On startup a node deletes its data directory and bootstraps every hosted partition (fetch SSTs, replay the log from the watermark).

## Alternatives considered
- **One RocksDB per partition, with versions as column families:** swapping and dropping a column family is cheaper, but the column families share one WAL, memtable flush and compaction scheduling, and dropping a big one stalls the others. Separate instances isolate a version's lifecycle completely.
- **A read-write lock around the swap:** every read would write the lock's shared state. Phase 0's benchmark showed RW locks collapse under contention, so we use reference counts on the hot path, with a single CAS per read.
- **WAL plus restart from local state:** faster restarts for big partitions, but recovering the local DB *and* finding the right log offset to resume from is a classic source of bugs (offset/data mismatch after a crash). Rebuilding is simple and is measured by the bootstrap time. Revisit if bootstrap time becomes a problem.

## Consequences
- A swap costs one pointer write, and readers never block.
- Memory per open instance is small but not zero (table readers, filters). With P partitions × 2 versions per node, that's fine at our scale.
- Restart time = download + ingest + replay. Ingest is fast (hard links), and replay is bounded by the update rate since the watermark.
