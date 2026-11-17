# Phase 0: concurrency concepts used in Strata

## Happens-before
The Java Memory Model doesn't promise that a write made by one thread is ever seen by another, *unless* the write happens-before the read. Edges that create happens-before:
- program order within one thread;
- unlocking a monitor → a later lock of the same monitor;
- a `volatile` write → a later `volatile` read of the same field;
- `Thread.start()` → everything in the started thread, and everything in a thread → another thread's `join()` returning;
- `java.util.concurrent` hand-offs (putting into a `BlockingQueue` → taking it out, completing a `CompletableFuture` → its dependents).

Happens-before is transitive. That's why "safe publication" works: build an object fully, then publish its reference through a `volatile` field or an `AtomicReference`, and readers that see the reference also see the fully built object. **Strata uses this for version swaps:** a new RocksDB version is fully ingested and replayed, and *then* its handle is set into an `AtomicReference`, so a reader that sees the new handle sees a ready database.

**Why `volatile` doesn't make `count++` safe:** `count++` is three steps (read, add, write). `volatile` makes each read and write visible, but it doesn't make the three steps atomic. Two threads can both read 5 and both write 6. Use `AtomicLong.incrementAndGet` (a CAS loop) or `LongAdder` (striped counters, best for hot statistics).

## Virtual threads
A virtual thread is a cheap thread scheduled by the JVM onto a few carrier OS threads. When it blocks on I/O the JVM *unmounts* it, and the carrier runs something else.
- **They help** with lots of concurrent tasks that mostly wait (network calls, sleeps, queues). Thread-per-request code stays simple and still scales to 100k concurrent requests.
- **They don't help** with CPU-bound work: there are still only N cores.
- **Pinning:** a virtual thread blocked inside `synchronized` (JDK 21) or inside a **native/JNI call** can't unmount, so it holds its carrier. RocksDB reads are JNI calls. Running them on virtual threads gives no benefit and can starve the carriers. **Strata therefore serves reads on a bounded platform-thread pool.** That's also what we want for the tail-latency experiments: a fixed pool gives a real queue whose depth the server reports to the client (C3).

## CompletableFuture
- `thenApply(f)`: transform the result with a plain function (`T -> U`).
- `thenCompose(f)`: chain another *asynchronous* step (`T -> CompletableFuture<U>`) without ending up with a nested `CompletableFuture<CompletableFuture<U>>`. It's like `flatMap`.
- **Where callbacks run:** non-`Async` callbacks run on whichever thread completes the future, or on the calling thread if it's already complete. A slow callback therefore runs on, for example, a gRPC Netty event-loop thread and stalls the network. The `...Async(fn, executor)` variants say exactly where. **The Strata client completes futures from gRPC callbacks, so user callbacks are moved to the client's executor.**
- `anyOf` returns when the first future completes. Hedging uses the same idea ("first response wins"), plus cancelling the loser.
