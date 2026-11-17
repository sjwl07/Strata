# Phase 0: map benchmark (JMH)

`bench/src/jmh/java/io/strata/bench/MapBenchmark.java`: 1M preloaded `Integer` keys, 95% `get` / 5% `put`, uniform random keys.
Run: `./gradlew :bench:jmh -Pjmh.includes=MapBenchmark -Pjmh.threads=N`.
Machine: 8-core laptop, JDK 21. Short runs (2×2 s warmup, 3×2 s measurement, 1 fork), so the error bars are wide. Read these for the *shape*, not exact values.

| Threads | synchronized HashMap | Collections.synchronizedMap | ConcurrentHashMap | HashMap + ReentrantReadWriteLock |
|---|---|---|---|---|
| 1 | 8.96 ops/µs | 8.93 | 9.26 | 6.74 |
| 4 | 5.13 | 5.66 | 19.4 | 2.42 |
| 8 | 4.49 | 3.38 | 21.6 | 2.27 |

## Why
- **One global lock gets slower as threads are added.** With `synchronized`, every get needs the same monitor. More threads don't add throughput; they add contention. The lock word's cache line bounces between cores, and threads park and unpark. Total throughput *falls* from 9 to about 4.5 ops/µs.
- **ConcurrentHashMap scales** because `get` takes no lock at all: it reads a `volatile` table slot. Writes lock only one bin, so 5% writes rarely collide. Throughput more than doubles at 4–8 threads. It's limited by memory bandwidth and cache misses on a 1M-entry table, not by locking.
- **The read-write lock disappoints** even though 95% of operations are reads. Every `readLock().lock()` still does a CAS on the lock's shared state counter, so all readers write to the same cache line. The critical section (one hash lookup) is so short that this bookkeeping dominates. RW locks pay off only when the protected read is long.
- **Dead-code elimination:** if a benchmark computes a value and throws it away, the JIT may delete the computation and you measure nothing. Returning the value from the `@Benchmark` method (or passing it to `Blackhole.consume`) makes JMH "use" it, so the read can't be removed.

**Lesson for Strata:** the read path must avoid shared mutable state that every request writes to. That's why the storage node's version pointer is an `AtomicReference` read with a plain volatile load, and why per-version reference counts are the only shared write on the hot path (see Phase 1).
