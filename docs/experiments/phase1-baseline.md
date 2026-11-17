# Phase 1: single-node read baseline and swap under load

**Setup.** One storage node and one store with 8 partitions; 1,000,000 keys × 100-byte random values (about 120 MB of SST). Reads go through real gRPC over loopback (Netty, plaintext), driven by an **open-loop** generator that sends at a fixed rate and measures latency from each request's *intended* start time (no coordinated omission). Keys are either uniform or Zipfian (θ = 0.99, scrambled). The node uses 8 serving threads, a 64 MB shared block cache and no WAL.

**Hardware.** Laptop, Intel i3-1215U (2 performance + 4 efficiency cores, 8 threads), 7 GB RAM, NVMe SSD, Linux 7.0, JDK 21, G1, `-Xmx1g`. **The load generator, gRPC client and server share the same 8 hardware threads**, so the high-rate rows measure the whole machine saturating, not the server alone.

Reproduce: `./gradlew :bench:installDist && cd bench && build/install/bench/bin/bench phase1 --keys=1000000 --rates=2000,10000,20000,40000 --seconds=15`. Raw data: `data/phase1-baseline.csv`, `data/phase1-swap-timeline.csv`.

## Build and load times
| Step | Time |
|---|---|
| Push job: generate, partition, sort and write 8 SSTs (1M keys) | 3.4 s |
| Node: ingest v1 (hard-link 8 SSTs into 8 fresh RocksDB instances) | 0.17 s |
| Node: prepare v2 under load (ingest, ready to swap) | 0.16 s |

Ingestion is nearly free because `ingestExternalFile` links the pre-built sorted files into the LSM tree, with no re-writing and no compaction. This is the whole reason read-only serving stores are built from offline SSTs (as Voldemort's read-only stores were, FAST '12).

## Read latency vs request rate
| Rate (req/s) | Keys | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| 2,000 | uniform | 0.29 ms | 0.50 ms | 2.1 ms | 16.8 ms |
| 2,000 | zipfian | 0.29 ms | 0.49 ms | 1.3 ms | 12.1 ms |
| 10,000 | uniform | 0.12 ms | 0.32 ms | 11.4 ms | 16.9 ms |
| 10,000 | zipfian | 0.12 ms | 0.43 ms | 7.9 ms | 11.6 ms |
| 20,000 | uniform | 0.15 ms | 1.19 ms | 4.9 ms | 8.5 ms |
| 20,000 | zipfian | 0.16 ms | 1.04 ms | 2.2 ms | 5.1 ms |
| 40,000 | uniform | 1.23 ms | 11.6 ms | 19.5 ms | 24.7 ms |
| 40,000 | zipfian | 0.99 ms | 19.5 ms | 34.8 ms | 45.2 ms |

Observations:
- **Median is about 0.15 ms up to 20k req/s.** That's mostly gRPC and loopback cost. A RocksDB point read from the block cache is a few microseconds.
- **p50 at 2k/s is *higher* than at 10k/s.** At low rates threads park between requests, and waking them costs tens of microseconds (CPU frequency scaling and the idle-state exit). A well-known effect, and why latency is reported at several rates.
- **The p99.9 at 10k is noisy (5–11 ms)** because of single events (GC pauses, laptop background work), as the per-second timeline shows. The tail is dominated by environment rather than the store, which is exactly the problem Phase 5 attacks on the client side.
- **At 40k/s the machine saturates** (client and server share the CPU), and queueing appears in p99.

## Swap under load (Zipfian, 5,000 req/s)
`prepare(v2)` started at t = 10 s, took 155 ms, and was followed by the swap. Per-second p99 (ms):

| t (s) | 0–4 | 5 | 6 | 7–9 | **10** | 11 | 12–18 |
|---|---|---|---|---|---|---|---|
| p99 | 0.37–0.42 | 5.7 | 3.0 | 0.38 | **0.76** | 0.45 | 0.38–0.47 |

The swap second shows a small p99 bump (0.38 → 0.76 ms) and zero errors. The bigger blip at t = 5–6 s happened *before* the swap and is unrelated environmental noise, a reminder to always look at the timeline, not one number. Readers never block on a swap: they follow an `AtomicReference` and pin the old version with a reference count until their read finishes (see `PartitionVersion`).

## Limitations
- 1M keys, not the 50M in the original plan: the machine has about 8 GB of free disk and 2 GB of free RAM. The dataset fits in the OS page cache, so this measures the CPU and RPC path, not disk.
- Single run per configuration, so treat differences under about 2× in p99.9 as noise.
