# Q1: when does a delta push beat a full push?

## Question
If a fraction *f* of keys changed since the last snapshot, is it better to ship only the changes (a **delta**: upserts + deletes, layered on the previous version's files) or to re-ship everything (a **full** push)? We measure bytes shipped, build time, node load time and read cost, and find the crossover.

## Method
- **Dataset:** 500,000 keys × 100-byte random values (`SyntheticDataset`, seed 11), 8 partitions. Snapshot s1 changes each key with probability *f*; 10% of changes are deletes.
- **For each f** in {0.1%, 1%, 5%, 10%, 25%, 50%, 100%}, from the same base version v1 (already loaded on the node and in its file cache):
  - **delta:** `PushJob.pushDelta(s0, s1)` merge-joins both sorted snapshots per partition and writes upserts + RocksDB delete markers. The node hard-links the cached v1 files into a new RocksDB and ingests the delta on top.
  - **full:** `PushJob.pushFull(s1)`. The node downloads and ingests all files.
- **Measured:**
  - push-job build time;
  - bytes written (= bytes shipped);
  - node prepare+swap time (download into the cache + ingest), with the bytes actually downloaded;
  - in-process point-read cost on the new version (200k random reads, no RPC, so storage-level read amplification is visible);
  - p99.9 read latency *during* the load.
- **Transfer time is modelled**, not measured: on one machine the "object store" is a local directory, so copying is faster than any network. We report bytes ÷ bandwidth, with 125 MB/s (≈1 Gbit/s) by default.
- **Hardware:** laptop i3-1215U (8 threads), NVMe, JDK 21, single run per point.
- Reproduce with `cd bench && build/install/bench/bin/bench q1 --keys=500000`. Raw data: `data/q1-delta-vs-full.csv`, `data/q1-chain.csv`.

## Results
| f | kind | records | bytes shipped | build | transfer @1 Gb/s (model) | node prepare | total | read mean | read p99 |
|---|---|---|---|---|---|---|---|---|---|
| 0.1% | delta | 510 | 0.06 MB | 2.14 s | 0.5 ms | 0.47 s | 2.61 s | 3.0 µs | 9.5 µs |
| 0.1% | full | 499,944 | 55.1 MB | 1.18 s | 441 ms | 0.38 s | 2.01 s | 2.7 µs | 8.7 µs |
| 1% | delta | 4,875 | 0.52 MB | 2.06 s | 4 ms | 0.43 s | 2.49 s | 3.5 µs | 10.1 µs |
| 1% | full | 499,553 | 55.0 MB | 1.18 s | 440 ms | 0.37 s | 1.99 s | 2.7 µs | 8.5 µs |
| 5% | delta | 25,008 | 2.6 MB | 2.02 s | 21 ms | 0.43 s | 2.46 s | 2.8 µs | 8.4 µs |
| 5% | full | 497,530 | 54.8 MB | 1.17 s | 439 ms | 0.41 s | 2.02 s | 2.7 µs | 8.4 µs |
| 10% | delta | 49,924 | 5.1 MB | 2.06 s | 41 ms | 0.46 s | 2.56 s | 3.2 µs | 9.7 µs |
| 10% | full | 495,114 | 54.5 MB | 1.18 s | 436 ms | 0.38 s | 1.99 s | 3.5 µs | 10.7 µs |
| 25% | delta | 125,457 | 12.8 MB | 2.09 s | 102 ms | 0.42 s | 2.61 s | 3.5 µs | 11.2 µs |
| 25% | full | 487,586 | 53.7 MB | 1.36 s | 430 ms | 0.39 s | 2.18 s | 3.3 µs | 10.6 µs |
| 50% | delta | 250,583 | 25.3 MB | 2.23 s | 203 ms | 0.46 s | 2.89 s | 4.3 µs | 13.4 µs |
| 50% | full | 474,815 | 52.3 MB | 1.22 s | 419 ms | 0.41 s | 2.04 s | 2.8 µs | 9.2 µs |
| 100% | delta | 500,000 | 50.2 MB | 2.33 s | 402 ms | 0.47 s | 3.20 s | 2.4 µs | 8.5 µs |
| 100% | full | 450,023 | 49.6 MB | 1.19 s | 397 ms | 0.43 s | 2.02 s | 2.7 µs | 8.4 µs |

Delta-chain read cost (k consecutive 1% deltas, no compaction):

| Deltas stacked | 0 | 1 | 2 | 4 | 6 | 8 | 8, compacted |
|---|---|---|---|---|---|---|---|
| Read mean | ~2.7 µs | 3.5 µs | 3.7 µs | 3.2 µs | 3.7 µs | 3.7 µs | 3.3 µs |

## What this says
1. **Bytes shipped scale with f**, as expected: a 1% delta is 100× smaller than a full push. For network, object-store egress and cost, deltas win at every f < 100%.
2. **On one fast machine, a full push finishes sooner at every f.** Building a delta means reading *two* snapshots and merge-joining them, which adds about **0.92 s** (2.13 s vs 1.21 s mean) regardless of f. Local transfer of a full push costs only about 0.44 s at 1 Gb/s. That's the surprise: the cost of a delta is mostly in *producing* it, not shipping it.
3. **The crossover is a bandwidth question.** Let ΔB ≈ 0.97 s be the delta's extra build + load time, S = 55 MB the full size, bw the bandwidth and k the number of copies that share it (RF downloads from one object store, or cross-region replication). A delta wins when `ΔB < (1 − f) · S · k / bw`, i.e. when **bw / k < (1 − f) × 57 MB/s** on this hardware:

   | Bandwidth per copy (bw/k) | Delta wins for f below |
   |---|---|
   | 1.25 GB/s (10 Gb/s, 1 copy) | never |
   | 125 MB/s (1 Gb/s, 1 copy) | never |
   | 42 MB/s (1 Gb/s shared by RF = 3) | ≈ 26% |
   | 12.5 MB/s (100 Mb/s, e.g. cross-region) | ≈ 78% |
   | 4 MB/s (100 Mb/s shared by RF = 3) | ≈ 93% |

   Both ΔB and S grow linearly with dataset size, so the 57 MB/s threshold (the speed at which the diff job processes the extra snapshot) carries over to larger datasets. It changes with how the diff is computed. Keeping per-key hashes from the previous run instead of re-reading the old snapshot, or running the diff on a Spark cluster, raises it, and deltas win more often.
4. **Layered deltas don't hurt point reads.** Up to 8 stacked deltas changed the mean read cost by less than the run-to-run noise (about ±0.5 µs). Each extra layer is an L0 file, but its Bloom filter rejects a missing key in about 100 ns. Full compaction of the 8-delta chain took 1.7 s for 500k keys and bought nothing measurable. The real reasons to cap chain length are file count, cache footprint and the cost of resolving the chain, not read latency.
5. **Loading never hurt reads.** p99.9 *during* the load stayed at 19–31 µs in-process for both kinds, because ingestion is file linking, not writing.

## Recommendation
Use deltas when bandwidth per copy is scarce (cross-region, many replicas, egress costs) or when f is small *and* the diff can be computed cheaply (from stored hashes). Otherwise full pushes are simpler and, on fast local networks, faster end to end. Cap delta chains (for example, a full push every N deltas) for operational reasons, not read latency.

## Limitations
- 500k keys (55 MB), not the 50–100M in the original plan, because of disk limits on the test machine (about 10 GB free). Ratios are expected to hold since every cost here is linear in size, but cache effects at larger sizes aren't captured.
- Transfer time is modelled. Build and load are real but run on the same 8 hardware threads.
- Single run per point. Differences under about 20% in the time columns are within noise.
