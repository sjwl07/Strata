# Q2: how often does a naive batch/stream merge serve wrong data, and what does correctness cost?

## Question
A serving store gets each key from a periodic batch snapshot *and* a stream of updates. Late events, duplicate deliveries, producer clock skew and version swaps all interact. How often does each merge strategy serve a wrong value, which mechanism fixes which anomaly, and what does the correct strategy cost?

## Method
- **Strategies** (`MergeMode`, ADR 0003/0004):
  - `NAIVE`: apply updates in arrival order, swap without replay;
  - `LWW`: last-writer-wins on (ts, source), no watermark filter, no replay;
  - `FULL`: LWW + drop updates at or below the version's watermark + replay from (watermark − skew) before swap, then dual-write;
  - `FULL-noskew`: FULL with its clock-skew bound set to 0 while the workload *does* contain skewed producers. This is the "right code, wrong config" case.
- **Workload** (`checker/HistoryGenerator`, seeded, reproducible): 24 keys (high contention), 1,500 updates per history about 10 ms apart, 2 partitions. In each history:

  | Mess | Share | Size |
  |---|---|---|
  | Late events | 10% | up to 3 s late |
  | Future-dated events (producer clock skew) | 5% | up to 200 ms ahead |
  | Duplicate deliveries | 3% | |
  | Deletes | 10% | |

  On top of that: 3 producers, a push every ~2.5 s whose cutoff is 0.3–2 s before it arrives, a random 0–400 ms gap between prepare and swap, and a 30% chance per push that replica B crashes and rebuilds from scratch.
- **Oracle:** with cutoff W served and the log consumed, the right value is the LWW-max over {all events with ts ≤ W} ∪ {delivered events with ts > W}, i.e. the newest update the system could know. Every 100 ms of history time, every key is read from **both replicas** (real RocksDB engines, real push job, in-memory log) and compared.
- **Anomaly types:**

  | Anomaly | Meaning |
  |---|---|
  | `STALE` | older value than the newest known |
  | `RESURRECTED` | value served for a deleted or absent key |
  | `LOST` | "not found" for a live key |
  | `REGRESSED` | a later read returned an older value than an earlier read |
  | `DIVERGED` | the replicas disagree at the end |
- Reproduce with `cd bench && build/install/bench/bin/bench q2 --histories=50`. Raw data: `data/q2-*.csv`, `data/q2-correctness.md`.
- **Hardware:** laptop i3-1215U (8 threads), 7 GB RAM, NVMe, JDK 21.

## Result 1: anomaly rates (50 histories, 501,456 checked reads per mode)
| Mode | Wrong reads | % wrong | STALE | RESURRECTED | LOST | REGRESSED | DIVERGED |
|---|---|---|---|---|---|---|---|
| **FULL** | **0** | **0.000%** | 0 | 0 | 0 | 0 | 0 |
| FULL-noskew | 244 | 0.049% | 178 | 8 | 58 | 178 | 0 |
| LWW | 42,543 | 8.48% | 33,921 | 4,141 | 4,481 | 36,712 | 25 |
| NAIVE | 95,396 | 19.02% | 77,289 | 8,425 | 9,682 | 76,899 | 53 |

What each mechanism buys:
- **LWW alone halves the error rate** compared with NAIVE: late and duplicate events can no longer overwrite newer values. It **can't** fix the swap (`STALE` = updates between cutoff and swap thrown away) or keys absent from the snapshot (`RESURRECTED`: a late pre-cutoff write brings a deleted key back, because there's no record to compare it against).
- **The watermark filter + replay remove everything that's left.** Zero wrong reads, zero regressions, and replicas that crashed and rebuilt converge exactly (`DIVERGED` = 0).
- **FULL-noskew is the cautionary row.** With future-dated producers but a skew bound of 0, the replay starts slightly too late and misses events. 0.05% of reads are wrong, rare enough to slip through casual testing. The skew bound is a correctness parameter, not a tuning knob.

## Result 2: the error rate grows with the cutoff→swap delay
Mean of 12 histories per point; the delay is the time between a snapshot's cutoff and its arrival:

| Max cutoff→swap delay | FULL | LWW | NAIVE |
|---|---|---|---|
| 250 ms | 0% | 1.9% | 13.1% |
| 1 s | 0% | 5.3% | 16.0% |
| 2 s | 0% | 11.8% | 21.3% |
| 4 s | 0% | 11.3% | 22.4% |

LWW's errors are almost entirely the swap losing updates newer than the cutoff, so they grow with the window between cutoff and swap. They plateau once the window exceeds the push interval, because each swap then discards about a full interval's worth of updates. In production the window is *hours* (a nightly Spark job), so without replay essentially every key that was updated during the day reverts at the swap.

## Result 3: the cost of correctness
| Measurement | NAIVE | LWW | FULL |
|---|---|---|---|
| Stream apply throughput (one node, 4 partitions, 200k-key base, 300k updates) | 1.73M updates/s | 427k/s | 432k/s |

- **LWW/FULL are about 4× slower to apply** than blind writes, because each update is a read-modify-write (a RocksDB `get` before the `put`). The watermark check itself is free.
- 430k updates/s per node is still far above typical nearline update rates. If it weren't, a RocksDB merge operator could do the comparison during compaction instead of on the write path (future work).

Replay cost when preparing a new version, by the number of updates newer than its cutoff:

| Updates since cutoff | FULL prepare (ingest + replay) | LWW prepare (no replay) |
|---|---|---|
| 0 | 269 ms | 258 ms |
| 10,000 | 270 ms | 267 ms |
| 100,000 | 413 ms | 221 ms |
| 300,000 | 1,115 ms | 188 ms |

Replay costs about 3 µs per update and grows linearly with the backlog since the cutoff. It runs *before* the swap while the old version keeps serving, so it delays the swap but never blocks reads.

## Limitations
- Small keyspace and short histories are deliberate (they maximise interactions per second of testing). The rates therefore describe how often bugs fire under heavy contention, not the stale-read rate you'd see in production.
- The in-memory log substitutes for Kafka (ADR 0001). The Kafka adapter is tested separately where Docker exists.
- The cluster-level version of this check (5 nodes, kills during push and stream) is `ClusterFailoverTest`, which also ends with zero wrong values.
