# Q3: how much do adaptive replica selection and budgeted hedging cut tail latency?

## Question
With 3 replicas of every partition, one slow replica shouldn't make users wait. How much do (a) choosing replicas by observed load (C3) and (b) hedging slow requests to a second replica reduce p99/p99.9 under realistic slowness, and at what extra load?

## Method
- **Cluster** (`local-cluster`): controller + 6 storage nodes in one JVM, each with its own Netty gRPC server on loopback. Store: 12 partitions, RF 3, 100k keys × 100 B. **2 serving threads per node**, deliberately few, so a slow node really builds a queue the way a saturated production node does.
- **Load** (`OpenLoopLoad`): open-loop Zipfian (θ = 0.99) point reads at a fixed rate, latency measured from the *intended* send time (no coordinated omission). Each cell uses a fresh client: 3 s warm-up, then 15 s measured.
- **Faults** (`fault/Faults`, injected on the serving thread so queueing is real):

  | Fault | What it does |
  |---|---|
  | `slow-node` | node 1: 20% of reads take +10 ms (degraded disk) |
  | `gc-pauses` | node 1 freezes 100 ms every second (stop-the-world GC) |
  | `noisy-neighbours` | nodes 1 and 2: random 300 ms episodes (mean gap 2 s) adding +10 ms to every read |
- **Strategies** (`client/ReplicaSelector`, `StrataClient`):

  | Strategy | How it picks |
  |---|---|
  | `round-robin` | rotate; blind to load |
  | `least-outstanding` | fewest of this client's requests in flight |
  | `c3` | C3 score `R − S + (1 + os·w + q)³·S`, from client-observed latency R, server-reported service time S and queue depth q (piggybacked on every response) |
  | `+hedge5%` | after the rolling p95 latency, send a backup to the next-ranked replica; a token bucket caps backups at 5% of requests (capacity 10); the first answer wins and the loser is cancelled (RST_STREAM, and the server skips cancelled work) |
  | `+hedge-unbounded` | same, budget 100% |
- **Hardware:** laptop i3-1215U (8 threads shared by client, servers and controller), 7 GB RAM, JDK 21. Single run per cell.
- Reproduce with `cd bench && build/install/bench/bin/bench q3 --rate=2000`. Raw data: `data/q3-*.csv`.

## Result 1: strategies × faults at 2,000 req/s (ms)
| Fault | Strategy | p50 | p99 | p99.9 | max | extra load |
|---|---|---|---|---|---|---|
| none | round-robin | 0.37 | 1.60 | 4.0 | 10.4 | 0% |
| none | least-outstanding | 0.36 | 0.66 | 2.8 | 4.0 | 0% |
| none | **c3** | 0.33 | **0.66** | 3.3 | 13.5 | 0% |
| none | round-robin + hedge 5% | 0.35 | 0.67 | 2.7 | 3.9 | 0.1% |
| slow-node | round-robin | 0.36 | 10.49 | 14.7 | 23.0 | 0% |
| slow-node | least-outstanding | 0.36 | 10.47 | 10.7 | 13.1 | 0% |
| slow-node | **c3** | 0.33 | **0.63** | **2.8** | 4.3 | 0% |
| slow-node | round-robin + hedge 5% | 0.35 | 1.62 | 2.9 | 13.4 | 3.4% |
| slow-node | c3 + hedge 5% | 0.32 | 0.65 | 3.4 | 11.8 | 0.2% |
| slow-node | round-robin + hedge unbounded | 0.35 | 1.58 | 3.3 | 9.3 | 3.6% |
| gc-pauses | round-robin | 0.34 | 24.05 | 93.1 | 100.9 | 0% |
| gc-pauses | least-outstanding | 0.35 | 0.67 | 3.1 | 100.9 | 0% |
| gc-pauses | **c3** | 0.33 | **0.63** | **2.6** | **3.9** | 0% |
| gc-pauses | round-robin + hedge 5% | 0.36 | 1.53 | 28.3 | 78.8 | 1.0% |
| gc-pauses | c3 + hedge 5% | 0.30 | 1.51 | 8.3 | 19.5 | 0.9% |
| gc-pauses | round-robin + hedge unbounded | 0.35 | 1.74 | 5.3 | 10.7 | 2.2% |
| noisy-neighbours | round-robin | 0.35 | 35.85 | 103.6 | 129.0 | 0% |
| noisy-neighbours | least-outstanding | 0.36 | 10.42 | 10.6 | 13.2 | 0% |
| noisy-neighbours | **c3** | 0.33 | **0.57** | **2.6** | 13.6 | 0% |
| noisy-neighbours | round-robin + hedge 5% | 0.36 | 39.19 | 102.0 | 124.1 | 1.4% |
| noisy-neighbours | c3 + hedge 5% | 0.31 | 0.54 | 2.7 | 4.5 | 0.1% |
| noisy-neighbours | round-robin + hedge unbounded | 0.35 | 1.70 | 4.0 | 18.6 | 3.0% |

No cell had any errors.

## Result 2: at 5,000 req/s (near saturation of the slow node), ms
| Fault | Strategy | p99 | p99.9 | extra load |
|---|---|---|---|---|
| slow-node | round-robin | 18.6 | 41.4 | 0% |
| slow-node | c3 | **0.59** | **2.0** | 0% |
| slow-node | round-robin + hedge 5% | 4.07 | 16.0 | 4.3% |
| slow-node | round-robin + hedge unbounded | 2.17 | 8.5 | 5.3% |
| noisy-neighbours | round-robin | 200.7 | 291.5 | 0% |
| noisy-neighbours | c3 | 0.70 | 6.0 | 0% |
| noisy-neighbours | c3 + hedge 5% | **0.51** | **2.0** | 0.1% |
| noisy-neighbours | round-robin + hedge 5% | 73.3 | 182.2 | 1.1% |
| noisy-neighbours | round-robin + hedge unbounded | 1.53 | 12.8 | 5.1% |

## Result 3: the JVM's own GC (no injected faults, 5,000 req/s, 20 s), ms
| GC | Strategy | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| G1 | round-robin | 0.21 | 7.07 | 12.6 | 23.7 |
| ZGC | round-robin | 0.20 | 5.49 | 11.8 | 26.6 |
| G1 | c3 | 0.18 | 0.62 | 2.3 | 6.8 |
| ZGC | c3 | 0.17 | 0.53 | 1.6 | 7.7 |

## What this says
1. **Load-aware replica selection is the big win.** C3 kept p99 at about 0.6 ms and p99.9 at 2–3 ms under every fault, a **17× (slow node), 38× (GC pauses) and 63× (noisy neighbours) lower p99 than round-robin**, at zero extra load. It works because the servers report queue depth and service time with every response, and the cubic queue term moves traffic away *before* a queue forms.
2. **Least-outstanding is a weak signal at low concurrency.** With a handful of requests in flight per client, "outstanding" is usually 0 or 1 everywhere, so it can't tell a slow node from a fast one (p99 10.5 ms under `slow-node`, the same as round-robin). It did dodge GC pauses (requests pile up in flight during a freeze), except for the one request that lands just as the pause starts (max 100.9 ms).
3. **Hedging rescues load-blind routing, if the budget allows it when it matters.** Round-robin + hedging cut the slow-node p99 from 10.5 to 1.6 ms for 3.4% extra requests. But **a token bucket sized for the average fails for bursty faults**: under noisy-neighbour episodes the 5% budget (capacity 10 tokens) was used up within each 300 ms episode, so p99 stayed at 39 ms while only 1.4% extra load was used *on average*. The unbounded variant fixed it for 3% extra load. The lesson is that bucket *capacity* (burst size) matters as much as the rate.
4. **Hedging can make a good router slightly worse.** On top of C3 under GC pauses, hedging raised p99 from 0.63 to 1.5 ms and p99.9 from 2.6 to 8.3 ms. When a node freezes, all pending requests to it cross the hedge delay at once and their backups arrive together at the next-ranked replica, a burst of duplicates onto a node with 2 serving threads. C3 alone had already moved traffic away. Hedging is a backstop for routers that *can't* see load, not something to stack blindly.
5. **Unbounded hedging didn't melt the cluster here, because the delay already rate-limits it.** With the delay set at the rolling p95, at most about 5% of requests are still pending when it fires, so even the "unbounded" variant stayed at ≤5.3% extra load at 5,000 req/s. The budget is a safety net for when that assumption breaks: a fixed small delay, or a cluster-wide slowdown where p95 lags behind reality.
6. **GC choice matters less than routing.** Switching the JVM from G1 to ZGC trimmed p99.9 by about 30% (2.3 → 1.6 ms with C3), but routing changed p99 by more than 10×. Everything here runs in one JVM, so GC pauses hit client and servers together; a per-process comparison would be cleaner.

## Limitations
- One JVM and one machine: client, controller and 6 servers share 8 hardware threads, and "network" delay is loopback. The absolute numbers are small. The *relative* differences are the result.
- Faults are injected server-side as sleeps on the serving thread. That models slow I/O and stop-the-world pauses well, and CPU contention less well.
- C3's second half (client-side rate control and backpressure per server) isn't implemented. Only replica ranking is.
- A single client with a single replica-stats table. With many independent clients, C3's concurrency weight `w` matters (the paper sets it to the number of clients). Not explored here.
- Single run per cell. The p99.9 and max columns are noisy (one 15 s run at 2,000 req/s has only 30 samples above p99.9).
