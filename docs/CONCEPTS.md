# Strata: the project explained

## In one sentence
Strata is a store that answers lookups in about a millisecond. Its data comes from two places, a big recomputation every night (**batch**) and a steady flow of small real-time updates (**stream**), and it combines the two without ever serving a wrong or outdated answer. A smart client keeps lookups fast even when some servers slow down.

## A concrete example
Take "People You May Know" (illustrative only):
- **Every night**, a Spark job computes fresh suggestions for every member. That's batch: huge and slow, but complete.
- **During the day**, you connect with someone, so they shouldn't be suggested anymore. These changes arrive as Kafka events. That's stream: small, fast, continuous.
- **When you open the app**, a service needs your suggestions within milliseconds. That's the serving store, which is Strata.

ML features work the same way. "Profile views in the last 7 days" is computed in bulk and then updated by live events.

## The analogy: a printed directory with sticky notes
| Analogy | In Strata |
|---|---|
| A new printed edition delivered regularly | Batch push (version N) |
| Sticky notes with corrections during the day | Stream updates from Kafka |
| Readers looking up names | Client `get` / `batchGet` requests |
| Desks, each holding part of the directory | Partitions on storage nodes |
| 3 identical copies of each section | Replicas (RF=3) |
| Head librarian with the floor map | Controller |
| A shopper who picks the fastest checkout line | Smart client |
| Referee who knows the right answer | Correctness checker |
| Saboteur who slows desks down on purpose | Fault injector |

## The three problems (= the three research questions)

### Q1: reprinting the whole book when two pages changed
If only 1% of entries changed, re-shipping the whole dataset is wasteful. Shipping only the changed entries (a **delta**) costs something too: you have to diff the old snapshot against the new one. **At what fraction changed does the delta stop being worth it?** We measure this.

### Q2: the new edition throws away today's sticky notes
An edition is a **snapshot as of a cutoff time**: everything known up to, say, 00:00. It isn't "one day's data". Building and delivering it takes hours. Meanwhile the **old edition is still serving**, and sticky notes are applied to it, so readers see them right away.

| Time | Event | Reader sees |
|---|---|---|
| Yesterday | Old edition on the desk; Ravi = 111 | 111 |
| 00:00 | Cutoff: new edition is "photographed" (Ravi still 111) | 111 |
| 03:00 | Note "Ravi → 222" is stuck on the **old** edition | 222 ✅ |
| 09:00 | New edition delivered; old book *and its notes* binned | **111 ❌** |

**The bug:** a naive swap silently undoes every update between the cutoff and the delivery.

**The fix:**
1. Label each edition with its cutoff (the **watermark**).
2. Keep every note. Kafka retains the log, so we can re-read from any offset.
3. Before swapping, **re-apply every note newer than the cutoff** onto the new edition.
4. Then swap.

**The second trap, late notes:** a note written at 23:00 yesterday arrives at 10:00 today. Applying it blindly overwrites a newer value. So every value and every note carries a **timestamp**, and **the newer timestamp wins** (last-writer-wins, with a deterministic tie-break).

**Why it's hard in practice:** crashes halfway through replay, duplicate deliveries, equal timestamps, a second edition arriving before the first finished swapping. The **checker** knows the right answer for every key. It generates thousands of messy histories like these and counts every stale read. We run it against both the correct merge and a deliberately **naive** merge, and the difference is the Q2 result.

### Q3: one slow librarian makes everyone wait
Each section has 3 copies. When one desk is slow (GC pause, slow disk, noisy neighbour), requests that land there take 50× longer. A page that needs answers from 20 desks is only as fast as the slowest one. That's **tail latency** (p99/p999).

The smart client behaves like a regular at a supermarket:
- **picks a line** by how fast each cashier has been lately, not just by queue length (**adaptive replica selection**, the C3 paper). Servers report their queue depth and service time in every response (`ServerFeedback` in the proto);
- **joins a second line** if the first is unusually slow, and takes whichever finishes first (**hedging**), but on a strict **budget** (e.g. at most 5% extra requests), otherwise everyone hedges and the whole system slows;
- **tells the abandoned cashier to stop** (**cancellation**), so no work is wasted.

The fault injector makes desks slow in realistic ways, and we measure how much each strategy helps.

## What Strata is not
- Not a Redis or Cassandra clone: no arbitrary user writes. It's specialized for serving derived data.
- Not a Venice clone: we build only what's needed to answer Q1–Q3. The write-ups with numbers are the actual output.
- Not based on LinkedIn internals: open-source components and public papers only.

## Glossary
- **Batch push / version:** a complete dataset built offline and loaded as a new version.
- **Watermark:** the cutoff time a version reflects.
- **Delta push:** shipping only what changed between two versions.
- **LWW:** last-writer-wins by timestamp.
- **Replay-on-swap:** re-applying stream updates newer than the watermark before switching versions.
- **SST file:** RocksDB's sorted, immutable on-disk file. Built offline and ingested directly.
- **RF:** replication factor, the number of copies of each partition.
- **p99 / p999:** the latency that 99% / 99.9% of requests beat.
- **Hedging:** sending a backup request to another replica when the first is slow.
- **C3:** an adaptive replica-selection algorithm (Suresh et al., NSDI '15).
