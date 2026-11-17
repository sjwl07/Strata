# Your nightly snapshot is silently undoing your real-time updates

*Draft blog post based on Strata's Q2 experiment. Check the outside-activities/IP clause before publishing.*

Many systems serve **derived data**: recommendations, ML features, precomputed aggregates. The usual architecture has two inputs. A big batch job recomputes everything periodically and ships a fresh snapshot to the serving store. A stream of real-time updates patches values in between. Readers expect the newest value.

The trap is in the gap between the two.

## The bug
A snapshot isn't "today's data". It's the world **as of a cutoff**, say midnight. Building and shipping it takes hours. Meanwhile the old snapshot keeps serving and absorbing stream updates. At 03:00 Ravi changes his phone number from 111 to 222, the update is applied, and readers see 222. At 09:00 the new snapshot arrives. It was photographed at midnight, so it says 111. Swap it in, and **every update between midnight and 09:00 silently disappears**. Nothing errors. The data is just wrong until the next update for that key.

Two more traps:
- **Late events.** An update from 23:00 delivered at 10:00 overwrites a newer value if you apply updates in arrival order.
- **Deletes.** If a key was deleted before the cutoff, it's simply *absent* from the snapshot. A late pre-cutoff write then brings it back, because there's no record to compare its timestamp against.

## The fix (three rules)
1. **Total-order last-writer-wins.** Every value carries (event time, source id). The bigger one wins, with deterministic tie-breaks, so replicas converge whatever order they receive updates in, and duplicates are no-ops.
2. **The watermark filter.** A snapshot with cutoff W reflects every update with event time ≤ W, including deletes. So drop any update with ts ≤ W for that version. That's what kills resurrection.
3. **Replay before swap.** Before the new version serves, replay the log from `offsetForTime(W − max clock skew)` into it, then apply live updates to both versions until the pointer flips. Any update newer than W was produced no earlier than W − skew, so this range is guaranteed to cover them.

## Does it matter? Numbers
I built a randomized checker that throws everything at once: late events (10%, up to 3 s), producer clock skew (5%), duplicate deliveries, deletes, snapshot swaps with a random cutoff-to-swap gap, and replicas crashing and rebuilding. An oracle knows the newest value the system could possibly know, and every read is compared against it. Two real replicas (RocksDB, a real push job), 50 histories, about 500k reads per strategy:

| Strategy | Wrong reads |
|---|---|
| Apply in arrival order, swap blindly | **19%** |
| Last-writer-wins only | **8.5%** |
| LWW + watermark filter + replay-before-swap | **0** |
| …the same, but with the clock-skew bound set wrong | 0.05% |

LWW alone halves the damage but can't fix the swap or the resurrections. The swap errors grow with the cutoff-to-swap gap (1.9% at 250 ms, 11.8% at 2 s), and in production that gap is hours. The last row is the scariest: correct code with a wrong operational assumption serves stale data rarely enough to pass casual testing.

The cost: LWW turns every stream write into a read-modify-write, about 4× slower to apply in my measurements (1.7M → 430k updates/s per node). That's still far above most nearline update rates. Replay costs about 3 µs per update since the cutoff, and it happens before the swap while the old version keeps serving.

## Takeaways
- If you merge batch snapshots with streams, write down what "correct" means. Mine: the newest update the system could possibly know about.
- Snapshot cutoffs must travel with the data (the watermark), and the serving layer must use them.
- Treat the clock-skew bound and log retention (≥ cutoff-to-swap gap + skew) as correctness parameters.
- Build a checker that *also* proves your weaker modes fail. A green run is only meaningful if the checker can see red.

*Code, method and raw data: Strata, `docs/experiments/q2-merge-correctness.md`.*
