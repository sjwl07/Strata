# ADR 0004: Replay from (watermark − skew), then dual-write until the swap

- Status: accepted
- Date: 2026-11-17

## Context
Version N+1 is a snapshot with cutoff W, but it arrives hours (here: seconds) after W. Meanwhile version N has been serving and absorbing stream updates. Swapping naively throws away every update with ts > W, the "Ravi" problem in `CONCEPTS.md`. We must make N+1 include those updates before it serves, without stopping reads or the stream.

## Decision
On the partition's single writer thread (`PartitionReplica.install`):
1. **Replay** log records from `offsetForTime(W − maxClockSkew)` up to the current consume position into the new version only. The watermark filter drops the ones it already contains.
2. From then on, **dual-write**: each live update is applied to both the current and the new version.
3. **Swap** whenever the controller (or the standalone loader) says so. The new version is already caught up, so the swap is just a pointer flip.

**Why `W − skew` is a safe starting point:** an update with event time `ts > W` was produced at wall time `≥ ts − skew` (the producer clock can be at most `skew` ahead), so the broker appended it at `≥ W − skew`. Every update the snapshot may lack is therefore at or after `offsetForTime(W − skew)`. This relies on Kafka `LogAppendTime`, which is monotonic within a partition.

The same procedure bootstraps a fresh replica (no current version): replay from `W − skew` to the end of the log, then consume live.

## Alternatives considered
- **Replay at swap time** (pause, replay, swap): the swap's latency would include the replay, and reads in that window would be served by a version that's about to vanish. Replaying early and dual-writing makes the swap instant.
- **Replay from the beginning of retention:** correct but wasteful. Cost grows with retention instead of with the cutoff-to-swap delay.
- **Replay from the offset recorded at the cutoff** (have the push job record log offsets at W): it needs the batch pipeline to coordinate with Kafka. That's possible, but event time ≠ append time, so late events still require a skew/lateness margin. The time-based lookup is self-contained.

## Consequences
- Requires log retention ≥ (cutoff-to-swap delay + skew). With Kafka, set `retention.ms` accordingly.
- **If the skew bound is wrong, correctness breaks quietly.** The Q2 experiment includes `FULL-noskew` (bound set to 0 while 5% of events are future-dated by up to 200 ms) to show this.
- During dual-write, applying an update costs two read-modify-writes. Q2 measures replay time as a function of the backlog since the cutoff.
