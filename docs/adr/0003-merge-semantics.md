# ADR 0003: Merge semantics: watermark filter + last-writer-wins with a total order

- Status: accepted
- Date: 2026-11-17

## Context
Every key gets values from two sources: periodic batch snapshots (complete but stale on arrival) and a stream of updates (fresh but can arrive late, be duplicated, or be future-dated by producer clock skew). Replicas consume the stream independently and must converge. We need a precise definition of "correct" so the checker can verify it.

## Decision
1. **Every stored value carries `(ts, sourceId)`** and tombstones are stored, not deleted (`StoredValue`). Batch records get `ts = watermark` and `sourceId = BATCH (MAX_INT)`.
2. **Total order:** `ts`, then `sourceId`, then tombstone > live, then value bytes. An incoming update is written only if it's strictly greater than what's stored (LWW). The order is total, so the merge is commutative, associative and idempotent: replicas converge whatever the delivery order, and duplicates are no-ops.
3. **Watermark filter:** an update with `ts <= watermark` of the version it's applied to is dropped. A snapshot with cutoff W already reflects every update with event time ≤ W, *including deletes*. A key absent from the snapshot is an implicit tombstone at W, which per-record LWW alone can't see (there's no record to compare against). Without this filter, a late pre-cutoff write resurrects deleted keys (the checker's `RESURRECTED` anomaly in `LWW` mode).
4. **Definition of correct** (`checker/Oracle`): with version cutoff W served and the log consumed to now, the value of k is the LWW-max over {all events on k with ts ≤ W} ∪ {delivered events on k with ts > W}. That's the newest update the system could possibly know about.

Three `MergeMode`s exist so Q2 can measure what each piece buys: `NAIVE` (blind apply, no replay), `LWW` (2 only), `FULL` (2 + 3 + replay, ADR 0004).

## Alternatives considered
- **Arrival order (NAIVE):** simplest, but late events and retries regress values, and replicas that replay different ranges diverge.
- **Version vectors / CRDT registers:** handle concurrent writers without clocks, but derived data has one logical writer per key and event time is meaningful to the business. LWW on event time is the natural rule and is what Venice-style systems use.
- **Per-field merge:** finer-grained (update one feature of a row without overwriting others). Kept as a stretch goal: it's the same rule applied per field.

## Consequences
- Every stream apply is a read-modify-write (`get` then maybe `put`). Q2 measures the cost: LWW/FULL apply is slower than blind writes.
- Tombstones occupy space until the next version, which drops them (they're not in the snapshot).
- Correctness depends on event timestamps meaning something. A producer whose clock is far ahead would win everything until real time catches up. Bounded skew is an operational assumption (and is also needed by ADR 0004).
