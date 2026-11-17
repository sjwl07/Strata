# ADR 0006: Client-side replica selection (C3) with budgeted, cancellable hedging

- Status: accepted
- Date: 2026-11-17

## Context
Reads go to one of RF replicas. Replicas get slow for reasons the controller doesn't see quickly enough (GC pauses, a degraded disk, noisy neighbours). The client sees every request's latency and is the only component that can react within milliseconds.

## Decision
- **Routing:** the client polls the controller's cluster view (200 ms) and routes each key to its partition's *ready* replicas, falling back to non-ready ones last.
- **Ranking (default C3):** score `R − S + (1 + os·w + q)³ · S` per replica, with R = EWMA latency seen by the client, S = EWMA service time and q = EWMA queue depth reported by the server in `ServerFeedback` on every response, and os = this client's in-flight requests. Round-robin, random and least-outstanding are kept for comparison.
- **Hedging:** if a request is still pending after the rolling p95 latency (floor 1 ms), send a backup to the next-ranked replica, only if a **token bucket** allows it. Each primary deposits `budget` tokens (default 0.05) and each hedge costs one. The first success wins and the loser is **cancelled** (gRPC cancellation → RST_STREAM). The server checks `Context.isCancelled()` before doing the read, so cancelled work is skipped if it was still queued.
- **Failover:** `UNAVAILABLE`/`DEADLINE_EXCEEDED`/connection errors move on to the next replica immediately (outside the hedge budget), and the failed replica's EWMA is penalised.
- **`batchGet`** is scatter-gather: keys are grouped by partition, and each group is one hedged, failover-capable request, sent in parallel.
- User-visible futures complete on the client's own executor, never on gRPC transport threads.

## Alternatives considered
- **Server-side load balancer / proxy:** an extra hop and an extra failure domain, and it still can't see per-client latency. Client-side routing is what Cassandra drivers, Venice's router-less client and the C3 paper all use.
- **Power-of-two-choices on outstanding requests:** simple and good under high concurrency, but at low per-client concurrency the outstanding counts carry no information (Q3: least-outstanding was as bad as round-robin under a slow node).
- **Unbudgeted hedging:** fine when the hedge delay is a high percentile (it self-limits), dangerous when the delay is fixed or the whole cluster slows (doubling the load at the worst moment). The budget costs nothing when not needed.

## Consequences
- Q3: C3 cut p99 by 17–63× under injected faults at zero extra load. Hedging fixed load-blind routing for ~3% extra load.
- **The bucket needs burst capacity for bursty faults.** With 10 tokens, noisy-neighbour episodes drained it. Size it as `rate × episode length × budget`, or use a sliding-window budget.
- Hedging on top of C3 can hurt a little (correlated backup bursts after a pause). A future refinement would be to hedge only when the best alternative's score is clearly better, or to add jitter to hedge timers.
