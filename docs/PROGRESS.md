# Strata: progress

> Update this file at the end of every working session. It's the first thing to read after a break. For the step-by-step story with reasons, read `JOURNAL.md`.

## Current status (2026-11-17)
- **Phase:** all six phases built; **v0.1.0** tagged locally (not pushed).
- **State:** `./gradlew build` is green. All three research questions have write-ups with data. There's a one-command demo.
- **To do (yours):** read `JOURNAL.md` → `design.md` → the ADRs → the experiment write-ups; push (`git push origin main --tags`); check the offer-letter IP clause before publicising.

## Phase checklist
- [x] **Phase 0** – Java ramp-up: in-memory gRPC node, JMH map benchmark, concept notes
- [x] **Phase 1** – Versioned RocksDB store, SST push job, atomic ref-counted swap, baseline + swap under load
- [x] **Phase 2** – LWW + watermark + replay merge, correctness checker (CI gate), Kafka adapter, **Q2**
- [x] **Phase 3** – Delta pushes (cached base files + delta layer), **Q1**
- [x] **Phase 4** – Controller (heartbeats, HRW placement, push gate), cluster agent, local cluster, failover test
- [x] **Phase 5** – Smart client (C3, budgeted hedging, cancellation, failover), fault injection, **Q3**
- [x] **Phase 6** – Read-path profiling and fix, demo, admin CLI, multi-process smoke test, CI, docs, client publishing

## Deviations from `PLAN.md` (and why)
| Plan | What was built | Why |
|---|---|---|
| Kafka, MinIO and Spark in Docker | Interfaces + local implementations; real Kafka adapter tested via Testcontainers in CI | No Docker daemon on the dev machine (ADR 0001) |
| Spark push job | Plain-Java push job doing the same steps | Spark 3.5 doesn't support Java 21; too heavy for the disk |
| 50–100M keys | 0.1–1M keys | ~8 GB free disk, ~2 GB free RAM |
| jqwik property tests | Seeded random history generator + oracle (same idea, reproducible by seed) | Fewer dependencies; failing seeds are printed |
| toxiproxy network faults | Server-side `ServeHook` faults (slow disk, pauses, noisy neighbours) | No Docker; server-side faults also create real queueing |
| async-profiler flame graphs | JMH with the GC and stack profilers on the read path | Not installed; JMH profilers gave the actionable result |
| Separate processes for the chaos test | In-JVM `LocalCluster` with abrupt kill + disk loss; one manual multi-process smoke test | Same failure semantics, deterministic in CI |

## Ideas for later (stretch)
- Controller HA (etcd/jetcd lease) and persisted store specs and versions.
- C3 rate control (per-server sending-rate limits), and jitter or score thresholds for hedges (Q3 found hedge bursts after pauses).
- A RocksDB merge operator for the LWW comparison, to recover the 4× apply cost (Q2).
- Keeping per-key hashes from the previous push so deltas don't re-read the old snapshot (Q1 crossover).
- Running the push job on Spark 4 (Java 21), and Iceberg input.
- Per-field merge; read-your-writes tokens; a Feast online-store adapter.
- Run Q1–Q3 on real multi-machine hardware with Kafka.

## Open questions
- Check the LinkedIn offer letter's outside-activities/IP clause before publicising the project.

## Log
### 2026-10-24 / 25
- Explored the problem space and picked the project (`BACKGROUND.md`), named it Strata, set up the skeleton, wrote the plan docs. First commit `732a5a4`.

### 2026-11-14
- You resumed and drafted `StorageNodeService.get`. Reviewed at hint level.

### 2026-11-17
- You switched the working mode: Claude builds everything and records every step in `JOURNAL.md`.
- Built Phases 0–6, one commit per phase, tagged v0.1.0.
- Bugs found and fixed along the way (details in the journal):
  - a RocksDB instance leak that filled the disk;
  - a cleanup race;
  - cache pruning keeping old chains;
  - a flawed apply-throughput measurement;
  - a GC flag conflict;
  - a demo race.
- Housekeeping on your machine: deleted the DIP Claude session folders at your request (about 2.4 GB). Gradle caches about 0.9 GB in `~/.gradle`.
