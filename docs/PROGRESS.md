# Strata: progress

> Update this file at the end of every working session. It's the first thing to read after a break.

## Current status
- **Phase:** 0 – Java ramp-up and skeleton
- **State:** skeleton done and building. The user is working on Exercise 1.
- **Next step:** Exercise 1, the in-memory gRPC storage node (`docs/exercises/phase0/README.md`).
- **Then:** Exercise 2 (JMH map benchmark), then Exercise 3 (concept note), then Phase 1.

## Phase checklist
- [ ] **Phase 0** – Java ramp-up and skeleton
  - [x] Gradle multi-module skeleton, version catalog, Spotless, CI workflow
  - [x] gRPC API (`common/src/main/proto/strata/v1/storage_node.proto`)
  - [x] Exercise brief and acceptance tests (`docs/exercises/phase0/`)
  - [x] Connected to GitHub remote (`origin` → `https://github.com/sjwl07/Strata.git`)
  - [x] First commit and push (`732a5a4`, "Phase0: Project skeleton")
  - [ ] Exercise 1 – in-memory gRPC storage node
  - [ ] Exercise 2 – JMH map benchmark + `docs/notes/phase0-jmh.md`
  - [ ] Exercise 3 – `docs/notes/phase0-concepts.md`
- [ ] **Phase 1** – Single-node versioned store (RocksDB, SST ingestion, atomic swap)
- [ ] **Phase 2** – Streaming merge and correctness checker
- [ ] **Phase 3** – Spark push job and delta pushes (Q1)
- [ ] **Phase 4** – Controller, partitions, replication
- [ ] **Phase 5** – Smart client and fault injection (Q3)
- [ ] **Phase 6** – Hardening, write-up, v0.1

## Open questions / decisions pending
- Check the LinkedIn offer letter's outside-activities/IP clause before publicising the project.

## Log
### 2026-10-24 / 25
- Explored the problem space and picked the project. See `BACKGROUND.md`.
- Chose the name **Strata**: batch base layer plus stream layers on top.
- Agreed working mode: the user writes the core logic, and Claude writes scaffolding and gives hints, approaches or code on request (`CLAUDE.md`).
- Created the skeleton: modules `common`, `storage-node`, `bench`; Java 21, Gradle 8.14 wrapper, gRPC 1.68.1, protobuf 3.25.5, JUnit 5.11, Spotless with google-java-format 1.22.0. `./gradlew build` passes.
- Checked the Exercise 1 tests against a throwaway implementation: 5/5 pass. The implementation was deleted, so the exercise is still open.
- Added the remote. The user committed and pushed the skeleton (`732a5a4`).
- Wrote the project docs: `CLAUDE.md`, `docs/PLAN.md`, `docs/PROGRESS.md`, `docs/CONCEPTS.md`, `docs/BACKGROUND.md`.
