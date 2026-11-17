# Strata: notes for Claude

## Resuming after a break
1. Read `docs/PROGRESS.md` first: current phase, last step done, next step, open questions. `docs/JOURNAL.md` has the step-by-step history with reasons.
2. Use `docs/PLAN.md` for phase scope and exit criteria, and `docs/CONCEPTS.md` for the agreed explanations and terminology.
3. Check `git log --oneline -20` and `git status` to see what changed since the last log entry.
4. Give the user a short recap (where we are, what's next) before doing anything else.

## Working mode
- **Until 2026-11-17** the user wrote the core logic and Claude gave hints (see `docs/exercises/`).
- **From 2026-11-17** the user asked Claude to build the whole project, because they don't have time to learn it right now. Claude implements everything and records every step and its reasoning in `docs/JOURNAL.md`, so the user can review it later.
- Commit at meaningful milestones (one or more commits per phase). **Never push**; the user pushes.
- **No `Co-Authored-By` or other AI-attribution lines in commit messages** (the user asked for this on 2026-11-17).
- Explain the non-obvious Java and systems points in the journal and ADRs. The user will study the code afterwards.

## Keeping the docs current
At the end of any session where something changed, update `docs/PROGRESS.md` and append to `docs/JOURNAL.md`:
- current phase and status;
- what was done;
- next step;
- a dated log entry.

Record design decisions as ADRs in `docs/adr/`, starting from `0000-template.md`.

## Conventions
- Java 21, Gradle Kotlin DSL, versions pinned in `gradle/libs.versions.toml`, and shared module setup in `buildSrc/src/main/kotlin/strata.java-conventions.gradle.kts`.
- Package root `io.strata`; generated protobuf code goes in `io.strata.proto.v1`.
- Formatting: google-java-format through Spotless. Run `./gradlew spotlessApply`; `./gradlew build` fails on unformatted code.
- Tests: JUnit 5 and AssertJ. Use the gRPC in-process transport for service tests.
- Clean-room: open-source code and public papers only. No LinkedIn-internal code or designs.
- Commit locally at milestones; never push. Remote: `https://github.com/sjwl07/Strata.git`.
