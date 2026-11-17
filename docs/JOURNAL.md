# Strata: build journal

Every step, in order, with **what** was done and **why**. Read this from top to bottom to see how the project was built. The design decisions it mentions are written up properly as ADRs in `docs/adr/`.

Format of each entry:
- **Step**: what was built
- **Why**: the reasoning, including alternatives that were rejected
- **Learn**: the Java or systems idea worth studying in that step (pointers for your later review)

---

## 2026-11-17: Change of plan

**Step:** you asked me to build the whole project rather than giving hints. I updated `CLAUDE.md` with the new working mode and started this journal.

**Environment found on this machine** (it shapes several decisions below):
| Resource | Value | Consequence |
|---|---|---|
| CPU | 8 cores | Fine for an in-process multi-node cluster |
| RAM | 7 GB total, about 2 GB free | Experiments use 1–2M keys, not 50M. Small heaps |
| Disk | about 8.6 GB free | Datasets must stay at a few hundred MB |
| Docker | daemon not running, and starting it needs `sudo` | No Kafka, MinIO or Testcontainers runs here |
| JDK | 21 | As planned |

**Why this matters / decisions taken up front** (written up in ADR 0001):
1. **Kafka sits behind an interface (`UpdateLog`).** The default implementation is an in-memory log with Kafka semantics (partitions, offsets, append time, retention). A real `KafkaUpdateLog` is also included, with a Testcontainers test that runs only when Docker is available. Everything that matters for Q2 (ordering, offsets, replay) holds in both.
2. **MinIO sits behind an interface (`ObjectStore`)**, implemented on the local filesystem. The node still "downloads" files into its own cache and counts bytes, so Q1 measures transfer volume honestly.
3. **No Spark.** Spark 3.5 doesn't officially support Java 21, and its dependencies are around 300 MB. The push job is a plain-Java program that does the same steps Spark would (hash-partition, sort, write SSTs, diff snapshots for deltas). Running it on Spark 4 is listed as future work.
4. **The cluster runs in one JVM**: controller and N storage nodes, each with its own real Netty gRPC server on a loopback port, so requests really cross the network stack. "kill -9" is simulated by abruptly stopping a node and deleting its disk state.
5. **Experiment sizes are scaled down** (about 1M keys), and every write-up states the hardware.

---
