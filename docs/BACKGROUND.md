# Strata: background and project choice

## Who
A third-year CS student at IIIT-Delhi. They interned at LinkedIn on Project Kyoto data ingestion (Kafka/Brooklin CDC → Flink → Iceberg → Spark, with OpenHouse as the control plane), doing end-to-end integration tests, a local Docker Flink setup, CI for CDC Spark pipelines, and schema-aware test-data generation over 3,000+ Avro schemas. They have a PPO and join LinkedIn full-time in summer 2027.

The role is in online data infra: storage/replication, ML platforms, and Java distributed systems. They're also doing research on energy-aware SIMD (AVX2 vs AVX-512) for CPU LLM inference in llama.cpp.

## Goal for this project
A substantial 4–6 month project, built within about 10 months, that is about **learning** (above all for the LinkedIn role and modern Java) and is good enough for real people to use. No clones, CRUD apps or AI wrappers. Measurable systems questions are preferred.

## Ideas considered
| Idea | Verdict |
|---|---|
| Fault-injection correctness testing for CDC → lakehouse pipelines ("Jepsen for CDC") | Strong, but reinforces offline skills the user already has. A possible future project. |
| Cross-system schema-evolution breakage checker | Probably 6–8 weeks; better as a feature of the idea above. |
| Workload-aware lakehouse table maintenance | Users are hard to get, it's crowded (Amoro, S3 Tables), and it overlaps with OpenHouse. |
| Energy-aware CPU inference runtime (from the research) | Better as a continuation of the research than as a flagship. |
| **Hybrid batch + streaming serving store** | **Chosen**: it fills the online-serving and Java gap the role needs. |
| Training–serving skew detection | Good, but less of a distributed-systems project. |
| **Tail-latency-aware gRPC client** | **Chosen as the client layer** of the store. |
| Postgres → Iceberg CDC, local distributed environments, flaky-test detection, deterministic simulation testing library | Crowded, or nothing that would make it stand out. |

## Constraints
- Clean-room: open-source code and public knowledge only. No LinkedIn-internal code, incidents or designs.
- Check the offer letter's IP/outside-activities clause before publicising.
- Report honest numbers: state the hardware and dataset size, with no "LinkedIn-scale" claims.
