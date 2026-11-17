rootProject.name = "strata"

// Modules are added as each phase needs them (see README roadmap).
include(
    "common",
    "sst",
    "push-job",
    "storage-node",
    "kafka-log",
    "checker",
    "bench",
)
