plugins {
    id("strata.java-conventions")
    `java-library`
}

// An in-process cluster (controller + N storage nodes on real loopback ports) for integration
// tests, experiments and the quickstart demo (bench: `bench demo`).
dependencies {
    api(project(":controller"))
    api(project(":storage-node"))
    api(project(":push-job"))
    implementation(libs.grpc.netty.shaded)
}
