plugins {
    id("strata.java-conventions")
    `java-library`
    application
}

// An in-process cluster (controller + N storage nodes on real loopback ports) for integration
// tests, experiments and the quickstart demo.
dependencies {
    api(project(":controller"))
    api(project(":storage-node"))
    api(project(":push-job"))
    implementation(libs.grpc.netty.shaded)
}

application {
    mainClass = "io.strata.cluster.LocalClusterMain"
    applicationDefaultJvmArgs = listOf("-Xmx768m")
}
