plugins {
    id("strata.java-conventions")
    application
    alias(libs.plugins.jmh)
}

dependencies {
    implementation(project(":common"))
    implementation(project(":push-job"))
    implementation(project(":storage-node"))
    implementation(project(":checker"))
    implementation(project(":client"))
    implementation(project(":fault"))
    implementation(project(":local-cluster"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.hdrhistogram)
    jmh(project(":common"))
}

// Experiments: ./gradlew :bench:run --args="phase1 [options]"   (see docs/experiments/)
application {
    mainClass = "io.strata.bench.Experiments"
    applicationDefaultJvmArgs = listOf("-Xmx1g") // GC chosen via JAVA_OPTS (G1 is the default)
}

// Micro-benchmarks: ./gradlew :bench:jmh -Pjmh.includes=MapBenchmark -Pjmh.threads=4
jmh {
    jmhVersion = "1.37"
    providers.gradleProperty("jmh.includes").orNull?.let { includes = listOf(it) }
    providers.gradleProperty("jmh.threads").orNull?.let { threads = it.toInt() }
    resultFormat = "JSON"
}
