plugins {
    id("strata.java-conventions")
    alias(libs.plugins.jmh)
}

dependencies {
    jmh(project(":common"))
}

// Run with: ./gradlew :bench:jmh
// Narrow to one benchmark with: ./gradlew :bench:jmh -Pjmh.includes=MapBenchmark
jmh {
    jmhVersion = "1.37"
    providers.gradleProperty("jmh.includes").orNull?.let { includes = listOf(it) }
    resultFormat = "JSON"
}
