plugins {
    id("strata.java-conventions")
    `java-library`
}

dependencies {
    api(project(":common"))
    api(libs.kafka.clients)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.junit)
    testRuntimeOnly(libs.slf4j.simple)
}
