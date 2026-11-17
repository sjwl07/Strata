plugins {
    id("strata.java-conventions")
    `java-library`
    application
}

dependencies {
    api(project(":common"))
    api(project(":sst"))
    implementation(project(":kafka-log"))
    implementation(libs.grpc.netty.shaded)
    testImplementation(project(":push-job"))
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.grpc.inprocess)
}

application {
    mainClass = "io.strata.node.StorageNodeMain"
}
