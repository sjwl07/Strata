plugins {
    id("strata.java-conventions")
    application
}

dependencies {
    implementation(project(":common"))
    implementation(libs.grpc.netty.shaded)
    testImplementation(libs.grpc.inprocess)
}

application {
    mainClass = "io.strata.node.StorageNodeMain"
}
