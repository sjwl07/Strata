plugins {
    id("strata.java-conventions")
    `java-library`
    application
}

dependencies {
    api(project(":common"))
    implementation(libs.grpc.netty.shaded)
    testImplementation(libs.grpc.inprocess)
}

application {
    mainClass = "io.strata.controller.ControllerMain"
}
