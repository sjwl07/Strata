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
    applicationName = "controller"
}

// Second launcher in the same distribution: bin/strata-admin
val adminScripts by tasks.registering(CreateStartScripts::class) {
    mainClass = "io.strata.controller.AdminMain"
    applicationName = "strata-admin"
    outputDir = layout.buildDirectory.dir("admin-scripts").get().asFile
    classpath = tasks.named<CreateStartScripts>("startScripts").get().classpath
}
distributions {
    main {
        contents { from(adminScripts) { into("bin") } }
    }
}
