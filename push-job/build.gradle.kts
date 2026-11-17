plugins {
    id("strata.java-conventions")
    `java-library`
    application
}

dependencies {
    api(project(":sst"))
}

application {
    mainClass = "io.strata.push.PushJobMain"
}
