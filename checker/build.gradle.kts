plugins {
    id("strata.java-conventions")
    `java-library`
    application
}

dependencies {
    api(project(":storage-node"))
    api(project(":push-job"))
}

// ./gradlew :checker:run --args="--histories=200 --modes=FULL,LWW,NAIVE"
application {
    mainClass = "io.strata.checker.CheckerMain"
    applicationDefaultJvmArgs = listOf("-Xmx512m")
}

// CI runs a longer randomized check: ./gradlew :checker:test -Pchecker.histories=40 --rerun
tasks.test {
    systemProperty("checker.histories", providers.gradleProperty("checker.histories").getOrElse("8"))
}
