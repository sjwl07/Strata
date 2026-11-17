plugins {
    id("strata.java-conventions")
    `java-library`
}

// Server-side fault injection, plugged into a storage node through ServeHook.
dependencies {
    api(project(":common"))
}
