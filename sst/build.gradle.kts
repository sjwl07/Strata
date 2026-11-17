plugins {
    id("strata.java-conventions")
    `java-library`
}

// The on-disk format shared by the push job (writer) and storage nodes (reader).
dependencies {
    api(project(":common"))
    api(libs.rocksdbjni)
}
