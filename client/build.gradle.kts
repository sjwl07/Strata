plugins {
    id("strata.java-conventions")
    `java-library`
}

// The smart client (Project 3). Depends only on the API protos, gRPC and HdrHistogram, so it can be
// published and used on its own.
dependencies {
    api(project(":common"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.hdrhistogram)
    testImplementation(project(":local-cluster"))
    testImplementation(project(":fault"))
}

// The client is useful on its own: ./gradlew :client:publishToMavenLocal
apply(plugin = "maven-publish")
configure<JavaPluginExtension> {
    withSourcesJar()
    withJavadocJar()
}
configure<PublishingExtension> {
    publications {
        create<MavenPublication>("client") {
            artifactId = "strata-client"
            from(components["java"])
        }
    }
}
