plugins {
    `java-library`
    `maven-publish`
}

group = "net.teppan"
version = "0.1.10"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    // shazo is consumed from JitPack, which builds it on demand from its GitHub
    // tag. For offline/local development you can instead `publishToMavenLocal`
    // in the shazo project and add `mavenLocal()` here.
    maven { url = uri("https://jitpack.io") }
    mavenCentral()
}

dependencies {
    // Backbone's public API exposes shazo types (repositories, describers,
    // unit-of-work), so shazo is an `api` dependency.
    api("com.github.juanitadevelopment:shazo:v0.1.6")

    implementation("org.slf4j:slf4j-api:2.0.11")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.assertj:assertj-core:3.25.1")
    testImplementation("com.h2database:h2:2.2.224")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.11")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-serial"))
    options.encoding = "UTF-8"
}

tasks.javadoc {
    title = "Backbone 0.1.10 API"
    (options as StandardJavadocDocletOptions).apply {
        encoding = "UTF-8"
        charSet = "UTF-8"
        locale = "en"
        addStringOption("Xdoclint:all", "-quiet")
        addBooleanOption("html5", true)
        windowTitle = "Backbone 0.1.10 API"
        header = "<b>Backbone 0.1.10</b>"
        bottom = "Copyright &#169; 2026 net.teppan. All rights reserved."
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name = "Backbone"
                description = "A minimal transactional service and domain-event " +
                    "runtime built on the shazo persistence abstraction."
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
            }
        }
    }
}
