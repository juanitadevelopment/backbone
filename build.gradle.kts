plugins {
    `java-library`
    `maven-publish`
}

group = "net.teppan"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    // shazo 0.1.0 is resolved from the local Maven repository. Install it with
    // `./gradlew publishToMavenLocal` in the shazo project, or publish it to a
    // shared repository / JitPack and adjust the dependency coordinates below.
    mavenLocal()
    mavenCentral()
}

dependencies {
    // Backbone's public API exposes shazo types (repositories, describers,
    // unit-of-work), so shazo is an `api` dependency.
    api("net.teppan:shazo:0.1.0")

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
    title = "Backbone 0.1.0 API"
    (options as StandardJavadocDocletOptions).apply {
        encoding = "UTF-8"
        charSet = "UTF-8"
        locale = "en"
        addStringOption("Xdoclint:all", "-quiet")
        addBooleanOption("html5", true)
        windowTitle = "Backbone 0.1.0 API"
        header = "<b>Backbone 0.1.0</b>"
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
