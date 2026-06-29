plugins {
    // Lets Gradle auto-provision the Java 25 toolchain (e.g. on JitPack/CI,
    // where the running JDK may be older).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "backbone"
