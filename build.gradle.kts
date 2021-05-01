plugins {
    kotlin("jvm") version "1.4.32" apply false
    kotlin("plugin.serialization") version "1.4.32" apply false

    id("com.github.johnrengelman.shadow") version "7.0.0" apply false
    id("io.spring.dependency-management") version "1.0.6.RELEASE" apply false
}

group = "dev.brella"