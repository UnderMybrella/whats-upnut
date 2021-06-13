plugins {
    kotlin("jvm") version "1.5.10" apply false
    kotlin("plugin.serialization") version "1.5.10" apply false

    id("com.github.johnrengelman.shadow") version "7.0.0" apply false
    id("io.spring.dependency-management") version "1.0.6.RELEASE" apply false

    id("com.bmuschko.docker-remote-api") version "7.0.0" apply false
}

group = "dev.brella"