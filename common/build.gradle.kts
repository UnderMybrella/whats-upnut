import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application

    id("com.github.johnrengelman.shadow")
    id("io.spring.dependency-management")
}

group = "dev.brella"
version = "1.5.3"

repositories {
    mavenCentral()
    maven(url = "https://maven.brella.dev")
    maven(url = "https://kotlin.bintray.com/ktor")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.security:spring-security-bom:5.4.6")
    }
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.2.1")

    implementation("dev.brella:kornea-blaseball-base:2.2.9-alpha") {
        exclude("org.jetbrains.kotlinx", "kotlinx-serialization-core")
        exclude("org.jetbrains.kotlinx", "kotlinx-serialization-json")
    }
    implementation("dev.brella:kornea-errors:2.0.3-alpha") {
        exclude("org.jetbrains.kotlinx", "kotlinx-serialization-core")
        exclude("org.jetbrains.kotlinx", "kotlinx-serialization-json")
    }
    api("org.jetbrains.kotlinx:kotlinx-datetime:0.2.1")

//    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.5.0")

    implementation("org.springframework.data:spring-data-r2dbc:1.3.0")
    implementation("io.r2dbc:r2dbc-postgresql:0.8.7.RELEASE")
    implementation("io.r2dbc:r2dbc-pool:0.9.0.M1")

//    implementation("io.jsonwebtoken:jjwt-api:0.11.2")
//    implementation("io.jsonwebtoken:jjwt-impl:0.11.2")
//    // Uncomment the next line if you want to use RSASSA-PSS (PS256, PS384, PS512) algorithms:
//    implementation("org.bouncycastle:bcprov-jdk15on:1.68")
//    implementation("io.jsonwebtoken:jjwt-jackson:0.11.2") // or "io.jsonwebtoken:jjwt-gson:0.11.2' for gson

    testImplementation(kotlin("test-junit"))
}

tasks.test {
    useJUnit()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClass.set("io.ktor.server.netty.EngineMain")
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    mergeServiceFiles()
    append("META-INF/spring.handlers")
    append("META-INF/spring.schemas")
    append("META-INF/spring.tooling")
    transform(com.github.jengelman.gradle.plugins.shadow.transformers.PropertiesFileTransformer::class.java) {
        paths = listOf("META-INF/spring.factories")
        mergeStrategy = "append"
    }
}