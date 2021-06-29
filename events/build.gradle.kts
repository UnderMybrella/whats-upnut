import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application

    id("com.github.johnrengelman.shadow")
    id("io.spring.dependency-management")
    id("com.bmuschko.docker-remote-api")
}

group = "dev.brella"
version = "1.1.2"

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
    val ktor_version = "1.6.0"

    implementation(project(":common"))

    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation("io.ktor:ktor-serialization:$ktor_version")

    implementation("io.ktor:ktor-client-okhttp:$ktor_version")
    implementation("io.ktor:ktor-client-encoding:$ktor_version")
    implementation("io.ktor:ktor-client-core-jvm:$ktor_version")
    implementation("io.ktor:ktor-client-serialization:$ktor_version")
    implementation("io.ktor:ktor-client-encoding:$ktor_version")

    implementation("dev.brella:ktornea-utils:1.2.3-alpha")
    implementation("dev.brella:kornea-errors:2.0.3-alpha")

    implementation("dev.brella:discord4j-coroutines-rest:1.2.0")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.2.0")

    implementation("ch.qos.logback:logback-classic:1.2.3")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.5.0")

    implementation("org.springframework.data:spring-data-r2dbc:1.3.0")
    implementation("io.r2dbc:r2dbc-postgresql:0.8.7.RELEASE")
    implementation("io.r2dbc:r2dbc-pool:0.9.0.M1")

    implementation("io.jsonwebtoken:jjwt-api:0.11.2")
    implementation("io.jsonwebtoken:jjwt-impl:0.11.2")
    // Uncomment the next line if you want to use RSASSA-PSS (PS256, PS384, PS512) algorithms:
    implementation("org.bouncycastle:bcprov-jdk15on:1.68")
    implementation("io.jsonwebtoken:jjwt-jackson:0.11.2") // or "io.jsonwebtoken:jjwt-gson:0.11.2' for gson

    implementation("com.github.ben-manes.caffeine:caffeine:3.0.1")

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


tasks.create<com.bmuschko.gradle.docker.tasks.image.Dockerfile>("createDockerfile") {
    group = "docker"

    destFile.set(File(rootProject.buildDir, "docker/stream/Dockerfile"))
    from("azul/zulu-openjdk-alpine:11-jre")
    label(
        mapOf(
            "org.opencontainers.image.authors" to "UnderMybrella \"undermybrella@abimon.org\""
        )
    )
    copyFile(tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar").get().archiveFileName.get(), "/app/upnuts-stream.jar")

    copyFile("upnuts-r2dbc.json", "/app/upnuts-r2dbc.json")
    copyFile("logback.xml", "/app/logback.xml")
    copyFile("events.conf", "/app/events.conf")
    entryPoint("java")
    defaultCommand("-Dlogback.configurationFile=/app/logback.xml", "-Dupnut.r2dbc=/app/upnuts-r2dbc.json", "-jar", "/app/upnuts-stream.jar", "-config=/app/events.conf")

    exposePort(9796)
}

tasks.create<Sync>("syncShadowJarArchive") {
    group = "docker"

    dependsOn("assemble")
    from(
        tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar").get().archiveFile.get().asFile,
        File(rootProject.projectDir, "deployment/events.conf"),
        File(rootProject.projectDir, "deployment/upnuts-r2dbc.json"),
        File(rootProject.projectDir, "deployment/logback.xml")
    )
    into(
        tasks.named<com.bmuschko.gradle.docker.tasks.image.Dockerfile>("createDockerfile").get().destFile.get().asFile.parentFile
    )
}

tasks.named("createDockerfile") {
    dependsOn("syncShadowJarArchive")
}

tasks.create<com.bmuschko.gradle.docker.tasks.image.DockerBuildImage>("buildImage") {
    group = "docker"

    dependsOn("createDockerfile")
    inputDir.set(tasks.named<com.bmuschko.gradle.docker.tasks.image.Dockerfile>("createDockerfile").get().destFile.get().asFile.parentFile)

    images.addAll("undermybrella/upnuts-stream:$version", "undermybrella/upnuts-stream:latest")
}

tasks.create<com.bmuschko.gradle.docker.tasks.image.DockerPushImage>("pushImage") {
    group = "docker"
    dependsOn("buildImage")

    images.addAll("undermybrella/upnuts-stream:$version", "undermybrella/upnuts-stream:latest")
}