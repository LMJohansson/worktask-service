plugins {
    java
    id("io.quarkus") version "3.36.1"
}

repositories {
    mavenCentral()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))

    implementation("io.quarkus:quarkus-avro")
    implementation("io.quarkus:quarkus-apicurio-registry-avro")
    implementation("io.quarkus:quarkus-kafka-streams")
    implementation("io.quarkus:quarkus-hibernate-orm-panache")
    implementation("io.quarkus:quarkus-jdbc-postgresql")
    implementation("io.quarkus:quarkus-opentelemetry")
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation("io.quarkus:quarkus-smallrye-graphql")
    implementation("io.quarkus:quarkus-container-image-docker")
    implementation("io.quarkus:quarkus-kubernetes")

    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.rest-assured:rest-assured")
    // Apicurio Registry is needed for the end-to-end streams test: this app uses Kafka Streams (not a
    // messaging channel), so the Quarkus Apicurio Dev Service never auto-starts. A Testcontainers
    // QuarkusTestResource provides one explicitly. (Version managed by the Quarkus platform BOM.)
    testImplementation("org.testcontainers:testcontainers")
}

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
}

// CI/prod schema registration: registers member + union-of-references Avro schemas into an Apicurio
// registry. Point it with -Dapicurio.registry.url=<base-url> (or SCHEMA_REGISTRY_URL); topic names
// override with -Dworktask.topics.*. Dev/test register automatically via SchemaRegistrarStartup.
tasks.register<JavaExec>("registerSchemas") {
    group = "apicurio"
    description = "Register WorkTask member + union Avro schemas into an Apicurio registry"
    dependsOn("classes")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.worktaskservice.infrastructure.kafka.serde.SchemaRegistrarMain")
    System.getProperties().forEach { k, v ->
        val key = k.toString()
        if (key.startsWith("apicurio.") || key.startsWith("worktask.")) systemProperty(key, v.toString())
    }
    System.getenv("SCHEMA_REGISTRY_URL")?.let { environment("SCHEMA_REGISTRY_URL", it) }
}
