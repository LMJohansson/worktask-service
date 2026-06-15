plugins {
    java
    id("io.quarkus") version "3.36.1"
}

repositories {
    mavenCentral()
    // io.confluent:* (kafka-avro-serializer and the schema-registry client) are published here only,
    // not to Maven Central.
    maven { url = uri("https://packages.confluent.io/maven/") }
}

// Confluent Platform line for the Avro serde + Schema Registry client. 8.x targets Kafka 4.x / Avro
// 1.12 (matching the Quarkus 3.36 BOM); not managed by the Quarkus BOM, so pin it explicitly.
val confluentVersion = "8.2.1"

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))

    implementation("io.quarkus:quarkus-avro")
    implementation("io.quarkus:quarkus-confluent-registry-avro")
    // The Confluent Avro SerDes themselves (the Quarkus extension only provides native/registry glue).
    // Exclude the JAX-RS API it drags in — Quarkus already provides jakarta.ws.rs-api and a duplicate
    // breaks the build.
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion") {
        exclude(group = "jakarta.ws.rs", module = "jakarta.ws.rs-api")
    }
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
    // A schema registry is needed for the end-to-end streams test: this app uses Kafka Streams (not a
    // messaging channel), so the Quarkus Schema Registry Dev Service never auto-starts. A Testcontainers
    // GenericContainer runs Apicurio, whose Confluent-compatible ccompat API the SerDes resolve against
    // (matching dev mode). (Version managed by the Quarkus platform BOM.)
    testImplementation("org.testcontainers:testcontainers")
}

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
}

// CI/prod schema registration: registers member + union-of-references Avro schemas into a Confluent
// Schema Registry. Point it with -Dschema.registry.url=<base-url> (or SCHEMA_REGISTRY_URL); topic names
// override with -Dworktask.topics.*. Dev/test register automatically via SchemaRegistrarStartup.
tasks.register<JavaExec>("registerSchemas") {
    group = "schema-registry"
    description = "Register WorkTask member + union Avro schemas into a Confluent Schema Registry"
    dependsOn("classes")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.worktaskservice.infrastructure.kafka.serde.SchemaRegistrarMain")
    System.getProperties().forEach { k, v ->
        val key = k.toString()
        if (key.startsWith("schema.registry.") || key.startsWith("worktask.")) systemProperty(key, v.toString())
    }
    System.getenv("SCHEMA_REGISTRY_URL")?.let { environment("SCHEMA_REGISTRY_URL", it) }
}
