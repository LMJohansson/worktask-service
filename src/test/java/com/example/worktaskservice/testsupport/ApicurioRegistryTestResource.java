package com.example.worktaskservice.testsupport;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;

/**
 * Starts an Apicurio Registry container for tests and points the app's Avro SerDes at its
 * <b>Confluent-compatible REST API</b> (the {@code /apis/ccompat/v7} endpoint), matching what dev mode
 * uses (see {@code compose-devservices.yml}).
 *
 * <p>Needed because this service uses Kafka Streams (not a SmallRye messaging channel), so the Quarkus
 * Schema Registry Dev Service never auto-starts. Apicurio is self-contained (no external Kafka backing)
 * and its ccompat API implements the Confluent Schema Registry protocol — including the Avro schema
 * references the multi-type-topic union pattern depends on. The Quarkus Kafka Dev Services broker carries
 * the Kafka data; only the registry endpoint is provided here. Readiness is probed via the registry's
 * native v3 system-info endpoint (8080; Apicurio 3.x serves health on its separate management port 9000).
 */
public class ApicurioRegistryTestResource implements QuarkusTestResourceLifecycleManager {

    private static final DockerImageName IMAGE = DockerImageName.parse("apicurio/apicurio-registry:3.1.7");
    private static final int HTTP_PORT = 8080;
    private static final String CCOMPAT_PATH = "/apis/ccompat/v7";

    @SuppressWarnings("resource")
    private final GenericContainer<?> registry = new GenericContainer<>(IMAGE)
            .withExposedPorts(HTTP_PORT)
            .waitingFor(Wait.forHttp("/apis/registry/v3/system/info")
                    .forPort(HTTP_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @Override
    public Map<String, String> start() {
        registry.start();
        String url = "http://" + registry.getHost() + ":" + registry.getMappedPort(HTTP_PORT) + CCOMPAT_PATH;
        return Map.of("mp.messaging.connector.smallrye-kafka.schema.registry.url", url);
    }

    @Override
    public void stop() {
        registry.stop();
    }
}
