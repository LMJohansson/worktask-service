package com.example.worktaskservice.testsupport;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;

/**
 * Starts an Apicurio Registry container for tests and points the app's Avro SerDes at it.
 *
 * <p>Needed because this service uses Kafka Streams (not a SmallRye messaging channel), so the Quarkus
 * Apicurio Dev Service never auto-starts. Readiness is probed via the <b>REST API on 8080</b>, not
 * {@code /health} — Apicurio Registry 3.x serves health on its separate management port 9000 (8080
 * returns 404). In-memory storage is the 3.x default, so no extra configuration is required.
 */
public class ApicurioRegistryTestResource implements QuarkusTestResourceLifecycleManager {

    private static final DockerImageName IMAGE = DockerImageName.parse("apicurio/apicurio-registry:3.1.7");
    private static final int HTTP_PORT = 8080;

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
        String url = "http://" + registry.getHost() + ":" + registry.getMappedPort(HTTP_PORT);
        return Map.of("mp.messaging.connector.smallrye-kafka.schema.registry.url", url);
    }

    @Override
    public void stop() {
        registry.stop();
    }
}
