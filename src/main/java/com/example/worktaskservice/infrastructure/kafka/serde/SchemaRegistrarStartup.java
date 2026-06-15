package com.example.worktaskservice.infrastructure.kafka.serde;

import com.example.worktaskservice.infrastructure.config.TopicConfig;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;

/**
 * Dev/test bootstrap: registers the member + union-of-references schemas against the Redpanda-backed
 * Schema Registry at startup, so the {@code use.latest.version} / {@code auto.register.schemas=false}
 * SerDes can resolve them. Excluded from the production jar ({@code @UnlessBuildProfile("prod")}) —
 * CI/prod register out-of-band via the Gradle {@code registerSchemas} task ({@link SchemaRegistrarMain}).
 */
@ApplicationScoped
@UnlessBuildProfile("prod")
public class SchemaRegistrarStartup {

    private static final Logger LOG = Logger.getLogger(SchemaRegistrarStartup.class);

    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url",
            defaultValue = "http://localhost:8081")
    String registryUrl;

    @Inject
    TopicConfig topics;

    void onStart(@Observes StartupEvent event) {
        try {
            var registrar = new SchemaRegistrar(registryUrl);
            registrar.awaitReady(Duration.ofSeconds(60));   // tolerate a registry still starting under Dev Services
            registrar.registerAll(
                    topics.commandTopic(), topics.deadLetterTopic(), topics.eventTopic(), topics.compactTopic());
        } catch (Exception e) {
            // Don't abort startup (keeps registry-independent tests green); the streams serde path will
            // surface any gap. Surface the cause loudly for diagnosis.
            LOG.errorf(e, "Schema registration against %s failed", registryUrl);
        }
    }
}
