package com.example.worktaskservice.infrastructure.kafka.serde;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;

@ApplicationScoped
public class AvroSerdeFactory {

    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url",
            defaultValue = "http://localhost:8081")
    String registryUrl;

    public Serde<SpecificRecord> commandSerde() {
        return buildSerde();
    }

    /** Apicurio serde for the public compact topic (registry-backed, find-latest). */
    public Serde<com.example.worktaskservice.state.WorkTask> workTaskStateSerde() {
        return buildSerde();
    }

    /**
     * Registry-free serde for the internal state store + its changelog. The store must not depend on the
     * public registry (no {@code <changelog>-value} union artifact exists), so it uses a fixed-schema
     * Avro serde rather than the find-latest Apicurio serde.
     */
    public Serde<com.example.worktaskservice.state.WorkTask> workTaskStateStoreSerde() {
        return new LocalAvroSerde<>(com.example.worktaskservice.state.WorkTask.getClassSchema());
    }

    private <T extends SpecificRecord> Serde<T> buildSerde() {
        var serializer   = new AvroKafkaSerializer<T>();
        var deserializer = new AvroKafkaDeserializer<T>();
        // Multiple message types share one topic, so each topic's value schema is an Avro *union of
        // schema references* registered as `<topic>-value` (default TopicIdStrategy). The serializer must
        // use that latest union schema (find-latest) rather than the record's own single-type schema;
        // the deserializer resolves the schema via the globalId embedded in the message. Schemas are
        // registered out-of-band (SchemaRegistrar), so auto-register is off. UnionAwareAvroDatumProvider
        // reads the union into its concrete branch SpecificRecord (the default provider can't, since a
        // union has no single reader class).
        Map<String, Object> config = Map.of(
                "apicurio.registry.url", registryUrl,
                // Resolve <topic>-value in the same group the SchemaRegistrar writes to.
                "apicurio.registry.artifact.group-id", "worktask",
                "apicurio.registry.find-latest", "true",
                "apicurio.registry.auto-register", "false",
                // Pass the Class (not its name): Kafka's CLASS config would otherwise Class.forName() it
                // via a classloader that can't see this app class under Quarkus.
                "apicurio.registry.avro-datum-provider", UnionAwareAvroDatumProvider.class);
        serializer.configure(config, false);
        deserializer.configure(config, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
