package com.example.worktaskservice.infrastructure.kafka.serde;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
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

    /** Confluent serde for the public compact topic (registry-backed, use-latest-version). */
    public Serde<com.example.worktaskservice.state.WorkTask> workTaskStateSerde() {
        return buildSerde();
    }

    /**
     * Registry-free serde for the internal state store + its changelog. The store must not depend on the
     * public registry (no {@code <changelog>-value} subject exists), so it uses a fixed-schema Avro serde
     * rather than the use-latest-version Confluent serde.
     */
    public Serde<com.example.worktaskservice.state.WorkTask> workTaskStateStoreSerde() {
        return new LocalAvroSerde<>(com.example.worktaskservice.state.WorkTask.getClassSchema());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends SpecificRecord> Serde<T> buildSerde() {
        var serializer   = new KafkaAvroSerializer();
        var deserializer = new UnionAwareKafkaAvroDeserializer();
        // Multiple message types share one topic, so each topic's value schema is an Avro *union of
        // schema references* registered as `<topic>-value` (default TopicNameStrategy). The serializer must
        // use that latest union schema (use-latest-version) rather than the record's own single-type
        // schema; the deserializer resolves the writer schema via the id embedded in the message.
        // latest.compatibility.strict=false lets use-latest-version accept a record whose schema is a
        // *branch* of the latest union rather than equal to it. Schemas are registered out-of-band
        // (SchemaRegistrar), so auto-register is off. UnionAwareKafkaAvroDeserializer reads the union into
        // its concrete branch SpecificRecord (the stock deserializer can't, since a union has no single
        // reader class).
        Map<String, Object> config = Map.of(
                "schema.registry.url", registryUrl,
                "auto.register.schemas", false,
                "use.latest.version", true,
                "latest.compatibility.strict", false,
                "specific.avro.reader", true);
        serializer.configure(config, false);
        deserializer.configure(config, false);
        return (Serde<T>) Serdes.serdeFrom((Serializer) serializer, (Deserializer) deserializer);
    }
}
