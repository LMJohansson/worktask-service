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

    public Serde<com.example.worktaskservice.state.WorkTask> workTaskStateSerde() {
        return buildSerde();
    }

    private <T extends SpecificRecord> Serde<T> buildSerde() {
        var serializer   = new AvroKafkaSerializer<T>();
        var deserializer = new AvroKafkaDeserializer<T>();
        Map<String, Object> config = Map.of(
                "apicurio.registry.url", registryUrl,
                "apicurio.registry.avro-datum-provider",
                "io.apicurio.registry.serde.avro.ReflectAvroDatumProvider");
        serializer.configure(config, false);
        deserializer.configure(config, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
