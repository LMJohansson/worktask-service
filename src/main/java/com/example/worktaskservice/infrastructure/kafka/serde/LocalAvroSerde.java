package com.example.worktaskservice.infrastructure.kafka.serde;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Registry-free Avro Serde for a single, fixed schema — used for the Kafka Streams state store (and its
 * internal changelog). The store is Streams-internal infrastructure and must not depend on the public
 * schema registry: the registry-backed SerDes use {@code use.latest.version} against a {@code <topic>-value}
 * union subject that exists only for the public command/event/compact topics, not for changelog topics.
 * Reader schema == writer schema (the generated {@code SCHEMA$}); an incompatible state-schema change
 * requires a Streams reset, which is the normal lifecycle for internal stores.
 */
public final class LocalAvroSerde<T extends SpecificRecord> implements Serde<T> {

    private final Schema schema;

    public LocalAvroSerde(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Serializer<T> serializer() {
        var writer = new SpecificDatumWriter<T>(schema);
        return (topic, data) -> {
            if (data == null) return null;
            try {
                var out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                writer.write(data, encoder);
                encoder.flush();
                return out.toByteArray();
            } catch (IOException e) {
                throw new UncheckedIOException("Avro serialization failed for " + schema.getFullName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        var reader = new SpecificDatumReader<T>(schema);
        return (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                return reader.read(null, decoder);
            } catch (IOException e) {
                throw new UncheckedIOException("Avro deserialization failed for " + schema.getFullName(), e);
            }
        };
    }
}
