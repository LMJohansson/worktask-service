package com.example.worktaskservice.infrastructure.kafka.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Confluent Avro deserializer that handles multi-type topics whose value schema is a top-level
 * <b>union</b> of schema references.
 *
 * <p>With {@code use.latest.version=true} + {@code auto.register.schemas=false} the serializer writes
 * the schema id of the latest {@code <topic>-value} subject — the union — on the wire, so the writer
 * schema fetched on read is that union. The stock {@link KafkaAvroDeserializer} with
 * {@code specific.avro.reader=true} then tries to map the union to a single generated reader class,
 * which fails (a union has no single class; it maps to {@code Object}). Instead, read with the writer
 * (union) schema itself: Avro decodes the active branch and {@code SpecificData} instantiates that
 * branch's generated {@code SpecificRecord}. Single-type topics (the {@code compact-value} state schema)
 * are not unions and fall through to the stock path.
 */
public class UnionAwareKafkaAvroDeserializer extends KafkaAvroDeserializer {

    private final ConcurrentHashMap<Long, DatumReader<?>> unionReaderCache = new ConcurrentHashMap<>();

    @Override
    protected DatumReader<?> getDatumReader(Schema writerSchema, Schema readerSchema) throws ExecutionException {
        if (writerSchema != null && writerSchema.getType() == Schema.Type.UNION) {
            return unionReaderCache.computeIfAbsent(
                    SchemaNormalization.parsingFingerprint64(writerSchema),
                    k -> new SpecificDatumReader<>(writerSchema));
        }
        return super.getDatumReader(writerSchema, readerSchema);
    }
}
