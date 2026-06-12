package com.example.worktaskservice.infrastructure.kafka.serde;

import io.apicurio.registry.serde.avro.DefaultAvroDatumProvider;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Avro datum provider that deserializes multi-type topics whose value schema is a <b>union</b>.
 *
 * <p>{@link DefaultAvroDatumProvider} with {@code use-specific-avro-reader=true} tries to map the
 * resolved writer schema to a single generated class to use as the reader schema — which fails for a
 * union (no single class; the union maps to {@code Object}). Instead, read with the writer (union)
 * schema itself: Avro decodes the active branch and {@code SpecificData} instantiates that branch's
 * generated {@code SpecificRecord}. Writing is inherited unchanged (a {@code SpecificDatumWriter} over
 * the union picks the matching branch by the record's type).
 */
public class UnionAwareAvroDatumProvider<T> extends DefaultAvroDatumProvider<T> {

    private final ConcurrentHashMap<Long, DatumReader<T>> readerCache = new ConcurrentHashMap<>();

    @Override
    public DatumReader<T> createDatumReader(Schema schema) {
        return readerCache.computeIfAbsent(
                SchemaNormalization.parsingFingerprint64(schema),
                k -> new SpecificDatumReader<>(schema));
    }
}
