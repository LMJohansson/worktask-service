package com.example.worktaskservice.infrastructure.kafka.mapper;

import com.example.worktaskservice.domain.event.WorkTaskEvent;
import com.example.worktaskservice.domain.model.Subject;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

final class CloudEventHeaders {

    private static final DateTimeFormatter RFC3339 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    // This service's CloudEvents source: identifies the WorkTask bounded context as the message origin
    // of emitted records (urn:source:<domain>(.<subdomain>)?:<bounded-context>). NOT propagated from the
    // inbound command — the originating business source is carried as the WorkTask `source` payload attribute.
    private static final String SERVICE_SOURCE = "urn:source:work.tasks:worktask";

    private CloudEventHeaders() {}

    static Headers forEvent(WorkTaskEvent event, Subject subject, SpecificRecord avroRecord,
                            String schemaRegistryUrl, String traceparent, String tracestate,
                            String causationId) {
        var headers = new RecordHeaders();
        set(headers, "ce_specversion", "1.0");
        set(headers, "ce_type", ceType(avroRecord));
        set(headers, "ce_source", SERVICE_SOURCE);
        set(headers, "ce_id", UUID.randomUUID().toString());
        set(headers, "ce_time", RFC3339.format(event.occurredAt()));
        set(headers, "ce_subject", subject.toUrn());
        set(headers, "ce_datacontenttype", "application/avro");
        set(headers, "ce_dataschema", schemaRegistryUrl
                + "/apis/registry/v3/groups/worktask/artifacts/"
                + avroRecord.getSchema().getFullName());
        // Events are partitioned by subject; ce_partitionkey matches the Kafka record key (subjectId).
        set(headers, "ce_partitionkey", subject.id().toString());
        // CloudEvents Correlation extension: correlationid groups the business transaction,
        // causationid is the ce_id of the command that directly caused this event.
        set(headers, "ce_correlationid", event.correlationId().toString());
        if (causationId != null) set(headers, "ce_causationid", causationId);
        if (traceparent != null) set(headers, "ce_traceparent", traceparent);
        if (tracestate != null) set(headers, "ce_tracestate", tracestate);
        return headers;
    }

    private static String ceType(SpecificRecord record) {
        String simpleName = record.getClass().getSimpleName();
        // e.g. WorkTaskCreated → com.example.worktaskservice.worktask.created.v1
        String verb = toVerb(simpleName);
        return "com.example.worktaskservice.worktask." + verb + ".v1";
    }

    private static String toVerb(String className) {
        // Strip "WorkTask" prefix, lowercase remainder: WorkTaskCreated → created
        return className.replaceFirst("^WorkTask", "").toLowerCase();
    }

    private static void set(Headers headers, String key, String value) {
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    static String extractHeader(Headers headers, String key) {
        var header = headers.lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
