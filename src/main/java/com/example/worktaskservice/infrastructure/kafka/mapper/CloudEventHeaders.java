package com.example.worktaskservice.infrastructure.kafka.mapper;

import com.example.worktaskservice.domain.event.WorkTaskEvent;
import com.example.worktaskservice.domain.model.WorkTask;
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

    private CloudEventHeaders() {}

    static Headers forEvent(WorkTaskEvent event, WorkTask task, SpecificRecord avroRecord,
                            String schemaRegistryUrl, String traceparent, String tracestate) {
        var headers = new RecordHeaders();
        set(headers, "ce_specversion", "1.0");
        set(headers, "ce_type", ceType(avroRecord));
        set(headers, "ce_source", "/worktaskservice");
        set(headers, "ce_id", UUID.randomUUID().toString());
        set(headers, "ce_time", RFC3339.format(event.occurredAt()));
        set(headers, "ce_subject", task.subject().type() + "/" + task.subject().id());
        set(headers, "ce_datacontenttype", "application/avro");
        set(headers, "ce_dataschema", schemaRegistryUrl
                + "/apis/registry/v2/groups/default/artifacts/"
                + avroRecord.getSchema().getFullName());
        set(headers, "ce_partitionkey", task.id().toString());
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
