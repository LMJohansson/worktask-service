package com.example.worktaskservice.infrastructure.kafka.mapper;

import com.example.worktaskservice.domain.event.WorkTaskBegunEvent;
import com.example.worktaskservice.domain.event.WorkTaskCommandRejectedEvent;
import com.example.worktaskservice.domain.model.*;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CloudEventHeadersTest {

    private final EventAvroMapper mapper = new EventAvroMapper();

    private WorkTask task(UUID id) {
        var now = Instant.now();
        return WorkTask.reconstitute(id,
                new WorkTaskType("urn:worktask-type:billing.invoices:payment:process-refund"),
                new Subject(new SubjectType("urn:subject-type:billing.invoices:payment:invoice"), UUID.randomUUID().toString()),
                new Source("urn:source:work.tasks:worktask"),
                "title", null, 0, null, WorkTaskStatus.ASSIGNED, null, now, now);
    }

    @Test
    void tagsEventsWithUrnSubjectAndCorrelation() {
        var id = UUID.randomUUID();
        var correlationId = UUID.randomUUID();
        var causationId = UUID.randomUUID().toString();
        var task = task(id);
        var event = new WorkTaskBegunEvent(id, correlationId, Instant.now());

        Headers headers = mapper.toAvro(event, task.subject(), null, null, causationId).headers();

        assertEquals(task.subject().toUrn(), header(headers, "ce_subject"));
        // Events are partitioned by subject: ce_partitionkey is the subjectId, not the WorkTask id.
        assertEquals(task.subject().id().toString(), header(headers, "ce_partitionkey"));
        assertEquals(correlationId.toString(), header(headers, "ce_correlationid"));
        assertEquals(causationId, header(headers, "ce_causationid"));
    }

    @Test
    void tagsEventsWithThisServiceSource() {
        // ce_source identifies this bounded context as the message origin; it is NOT propagated from the
        // inbound command (the originating business source is the WorkTask `source` payload attribute).
        var id = UUID.randomUUID();
        var event = new WorkTaskBegunEvent(id, UUID.randomUUID(), Instant.now());

        Headers headers = mapper.toAvro(event, task(id).subject(), null, null, null).headers();

        assertEquals("urn:source:work.tasks:worktask", header(headers, "ce_source"));
    }

    @Test
    void tagsRejectionEventsWithCausationToo() {
        var id = UUID.randomUUID();
        var correlationId = UUID.randomUUID();
        var causationId = UUID.randomUUID().toString();
        var event = new WorkTaskCommandRejectedEvent(id, correlationId, Instant.now(),
                "BeginWorkTask", "invalid transition");

        Headers headers = mapper.toAvro(event, task(id).subject(), null, null, causationId).headers();

        assertEquals(correlationId.toString(), header(headers, "ce_correlationid"));
        assertEquals(causationId, header(headers, "ce_causationid"));
    }

    @Test
    void omitsCausationWhenInboundIdAbsent() {
        var id = UUID.randomUUID();
        var event = new WorkTaskBegunEvent(id, UUID.randomUUID(), Instant.now());

        Headers headers = mapper.toAvro(event, task(id).subject(), null, null, null).headers();

        assertNull(headers.lastHeader("ce_causationid"));
        assertNotNull(headers.lastHeader("ce_correlationid"));
    }

    private static String header(Headers headers, String key) {
        return CloudEventHeaders.extractHeader(headers, key);
    }
}
