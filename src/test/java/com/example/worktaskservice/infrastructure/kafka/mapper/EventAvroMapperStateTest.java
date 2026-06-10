package com.example.worktaskservice.infrastructure.kafka.mapper;

import com.example.worktaskservice.domain.model.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventAvroMapperStateTest {

    private final EventAvroMapper mapper = new EventAvroMapper();

    @Test
    void roundTripsStateThroughAvro() {
        var subject = new Subject(new SubjectType("urn:subject-type:billing.invoices:payment:invoice"), UUID.randomUUID());
        var now = Instant.now();
        var deadline = now.plusSeconds(3600);
        var original = WorkTask.reconstitute(UUID.randomUUID(),
                new WorkTaskType("urn:worktask-type:billing.invoices:payment:process-refund"), subject,
                "title", "description", 5, deadline,
                WorkTaskStatus.ASSIGNED, UUID.randomUUID(), now, now);

        WorkTask roundTripped = mapper.toDomain(mapper.toStateAvro(original));

        assertEquals(original.id(), roundTripped.id());
        assertEquals(original.type(), roundTripped.type());
        assertEquals(original.subject(), roundTripped.subject());
        assertEquals(original.title(), roundTripped.title());
        assertEquals(original.description(), roundTripped.description());
        assertEquals(original.priority(), roundTripped.priority());
        assertEquals(original.deadline(), roundTripped.deadline());
        assertEquals(original.status(), roundTripped.status());
        assertEquals(original.assigneeId(), roundTripped.assigneeId());
        assertEquals(original.createdAt(), roundTripped.createdAt());
        assertEquals(original.updatedAt(), roundTripped.updatedAt());
    }

    @Test
    void roundTripsNullableFieldsAsNull() {
        var subject = new Subject(new SubjectType("urn:subject-type:billing.invoices:payment:invoice"), UUID.randomUUID());
        var now = Instant.now();
        var original = WorkTask.reconstitute(UUID.randomUUID(),
                new WorkTaskType("urn:worktask-type:billing.invoices:payment:process-refund"), subject,
                "title", null, 0, null,
                WorkTaskStatus.DRAFT, null, now, now);

        WorkTask roundTripped = mapper.toDomain(mapper.toStateAvro(original));

        assertNull(roundTripped.description());
        assertNull(roundTripped.deadline());
        assertNull(roundTripped.assigneeId());
    }
}
