package com.example.worktaskservice.domain.model;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SubjectUrnTest {

    @Test
    void formatsCombinedUrn() {
        var id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        var subject = new Subject(new SubjectType("urn:subject-type:billing.invoices:payment:invoice"), id);

        assertEquals("urn:subject:billing.invoices:payment:invoice:550e8400-e29b-41d4-a716-446655440000",
                subject.toUrn());
    }

    @Test
    void roundTripsThroughUrn() {
        var original = new Subject(new SubjectType("urn:subject-type:billing.invoices:payment:invoice"),
                UUID.randomUUID());

        assertEquals(original, Subject.fromUrn(original.toUrn()));
    }

    @Test
    void rejectsMalformedUrns() {
        assertThrows(IllegalArgumentException.class, () -> Subject.fromUrn(null));
        assertThrows(IllegalArgumentException.class, () -> Subject.fromUrn("not-a-urn"));
        // missing uuid segment
        assertThrows(IllegalArgumentException.class,
                () -> Subject.fromUrn("urn:subject:billing.invoices:payment:invoice"));
        // wrong nid
        assertThrows(IllegalArgumentException.class,
                () -> Subject.fromUrn("urn:subject-type:billing.invoices:payment:invoice:"
                        + UUID.randomUUID()));
        // old slash form
        assertThrows(IllegalArgumentException.class,
                () -> new SubjectType("billing.invoices:payment/invoice"));
    }
}
