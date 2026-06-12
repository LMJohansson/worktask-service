package com.example.worktaskservice.domain.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SubjectUrnTest {

    private static final SubjectType TYPE =
            new SubjectType("urn:subject-type:billing.invoices:payment:invoice");

    @Test
    void formatsCombinedUrn() {
        var subject = new Subject(TYPE, "550e8400-e29b-41d4-a716-446655440000");

        assertEquals("urn:subject:billing.invoices:payment:invoice:550e8400-e29b-41d4-a716-446655440000",
                subject.toUrn());
    }

    @ParameterizedTest
    @ValueSource(strings = {"42", "42:7", "42:7:3", "1:2:3:4:5", "9999999999999999999999"})
    void acceptsAndRoundTripsNumericSubjectIds(String id) {
        var original = new Subject(TYPE, id);

        assertEquals("urn:subject:billing.invoices:payment:invoice:" + id, original.toUrn());
        assertEquals(original, Subject.fromUrn(original.toUrn()));
    }

    @Test
    void roundTripsUuidSubjectId() {
        var original = new Subject(TYPE, UUID.randomUUID().toString());

        assertEquals(original, Subject.fromUrn(original.toUrn()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "42:", ":7", "4.2", "a:b", "42:7:", "42::7", "-1", "x",
            "0", "01", "007", "1:0", "1:02"})   // strict-positive: no 0, no leading zeros
    void rejectsMalformedSubjectIds(String id) {
        assertThrows(IllegalArgumentException.class, () -> new Subject(TYPE, id));
    }

    @Test
    void rejectsNullSubjectId() {
        assertThrows(IllegalArgumentException.class, () -> new Subject(TYPE, null));
    }

    @Test
    void rejectsMalformedUrns() {
        assertThrows(IllegalArgumentException.class, () -> Subject.fromUrn(null));
        assertThrows(IllegalArgumentException.class, () -> Subject.fromUrn("not-a-urn"));
        // missing id segment
        assertThrows(IllegalArgumentException.class,
                () -> Subject.fromUrn("urn:subject:billing.invoices:payment:invoice"));
        // wrong nid
        assertThrows(IllegalArgumentException.class,
                () -> Subject.fromUrn("urn:subject-type:billing.invoices:payment:invoice:" + UUID.randomUUID()));
        // old slash form
        assertThrows(IllegalArgumentException.class,
                () -> new SubjectType("billing.invoices:payment/invoice"));
    }
}
