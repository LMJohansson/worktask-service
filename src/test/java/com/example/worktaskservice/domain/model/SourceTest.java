package com.example.worktaskservice.domain.model;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SourceTest {

    @Test
    void acceptsBareSourceWithoutTrailingId() {
        assertDoesNotThrow(() -> new Source("urn:source:work.tasks:worktask"));
        assertDoesNotThrow(() -> new Source("urn:source:billing.invoices:payment"));
    }

    @Test
    void acceptsIntegerTrailingId() {
        var source = new Source("urn:source:billing.invoices:payment:42");
        assertEquals("urn:source:billing.invoices:payment:42", source.value());
    }

    @Test
    void acceptsUuidTrailingId() {
        assertDoesNotThrow(() ->
                new Source("urn:source:billing:invoicing:550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test
    void rejectsMalformedUrns() {
        assertThrows(IllegalArgumentException.class, () -> new Source(null));
        assertThrows(IllegalArgumentException.class, () -> new Source("not-a-urn"));
        // wrong nid
        assertThrows(IllegalArgumentException.class, () -> new Source("urn:subject:work.tasks:worktask"));
        // slash form
        assertThrows(IllegalArgumentException.class, () -> new Source("urn:source:work.tasks/worktask"));
        // trailing id is neither a UUID nor a positive integer
        assertThrows(IllegalArgumentException.class, () -> new Source("urn:source:work.tasks:worktask:abc"));
        assertThrows(IllegalArgumentException.class, () -> new Source("urn:source:work.tasks:worktask:-1"));
        // strict-positive: no zero, no leading zeros
        assertThrows(IllegalArgumentException.class, () -> new Source("urn:source:work.tasks:worktask:0"));
        assertThrows(IllegalArgumentException.class, () -> new Source("urn:source:work.tasks:worktask:007"));
    }
}
