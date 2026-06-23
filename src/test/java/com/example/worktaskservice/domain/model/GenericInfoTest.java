package com.example.worktaskservice.domain.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GenericInfoTest {

    private static GenericInfo info(byte[] data) {
        return new GenericInfo("n", "urn:t", "application/avro", "http://schema", data);
    }

    @Test
    void rejectsBlankOrNullMetadata() {
        assertThrows(IllegalArgumentException.class,
                () -> new GenericInfo(null, "t", "ct", "ds", new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new GenericInfo(" ", "t", "ct", "ds", new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new GenericInfo("n", "", "ct", "ds", new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new GenericInfo("n", "t", null, "ds", new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new GenericInfo("n", "t", "ct", " ", new byte[0]));
    }

    @Test
    void rejectsNullData() {
        assertThrows(IllegalArgumentException.class,
                () -> new GenericInfo("n", "t", "ct", "ds", null));
    }

    @Test
    void equalityIsByValueIncludingData() {
        assertEquals(info(new byte[]{1, 2, 3}), info(new byte[]{1, 2, 3}));
        assertEquals(info(new byte[]{1, 2, 3}).hashCode(), info(new byte[]{1, 2, 3}).hashCode());
        assertNotEquals(info(new byte[]{1, 2, 3}), info(new byte[]{1, 2, 4}));
    }
}
