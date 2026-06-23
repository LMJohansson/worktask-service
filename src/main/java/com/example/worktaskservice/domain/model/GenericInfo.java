package com.example.worktaskservice.domain.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * A generic, self-describing payload attached to a WorkTask — analogous to the {@code data} section of
 * a CloudEvent. {@code data} is an opaque (typically Avro-encoded) object; {@code name}, {@code type},
 * {@code datacontenttype} and {@code dataschema} describe it so a recipient can unmarshal it.
 *
 * <p>A WorkTask may carry no {@code GenericInfo} at all, but when present every field is mandatory —
 * this guarantees a recipient always has what it needs to interpret {@code data}.
 *
 * <p>Set on {@code CreateWorkTask} and re-suppliable by the terminating commands
 * (Complete/Abort/Cancel), which send a result back; see {@link WorkTask}.
 */
public record GenericInfo(String name, String type, String datacontenttype, String dataschema, byte[] data) {

    public GenericInfo {
        requireText(name, "name");
        requireText(type, "type");
        requireText(datacontenttype, "datacontenttype");
        requireText(dataschema, "dataschema");
        if (data == null) {
            throw new IllegalArgumentException("GenericInfo data must not be null");
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("GenericInfo " + field + " must not be blank");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericInfo other)) return false;
        return name.equals(other.name)
                && type.equals(other.type)
                && datacontenttype.equals(other.datacontenttype)
                && dataschema.equals(other.dataschema)
                && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(name, type, datacontenttype, dataschema) + Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "GenericInfo[name=%s, type=%s, datacontenttype=%s, dataschema=%s, data=%d bytes]"
                .formatted(name, type, datacontenttype, dataschema, data.length);
    }
}
