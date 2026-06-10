package com.example.worktaskservice.domain.model;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record Subject(SubjectType type, UUID id) {

    // urn:subject:<NSS>:<uuid> — the combined wire form; <NSS> is shared with SubjectType.
    private static final String NID = "subject";
    private static final Pattern URN_PATTERN =
            Pattern.compile("^urn:" + NID + ":(" + UrnFormat.NSS + "):(" + UrnFormat.UUID + ")$");

    public Subject {
        if (type == null) throw new IllegalArgumentException("subject type must not be null");
        if (id == null) throw new IllegalArgumentException("subject id must not be null");
    }

    /** Combined URN: {@code urn:subject:<domain>(.<subdomain>)?:<bounded-context>:<aggregate>:<uuid>}. */
    public String toUrn() {
        return "urn:" + NID + ":" + type.nss() + ":" + id;
    }

    /** Parses the combined {@code urn:subject:<NSS>:<uuid>} form. */
    public static Subject fromUrn(String urn) {
        Matcher m = urn == null ? null : URN_PATTERN.matcher(urn);
        if (m == null || !m.matches()) {
            throw new IllegalArgumentException(
                    "Invalid Subject URN '%s'; expected urn:%s:<domain>(.<subdomain>)?:<bounded-context>:<aggregate>:<uuid>"
                            .formatted(urn, NID));
        }
        return new Subject(new SubjectType("urn:" + SubjectType.NID + ":" + m.group(1)),
                UUID.fromString(m.group(2)));
    }
}
