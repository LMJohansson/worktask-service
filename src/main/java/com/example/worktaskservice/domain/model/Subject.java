package com.example.worktaskservice.domain.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record Subject(SubjectType type, String id) {

    // urn:subject:<NSS>:<subject-id> — the combined wire form; <NSS> is shared with SubjectType.
    private static final String NID = "subject";
    // The subject id is a canonical UUID, or a colon-delimited sequence of positive integers
    // (arbitrary size/length, no leading zeros) — e.g. "42", "42:7", "42:7:3". It is a string
    // everywhere it matters: the URN tail and the Kafka partition key (so "01" ≠ "1").
    private static final String ID = "(?:" + UrnFormat.UUID + "|[1-9][0-9]*(?::[1-9][0-9]*)*)";
    private static final Pattern ID_PATTERN = Pattern.compile("^" + ID + "$");
    private static final Pattern URN_PATTERN =
            Pattern.compile("^urn:" + NID + ":(" + UrnFormat.NSS + "):(" + ID + ")$");

    public Subject {
        if (type == null) throw new IllegalArgumentException("subject type must not be null");
        if (id == null || !ID_PATTERN.matcher(id).matches()) {
            throw new IllegalArgumentException(
                    "Invalid subject id '%s'; expected a UUID or a colon-delimited sequence of positive integers"
                            .formatted(id));
        }
    }

    /** Combined URN: {@code urn:subject:<domain>(.<subdomain>)?:<bounded-context>:<aggregate>:<subject-id>}. */
    public String toUrn() {
        return "urn:" + NID + ":" + type.nss() + ":" + id;
    }

    /** Parses the combined {@code urn:subject:<NSS>:<subject-id>} form. */
    public static Subject fromUrn(String urn) {
        Matcher m = urn == null ? null : URN_PATTERN.matcher(urn);
        if (m == null || !m.matches()) {
            throw new IllegalArgumentException(
                    "Invalid Subject URN '%s'; expected urn:%s:<domain>(.<subdomain>)?:<bounded-context>:<aggregate>:<subject-id>"
                            .formatted(urn, NID));
        }
        return new Subject(new SubjectType("urn:" + SubjectType.NID + ":" + m.group(1)), m.group(2));
    }
}
