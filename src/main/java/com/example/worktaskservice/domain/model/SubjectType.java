package com.example.worktaskservice.domain.model;

import java.util.regex.Pattern;

public record SubjectType(String value) {

    // urn:subject-type:<domain>(.<subdomain>)?:<bounded-context>:<aggregate-name>
    // Shares its NSS with the urn:subject:<NSS>:<uuid> form used by Subject.
    static final String NID = "subject-type";
    private static final String PREFIX = "urn:" + NID + ":";
    private static final Pattern PATTERN =
            Pattern.compile("^" + PREFIX + UrnFormat.NSS + "$");

    public SubjectType {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Invalid SubjectType URN '%s'; expected urn:%s:<domain>(.<subdomain>)?:<bounded-context>:<aggregate-name>"
                            .formatted(value, NID));
        }
    }

    /** The namespace-specific string: the part after {@code urn:subject-type:}. */
    String nss() {
        return value.substring(PREFIX.length());
    }

    @Override
    public String toString() {
        return value;
    }
}
