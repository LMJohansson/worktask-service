package com.example.worktaskservice.domain.model;

import java.util.regex.Pattern;

public record WorkTaskType(String value) {

    // urn:worktask-type:<domain>(.<subdomain>)?:<bounded-context>:<task-name>
    static final String NID = "worktask-type";
    private static final Pattern PATTERN =
            Pattern.compile("^urn:" + NID + ":" + UrnFormat.NSS + "$");

    public WorkTaskType {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Invalid WorkTaskType URN '%s'; expected urn:%s:<domain>(.<subdomain>)?:<bounded-context>:<task-name>"
                            .formatted(value, NID));
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
