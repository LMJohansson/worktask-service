package com.example.worktaskservice.domain.model;

import java.util.regex.Pattern;

public record SubjectType(String value) {

    // Same structural format as WorkTaskType: <domain>(.<subdomain>)?:<bounded-context>/<aggregate-name>
    private static final Pattern PATTERN =
            Pattern.compile("^[a-z][a-z0-9]*(\\.[a-z][a-z0-9]*)*:[a-z][a-z0-9-]*/[a-z][a-z0-9-]+$");

    public SubjectType {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Invalid SubjectType format '%s'; expected <domain>(.<subdomain>)?:<bounded-context>/<aggregate-name>"
                            .formatted(value));
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
