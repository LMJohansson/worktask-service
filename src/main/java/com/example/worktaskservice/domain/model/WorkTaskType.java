package com.example.worktaskservice.domain.model;

import java.util.regex.Pattern;

public record WorkTaskType(String value) {

    private static final Pattern PATTERN =
            Pattern.compile("^[a-z][a-z0-9]*(\\.[a-z][a-z0-9]*)*:[a-z][a-z0-9-]*/[a-z][a-z0-9-]+$");

    public WorkTaskType {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Invalid WorkTaskType format '%s'; expected <domain>(.<subdomain>)?:<bounded-context>/<task-name>"
                            .formatted(value));
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
