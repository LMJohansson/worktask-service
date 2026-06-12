package com.example.worktaskservice.domain.model;

import java.util.regex.Pattern;

/**
 * The originating source of a WorkTask, recorded as a durable domain attribute.
 *
 * <p>Format: {@code urn:source:<domain>(.<subdomain>)?:<bounded-context>(:<id>)?}, where the
 * optional trailing {@code <id>} identifies a specific originating instance and is either a
 * canonical UUID or a non-negative integer — e.g. {@code urn:source:work.tasks:worktask},
 * {@code urn:source:billing.invoices:payment:42}, or
 * {@code urn:source:billing:invoicing:550e8400-e29b-41d4-a716-446655440000}.
 *
 * <p>Distinct from the CloudEvents {@code ce_source} envelope header (the messaging origin),
 * which is propagated independently.
 */
public record Source(String value) {

    // urn:source:<domain>(.<subdomain>)?:<bounded-context>(:<uuid|non-negative-integer>)?
    static final String NID = "source";
    private static final String ID = "(?:" + UrnFormat.UUID + "|[0-9]+)";
    private static final Pattern PATTERN =
            Pattern.compile("^urn:" + NID + ":" + UrnFormat.DOMAIN_CONTEXT + "(?::" + ID + ")?$");

    public Source {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Invalid Source URN '%s'; expected urn:%s:<domain>(.<subdomain>)?:<bounded-context>(:<uuid|number>)?"
                            .formatted(value, NID));
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
