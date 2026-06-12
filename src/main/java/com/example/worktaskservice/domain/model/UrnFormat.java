package com.example.worktaskservice.domain.model;

/**
 * Shared building blocks for the URN identifiers used by {@link WorkTaskType},
 * {@link SubjectType}, {@link Subject}, and {@link Source}.
 *
 * <p>The first three share a common namespace-specific string (NSS) of the form
 * {@code <domain>(.<subdomain>)?:<bounded-context>:<name>}, differing only by their
 * URN namespace identifier (NID): {@code worktask-type}, {@code subject-type}, and
 * {@code subject} (the last additionally appending a UUID segment). {@link Source} uses
 * the shorter {@link #DOMAIN_CONTEXT} prefix (no {@code <name>} segment).
 */
final class UrnFormat {

    private UrnFormat() {}

    /** {@code <domain>(.<subdomain>)?:<bounded-context>} — no anchors, no capturing groups. */
    static final String DOMAIN_CONTEXT = "[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9]*)*:[a-z][a-z0-9-]*";

    /** {@code <domain>(.<subdomain>)?:<bounded-context>:<name>} — no anchors, no capturing groups. */
    static final String NSS = DOMAIN_CONTEXT + ":[a-z][a-z0-9-]+";

    /** Canonical UUID textual representation. */
    static final String UUID = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
}
