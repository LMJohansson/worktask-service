package com.example.worktaskservice.domain.event;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskUnassignedEvent(
        UUID id,
        UUID correlationId,
        Instant occurredAt
) implements WorkTaskEvent {}
