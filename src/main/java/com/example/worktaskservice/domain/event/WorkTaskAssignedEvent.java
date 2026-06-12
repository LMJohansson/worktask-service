package com.example.worktaskservice.domain.event;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskAssignedEvent(
        UUID id,
        UUID correlationId,
        Instant occurredAt,
        UUID assigneeId
) implements WorkTaskEvent {}
