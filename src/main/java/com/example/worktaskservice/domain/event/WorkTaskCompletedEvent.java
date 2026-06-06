package com.example.worktaskservice.domain.event;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskCompletedEvent(
        UUID workTaskId,
        UUID correlationId,
        Instant occurredAt
) implements WorkTaskEvent {}
