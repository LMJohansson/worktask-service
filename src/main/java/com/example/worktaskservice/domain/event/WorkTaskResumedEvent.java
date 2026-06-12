package com.example.worktaskservice.domain.event;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskResumedEvent(
        UUID id,
        UUID correlationId,
        Instant occurredAt
) implements WorkTaskEvent {}
