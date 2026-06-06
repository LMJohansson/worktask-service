package com.example.worktaskservice.domain.event;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskCommandRejectedEvent(
        UUID workTaskId,
        UUID correlationId,
        Instant occurredAt,
        String commandType,
        String rejectionReason
) implements WorkTaskEvent {}
