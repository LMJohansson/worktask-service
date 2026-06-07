package com.example.worktaskservice.domain.event;

import com.example.worktaskservice.domain.model.Subject;
import com.example.worktaskservice.domain.model.WorkTaskType;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskCreatedEvent(
        UUID workTaskId,
        UUID correlationId,
        Instant occurredAt,
        WorkTaskType type,
        Subject subject,
        String title,
        String description,
        int priority,
        Instant deadline
) implements WorkTaskEvent {}
