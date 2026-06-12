package com.example.worktaskservice.domain.event;

import com.example.worktaskservice.domain.model.Source;
import com.example.worktaskservice.domain.model.Subject;
import com.example.worktaskservice.domain.model.WorkTaskType;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskCreatedEvent(
        UUID id,
        UUID correlationId,
        Instant occurredAt,
        WorkTaskType type,
        Subject subject,
        Source source,
        String title,
        String description,
        int priority,
        Instant deadline
) implements WorkTaskEvent {}
