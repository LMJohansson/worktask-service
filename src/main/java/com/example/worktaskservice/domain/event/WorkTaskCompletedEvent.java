package com.example.worktaskservice.domain.event;

import com.example.worktaskservice.domain.model.GenericInfo;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskCompletedEvent(
        UUID id,
        UUID correlationId,
        Instant occurredAt,
        GenericInfo genericInfo
) implements WorkTaskEvent {}
