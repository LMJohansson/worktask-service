package com.example.worktaskservice.infrastructure.graphql.dto;

import com.example.worktaskservice.domain.model.WorkTask;
import com.example.worktaskservice.domain.model.WorkTaskStatus;

import java.time.Instant;
import java.util.UUID;

public record WorkTaskDto(
        UUID id,
        String type,
        String subject,
        String source,
        String title,
        String description,
        int priority,
        Instant deadline,
        WorkTaskStatus status,
        UUID assigneeId,
        Instant createdAt,
        Instant updatedAt) {

    public static WorkTaskDto from(WorkTask task) {
        return new WorkTaskDto(
                task.id(),
                task.type().value(),
                task.subject().toUrn(),
                task.source().value(),
                task.title(),
                task.description(),
                task.priority(),
                task.deadline(),
                task.status(),
                task.assigneeId(),
                task.createdAt(),
                task.updatedAt());
    }
}
