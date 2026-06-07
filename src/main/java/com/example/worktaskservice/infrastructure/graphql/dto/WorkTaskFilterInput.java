package com.example.worktaskservice.infrastructure.graphql.dto;

import com.example.worktaskservice.domain.model.WorkTaskStatus;

import java.util.UUID;

public record WorkTaskFilterInput(
        WorkTaskStatus status,
        String type,
        String subjectType,
        UUID subjectId,
        UUID assigneeId) {
}
