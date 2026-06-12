package com.example.worktaskservice.application.port;

import com.example.worktaskservice.domain.model.SubjectType;
import com.example.worktaskservice.domain.model.WorkTaskStatus;
import com.example.worktaskservice.domain.model.WorkTaskType;

import java.util.UUID;

public record WorkTaskFilter(
        WorkTaskStatus status,
        WorkTaskType type,
        SubjectType subjectType,
        String subjectId,
        UUID assigneeId) {

    public static final WorkTaskFilter EMPTY = new WorkTaskFilter(null, null, null, null, null);
}
