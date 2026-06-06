package com.example.worktaskservice.domain.command;

import com.example.worktaskservice.domain.model.Subject;
import com.example.worktaskservice.domain.model.WorkTaskType;

import java.util.UUID;

public record CreateWorkTaskCommand(
        UUID workTaskId,
        UUID correlationId,
        WorkTaskType type,
        Subject subject,
        String title,
        String description
) {}
