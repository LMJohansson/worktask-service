package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record AssignWorkTaskCommand(
        UUID workTaskId,
        UUID correlationId,
        UUID assigneeId  // null = unassign
) {}
