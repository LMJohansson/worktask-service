package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record AssignWorkTaskCommand(
        UUID id,
        UUID correlationId,
        UUID assigneeId  // null = unassign
) {}
