package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record AbortWorkTaskCommand(UUID workTaskId, UUID correlationId, String reason) {}
