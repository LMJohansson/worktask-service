package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record CompleteWorkTaskCommand(UUID workTaskId, UUID correlationId) {}
