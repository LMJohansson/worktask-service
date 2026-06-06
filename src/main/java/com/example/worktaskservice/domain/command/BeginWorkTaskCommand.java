package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record BeginWorkTaskCommand(UUID workTaskId, UUID correlationId) {}
