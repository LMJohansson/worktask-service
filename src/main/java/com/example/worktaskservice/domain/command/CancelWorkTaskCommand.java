package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record CancelWorkTaskCommand(UUID workTaskId, UUID correlationId, String reason) {}
