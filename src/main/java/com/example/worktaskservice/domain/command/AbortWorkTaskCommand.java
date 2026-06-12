package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record AbortWorkTaskCommand(UUID id, UUID correlationId, String reason) {}
