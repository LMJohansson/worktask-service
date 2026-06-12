package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record CancelWorkTaskCommand(UUID id, UUID correlationId, String reason) {}
