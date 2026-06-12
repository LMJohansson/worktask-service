package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record PauseWorkTaskCommand(UUID id, UUID correlationId) {}
