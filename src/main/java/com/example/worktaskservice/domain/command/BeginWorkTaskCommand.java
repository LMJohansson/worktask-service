package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record BeginWorkTaskCommand(UUID id, UUID correlationId) {}
