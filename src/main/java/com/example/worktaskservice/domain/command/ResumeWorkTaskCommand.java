package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record ResumeWorkTaskCommand(UUID id, UUID correlationId) {}
