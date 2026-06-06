package com.example.worktaskservice.domain.command;

import java.util.UUID;

public record ResumeWorkTaskCommand(UUID workTaskId, UUID correlationId) {}
