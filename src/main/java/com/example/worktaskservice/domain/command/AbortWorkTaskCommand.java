package com.example.worktaskservice.domain.command;

import com.example.worktaskservice.domain.model.GenericInfo;

import java.util.UUID;

public record AbortWorkTaskCommand(UUID id, UUID correlationId, String reason, GenericInfo genericInfo) {}
