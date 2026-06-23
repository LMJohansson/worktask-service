package com.example.worktaskservice.domain.command;

import com.example.worktaskservice.domain.model.GenericInfo;

import java.util.UUID;

public record CompleteWorkTaskCommand(UUID id, UUID correlationId, GenericInfo genericInfo) {}
