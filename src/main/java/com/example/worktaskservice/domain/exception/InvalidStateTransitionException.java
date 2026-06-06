package com.example.worktaskservice.domain.exception;

import com.example.worktaskservice.domain.model.WorkTaskStatus;

public class InvalidStateTransitionException extends RuntimeException {

    private final WorkTaskStatus currentStatus;
    private final String commandType;

    public InvalidStateTransitionException(WorkTaskStatus currentStatus, String commandType) {
        super("Cannot apply %s to a task in state %s".formatted(commandType, currentStatus));
        this.currentStatus = currentStatus;
        this.commandType = commandType;
    }

    public WorkTaskStatus currentStatus() { return currentStatus; }
    public String commandType() { return commandType; }
}
