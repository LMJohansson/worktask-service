package com.example.worktaskservice.domain.model;

import com.example.worktaskservice.domain.command.*;
import com.example.worktaskservice.domain.event.*;
import com.example.worktaskservice.domain.exception.InvalidStateTransitionException;

import java.time.Instant;
import java.util.UUID;

public final class WorkTask {

    private final UUID id;
    private final WorkTaskType type;
    private final Subject subject;
    private String title;
    private String description;
    private WorkTaskStatus status;
    private UUID assigneeId;
    private final Instant createdAt;
    private Instant updatedAt;

    private WorkTask(UUID id, WorkTaskType type, Subject subject,
                     String title, String description, Instant createdAt) {
        this.id = id;
        this.type = type;
        this.subject = subject;
        this.title = title;
        this.description = description;
        this.status = WorkTaskStatus.DRAFT;
        this.assigneeId = null;
        this.createdAt = createdAt;
        this.updatedAt = createdAt;
    }

    public static WorkTaskCreatedEvent create(CreateWorkTaskCommand cmd, Instant now) {
        return new WorkTaskCreatedEvent(
                cmd.workTaskId(), cmd.correlationId(), now,
                cmd.type(), cmd.subject(), cmd.title(), cmd.description());
    }

    public static WorkTask reconstitute(UUID id, WorkTaskType type, Subject subject,
                                        String title, String description,
                                        WorkTaskStatus status, UUID assigneeId,
                                        Instant createdAt, Instant updatedAt) {
        var task = new WorkTask(id, type, subject, title, description, createdAt);
        task.status = status;
        task.assigneeId = assigneeId;
        task.updatedAt = updatedAt;
        return task;
    }

    public WorkTaskEvent apply(Object command, Instant now) {
        return switch (command) {
            case CreateWorkTaskCommand cmd -> throw new InvalidStateTransitionException(status, "Create");
            case AssignWorkTaskCommand cmd -> applyAssign(cmd, now);
            case BeginWorkTaskCommand cmd -> {
                requireStatus(cmd, WorkTaskStatus.ASSIGNED);
                status = WorkTaskStatus.IN_PROGRESS;
                updatedAt = now;
                yield new WorkTaskBegunEvent(id, cmd.correlationId(), now);
            }
            case PauseWorkTaskCommand cmd -> {
                requireStatus(cmd, WorkTaskStatus.IN_PROGRESS);
                status = WorkTaskStatus.PAUSED;
                updatedAt = now;
                yield new WorkTaskPausedEvent(id, cmd.correlationId(), now);
            }
            case ResumeWorkTaskCommand cmd -> {
                requireStatus(cmd, WorkTaskStatus.PAUSED);
                status = WorkTaskStatus.IN_PROGRESS;
                updatedAt = now;
                yield new WorkTaskResumedEvent(id, cmd.correlationId(), now);
            }
            case CompleteWorkTaskCommand cmd -> {
                requireStatus(cmd, WorkTaskStatus.IN_PROGRESS);
                status = WorkTaskStatus.COMPLETED;
                updatedAt = now;
                yield new WorkTaskCompletedEvent(id, cmd.correlationId(), now);
            }
            case AbortWorkTaskCommand cmd -> {
                requireOneOf(cmd, WorkTaskStatus.ASSIGNED, WorkTaskStatus.IN_PROGRESS, WorkTaskStatus.PAUSED);
                status = WorkTaskStatus.ABORTED;
                updatedAt = now;
                yield new WorkTaskAbortedEvent(id, cmd.correlationId(), now);
            }
            case CancelWorkTaskCommand cmd -> {
                requireOneOf(cmd, WorkTaskStatus.DRAFT, WorkTaskStatus.ASSIGNED,
                        WorkTaskStatus.IN_PROGRESS, WorkTaskStatus.PAUSED);
                status = WorkTaskStatus.CANCELLED;
                updatedAt = now;
                yield new WorkTaskCancelledEvent(id, cmd.correlationId(), now);
            }
            default -> throw new IllegalArgumentException("Unknown command type: " + command.getClass().getSimpleName());
        };
    }

    private WorkTaskEvent applyAssign(AssignWorkTaskCommand cmd, Instant now) {
        if (cmd.assigneeId() != null) {
            requireOneOf(cmd, WorkTaskStatus.DRAFT, WorkTaskStatus.ASSIGNED,
                    WorkTaskStatus.IN_PROGRESS, WorkTaskStatus.PAUSED);
            boolean wasUnassigned = status == WorkTaskStatus.DRAFT;
            assigneeId = cmd.assigneeId();
            status = WorkTaskStatus.ASSIGNED;
            updatedAt = now;
            return wasUnassigned
                    ? new WorkTaskAssignedEvent(id, cmd.correlationId(), now, assigneeId)
                    : new WorkTaskReassignedEvent(id, cmd.correlationId(), now, assigneeId);
        } else {
            requireOneOf(cmd, WorkTaskStatus.ASSIGNED, WorkTaskStatus.IN_PROGRESS, WorkTaskStatus.PAUSED);
            assigneeId = null;
            status = WorkTaskStatus.DRAFT;
            updatedAt = now;
            return new WorkTaskUnassignedEvent(id, cmd.correlationId(), now);
        }
    }

    private void requireStatus(Object cmd, WorkTaskStatus required) {
        if (status != required) {
            throw new InvalidStateTransitionException(status, cmd.getClass().getSimpleName());
        }
    }

    private void requireOneOf(Object cmd, WorkTaskStatus... allowed) {
        for (WorkTaskStatus s : allowed) {
            if (status == s) return;
        }
        throw new InvalidStateTransitionException(status, cmd.getClass().getSimpleName());
    }

    public UUID id() { return id; }
    public WorkTaskType type() { return type; }
    public Subject subject() { return subject; }
    public String title() { return title; }
    public String description() { return description; }
    public WorkTaskStatus status() { return status; }
    public UUID assigneeId() { return assigneeId; }
    public Instant createdAt() { return createdAt; }
    public Instant updatedAt() { return updatedAt; }
}
