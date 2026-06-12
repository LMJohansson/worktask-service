package com.example.worktaskservice.application;

import com.example.worktaskservice.application.port.WorkTaskEventPublisher;
import com.example.worktaskservice.application.port.WorkTaskRepository;
import com.example.worktaskservice.domain.command.CreateWorkTaskCommand;
import com.example.worktaskservice.domain.event.WorkTaskCreatedEvent;
import com.example.worktaskservice.domain.event.WorkTaskEvent;
import com.example.worktaskservice.domain.model.WorkTask;

import java.time.Instant;
import java.util.UUID;

public class WorkTaskCommandHandler {

    private final WorkTaskRepository repository;
    private final WorkTaskEventPublisher publisher;

    public WorkTaskCommandHandler(WorkTaskRepository repository, WorkTaskEventPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    public WorkTaskEvent handle(CreateWorkTaskCommand cmd, Instant now) {
        WorkTaskCreatedEvent event = WorkTask.create(cmd, now);
        WorkTask task = applyCreated(event, now);
        repository.save(task);
        publisher.publish(event);
        return event;
    }

    public WorkTaskEvent handle(Object cmd, Instant now) {
        UUID id = extractWorkTaskId(cmd);
        WorkTask task = repository.find(id)
                .orElseThrow(() -> new IllegalArgumentException("WorkTask not found: " + id));
        WorkTaskEvent event = task.apply(cmd, now);
        repository.save(task);
        publisher.publish(event);
        return event;
    }

    private WorkTask applyCreated(WorkTaskCreatedEvent e, Instant now) {
        return WorkTask.reconstitute(
                e.id(), e.type(), e.subject(), e.source(),
                e.title(), e.description(), e.priority(), e.deadline(),
                com.example.worktaskservice.domain.model.WorkTaskStatus.DRAFT,
                null, now, now);
    }

    private java.util.UUID extractWorkTaskId(Object cmd) {
        return switch (cmd) {
            case com.example.worktaskservice.domain.command.AssignWorkTaskCommand c -> c.id();
            case com.example.worktaskservice.domain.command.BeginWorkTaskCommand c -> c.id();
            case com.example.worktaskservice.domain.command.PauseWorkTaskCommand c -> c.id();
            case com.example.worktaskservice.domain.command.ResumeWorkTaskCommand c -> c.id();
            case com.example.worktaskservice.domain.command.CompleteWorkTaskCommand c -> c.id();
            case com.example.worktaskservice.domain.command.AbortWorkTaskCommand c -> c.id();
            case com.example.worktaskservice.domain.command.CancelWorkTaskCommand c -> c.id();
            default -> throw new IllegalArgumentException("Unknown command: " + cmd.getClass().getSimpleName());
        };
    }
}
