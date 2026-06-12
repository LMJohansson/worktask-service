package com.example.worktaskservice.domain.event;

import java.time.Instant;
import java.util.UUID;

public sealed interface WorkTaskEvent
        permits WorkTaskCreatedEvent, WorkTaskAssignedEvent, WorkTaskReassignedEvent,
                WorkTaskUnassignedEvent, WorkTaskBegunEvent, WorkTaskPausedEvent,
                WorkTaskResumedEvent, WorkTaskCompletedEvent, WorkTaskAbortedEvent,
                WorkTaskCancelledEvent, WorkTaskCommandRejectedEvent {

    UUID id();
    UUID correlationId();
    Instant occurredAt();
}
