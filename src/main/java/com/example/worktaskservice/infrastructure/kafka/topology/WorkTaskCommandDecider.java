package com.example.worktaskservice.infrastructure.kafka.topology;

import com.example.worktaskservice.domain.command.CreateWorkTaskCommand;
import com.example.worktaskservice.domain.event.WorkTaskCommandRejectedEvent;
import com.example.worktaskservice.domain.event.WorkTaskCreatedEvent;
import com.example.worktaskservice.domain.event.WorkTaskEvent;
import com.example.worktaskservice.domain.exception.InvalidStateTransitionException;
import com.example.worktaskservice.domain.model.Subject;
import com.example.worktaskservice.domain.model.WorkTask;
import com.example.worktaskservice.domain.model.WorkTaskStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Pure decision logic for the {@code StateTransitionProcessor}: given a command, the current
 * WorkTask state, and whether an active duplicate already exists on the subject, it decides the
 * resulting domain event, the new materialized state, and the uniqueness-index update.
 *
 * <p>Free of Kafka, state stores, the schema registry, and any I/O — the processor supplies the
 * store/index lookups and applies the side effects — so the uniqueness rule and the state machine
 * can be unit-tested directly with plain objects.
 */
final class WorkTaskCommandDecider {

    /** Rejection reason emitted when an active task of the same type already exists on a subject. */
    static final String DUPLICATE_REASON = "DUPLICATE_ACTIVE_TASK_FOR_SUBJECT";

    /** How the processor should update the {@code subject-active-index} after an accepted command. */
    enum IndexUpdate { NONE, ADD, REMOVE }

    sealed interface Outcome permits Accepted, Rejected, Dropped {}

    /** The command was applied: emit {@code event}, persist {@code state}, and apply {@code index}. */
    record Accepted(WorkTaskEvent event, WorkTask state, IndexUpdate index) implements Outcome {}

    /** The command was rejected: emit {@code event}; {@code subject} tags the CloudEvents headers. */
    record Rejected(WorkTaskCommandRejectedEvent event, Subject subject) implements Outcome {}

    /** No state and no subject to act on (a non-create command for an unknown task): drop silently. */
    record Dropped() implements Outcome {}

    private WorkTaskCommandDecider() {}

    static Outcome decide(Object command, UUID workTaskId, WorkTask current,
                          boolean activeDuplicateExists, Instant now) {
        if (command instanceof CreateWorkTaskCommand cmd) {
            if (activeDuplicateExists) {
                return new Rejected(reject(workTaskId, cmd, DUPLICATE_REASON, now), cmd.subject());
            }
            WorkTaskCreatedEvent event = WorkTask.create(cmd, now);
            WorkTask state = WorkTask.reconstitute(
                    event.id(), event.type(), event.subject(), event.source(),
                    event.title(), event.description(), event.priority(), event.deadline(),
                    WorkTaskStatus.DRAFT, null, now, now);
            return new Accepted(event, state, IndexUpdate.ADD);
        }
        if (current == null) {
            return new Dropped();
        }
        try {
            WorkTaskEvent event = current.apply(command, now);
            IndexUpdate index = isTerminal(current.status()) ? IndexUpdate.REMOVE : IndexUpdate.NONE;
            return new Accepted(event, current, index);
        } catch (InvalidStateTransitionException e) {
            return new Rejected(reject(workTaskId, command, e.getMessage(), now), current.subject());
        }
    }

    private static WorkTaskCommandRejectedEvent reject(UUID workTaskId, Object cmd, String reason, Instant now) {
        return new WorkTaskCommandRejectedEvent(
                workTaskId, UUID.randomUUID(), now, cmd.getClass().getSimpleName(), reason);
    }

    static boolean isTerminal(WorkTaskStatus status) {
        return status == WorkTaskStatus.COMPLETED
                || status == WorkTaskStatus.ABORTED
                || status == WorkTaskStatus.CANCELLED;
    }
}
