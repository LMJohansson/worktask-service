package com.example.worktaskservice.domain.model;

import com.example.worktaskservice.domain.command.*;
import com.example.worktaskservice.domain.event.*;
import com.example.worktaskservice.domain.exception.InvalidStateTransitionException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WorkTaskStateMachineTest {

    private static final UUID ID      = UUID.randomUUID();
    private static final UUID CORR    = UUID.randomUUID();
    private static final UUID ASSIGNEE = UUID.randomUUID();
    private static final WorkTaskType TYPE    = new WorkTaskType("urn:worktask-type:billing.invoices:payment:process-refund");
    private static final Subject      SUBJECT = new Subject(new SubjectType("urn:subject-type:billing.invoices:payment:invoice"), UUID.randomUUID());
    private static final Instant      NOW     = Instant.now();

    private WorkTask taskInState(WorkTaskStatus status) {
        UUID assigneeId = status == WorkTaskStatus.DRAFT ? null : ASSIGNEE;
        return WorkTask.reconstitute(ID, TYPE, SUBJECT, "title", null, 1, NOW.plusSeconds(3600),
                status, assigneeId, NOW, NOW);
    }

    // -------------------------------------------------------------------------
    // Create
    // -------------------------------------------------------------------------

    @Nested class Create {

        @Test void producesWorkTaskCreatedEvent() {
            Instant deadline = NOW.plusSeconds(3600);
            var cmd = new CreateWorkTaskCommand(ID, CORR, TYPE, SUBJECT, "My Task", "desc", 5, deadline);
            WorkTaskCreatedEvent event = WorkTask.create(cmd, NOW);

            assertInstanceOf(WorkTaskCreatedEvent.class, event);
            assertEquals(ID,      event.workTaskId());
            assertEquals(CORR,    event.correlationId());
            assertEquals(NOW,     event.occurredAt());
            assertEquals(TYPE,    event.type());
            assertEquals(SUBJECT, event.subject());
            assertEquals("My Task", event.title());
            assertEquals("desc",    event.description());
            assertEquals(5,        event.priority());
            assertEquals(deadline, event.deadline());
        }

        @Test void applyCreateOnExistingTaskThrows() {
            var cmd = new CreateWorkTaskCommand(ID, CORR, TYPE, SUBJECT, "x", null, 0, null);
            var task = taskInState(WorkTaskStatus.DRAFT);

            var ex = assertThrows(InvalidStateTransitionException.class,
                    () -> task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.DRAFT, ex.currentStatus());
        }
    }

    // -------------------------------------------------------------------------
    // Assign / Reassign / Unassign
    // -------------------------------------------------------------------------

    @Nested class Assign {

        @Test void fromDraftProducesAssignedEvent() {
            var task = taskInState(WorkTaskStatus.DRAFT);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, ASSIGNEE);

            var event = assertInstanceOf(WorkTaskAssignedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.ASSIGNED, task.status());
            assertEquals(ASSIGNEE, task.assigneeId());
            assertEquals(ASSIGNEE, event.assigneeId());
            assertEquals(ID, event.workTaskId());
        }

        @Test void fromAssignedProducesReassignedEvent() {
            var task = taskInState(WorkTaskStatus.ASSIGNED);
            UUID newAssignee = UUID.randomUUID();
            var cmd = new AssignWorkTaskCommand(ID, CORR, newAssignee);

            var event = assertInstanceOf(WorkTaskReassignedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.ASSIGNED, task.status());
            assertEquals(newAssignee, task.assigneeId());
            assertEquals(newAssignee, event.assigneeId());
        }

        @Test void fromInProgressProducesReassignedEvent() {
            var task = taskInState(WorkTaskStatus.IN_PROGRESS);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, ASSIGNEE);

            assertInstanceOf(WorkTaskReassignedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.ASSIGNED, task.status());
        }

        @Test void fromPausedProducesReassignedEvent() {
            var task = taskInState(WorkTaskStatus.PAUSED);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, ASSIGNEE);

            assertInstanceOf(WorkTaskReassignedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.ASSIGNED, task.status());
        }

        @Test void unassignFromAssignedProducesUnassignedEvent() {
            var task = taskInState(WorkTaskStatus.ASSIGNED);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, null);

            assertInstanceOf(WorkTaskUnassignedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.DRAFT, task.status());
            assertNull(task.assigneeId());
        }

        @Test void unassignFromInProgressProducesUnassignedEvent() {
            var task = taskInState(WorkTaskStatus.IN_PROGRESS);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, null);

            assertInstanceOf(WorkTaskUnassignedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.DRAFT, task.status());
        }

        @Test void unassignFromPausedProducesUnassignedEvent() {
            var task = taskInState(WorkTaskStatus.PAUSED);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, null);

            assertInstanceOf(WorkTaskUnassignedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.DRAFT, task.status());
        }

        @Test void unassignFromDraftThrows() {
            var task = taskInState(WorkTaskStatus.DRAFT);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, null);

            var ex = assertThrows(InvalidStateTransitionException.class,
                    () -> task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.DRAFT, ex.currentStatus());
        }

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"COMPLETED", "ABORTED", "CANCELLED"})
        void assignInTerminalStateThrows(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, ASSIGNEE);

            assertThrows(InvalidStateTransitionException.class, () -> task.apply(cmd, NOW));
        }

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"COMPLETED", "ABORTED", "CANCELLED"})
        void unassignInTerminalStateThrows(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new AssignWorkTaskCommand(ID, CORR, null);

            assertThrows(InvalidStateTransitionException.class, () -> task.apply(cmd, NOW));
        }
    }

    // -------------------------------------------------------------------------
    // Begin
    // -------------------------------------------------------------------------

    @Nested class Begin {

        @Test void fromAssignedProducesBegunEvent() {
            var task = taskInState(WorkTaskStatus.ASSIGNED);
            var cmd  = new BeginWorkTaskCommand(ID, CORR);

            var event = assertInstanceOf(WorkTaskBegunEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.IN_PROGRESS, task.status());
            assertEquals(ID, event.workTaskId());
        }

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"DRAFT", "IN_PROGRESS", "PAUSED", "COMPLETED", "ABORTED", "CANCELLED"})
        void fromNonAssignedThrows(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new BeginWorkTaskCommand(ID, CORR);

            var ex = assertThrows(InvalidStateTransitionException.class,
                    () -> task.apply(cmd, NOW));
            assertEquals(status, ex.currentStatus());
        }
    }

    // -------------------------------------------------------------------------
    // Pause
    // -------------------------------------------------------------------------

    @Nested class Pause {

        @Test void fromInProgressProducesPausedEvent() {
            var task = taskInState(WorkTaskStatus.IN_PROGRESS);
            var cmd  = new PauseWorkTaskCommand(ID, CORR);

            var event = assertInstanceOf(WorkTaskPausedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.PAUSED, task.status());
            assertEquals(ID, event.workTaskId());
        }

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"DRAFT", "ASSIGNED", "PAUSED", "COMPLETED", "ABORTED", "CANCELLED"})
        void fromNonInProgressThrows(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new PauseWorkTaskCommand(ID, CORR);

            var ex = assertThrows(InvalidStateTransitionException.class,
                    () -> task.apply(cmd, NOW));
            assertEquals(status, ex.currentStatus());
        }
    }

    // -------------------------------------------------------------------------
    // Resume
    // -------------------------------------------------------------------------

    @Nested class Resume {

        @Test void fromPausedProducesResumedEvent() {
            var task = taskInState(WorkTaskStatus.PAUSED);
            var cmd  = new ResumeWorkTaskCommand(ID, CORR);

            var event = assertInstanceOf(WorkTaskResumedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.IN_PROGRESS, task.status());
            assertEquals(ID, event.workTaskId());
        }

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"DRAFT", "ASSIGNED", "IN_PROGRESS", "COMPLETED", "ABORTED", "CANCELLED"})
        void fromNonPausedThrows(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new ResumeWorkTaskCommand(ID, CORR);

            var ex = assertThrows(InvalidStateTransitionException.class,
                    () -> task.apply(cmd, NOW));
            assertEquals(status, ex.currentStatus());
        }
    }

    // -------------------------------------------------------------------------
    // Complete
    // -------------------------------------------------------------------------

    @Nested class Complete {

        @Test void fromInProgressProducesCompletedEvent() {
            var task = taskInState(WorkTaskStatus.IN_PROGRESS);
            var cmd  = new CompleteWorkTaskCommand(ID, CORR);

            var event = assertInstanceOf(WorkTaskCompletedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.COMPLETED, task.status());
            assertEquals(ID, event.workTaskId());
        }

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"DRAFT", "ASSIGNED", "PAUSED", "COMPLETED", "ABORTED", "CANCELLED"})
        void fromNonInProgressThrows(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new CompleteWorkTaskCommand(ID, CORR);

            assertThrows(InvalidStateTransitionException.class, () -> task.apply(cmd, NOW));
        }
    }

    // -------------------------------------------------------------------------
    // Abort
    // -------------------------------------------------------------------------

    @Nested class Abort {

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"ASSIGNED", "IN_PROGRESS", "PAUSED"})
        void fromActiveStateProducesAbortedEvent(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new AbortWorkTaskCommand(ID, CORR, "external dependency failed");

            var event = assertInstanceOf(WorkTaskAbortedEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.ABORTED, task.status());
            assertEquals(ID, event.workTaskId());
        }

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"DRAFT", "COMPLETED", "ABORTED", "CANCELLED"})
        void fromNonActiveStateThrows(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new AbortWorkTaskCommand(ID, CORR, null);

            var ex = assertThrows(InvalidStateTransitionException.class,
                    () -> task.apply(cmd, NOW));
            assertEquals(status, ex.currentStatus());
        }
    }

    // -------------------------------------------------------------------------
    // Cancel
    // -------------------------------------------------------------------------

    @Nested class Cancel {

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"DRAFT", "ASSIGNED", "IN_PROGRESS", "PAUSED"})
        void fromCancellableStateProducesCancelledEvent(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new CancelWorkTaskCommand(ID, CORR, "no longer needed");

            var event = assertInstanceOf(WorkTaskCancelledEvent.class, task.apply(cmd, NOW));
            assertEquals(WorkTaskStatus.CANCELLED, task.status());
            assertEquals(ID, event.workTaskId());
        }

        @ParameterizedTest
        @EnumSource(value = WorkTaskStatus.class, names = {"COMPLETED", "ABORTED", "CANCELLED"})
        void fromTerminalStateThrows(WorkTaskStatus status) {
            var task = taskInState(status);
            var cmd  = new CancelWorkTaskCommand(ID, CORR, null);

            var ex = assertThrows(InvalidStateTransitionException.class,
                    () -> task.apply(cmd, NOW));
            assertEquals(status, ex.currentStatus());
        }
    }

    // -------------------------------------------------------------------------
    // Shared event field contract
    // -------------------------------------------------------------------------

    @Nested class EventFields {

        @Test void correlationIdAndOccurredAtPropagated() {
            var task = taskInState(WorkTaskStatus.ASSIGNED);
            Instant ts  = Instant.parse("2026-01-01T00:00:00Z");
            var event = task.apply(new BeginWorkTaskCommand(ID, CORR), ts);

            assertEquals(ID,   event.workTaskId());
            assertEquals(CORR, event.correlationId());
            assertEquals(ts,   event.occurredAt());
        }

        @Test void updatedAtAdvancesOnApply() {
            var task = taskInState(WorkTaskStatus.ASSIGNED);
            Instant later = NOW.plusSeconds(60);
            task.apply(new BeginWorkTaskCommand(ID, CORR), later);

            assertEquals(later, task.updatedAt());
        }

        @Test void createdAtNeverChanges() {
            var task = taskInState(WorkTaskStatus.ASSIGNED);
            task.apply(new BeginWorkTaskCommand(ID, CORR), NOW.plusSeconds(10));
            task.apply(new PauseWorkTaskCommand(ID, CORR), NOW.plusSeconds(20));

            assertEquals(NOW, task.createdAt());
        }
    }
}
