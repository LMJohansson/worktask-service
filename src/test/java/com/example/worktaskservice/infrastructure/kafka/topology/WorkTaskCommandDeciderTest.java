package com.example.worktaskservice.infrastructure.kafka.topology;

import com.example.worktaskservice.domain.command.*;
import com.example.worktaskservice.domain.event.WorkTaskBegunEvent;
import com.example.worktaskservice.domain.event.WorkTaskCreatedEvent;
import com.example.worktaskservice.domain.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.util.UUID;

import static com.example.worktaskservice.infrastructure.kafka.topology.WorkTaskCommandDecider.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the subject-uniqueness rule and the state-machine decision the
 * {@code StateTransitionProcessor} delegates to — pure, registry-free, no Kafka.
 */
class WorkTaskCommandDeciderTest {

    private static final UUID ID      = UUID.randomUUID();
    private static final UUID CORR    = UUID.randomUUID();
    private static final WorkTaskType TYPE   = new WorkTaskType("urn:worktask-type:billing.invoices:payment:process-refund");
    private static final Subject     SUBJECT = new Subject(new SubjectType("urn:subject-type:billing.invoices:payment:invoice"), UUID.randomUUID().toString());
    private static final Source      SOURCE  = new Source("urn:source:billing.invoices:payment:42");
    private static final Instant     NOW     = Instant.now();

    private static CreateWorkTaskCommand create() {
        return new CreateWorkTaskCommand(ID, CORR, TYPE, SUBJECT, SOURCE, "Process refund", null, 0, null);
    }

    private static WorkTask taskInState(WorkTaskStatus status) {
        UUID assigneeId = status == WorkTaskStatus.DRAFT ? null : UUID.randomUUID();
        return WorkTask.reconstitute(ID, TYPE, SUBJECT, SOURCE, "title", null, 0, null,
                status, assigneeId, NOW, NOW);
    }

    // ---- Create + uniqueness rule -------------------------------------------------

    @Test
    void acceptsCreateWhenNoActiveDuplicate() {
        var outcome = decide(create(), ID, null, false, NOW);

        var accepted = assertInstanceOf(Accepted.class, outcome);
        assertInstanceOf(WorkTaskCreatedEvent.class, accepted.event());
        assertEquals(IndexUpdate.ADD, accepted.index());
        assertEquals(WorkTaskStatus.DRAFT, accepted.state().status());
        assertEquals(SOURCE, accepted.state().source());
        assertEquals(ID, accepted.state().id());
    }

    @Test
    void rejectsCreateWhenActiveDuplicateExists() {
        var outcome = decide(create(), ID, null, true, NOW);

        var rejected = assertInstanceOf(Rejected.class, outcome);
        assertEquals(DUPLICATE_REASON, rejected.event().rejectionReason());
        assertEquals("CreateWorkTaskCommand", rejected.event().commandType());
        assertEquals(ID, rejected.event().id());
        assertEquals(SUBJECT, rejected.subject());
    }

    // ---- Index lifecycle on transitions -------------------------------------------

    @Test
    void leavesIndexUnchangedOnNonTerminalTransition() {
        var outcome = decide(new BeginWorkTaskCommand(ID, CORR), ID, taskInState(WorkTaskStatus.ASSIGNED), false, NOW);

        var accepted = assertInstanceOf(Accepted.class, outcome);
        assertInstanceOf(WorkTaskBegunEvent.class, accepted.event());
        assertEquals(IndexUpdate.NONE, accepted.index());
    }

    @Test
    void releasesIndexOnComplete() {
        var outcome = decide(new CompleteWorkTaskCommand(ID, CORR), ID, taskInState(WorkTaskStatus.IN_PROGRESS), false, NOW);

        var accepted = assertInstanceOf(Accepted.class, outcome);
        assertEquals(IndexUpdate.REMOVE, accepted.index());
        assertEquals(WorkTaskStatus.COMPLETED, accepted.state().status());
    }

    @ParameterizedTest
    @EnumSource(value = WorkTaskStatus.class, names = {"DRAFT", "ASSIGNED", "IN_PROGRESS", "PAUSED"})
    void releasesIndexOnCancelFromAnyActiveState(WorkTaskStatus status) {
        var outcome = decide(new CancelWorkTaskCommand(ID, CORR, "no longer needed"), ID, taskInState(status), false, NOW);

        var accepted = assertInstanceOf(Accepted.class, outcome);
        assertEquals(IndexUpdate.REMOVE, accepted.index());
        assertEquals(WorkTaskStatus.CANCELLED, accepted.state().status());
    }

    // ---- Rejections and drops -----------------------------------------------------

    @Test
    void rejectsInvalidTransition() {
        // Begin is only valid from ASSIGNED; from DRAFT it is an invalid transition.
        var outcome = decide(new BeginWorkTaskCommand(ID, CORR), ID, taskInState(WorkTaskStatus.DRAFT), false, NOW);

        var rejected = assertInstanceOf(Rejected.class, outcome);
        assertEquals("BeginWorkTaskCommand", rejected.event().commandType());
        assertEquals(SUBJECT, rejected.subject());
        assertNotEquals(DUPLICATE_REASON, rejected.event().rejectionReason());
    }

    @Test
    void dropsNonCreateCommandForUnknownTask() {
        var outcome = decide(new BeginWorkTaskCommand(ID, CORR), ID, null, false, NOW);

        assertInstanceOf(Dropped.class, outcome);
    }
}
