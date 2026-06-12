package com.example.worktaskservice.infrastructure.persistence;

import com.example.worktaskservice.application.port.WorkTaskFilter;
import com.example.worktaskservice.domain.model.*;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class PanacheWorkTaskRepositoryTest {

    private static final WorkTaskType TYPE = new WorkTaskType("urn:worktask-type:billing.invoices:payment:process-refund");
    private static final SubjectType SUBJECT_TYPE = new SubjectType("urn:subject-type:billing.invoices:payment:invoice");
    private static final Source SOURCE = new Source("urn:source:work.tasks:worktask");

    @Inject
    PanacheWorkTaskRepository repository;

    private WorkTask newTask(WorkTaskStatus status, UUID assigneeId, String subjectId) {
        var now = Instant.now();
        return WorkTask.reconstitute(UUID.randomUUID(), TYPE, new Subject(SUBJECT_TYPE, subjectId), SOURCE,
                "title", null, 0, null, status, assigneeId, now, now);
    }

    private static String randomSubjectId() {
        return UUID.randomUUID().toString();
    }

    @Test
    @TestTransaction
    void filtersByStatus() {
        var draft = newTask(WorkTaskStatus.DRAFT, null, randomSubjectId());
        var assigned = newTask(WorkTaskStatus.ASSIGNED, UUID.randomUUID(), randomSubjectId());
        repository.save(draft);
        repository.save(assigned);

        var page = repository.findAll(new WorkTaskFilter(WorkTaskStatus.DRAFT, null, null, null, null), 0, 20);

        assertEquals(1, page.totalCount());
        assertEquals(draft.id(), page.items().getFirst().id());
    }

    @Test
    @TestTransaction
    void filtersByNumericSubjectAndAssignee() {
        UUID assigneeId = UUID.randomUUID();
        String subjectId = "42:7";   // colon-delimited numeric subject id (non-UUID)
        var matching = newTask(WorkTaskStatus.ASSIGNED, assigneeId, subjectId);
        var other = newTask(WorkTaskStatus.ASSIGNED, UUID.randomUUID(), randomSubjectId());
        repository.save(matching);
        repository.save(other);

        var page = repository.findAll(new WorkTaskFilter(null, null, null, subjectId, assigneeId), 0, 20);

        assertEquals(1, page.totalCount());
        assertEquals(matching.id(), page.items().getFirst().id());
    }

    @Test
    @TestTransaction
    void paginatesResults() {
        // Scope the query to a single shared subject so the count is independent of any other
        // rows committed to the read model (e.g. by the streams topology in other tests).
        String subjectId = randomSubjectId();
        for (int i = 0; i < 5; i++) {
            repository.save(newTask(WorkTaskStatus.DRAFT, null, subjectId));
        }
        var filter = new WorkTaskFilter(null, null, null, subjectId, null);

        var firstPage = repository.findAll(filter, 0, 2);
        var secondPage = repository.findAll(filter, 1, 2);

        assertEquals(5, firstPage.totalCount());
        assertEquals(2, firstPage.items().size());
        assertEquals(2, secondPage.items().size());
        assertEquals(0, firstPage.page());
        assertEquals(1, secondPage.page());
    }
}
