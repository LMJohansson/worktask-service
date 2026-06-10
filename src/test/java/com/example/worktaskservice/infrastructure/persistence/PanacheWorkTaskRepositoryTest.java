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

    @Inject
    PanacheWorkTaskRepository repository;

    private WorkTask newTask(WorkTaskStatus status, UUID assigneeId, UUID subjectId) {
        var now = Instant.now();
        return WorkTask.reconstitute(UUID.randomUUID(), TYPE, new Subject(SUBJECT_TYPE, subjectId),
                "title", null, 0, null, status, assigneeId, now, now);
    }

    @Test
    @TestTransaction
    void filtersByStatus() {
        var draft = newTask(WorkTaskStatus.DRAFT, null, UUID.randomUUID());
        var assigned = newTask(WorkTaskStatus.ASSIGNED, UUID.randomUUID(), UUID.randomUUID());
        repository.save(draft);
        repository.save(assigned);

        var page = repository.findAll(new WorkTaskFilter(WorkTaskStatus.DRAFT, null, null, null, null), 0, 20);

        assertEquals(1, page.totalCount());
        assertEquals(draft.id(), page.items().getFirst().id());
    }

    @Test
    @TestTransaction
    void filtersBySubjectAndAssignee() {
        UUID assigneeId = UUID.randomUUID();
        UUID subjectId = UUID.randomUUID();
        var matching = newTask(WorkTaskStatus.ASSIGNED, assigneeId, subjectId);
        var other = newTask(WorkTaskStatus.ASSIGNED, UUID.randomUUID(), UUID.randomUUID());
        repository.save(matching);
        repository.save(other);

        var page = repository.findAll(new WorkTaskFilter(null, null, null, subjectId, assigneeId), 0, 20);

        assertEquals(1, page.totalCount());
        assertEquals(matching.id(), page.items().getFirst().id());
    }

    @Test
    @TestTransaction
    void paginatesResults() {
        for (int i = 0; i < 5; i++) {
            repository.save(newTask(WorkTaskStatus.DRAFT, null, UUID.randomUUID()));
        }

        var firstPage = repository.findAll(WorkTaskFilter.EMPTY, 0, 2);
        var secondPage = repository.findAll(WorkTaskFilter.EMPTY, 1, 2);

        assertEquals(5, firstPage.totalCount());
        assertEquals(2, firstPage.items().size());
        assertEquals(2, secondPage.items().size());
        assertEquals(0, firstPage.page());
        assertEquals(1, secondPage.page());
    }
}
