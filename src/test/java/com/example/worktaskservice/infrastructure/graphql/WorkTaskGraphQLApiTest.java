package com.example.worktaskservice.infrastructure.graphql;

import com.example.worktaskservice.domain.model.*;
import com.example.worktaskservice.infrastructure.persistence.PanacheWorkTaskRepository;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

/**
 * Exercises the GraphQL endpoint over HTTP, so test data must be committed
 * (the GraphQL request runs in its own transaction and cannot see
 * uncommitted changes) — hence {@code QuarkusTransaction.requiringNew()}
 * instead of {@code @TestTransaction}, with explicit cleanup afterwards.
 */
@QuarkusTest
class WorkTaskGraphQLApiTest {

    private static final WorkTaskType TYPE = new WorkTaskType("urn:worktask-type:billing.invoices:payment:process-refund");
    private static final SubjectType SUBJECT_TYPE = new SubjectType("urn:subject-type:billing.invoices:payment:invoice");

    @Inject
    PanacheWorkTaskRepository repository;

    private static final Source SOURCE = new Source("urn:source:work.tasks:worktask");

    private WorkTask newTask(String title, WorkTaskStatus status, UUID assigneeId, int priority) {
        return newTask(title, status, assigneeId, priority, null);
    }

    private WorkTask newTask(String title, WorkTaskStatus status, UUID assigneeId, int priority, GenericInfo info) {
        var now = Instant.now();
        return WorkTask.reconstitute(UUID.randomUUID(), TYPE, new Subject(SUBJECT_TYPE, UUID.randomUUID().toString()), SOURCE,
                title, null, priority, null, info, status, assigneeId, now, now);
    }

    private void persist(WorkTask... tasks) {
        QuarkusTransaction.requiringNew().run(() -> {
            for (WorkTask task : tasks) {
                repository.save(task);
            }
        });
    }

    @AfterEach
    void cleanUp() {
        QuarkusTransaction.requiringNew().run(() -> repository.deleteAll());
    }

    private static String graphQlBody(String query) {
        return "{\"query\": \"" + query.replace("\"", "\\\"") + "\"}";
    }

    @Test
    void queriesSingleWorkTaskById() {
        var task = newTask("Process refund", WorkTaskStatus.DRAFT, null, 3);
        persist(task);

        String query = "{ workTask(id: \"" + task.id() + "\") { id source title status priority } }";

        given()
                .contentType(ContentType.JSON)
                .body(graphQlBody(query))
        .when()
                .post("/graphql")
        .then()
                .statusCode(200)
                .body("data.workTask.id", equalTo(task.id().toString()))
                .body("data.workTask.source", equalTo(SOURCE.value()))
                .body("data.workTask.title", equalTo("Process refund"))
                .body("data.workTask.status", equalTo("DRAFT"))
                .body("data.workTask.priority", equalTo(3));
    }

    @Test
    void exposesGenericInfoWithBase64Data() {
        // data bytes {1, 2, 3} encode to Base64 "AQID"
        var info = new GenericInfo("refund-result", "urn:worktask-result:refund", "application/avro",
                "http://registry/subjects/refund/versions/1", new byte[]{1, 2, 3});
        var task = newTask("Completed refund", WorkTaskStatus.COMPLETED, null, 0, info);
        persist(task);

        String query = "{ workTask(id: \"" + task.id() + "\") "
                + "{ genericInfo { name type datacontenttype dataschema dataBase64 } } }";

        given()
                .contentType(ContentType.JSON)
                .body(graphQlBody(query))
        .when()
                .post("/graphql")
        .then()
                .statusCode(200)
                .body("data.workTask.genericInfo.name", equalTo("refund-result"))
                .body("data.workTask.genericInfo.type", equalTo("urn:worktask-result:refund"))
                .body("data.workTask.genericInfo.datacontenttype", equalTo("application/avro"))
                .body("data.workTask.genericInfo.dataschema", equalTo("http://registry/subjects/refund/versions/1"))
                .body("data.workTask.genericInfo.dataBase64", equalTo("AQID"));
    }

    @Test
    void listsWorkTasksFilteredByStatus() {
        var draft = newTask("Draft task", WorkTaskStatus.DRAFT, null, 0);
        var assigned = newTask("Assigned task", WorkTaskStatus.ASSIGNED, UUID.randomUUID(), 0);
        persist(draft, assigned);

        String query = "{ workTasks(filter: { status: DRAFT }, page: 0, size: 10) "
                + "{ totalCount items { id status } } }";

        given()
                .contentType(ContentType.JSON)
                .body(graphQlBody(query))
        .when()
                .post("/graphql")
        .then()
                .statusCode(200)
                .body("data.workTasks.totalCount", equalTo(1))
                .body("data.workTasks.items[0].id", equalTo(draft.id().toString()))
                .body("data.workTasks.items[0].status", equalTo("DRAFT"));
    }
}
