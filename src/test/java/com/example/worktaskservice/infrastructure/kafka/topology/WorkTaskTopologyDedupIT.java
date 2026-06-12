package com.example.worktaskservice.infrastructure.kafka.topology;

import com.example.worktaskservice.application.port.WorkTaskFilter;
import com.example.worktaskservice.commands.CancelWorkTask;
import com.example.worktaskservice.commands.CreateWorkTask;
import com.example.worktaskservice.domain.model.Subject;
import com.example.worktaskservice.domain.model.SubjectType;
import com.example.worktaskservice.domain.model.WorkTaskStatus;
import com.example.worktaskservice.infrastructure.config.TopicConfig;
import com.example.worktaskservice.infrastructure.persistence.PanacheWorkTaskRepository;
import com.example.worktaskservice.testsupport.ApicurioRegistryTestResource;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end proof of the subject-uniqueness rule through the real Kafka Streams topology: produces
 * Avro command records (resolved against the Apicurio union schemas) keyed by {@code subjectId} and
 * asserts the materialized read model. Assertions are ordering-based — because same-subject commands
 * share a partition, observing a later command's effect proves the earlier one was processed — so no
 * fragile "wait then check it didn't happen" timing is needed.
 */
@QuarkusTest
@QuarkusTestResource(ApicurioRegistryTestResource.class)
class WorkTaskTopologyDedupIT {

    private static final String TYPE_A = "urn:worktask-type:billing.invoices:payment:process-refund";
    private static final String TYPE_B = "urn:worktask-type:billing.invoices:payment:issue-credit";
    private static final SubjectType SUBJECT_TYPE = new SubjectType("urn:subject-type:billing.invoices:payment:invoice");
    private static final String SOURCE = "urn:source:work.tasks:worktask";
    private static final Duration TIMEOUT = Duration.ofSeconds(45);

    @Inject TopicConfig topics;
    @Inject PanacheWorkTaskRepository repository;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrap;

    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url")
    String registryUrl;

    @AfterEach
    void cleanUp() {
        QuarkusTransaction.requiringNew().run(() -> repository.deleteAll());
    }

    @Test
    void enforcesOneActiveTaskOfATypePerSubject() throws Exception {
        UUID subjectId = UUID.randomUUID();
        String subjectUrn = new Subject(SUBJECT_TYPE, subjectId).toUrn();

        try (var producer = newProducer()) {
            UUID first = UUID.randomUUID();

            // 1. First create of type A → one active task.
            send(producer, subjectId, create(first, subjectUrn, TYPE_A));
            awaitCount(subjectId, 1);

            // 2. Duplicate create of type A on the same subject → must be rejected.
            send(producer, subjectId, create(UUID.randomUUID(), subjectUrn, TYPE_A));

            // 3. Different type on the same subject → allowed. Same-partition ordering means once this is
            //    visible (count == 2, not 3) the duplicate at step 2 has been processed and rejected.
            send(producer, subjectId, create(UUID.randomUUID(), subjectUrn, TYPE_B));
            awaitCount(subjectId, 2);

            // 4. Cancel the first task (terminal) → releases the type-A slot for the subject.
            send(producer, subjectId, cancel(first));
            awaitStatus(first, WorkTaskStatus.CANCELLED);

            // 5. Re-create type A now that the previous one is terminal → allowed (count becomes 3).
            send(producer, subjectId, create(UUID.randomUUID(), subjectUrn, TYPE_A));
            awaitCount(subjectId, 3);
        }
    }

    // --- producing -------------------------------------------------------------

    private KafkaProducer<String, SpecificRecord> newProducer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
        props.put("apicurio.registry.url", registryUrl);
        props.put("apicurio.registry.artifact.group-id", "worktask");
        props.put("apicurio.registry.find-latest", "true");
        props.put("apicurio.registry.auto-register", "false");
        return new KafkaProducer<>(props);
    }

    private void send(KafkaProducer<String, SpecificRecord> producer, UUID subjectId, SpecificRecord cmd)
            throws Exception {
        producer.send(new ProducerRecord<>(topics.commandTopic(), subjectId.toString(), cmd)).get();
    }

    private static CreateWorkTask create(UUID id, String subjectUrn, String type) {
        var cmd = new CreateWorkTask();
        cmd.setId(id);
        cmd.setCorrelationId(UUID.randomUUID());
        cmd.setType(type);
        cmd.setSubject(subjectUrn);
        cmd.setSource(SOURCE);
        cmd.setTitle("E2E task");
        cmd.setDescription(null);
        cmd.setPriority(0);
        cmd.setDeadline(null);
        return cmd;
    }

    private static CancelWorkTask cancel(UUID id) {
        var cmd = new CancelWorkTask();
        cmd.setId(id);
        cmd.setCorrelationId(UUID.randomUUID());
        cmd.setReason("done");
        return cmd;
    }

    // --- read-model polling ----------------------------------------------------

    private long countForSubject(UUID subjectId) {
        return QuarkusTransaction.requiringNew().call(() ->
                repository.findAll(new WorkTaskFilter(null, null, null, subjectId, null), 0, 100).totalCount());
    }

    private void awaitCount(UUID subjectId, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        while (System.nanoTime() < deadline) {
            last = countForSubject(subjectId);
            if (last == expected) return;
            Thread.sleep(250);
        }
        fail("Expected " + expected + " task(s) for subject " + subjectId + " but found " + last);
    }

    private void awaitStatus(UUID workTaskId, WorkTaskStatus expected) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        WorkTaskStatus last = null;
        while (System.nanoTime() < deadline) {
            last = QuarkusTransaction.requiringNew().call(() ->
                    repository.find(workTaskId).map(t -> t.status()).orElse(null));
            if (last == expected) return;
            Thread.sleep(250);
        }
        fail("Expected status " + expected + " for task " + workTaskId + " but was " + last);
    }
}
