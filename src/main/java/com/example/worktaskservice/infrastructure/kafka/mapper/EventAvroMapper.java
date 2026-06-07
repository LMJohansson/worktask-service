package com.example.worktaskservice.infrastructure.kafka.mapper;

import com.example.worktaskservice.domain.event.*;
import com.example.worktaskservice.domain.model.WorkTask;
import com.example.worktaskservice.events.*;
import com.example.worktaskservice.state.WorkTaskStatus;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class EventAvroMapper {

    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url",
            defaultValue = "http://localhost:8081")
    String schemaRegistryUrl;

    public record OutboundEvent(SpecificRecord avro, Headers headers) {}

    public OutboundEvent toAvro(WorkTaskEvent event, WorkTask task,
                                String traceparent, String tracestate) {
        SpecificRecord avro = toAvroRecord(event);
        Headers headers = CloudEventHeaders.forEvent(event, task, avro,
                schemaRegistryUrl, traceparent, tracestate);
        return new OutboundEvent(avro, headers);
    }

    public com.example.worktaskservice.state.WorkTask toStateAvro(WorkTask task) {
        var avro = new com.example.worktaskservice.state.WorkTask();
        avro.setId(toFixed(task.id()));
        avro.setType(task.type().value());
        avro.setSubjectType(task.subject().type().value());
        avro.setSubjectId(toFixed(task.subject().id()));
        avro.setTitle(task.title());
        avro.setDescription(task.description());
        avro.setPriority(task.priority());
        avro.setDeadline(task.deadline());
        avro.setStatus(WorkTaskStatus.valueOf(task.status().name()));
        avro.setAssigneeId(task.assigneeId() != null ? toFixed(task.assigneeId()) : null);
        avro.setCreatedAt(toNanos(task.createdAt()));
        avro.setUpdatedAt(toNanos(task.updatedAt()));
        return avro;
    }

    private SpecificRecord toAvroRecord(WorkTaskEvent event) {
        return switch (event) {
            case WorkTaskCreatedEvent e -> {
                var avro = new WorkTaskCreated();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setType(e.type().value());
                avro.setSubjectType(e.subject().type().value());
                avro.setSubjectId(toFixed(e.subject().id()));
                avro.setTitle(e.title());
                avro.setDescription(e.description());
                avro.setPriority(e.priority());
                avro.setDeadline(e.deadline());
                yield avro;
            }
            case WorkTaskAssignedEvent e -> {
                var avro = new WorkTaskAssigned();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setAssigneeId(toFixed(e.assigneeId()));
                yield avro;
            }
            case WorkTaskReassignedEvent e -> {
                var avro = new WorkTaskReassigned();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setAssigneeId(toFixed(e.assigneeId()));
                yield avro;
            }
            case WorkTaskUnassignedEvent e -> {
                var avro = new WorkTaskUnassigned();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskBegunEvent e -> {
                var avro = new WorkTaskBegun();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskPausedEvent e -> {
                var avro = new WorkTaskPaused();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskResumedEvent e -> {
                var avro = new WorkTaskResumed();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskCompletedEvent e -> {
                var avro = new WorkTaskCompleted();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskAbortedEvent e -> {
                var avro = new WorkTaskAborted();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskCancelledEvent e -> {
                var avro = new WorkTaskCancelled();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskCommandRejectedEvent e -> {
                var avro = new WorkTaskCommandRejected();
                avro.setWorkTaskId(toFixed(e.workTaskId()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setCommandType(e.commandType());
                avro.setRejectionReason(e.rejectionReason());
                yield avro;
            }
        };
    }

    static UUID toFixed(UUID uuid) {
        return uuid;
    }

    static Instant toNanos(Instant instant) {
        return instant;
    }
}
