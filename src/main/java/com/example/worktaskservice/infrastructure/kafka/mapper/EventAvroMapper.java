package com.example.worktaskservice.infrastructure.kafka.mapper;

import com.example.worktaskservice.domain.event.*;
import com.example.worktaskservice.domain.model.GenericInfo;
import com.example.worktaskservice.domain.model.Subject;
import com.example.worktaskservice.domain.model.WorkTask;
import com.example.worktaskservice.events.*;
import com.example.worktaskservice.state.WorkTaskStatus;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class EventAvroMapper {

    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url",
            defaultValue = "http://localhost:8081")
    String schemaRegistryUrl;

    public record OutboundEvent(SpecificRecord avro, Headers headers) {}

    public OutboundEvent toAvro(WorkTaskEvent event, Subject subject,
                                String traceparent, String tracestate, String causationId) {
        SpecificRecord avro = toAvroRecord(event);
        Headers headers = CloudEventHeaders.forEvent(event, subject, avro,
                schemaRegistryUrl, traceparent, tracestate, causationId);
        return new OutboundEvent(avro, headers);
    }

    public com.example.worktaskservice.state.WorkTask toStateAvro(WorkTask task) {
        var avro = new com.example.worktaskservice.state.WorkTask();
        avro.setId(toFixed(task.id()));
        avro.setType(task.type().value());
        avro.setSubject(task.subject().toUrn());
        avro.setSource(task.source().value());
        avro.setTitle(task.title());
        avro.setDescription(task.description());
        avro.setPriority(task.priority());
        avro.setDeadline(task.deadline());
        avro.setGenericInfo(toStateGenericInfo(task.genericInfo()));
        avro.setStatus(WorkTaskStatus.valueOf(task.status().name()));
        avro.setAssigneeId(task.assigneeId() != null ? toFixed(task.assigneeId()) : null);
        avro.setCreatedAt(toNanos(task.createdAt()));
        avro.setUpdatedAt(toNanos(task.updatedAt()));
        return avro;
    }

    public WorkTask toDomain(com.example.worktaskservice.state.WorkTask state) {
        return WorkTask.reconstitute(
                state.getId(),
                new com.example.worktaskservice.domain.model.WorkTaskType(state.getType()),
                com.example.worktaskservice.domain.model.Subject.fromUrn(state.getSubject()),
                new com.example.worktaskservice.domain.model.Source(state.getSource()),
                state.getTitle(), state.getDescription(), state.getPriority(), state.getDeadline(),
                toDomainGenericInfo(state.getGenericInfo()),
                com.example.worktaskservice.domain.model.WorkTaskStatus.valueOf(state.getStatus().name()),
                state.getAssigneeId(),
                state.getCreatedAt(), state.getUpdatedAt());
    }

    private SpecificRecord toAvroRecord(WorkTaskEvent event) {
        return switch (event) {
            case WorkTaskCreatedEvent e -> {
                var avro = new WorkTaskCreated();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setType(e.type().value());
                avro.setSubject(e.subject().toUrn());
                avro.setSource(e.source().value());
                avro.setTitle(e.title());
                avro.setDescription(e.description());
                avro.setPriority(e.priority());
                avro.setDeadline(e.deadline());
                avro.setGenericInfo(toEventGenericInfo(e.genericInfo()));
                yield avro;
            }
            case WorkTaskAssignedEvent e -> {
                var avro = new WorkTaskAssigned();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setAssigneeId(toFixed(e.assigneeId()));
                yield avro;
            }
            case WorkTaskReassignedEvent e -> {
                var avro = new WorkTaskReassigned();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setAssigneeId(toFixed(e.assigneeId()));
                yield avro;
            }
            case WorkTaskUnassignedEvent e -> {
                var avro = new WorkTaskUnassigned();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskBegunEvent e -> {
                var avro = new WorkTaskBegun();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskPausedEvent e -> {
                var avro = new WorkTaskPaused();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskResumedEvent e -> {
                var avro = new WorkTaskResumed();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                yield avro;
            }
            case WorkTaskCompletedEvent e -> {
                var avro = new WorkTaskCompleted();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setGenericInfo(toEventGenericInfo(e.genericInfo()));
                yield avro;
            }
            case WorkTaskAbortedEvent e -> {
                var avro = new WorkTaskAborted();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setGenericInfo(toEventGenericInfo(e.genericInfo()));
                yield avro;
            }
            case WorkTaskCancelledEvent e -> {
                var avro = new WorkTaskCancelled();
                avro.setId(toFixed(e.id()));
                avro.setCorrelationId(toFixed(e.correlationId()));
                avro.setOccurredAt(toNanos(e.occurredAt()));
                avro.setGenericInfo(toEventGenericInfo(e.genericInfo()));
                yield avro;
            }
            case WorkTaskCommandRejectedEvent e -> {
                var avro = new WorkTaskCommandRejected();
                avro.setId(toFixed(e.id()));
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

    private static com.example.worktaskservice.events.GenericInfo toEventGenericInfo(GenericInfo info) {
        if (info == null) {
            return null;
        }
        var avro = new com.example.worktaskservice.events.GenericInfo();
        avro.setName(info.name());
        avro.setType(info.type());
        avro.setDatacontenttype(info.datacontenttype());
        avro.setDataschema(info.dataschema());
        avro.setData(ByteBuffer.wrap(info.data()));
        return avro;
    }

    private static com.example.worktaskservice.state.GenericInfo toStateGenericInfo(GenericInfo info) {
        if (info == null) {
            return null;
        }
        var avro = new com.example.worktaskservice.state.GenericInfo();
        avro.setName(info.name());
        avro.setType(info.type());
        avro.setDatacontenttype(info.datacontenttype());
        avro.setDataschema(info.dataschema());
        avro.setData(ByteBuffer.wrap(info.data()));
        return avro;
    }

    private static GenericInfo toDomainGenericInfo(com.example.worktaskservice.state.GenericInfo avro) {
        if (avro == null) {
            return null;
        }
        return new GenericInfo(
                avro.getName(), avro.getType(), avro.getDatacontenttype(), avro.getDataschema(),
                toBytes(avro.getData()));
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }
}
