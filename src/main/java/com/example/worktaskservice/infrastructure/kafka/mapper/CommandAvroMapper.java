package com.example.worktaskservice.infrastructure.kafka.mapper;

import com.example.worktaskservice.commands.*;
import com.example.worktaskservice.domain.command.*;
import com.example.worktaskservice.domain.model.GenericInfo;
import com.example.worktaskservice.domain.model.Source;
import com.example.worktaskservice.domain.model.Subject;
import com.example.worktaskservice.domain.model.WorkTaskType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;
import java.util.UUID;

public class CommandAvroMapper {

    public record InboundCommand(Object command, UUID workTaskId, String traceparent, String tracestate,
                                 String causationId) {}

    public InboundCommand map(SpecificRecord avro, Headers headers) {
        String traceparent = CloudEventHeaders.extractHeader(headers, "ce_traceparent");
        String tracestate  = CloudEventHeaders.extractHeader(headers, "ce_tracestate");
        // The inbound command's CloudEvents id is the causation id of any event it produces.
        String causationId = CloudEventHeaders.extractHeader(headers, "ce_id");
        // The WorkTask id addresses the per-task state store, independent of the record key (subjectId).
        return new InboundCommand(toDomain(avro), extractId(avro), traceparent, tracestate, causationId);
    }

    private Object toDomain(SpecificRecord avro) {
        return switch (avro) {
            case CreateWorkTask cmd -> new CreateWorkTaskCommand(
                    toUUID(cmd.getId()),
                    toUUID(cmd.getCorrelationId()),
                    new WorkTaskType(cmd.getType()),
                    Subject.fromUrn(cmd.getSubject()),
                    new Source(cmd.getSource()),
                    cmd.getTitle(),
                    cmd.getDescription(),
                    cmd.getPriority(),
                    cmd.getDeadline(),
                    toGenericInfo(cmd.getGenericInfo()));
            case AssignWorkTask cmd -> new AssignWorkTaskCommand(
                    toUUID(cmd.getId()),
                    toUUID(cmd.getCorrelationId()),
                    cmd.getAssigneeId() != null ? toUUID(cmd.getAssigneeId()) : null);
            case BeginWorkTask cmd -> new BeginWorkTaskCommand(
                    toUUID(cmd.getId()), toUUID(cmd.getCorrelationId()));
            case PauseWorkTask cmd -> new PauseWorkTaskCommand(
                    toUUID(cmd.getId()), toUUID(cmd.getCorrelationId()));
            case ResumeWorkTask cmd -> new ResumeWorkTaskCommand(
                    toUUID(cmd.getId()), toUUID(cmd.getCorrelationId()));
            case CompleteWorkTask cmd -> new CompleteWorkTaskCommand(
                    toUUID(cmd.getId()), toUUID(cmd.getCorrelationId()), toGenericInfo(cmd.getGenericInfo()));
            case AbortWorkTask cmd -> new AbortWorkTaskCommand(
                    toUUID(cmd.getId()), toUUID(cmd.getCorrelationId()), cmd.getReason(),
                    toGenericInfo(cmd.getGenericInfo()));
            case CancelWorkTask cmd -> new CancelWorkTaskCommand(
                    toUUID(cmd.getId()), toUUID(cmd.getCorrelationId()), cmd.getReason(),
                    toGenericInfo(cmd.getGenericInfo()));
            default -> throw new IllegalArgumentException("Unknown Avro command: " + avro.getClass().getSimpleName());
        };
    }

    private static UUID extractId(SpecificRecord avro) {
        return switch (avro) {
            case CreateWorkTask cmd   -> cmd.getId();
            case AssignWorkTask cmd   -> cmd.getId();
            case BeginWorkTask cmd    -> cmd.getId();
            case PauseWorkTask cmd    -> cmd.getId();
            case ResumeWorkTask cmd   -> cmd.getId();
            case CompleteWorkTask cmd -> cmd.getId();
            case AbortWorkTask cmd    -> cmd.getId();
            case CancelWorkTask cmd   -> cmd.getId();
            default -> throw new IllegalArgumentException("Unknown Avro command: " + avro.getClass().getSimpleName());
        };
    }

    private static UUID toUUID(UUID uuid) {
        return uuid;
    }

    private static GenericInfo toGenericInfo(com.example.worktaskservice.commands.GenericInfo avro) {
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
