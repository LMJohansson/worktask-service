package com.example.worktaskservice.infrastructure.kafka.mapper;

import com.example.worktaskservice.commands.*;
import com.example.worktaskservice.domain.command.*;
import com.example.worktaskservice.domain.model.Subject;
import com.example.worktaskservice.domain.model.WorkTaskType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;

import java.util.UUID;

public class CommandAvroMapper {

    public record InboundCommand(Object command, String traceparent, String tracestate,
                                 String causationId, String source) {}

    public InboundCommand map(SpecificRecord avro, Headers headers) {
        String traceparent = CloudEventHeaders.extractHeader(headers, "ce_traceparent");
        String tracestate  = CloudEventHeaders.extractHeader(headers, "ce_tracestate");
        // The inbound command's CloudEvents id is the causation id of any event it produces.
        String causationId = CloudEventHeaders.extractHeader(headers, "ce_id");
        // The originating source is propagated through to the produced event(s).
        String source = CloudEventHeaders.extractHeader(headers, "ce_source");
        return new InboundCommand(toDomain(avro), traceparent, tracestate, causationId, source);
    }

    private Object toDomain(SpecificRecord avro) {
        return switch (avro) {
            case CreateWorkTask cmd -> new CreateWorkTaskCommand(
                    toUUID(cmd.getWorkTaskId()),
                    toUUID(cmd.getCorrelationId()),
                    new WorkTaskType(cmd.getType()),
                    Subject.fromUrn(cmd.getSubject()),
                    cmd.getTitle(),
                    cmd.getDescription(),
                    cmd.getPriority(),
                    cmd.getDeadline());
            case AssignWorkTask cmd -> new AssignWorkTaskCommand(
                    toUUID(cmd.getWorkTaskId()),
                    toUUID(cmd.getCorrelationId()),
                    cmd.getAssigneeId() != null ? toUUID(cmd.getAssigneeId()) : null);
            case BeginWorkTask cmd -> new BeginWorkTaskCommand(
                    toUUID(cmd.getWorkTaskId()), toUUID(cmd.getCorrelationId()));
            case PauseWorkTask cmd -> new PauseWorkTaskCommand(
                    toUUID(cmd.getWorkTaskId()), toUUID(cmd.getCorrelationId()));
            case ResumeWorkTask cmd -> new ResumeWorkTaskCommand(
                    toUUID(cmd.getWorkTaskId()), toUUID(cmd.getCorrelationId()));
            case CompleteWorkTask cmd -> new CompleteWorkTaskCommand(
                    toUUID(cmd.getWorkTaskId()), toUUID(cmd.getCorrelationId()));
            case AbortWorkTask cmd -> new AbortWorkTaskCommand(
                    toUUID(cmd.getWorkTaskId()), toUUID(cmd.getCorrelationId()), cmd.getReason());
            case CancelWorkTask cmd -> new CancelWorkTaskCommand(
                    toUUID(cmd.getWorkTaskId()), toUUID(cmd.getCorrelationId()), cmd.getReason());
            default -> throw new IllegalArgumentException("Unknown Avro command: " + avro.getClass().getSimpleName());
        };
    }

    private static UUID toUUID(UUID uuid) {
        return uuid;
    }
}
