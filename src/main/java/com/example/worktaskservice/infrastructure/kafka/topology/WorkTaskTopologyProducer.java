package com.example.worktaskservice.infrastructure.kafka.topology;

import com.example.worktaskservice.domain.event.WorkTaskCommandRejectedEvent;
import com.example.worktaskservice.domain.event.WorkTaskCreatedEvent;
import com.example.worktaskservice.domain.event.WorkTaskEvent;
import com.example.worktaskservice.domain.exception.InvalidStateTransitionException;
import com.example.worktaskservice.domain.model.WorkTask;
import com.example.worktaskservice.infrastructure.config.TopicConfig;
import com.example.worktaskservice.infrastructure.kafka.mapper.CommandAvroMapper;
import com.example.worktaskservice.infrastructure.kafka.mapper.EventAvroMapper;
import com.example.worktaskservice.infrastructure.kafka.serde.AvroSerdeFactory;
import com.example.worktaskservice.infrastructure.persistence.PanacheWorkTaskRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class WorkTaskTopologyProducer {

    private static final Logger LOG = Logger.getLogger(WorkTaskTopologyProducer.class);
    private static final String STORE_NAME   = "worktask-store";
    private static final String EVENT_SINK   = "event-sink";
    private static final String COMPACT_SINK = "compact-sink";
    private static final String DLT_SINK     = "dead-letter-sink";

    @Inject TopicConfig topics;
    @Inject EventAvroMapper eventMapper;
    @Inject PanacheWorkTaskRepository repository;
    @Inject AvroSerdeFactory serdeFactory;

    @Produces
    public Topology topology() {
        var commandSerde = serdeFactory.commandSerde();
        var eventSerde   = serdeFactory.commandSerde();
        var stateSerde   = serdeFactory.workTaskStateSerde();

        Topology topology = new Topology();

        topology.addSource("command-source",
                Serdes.String().deserializer(),
                commandSerde.deserializer(),
                topics.commandTopic());

        topology.addProcessor("state-transition",
                () -> new StateTransitionProcessor(),
                "command-source");

        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        Serdes.String(),
                        stateSerde),
                "state-transition");

        topology.addSink(EVENT_SINK,
                topics.eventTopic(),
                Serdes.String().serializer(),
                eventSerde.serializer(),
                "state-transition");

        topology.addSink(COMPACT_SINK,
                topics.compactTopic(),
                Serdes.String().serializer(),
                stateSerde.serializer(),
                "state-transition");

        topology.addSink(DLT_SINK,
                topics.deadLetterTopic(),
                Serdes.String().serializer(),
                commandSerde.serializer(),
                "state-transition");

        return topology;
    }

    private class StateTransitionProcessor implements Processor<String, SpecificRecord, String, SpecificRecord> {

        private ProcessorContext<String, SpecificRecord> context;
        private KeyValueStore<String, com.example.worktaskservice.state.WorkTask> store;
        private final CommandAvroMapper commandMapper = new CommandAvroMapper();

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext<String, SpecificRecord> context) {
            this.context = context;
            this.store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(Record<String, SpecificRecord> record) {
            if (record.value() == null) {
                LOG.warnf("Unparsable message on key %s → dead-letter", record.key());
                context.forward(record, DLT_SINK);
                return;
            }

            CommandAvroMapper.InboundCommand inbound;
            try {
                inbound = commandMapper.map(record.value(), record.headers());
            } catch (Exception e) {
                LOG.warnf("Failed to map command on key %s: %s", record.key(), e.getMessage());
                context.forward(record, DLT_SINK);
                return;
            }

            String traceparent = inbound.traceparent();
            String tracestate  = inbound.tracestate();
            Instant now = Instant.now();
            WorkTask task = loadOrNull(record.key());

            try {
                WorkTaskEvent event;
                if (inbound.command() instanceof com.example.worktaskservice.domain.command.CreateWorkTaskCommand cmd) {
                    event = WorkTask.create(cmd, now);
                    task = reconstitute((WorkTaskCreatedEvent) event, now);
                } else {
                    if (task == null) {
                        forwardRejection(record.key(), inbound.command(), "WorkTask not found",
                                traceparent, tracestate, now);
                        return;
                    }
                    event = task.apply(inbound.command(), now);
                }

                store.put(record.key(), eventMapper.toStateAvro(task));

                EventAvroMapper.OutboundEvent outbound = eventMapper.toAvro(event, task, traceparent, tracestate);
                context.forward(
                        new Record<>(record.key(), outbound.avro(), now.toEpochMilli(), outbound.headers()),
                        EVENT_SINK);

                var stateAvro = eventMapper.toStateAvro(task);
                context.forward(
                        new Record<>(record.key(), stateAvro, now.toEpochMilli()),
                        COMPACT_SINK);

            } catch (InvalidStateTransitionException e) {
                forwardRejection(record.key(), inbound.command(), e.getMessage(),
                        traceparent, tracestate, now);
            }
        }

        private void forwardRejection(String key, Object cmd, String reason,
                                      String traceparent, String tracestate, Instant now) {
            UUID workTaskId = UUID.fromString(key);
            var rejEvent = new WorkTaskCommandRejectedEvent(
                    workTaskId, UUID.randomUUID(), now,
                    cmd.getClass().getSimpleName(), reason);
            WorkTask dummyTask = loadOrNull(key);
            if (dummyTask == null) return;
            EventAvroMapper.OutboundEvent outbound = eventMapper.toAvro(rejEvent, dummyTask, traceparent, tracestate);
            context.forward(
                    new Record<>(key, outbound.avro(), now.toEpochMilli(), outbound.headers()),
                    EVENT_SINK);
        }

        private WorkTask loadOrNull(String key) {
            var state = store.get(key);
            if (state == null) return null;
            return toWorkTask(state);
        }

        private WorkTask reconstitute(WorkTaskCreatedEvent e, Instant now) {
            return WorkTask.reconstitute(
                    e.workTaskId(), e.type(), e.subject(),
                    e.title(), e.description(),
                    com.example.worktaskservice.domain.model.WorkTaskStatus.DRAFT,
                    null, now, now);
        }

        private WorkTask toWorkTask(com.example.worktaskservice.state.WorkTask s) {
            return WorkTask.reconstitute(
                    s.getId(),
                    new com.example.worktaskservice.domain.model.WorkTaskType(s.getType()),
                    new com.example.worktaskservice.domain.model.Subject(
                            new com.example.worktaskservice.domain.model.SubjectType(s.getSubjectType()),
                            s.getSubjectId()),
                    s.getTitle(), s.getDescription(),
                    com.example.worktaskservice.domain.model.WorkTaskStatus.valueOf(s.getStatus().name()),
                    s.getAssigneeId(),
                    s.getCreatedAt(), s.getUpdatedAt());
        }
    }
}
