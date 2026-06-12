package com.example.worktaskservice.infrastructure.kafka.topology;

import com.example.worktaskservice.domain.command.CreateWorkTaskCommand;
import com.example.worktaskservice.domain.model.WorkTask;
import com.example.worktaskservice.domain.model.WorkTaskType;
import com.example.worktaskservice.infrastructure.config.TopicConfig;
import com.example.worktaskservice.infrastructure.kafka.mapper.CommandAvroMapper;
import com.example.worktaskservice.infrastructure.kafka.mapper.EventAvroMapper;
import com.example.worktaskservice.infrastructure.kafka.serde.AvroSerdeFactory;
import com.example.worktaskservice.infrastructure.persistence.PanacheWorkTaskRepository;
import io.quarkus.narayana.jta.QuarkusTransaction;
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
    private static final String STORE_NAME      = "worktask-store";
    private static final String INDEX_STORE     = "subject-active-index";
    private static final String EVENT_SINK      = "event-sink";
    private static final String COMPACT_SINK    = "compact-sink";
    private static final String DLT_SINK        = "dead-letter-sink";
    private static final String COMPACT_SOURCE  = "compact-source";
    private static final String DATABASE_SINK   = "database-sink";

    @Inject TopicConfig topics;
    @Inject EventAvroMapper eventMapper;
    @Inject PanacheWorkTaskRepository repository;
    @Inject AvroSerdeFactory serdeFactory;

    @Produces
    public Topology topology() {
        Topology topology = new Topology();

        try (var commandSerde = serdeFactory.commandSerde();
             var eventSerde = serdeFactory.commandSerde();
             var stateSerde = serdeFactory.workTaskStateSerde();
             // Registry-free serde for the internal store + changelog (no public <topic>-value artifact).
             var storeSerde = serdeFactory.workTaskStateStoreSerde()) {

            topology.addSource("command-source",
                    Serdes.String().deserializer(),
                    commandSerde.deserializer(),
                    topics.commandTopic());

            topology.addProcessor("state-transition",
                    StateTransitionProcessor::new,
                    "command-source");

            // Changelog logging is intentionally left enabled (the default): this store's own
            // changelog is its durable, synchronously-restored backing. The public compact topic
            // (COMPACT_SINK below) is a *separate parallel projection*, not the store's changelog —
            // it cannot double as one because it is a public-API topic carrying CloudEvents ce_
            // headers under FORWARD compatibility, whereas a Streams changelog is name-managed,
            // header-less, and value-serde only. The store must also be written synchronously by
            // the processor (read-modify-write across sequential commands on the same key), so it
            // cannot be a read-only materialization of the asynchronously-consumed compact topic.
            // Both writes commit in one exactly_once_v2 transaction, so they cannot diverge.
            topology.addStateStore(
                    Stores.keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(STORE_NAME),
                            Serdes.String(),
                            storeSerde),
                    "state-transition");

            // Secondary index enforcing at most one active task of a given type per subject.
            // Key: "<subjectId>|<workTaskType>", value: the active WorkTask id. Co-partitioned with the
            // main store because the command topic is keyed by subjectId (see the keying model in process()).
            topology.addStateStore(
                    Stores.keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(INDEX_STORE),
                            Serdes.String(),
                            Serdes.String()),
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

            topology.addSource(COMPACT_SOURCE,
                Serdes.String().deserializer(),
                stateSerde.deserializer(),
                topics.compactTopic());

            topology.addProcessor(DATABASE_SINK,
                DatabaseSinkProcessor::new,
                COMPACT_SOURCE);
        }

        return topology;
    }

    private class StateTransitionProcessor implements Processor<String, SpecificRecord, String, SpecificRecord> {

        private ProcessorContext<String, SpecificRecord> context;
        private KeyValueStore<String, com.example.worktaskservice.state.WorkTask> store;
        private KeyValueStore<String, String> index;
        private final CommandAvroMapper commandMapper = new CommandAvroMapper();

        @Override
        public void init(ProcessorContext<String, SpecificRecord> context) {
            this.context = context;
            this.store = context.getStateStore(STORE_NAME);
            this.index = context.getStateStore(INDEX_STORE);
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

            // Keying model: the record key is the subjectId (the command topic is partitioned by subject so
            // all activity for a subject is co-located and totally ordered). The per-task state is addressed
            // by the WorkTask id carried in the command — the local store is co-partitioned for free.
            String workTaskKey = inbound.workTaskId().toString();
            WorkTask task = loadOrNull(workTaskKey);

            boolean duplicate = inbound.command() instanceof CreateWorkTaskCommand cmd
                    && index.get(indexKey(cmd.subject().id(), cmd.type())) != null;

            var outcome = WorkTaskCommandDecider.decide(
                    inbound.command(), inbound.workTaskId(), task, duplicate, now);

            switch (outcome) {
                case WorkTaskCommandDecider.Dropped ignored ->
                    // No stored task on this partition: the producer mis-routed the command, or the task
                    // never existed. There is no subject to tag a rejection with, so drop it.
                    LOG.warnf("WorkTask %s not found for %s on subject %s → dropped",
                            workTaskKey, inbound.command().getClass().getSimpleName(), record.key());

                case WorkTaskCommandDecider.Rejected rejected ->
                    forwardRejection(record.key(), rejected, traceparent, tracestate,
                            inbound.causationId(), now);

                case WorkTaskCommandDecider.Accepted accepted -> {
                    WorkTask state = accepted.state();
                    var stateAvro = eventMapper.toStateAvro(state);
                    store.put(workTaskKey, stateAvro);

                    String idxKey = indexKey(state.subject().id(), state.type());
                    switch (accepted.index()) {
                        case ADD    -> index.put(idxKey, workTaskKey);
                        case REMOVE -> index.delete(idxKey);
                        case NONE   -> { /* active task unchanged */ }
                    }

                    EventAvroMapper.OutboundEvent outbound =
                            eventMapper.toAvro(accepted.event(), state.subject(), traceparent, tracestate,
                                    inbound.causationId());
                    // Events keep the subjectId key; the compact projection is re-keyed by WorkTask id.
                    context.forward(
                            new Record<>(record.key(), outbound.avro(), now.toEpochMilli(), outbound.headers()),
                            EVENT_SINK);
                    context.forward(
                            new Record<>(state.id().toString(), stateAvro, now.toEpochMilli()),
                            COMPACT_SINK);
                }
            }
        }

        private void forwardRejection(String recordKey, WorkTaskCommandDecider.Rejected rejected,
                                      String traceparent, String tracestate, String causationId,
                                      Instant now) {
            EventAvroMapper.OutboundEvent outbound = eventMapper.toAvro(
                    rejected.event(), rejected.subject(), traceparent, tracestate, causationId);
            context.forward(
                    new Record<>(recordKey, outbound.avro(), now.toEpochMilli(), outbound.headers()),
                    EVENT_SINK);
        }

        private WorkTask loadOrNull(String workTaskKey) {
            var state = store.get(workTaskKey);
            if (state == null) return null;
            return eventMapper.toDomain(state);
        }

        private static String indexKey(String subjectId, WorkTaskType type) {
            return subjectId + "|" + type.value();
        }
    }

    private class DatabaseSinkProcessor
            implements Processor<String, com.example.worktaskservice.state.WorkTask, Void, Void> {

        @Override
        public void process(Record<String, com.example.worktaskservice.state.WorkTask> record) {
            if (record.value() == null) {
                return;
            }
            WorkTask task = eventMapper.toDomain(record.value());
            QuarkusTransaction.requiringNew().run(() -> repository.save(task));
        }
    }
}
