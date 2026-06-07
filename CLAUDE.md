# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Status

Implementation in progress. Gradle build files and source code are being scaffolded.

## Runtime

- **Java 25** (openjdk-25) as configured in `.idea/misc.xml`
- Project name: `WorkTaskService` — a service for managing work tasks

## Build System

Gradle with Kotlin DSL, Quarkus 3.36.1.

```bash
# Build
./gradlew build

# Run tests
./gradlew test

# Run a single test class
./gradlew test --tests "com.example.worktaskservice.SomeTest"

# Run a single test method
./gradlew test --tests "com.example.worktaskservice.SomeTest.someMethod"

# Start Quarkus dev mode (live reload, Dev UI at http://localhost:8080/q/dev)
./gradlew quarkusDev

# Package as uber-jar
./gradlew quarkusBuild

# Native build (GraalVM required)
./gradlew build -Dquarkus.package.type=native
```

## Code Style

- General Clean Code best practices
- Records preferred for immutable value objects and DTOs
- No UUID wrapper types — use `UUID` directly; field/parameter names provide semantic clarity

## Architecture

The Work Task Service manages Work Tasks in accordance with Event Driven Architecture (EDA) and Domain Driven Design (DDD).

### Bounded Context and Topic Naming

Topic naming convention: `<domain>(.<subdomain>)?.<bounded context>.[public|private].<aggregate>(.<suffix>)?`

The three operative topics for this service are all **public** (part of the public API):

| Purpose            | Suffix    | Retention          |
|--------------------|-----------|--------------------|
| Inbound commands   | `command` | Delete (short)     |
| Domain events      | `event`   | Delete (long)      |
| Materialized state | `compact` | Compact (infinite) |

Example: `work.tasks.worktask.public.worktask.command`

### WorkTask Model

Every WorkTask has:
- **`type`** (`WorkTaskType`) — identifies the action to be performed, format: `<domain>(.<subdomain>)?:<bounded-context>/<task-name>` (e.g. `billing.invoices:payment/process-refund`). Immutable after creation.
- **`subject`** (`Subject`) — the aggregate in another domain/bounded context that the task acts on. Groups a `SubjectType` (same format as `WorkTaskType`, e.g. `billing.invoices:payment/invoice`) and a `UUID`. Immutable after creation.
- **`priority`** (`int`) — numeric priority ranking, defaults to `0`. Immutable after creation.
- **`deadline`** (`Instant`, nullable) — optional due-by timestamp. Immutable after creation.

### WorkTask Lifecycle

Commands drive a state machine. `Reassign` and `Unassign` are special cases of `Assign` — a single `AssignWorkTask` command with a nullable `assigneeId`:
- non-null `assigneeId` → ASSIGNED (assign or reassign)
- null `assigneeId` → DRAFT (unassign)

Valid transitions:

| Command  | Valid from                            | Resulting state |
|----------|---------------------------------------|-----------------|
| Create   | (none — initial)                      | DRAFT           |
| Assign (assigneeId non-null) | DRAFT, ASSIGNED, IN_PROGRESS, PAUSED | ASSIGNED |
| Assign (assigneeId null)     | ASSIGNED, IN_PROGRESS, PAUSED        | DRAFT    |
| Begin    | ASSIGNED                              | IN_PROGRESS     |
| Pause    | IN_PROGRESS                           | PAUSED          |
| Resume   | PAUSED                                | IN_PROGRESS     |
| Complete | IN_PROGRESS                           | COMPLETED       |
| Abort    | ASSIGNED, IN_PROGRESS, PAUSED         | ABORTED         |
| Cancel   | DRAFT, ASSIGNED, IN_PROGRESS, PAUSED  | CANCELLED       |

Terminal states: `COMPLETED`, `ABORTED`, `CANCELLED`.

Three separate domain events are emitted from the unified Assign command: `WorkTaskAssigned` (from DRAFT), `WorkTaskReassigned` (from ASSIGNED/IN_PROGRESS/PAUSED with non-null assignee), `WorkTaskUnassigned` (null assignee).

### Kafka Streams Topology

```
[command topic]
      │
      ├── (unparsable message) ──► [dead-letter topic]
      │
      ▼
 StateTransitionProcessor
      │  loads current WorkTask from KeyValueStore, applies transition
      │
      ├── (invalid transition) ──► [event topic]  — WorkTaskCommandRejected event
      │
      ├──► [event topic]    — domain event (result of successful command)
      └──► [compact topic]  — full materialized WorkTask state (same key)
                               (both writes in one Kafka Streams transaction)
      │
      ▼
 DatabaseSink (processor or separate consumer)
      └──► persistent database (read model / query side)
```

Atomicity between the `event` and `compact` topics is achieved via Kafka Streams exactly-once semantics — configure `processing.guarantee=exactly_once_v2`. The KeyValueStore is the authoritative in-flight state between commands.

Only unparsable (undeserializable) messages go to the dead-letter topic. Invalid commands that fail state-transition validation produce a rejection event on the event topic instead.

### Keys and Identifiers

- WorkTask IDs are **UUID v7** (time-ordered, sortable) used as the Kafka record key
- Other identifiers (assignee, correlation, subject) use UUID v4 or v7 as convenient
- Kafka record keys are serialized as `String` (UUID `toString()`)
- Domain model uses `UUID` directly — no wrapper types

### CloudEvents and Observability

All Kafka records (event topic + compact topic) use the **Kafka Protocol Binding for CloudEvents 1.0, binary content mode**: the record value is raw Avro bytes and all CloudEvents attributes are Kafka record headers with the `ce_` prefix.

| Kafka Header | Value / convention |
|---|---|
| `ce_specversion` | `1.0` |
| `ce_type` | `com.example.worktaskservice.worktask.<verb>.v1` |
| `ce_source` | `/worktaskservice` |
| `ce_id` | UUID v4 (per-event; distinct from WorkTask ID) |
| `ce_time` | RFC 3339 timestamp |
| `ce_subject` | `{subjectType}/{subjectId}` — e.g. `billing.invoices:payment/invoice/550e8400-…` |
| `ce_datacontenttype` | `application/avro` |
| `ce_dataschema` | `{SCHEMA_REGISTRY_URL}/apis/registry/v2/groups/default/artifacts/{avro.schema.fullName}` |
| `ce_partitionkey` | WorkTask ID (UUID string) — [Partitioning extension](https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/partitioning.md); matches the Kafka record key |
| `ce_traceparent` | W3C Trace Context — [Distributed Tracing extension](https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/distributed-tracing.md); propagated from inbound command |
| `ce_tracestate` | W3C Trace Context — Distributed Tracing extension; propagated from inbound command (if present) |

Inbound command records carry the same `ce_` headers. The topology extracts `ce_traceparent`/`ce_tracestate` to restore OTel context before processing. Use OTel semantic conventions for span and metric naming (`messaging.*`, `db.*`).

### Avro Schemas

All Kafka message schemas are `.avsc` files under `src/main/avro/`. Java classes are **generated** from these schemas by the `quarkus-avro` extension at build time — do not hand-write DTOs for commands, events, or state.

#### Type conventions

| Logical type | Avro encoding |
|---|---|
| UUID | `{"type": "fixed", "size": 16, "logicalType": "uuid", "name": "UUID"}` — 16-byte binary |
| Timestamp | `{"type": "long", "logicalType": "timestamp-nanos"}` — epoch nanoseconds |

Define the `UUID` fixed type inline on first use within each schema file; reference it by name (`UUID`) for subsequent uses in the same schema. Nullable UUIDs: `["null", <UUID-type>]`.

#### Schema files

```
src/main/avro/
  commands/
    CreateWorkTask.avsc      ← type, subjectType, subjectId, title, description, priority, deadline
    AssignWorkTask.avsc      ← assigneeId (nullable — null means unassign)
    BeginWorkTask.avsc
    PauseWorkTask.avsc
    ResumeWorkTask.avsc
    CompleteWorkTask.avsc
    AbortWorkTask.avsc       ← reason (nullable)
    CancelWorkTask.avsc      ← reason (nullable)
  events/
    WorkTaskCreated.avsc     ← type, subjectType, subjectId, title, description, priority, deadline
    WorkTaskAssigned.avsc
    WorkTaskReassigned.avsc
    WorkTaskUnassigned.avsc
    WorkTaskBegun.avsc
    WorkTaskPaused.avsc
    WorkTaskResumed.avsc
    WorkTaskCompleted.avsc
    WorkTaskAborted.avsc
    WorkTaskCancelled.avsc
    WorkTaskCommandRejected.avsc  ← rejectionReason, commandType
  state/
    WorkTask.avsc            ← compact topic payload (full materialized state)
```

All command schemas share base fields: `workTaskId` (UUID), `correlationId` (UUID).
All event schemas share base fields: `workTaskId` (UUID), `correlationId` (UUID), `occurredAt` (timestamp-nanos).

Namespace convention: `com.example.worktaskservice.<commands|events|state>`

Use Schema Registry (Apicurio). Compatibility mode differs by message direction:
- **Commands** (inbound, consumed by this service): `BACKWARD` — this service must keep reading commands written against older schema versions as producers upgrade independently.
- **Events and state** (outbound, produced by this service): `FORWARD` — downstream consumers must keep reading new schema versions with their older schema, since they may upgrade later than this service does.

### Package Structure

```
src/main/java/com/example/worktaskservice/
│
├── domain/                          ← pure domain model, no framework dependencies
│   ├── model/
│   │   ├── WorkTask.java            ← aggregate root (UUID id, WorkTaskType type, Subject subject, …)
│   │   ├── WorkTaskStatus.java      ← enum: DRAFT, ASSIGNED, IN_PROGRESS, PAUSED,
│   │   │                               COMPLETED, ABORTED, CANCELLED
│   │   ├── WorkTaskType.java        ← validated string: <domain>(.<subdomain>)?:<bc>/<task-name>
│   │   ├── SubjectType.java         ← same format as WorkTaskType; shared validation utility
│   │   └── Subject.java             ← record: SubjectType type + UUID id
│   ├── command/                     ← command records (mapped FROM Avro-generated classes)
│   ├── event/                       ← domain event records
│   └── exception/
│       └── InvalidStateTransitionException.java
│
├── application/                     ← use-case orchestration
│   ├── WorkTaskCommandHandler.java
│   └── port/
│       ├── WorkTaskRepository.java       ← outbound port (interface)
│       └── WorkTaskEventPublisher.java   ← outbound port (interface)
│
└── infrastructure/                  ← Quarkus / framework adapters
    ├── kafka/
    │   ├── topology/
    │   │   └── WorkTaskTopologyProducer.java  ← @Produces Topology bean
    │   ├── serde/
    │   └── mapper/
    │       ├── CommandAvroMapper.java   ← Avro SpecificRecord → domain command; reads CE trace headers
    │       └── EventAvroMapper.java     ← domain event → Avro SpecificRecord; builds ce_ headers
    ├── persistence/
    │   ├── WorkTaskEntity.java      ← JPA entity
    │   └── PanacheWorkTaskRepository.java
    └── config/
        └── TopicConfig.java         ← topic name constants / @ConfigProperty
```

### Key Quarkus Extensions

```kotlin
// build.gradle.kts
implementation("io.quarkus:quarkus-avro")                        // Avro codegen (native, no plugin needed)
implementation("io.quarkus:quarkus-apicurio-registry-avro")      // Schema Registry + Avro SerDes
implementation("io.quarkus:quarkus-kafka-streams")
implementation("io.quarkus:quarkus-hibernate-orm-panache")
implementation("io.quarkus:quarkus-jdbc-postgresql")
implementation("io.quarkus:quarkus-opentelemetry")
implementation("io.quarkus:quarkus-smallrye-health")
```

Quarkus Dev Services auto-provisions Kafka, PostgreSQL, and Apicurio Schema Registry in dev and test mode — no manual Docker Compose setup required.
