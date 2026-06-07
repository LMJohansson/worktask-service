# WorkTask Service

A **Java 25 / Quarkus / Kafka Streams** service for managing Work Tasks,
built using **Event Driven Architecture (EDA)** and **Domain Driven Design
(DDD)**.

External clients drive a `WorkTask` through its lifecycle by sending
commands; the service validates each transition, persists the resulting
state, and publishes domain events and a materialized read-model — all
atomically, via Kafka Streams exactly-once semantics.

The full async API contract is published as an [AsyncAPI](https://www.asyncapi.com/)
specification — see [`src/main/resources/asyncapi.yaml`](src/main/resources/asyncapi.yaml).

## Tech stack

- **Java 25** (openjdk-25)
- **Quarkus 3.36.1** (Gradle Kotlin DSL)
- **Kafka Streams** (low-level `Topology` API, `exactly_once_v2`)
- **Avro** schemas + **Apicurio Schema Registry**
- **PostgreSQL** (read-model persistence via Hibernate ORM with Panache)
- **CloudEvents 1.0** (Kafka Protocol Binding, binary content mode)
- **OpenTelemetry**

## Architecture

### Bounded context & topic naming

Topics follow the convention
`<domain>(.<subdomain>)?.<bounded context>.[public|private].<aggregate>(.<suffix>)?`.
The three operative topics are public (part of the API contract); a fourth,
private topic exists for operational dead-lettering:

| Purpose            | Topic                                              | Suffix    | Retention          |
|--------------------|----------------------------------------------------|-----------|--------------------|
| Inbound commands   | `work.tasks.worktask.public.worktask.command`      | `command` | Delete (short)     |
| Domain events      | `work.tasks.worktask.public.worktask.event`        | `event`   | Delete (long)      |
| Materialized state | `work.tasks.worktask.public.worktask.compact`      | `compact` | Compact (infinite) |
| Dead letter (internal) | `work.tasks.worktask.private.worktask.dead-letter` | —     | —                  |

### WorkTask domain model

Every `WorkTask` has:

- **`type`** (`WorkTaskType`) — the action to be performed, format
  `<domain>(.<subdomain>)?:<bounded-context>/<task-name>`
  (e.g. `billing.invoices:payment/process-refund`). Immutable.
- **`subject`** (`Subject`) — the aggregate in another bounded context that
  the task acts on: a `SubjectType` (same format as `WorkTaskType`) plus a
  `UUID`. Immutable.
- **`title`** / **`description`** — descriptive text; `description` is optional.
- **`priority`** (`int`) — numeric priority ranking, defaults to `0`. Immutable.
- **`deadline`** (`Instant`, optional) — due-by timestamp. Immutable.

`type`, `subject`, `title`, `description`, `priority`, and `deadline` are all
set at creation time and never change afterward.

### Lifecycle / state machine

Commands drive a state machine. `Reassign` and `Unassign` are special cases
of `Assign` — a single `AssignWorkTask` command with a nullable `assigneeId`
(non-null → `ASSIGNED`, null → `DRAFT`):

| Command                      | Valid from                              | Resulting state |
|------------------------------|------------------------------------------|-----------------|
| Create                       | *(none — initial)*                       | `DRAFT`         |
| Assign (`assigneeId` non-null) | `DRAFT`, `ASSIGNED`, `IN_PROGRESS`, `PAUSED` | `ASSIGNED`  |
| Assign (`assigneeId` null)   | `ASSIGNED`, `IN_PROGRESS`, `PAUSED`       | `DRAFT`         |
| Begin                        | `ASSIGNED`                                | `IN_PROGRESS`   |
| Pause                        | `IN_PROGRESS`                             | `PAUSED`        |
| Resume                       | `PAUSED`                                  | `IN_PROGRESS`   |
| Complete                     | `IN_PROGRESS`                             | `COMPLETED`     |
| Abort                        | `ASSIGNED`, `IN_PROGRESS`, `PAUSED`       | `ABORTED`       |
| Cancel                       | `DRAFT`, `ASSIGNED`, `IN_PROGRESS`, `PAUSED` | `CANCELLED`  |

`COMPLETED`, `ABORTED`, and `CANCELLED` are terminal. The unified `Assign`
command emits one of three distinct domain events depending on context:
`WorkTaskAssigned` (from `DRAFT`), `WorkTaskReassigned` (from
`ASSIGNED`/`IN_PROGRESS`/`PAUSED` with a non-null assignee), or
`WorkTaskUnassigned` (null assignee).

### Kafka Streams topology

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
 DatabaseSink
      └──► PostgreSQL (read model / query side)
```

Implemented in `WorkTaskTopologyProducer` using the low-level `Topology` API
with named sink nodes (`event-sink`, `compact-sink`, `dead-letter-sink`) and
a persistent `KeyValueStore` (`worktask-store`) holding the authoritative
in-flight state. Atomicity between the `event` and `compact` topics is
achieved via `processing.guarantee=exactly_once_v2`. Only genuinely
unparsable (undeserializable) records go to the dead-letter topic — commands
that fail state-transition validation instead produce a
`WorkTaskCommandRejected` event on the event topic.

### CloudEvents binding

All Kafka records on the `event` and `compact` topics use the **CloudEvents
1.0 Kafka Protocol Binding, binary content mode**: the record value is raw
Avro bytes, and every CloudEvents attribute is carried as a `ce_`-prefixed
Kafka header (`ce_specversion`, `ce_type`, `ce_source`, `ce_id`, `ce_time`,
`ce_subject`, `ce_datacontenttype`, `ce_dataschema`, `ce_partitionkey`,
`ce_traceparent`, `ce_tracestate`). Inbound command records carry the same
header set; trace context is extracted and propagated end-to-end. See the
[AsyncAPI spec](src/main/resources/asyncapi.yaml) for the full per-message
contract, including concrete `ce_type` values.

## Implementation notes

### Package structure

```
src/main/java/com/example/worktaskservice/
├── domain/            ← pure domain model, no framework dependencies
│   ├── model/         ← WorkTask, WorkTaskStatus, WorkTaskType, Subject, SubjectType
│   ├── command/       ← command records (mapped from Avro-generated classes)
│   ├── event/         ← domain event records
│   └── exception/
├── application/       ← use-case orchestration (WorkTaskCommandHandler, ports)
└── infrastructure/    ← Quarkus / framework adapters
    ├── kafka/         ← topology, serde, Avro ⇄ domain mappers
    ├── persistence/   ← JPA entity + Panache repository
    └── config/        ← topic name configuration
```

### Avro-codegen-first

All Kafka message schemas live as `.avsc` files under `src/main/avro/`
(`commands/`, `events/`, `state/`). Java classes are **generated** from these
schemas at build time by the `quarkus-avro` extension — commands, events, and
state are never hand-written as DTOs; only the pure domain records are.

Type conventions:

| Logical type | Avro encoding |
|---|---|
| UUID | `{"type": "fixed", "size": 16, "logicalType": "uuid", "name": "UUID"}` (16-byte binary) |
| Timestamp | `{"type": "long", "logicalType": "timestamp-nanos"}` (epoch nanoseconds) |

### Schema Registry

Schemas are registered with an Apicurio Schema Registry. Compatibility mode
differs by message direction:

- **Commands** (consumed by this service): `BACKWARD` — the service must keep
  reading commands written against older schema versions as producers
  upgrade independently.
- **Events & state** (produced by this service): `FORWARD` — downstream
  consumers must keep reading newer schema versions with their older schema,
  since they may upgrade later than this service does.

### Identifiers

WorkTask IDs are **UUID v7** (time-ordered, sortable) and serve as the Kafka
record key (serialized as `String` via `toString()`). Other identifiers
(assignee, correlation, subject) use UUID v4 or v7 as convenient. The domain
model uses `UUID` directly — no wrapper types.

## Developer guide

### Prerequisites

- Java 25 (openjdk-25)
- Docker (for Dev Services — Kafka, PostgreSQL, and an Apicurio Schema
  Registry are auto-provisioned in dev/test mode; no manual Docker Compose
  setup required)

### Build & run

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

In dev mode, Quarkus Dev Services automatically provisions Kafka, PostgreSQL,
and an Apicurio Schema Registry, and creates the four topics listed above.
Note that changes to Dev Services topic/partition configuration in
`application.properties` require a full restart of `quarkusDev` (not just
live reload), since that configuration is build-time.

### Testing

The domain-layer state machine is exhaustively covered by
[`WorkTaskStateMachineTest`](src/test/java/com/example/worktaskservice/domain/model/WorkTaskStateMachineTest.java)
— 60+ tests covering every valid transition, every invalid-transition
rejection, the three-way `Assign`/`Reassign`/`Unassign` event split, and
shared event-field contracts (`correlationId` propagation, `occurredAt`,
`updatedAt` advancement, `createdAt` immutability). New domain behavior
should follow the same `@Nested`-per-command, `@ParameterizedTest`-over-states
structure.

## Project status

Implementation in progress — the core domain model, state machine, Kafka
Streams topology, persistence, and CloudEvents binding are scaffolded and
tested; build and `quarkusDev` run cleanly with all Dev Services healthy.

## License

Licensed under the BSD Zero Clause License — see [LICENSE](LICENSE).
