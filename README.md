<img src="./tarodomain.png" alt="TaroDomain logo" width="300">

# tarodomain

> **This is the way to build scalable and beautiful systems.**

**TaroDomain Framework — Complete Design Specification**

Version 1.0 | Language & Platform Agnostic

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Core Domain Primitives](#2-core-domain-primitives)
3. [Event Sourcing Model](#3-event-sourcing-model)
4. [Command Model](#4-command-model)
5. [Aggregate Root](#5-aggregate-root)
6. [Repository Pattern](#6-repository-pattern)
7. [Event Store](#7-event-store)
8. [Snapshot Store](#8-snapshot-store)
9. [Saga Store](#9-saga-store)
10. [Event Bus & Dispatch](#10-event-bus--dispatch)
11. [Command Pipeline](#11-command-pipeline)
12. [Projection System](#12-projection-system)
13. [Process Manager (Saga) System](#13-process-manager-saga-system)
14. [Idempotency](#14-idempotency)
15. [Consistency Modes](#15-consistency-modes)
16. [Concurrency Control](#16-concurrency-control)
17. [Compensation Tracking](#17-compensation-tracking)
18. [Subscription & Catch-Up](#18-subscription--catch-up)
19. [Timeout Scheduling](#19-timeout-scheduling)
20. [Rebuild System](#20-rebuild-system)
21. [Dead Letter Queue](#21-dead-letter-queue)
22. [Error Hierarchy](#22-error-hierarchy)
23. [Dependency Injection](#23-dependency-injection)
24. [Provider Abstraction](#24-provider-abstraction)
25. [Builder DSL](#25-builder-dsl)
26. [Worker Entry Points](#26-worker-entry-points)
27. [Database Schema](#27-database-schema)
28. [Causation & Correlation](#28-causation--correlation)
29. [Validation](#29-validation)
30. [Glossary](#30-glossary)

---

## 1. Architecture Overview

### 1.1 Design Principles

| Principle | Implementation |
|---|---|
| **Event Sourcing** | Aggregates persist as append-only event sequences. State is reconstructed by replaying events. |
| **CQRS** | Commands (writes) and queries (reads) are separated. Projections build read-optimized models from events. |
| **DDD** | Aggregates, value objects, domain events, commands. No infrastructure concerns in domain code. |
| **OOP over functional** | Classes, inheritance, polymorphism. No decorators, no functional pipelines. |
| **Convention over configuration** | `on[EventName]`, `startsWhen[EventName]`, etc. — method name conventions for dispatch. |
| **Platform-agnostic** | Core framework has no cloud-specific code. Adapters implement platform-specific persistence. |
| **Dual consistency** | Strong (synchronous, transactional) or eventual (queue-based, async). Per-command override. |
| **Idempotent everywhere** | Commands, projections, and sagas are all idempotent via deduplication tables. |

### 1.2 Layered Architecture

```
┌───────────────────────────────────────────────────────────┐
│                    Worker Entry Point                     │
│  (HTTP fetch / Queue consumer / Cron scheduled)           │
├───────────────────────────────────────────────────────────┤
│                    Framework (builder)                    │
│  Routes commands to strong/eventual pipeline              │
├──────────────────────┬────────────────────────────────────┤
│  Command Pipeline    │      Event Bus                     │
│  (strong / eventual) │  (InMemory / Queue / Cron)         │
├──────────────────────┼──────────────────────┬─────────────┤
│  Command Handler     │  Projection Handler  │ Saga Handler│
│  (loads aggregate,   │  (updates read       │ (coordinates│
│   executes, saves)   │   models)            │  multi-agg  │
│                      │                      │  processes) │
├──────────────────────┴──────────────────────┴─────────────┤
│                  Domain Layer (pure)                      │
│  AggregateRoot, DomainEvent, Command, ValueObject         │
├───────────────────────────────────────────────────────────┤
│              Persistence Layer (adapters)                 │
│  EventStore, SnapshotStore, SagaStore, Repository         │
├───────────────────────────────────────────────────────────┤
│              Platform Layer (provider)                    │
│  Cloudflare (D1/Queue/Cron) / AWS (DynamoDB/SQS) / Local  │
└───────────────────────────────────────────────────────────┘
```

### 1.3 Data Flow

```
HTTP POST → Command → Pipeline → Handler → Aggregate.raise() → Event
                                                        │
                    ┌───────────────────────────────────┘
                    ▼
              EventStore.append  (durable)
                    │
                    ▼
              EventBus.publish  (dispatch)
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
     Projection    Saga    Integration
    (read model)  (coord)   (webhook)
```

---

## 2. Core Domain Primitives

### 2.1 DomainEvent

**Purpose:** Immutable record of something that happened in the domain.

**Properties:**

| Property | Type | Description |
|---|---|---|
| `id` | UUIDv7 (string) | Globally unique, time-ordered (sortable) |
| `eventName` | string | Event type name (default: class name; overridable for minification) |
| `eventVersion` | string | Schema version (semver: "1.0.0"). Used by upcasters. |
| `correlationId` | UUIDv7 | End-to-end trace ID. Inherited from command. |
| `causationId` | UUIDv7 \| null | ID of the command/event that caused this event. null = root. |
| `metadata` | Map<string, primitive> | Free-form metadata (userId, tenantId, IP, etc.). Frozen. |
| `occurredAt` | DateTime | When the event was raised. |
| **Business fields** | user-defined | Declared as constructor parameters on subclass. |

**Rules:**

- Immutable (frozen after construction).
- `id` auto-generated as UUIDv7 if not provided.
- `correlationId` auto-generated if not provided (new trace root).
- `causationId` defaults to `null` (root command).

**Serialization (`SerializedEvent`):**

| Field | Storage column |
|---|---|
| `event_id` | Primary key |
| `event_type` | `eventName` |
| `event_version` | `eventVersion` |
| `causation_id` | `causationId` (nullable) |
| `correlation_id` | `correlationId` |
| `data` | JSON of business fields |
| `metadata` | JSON of metadata |
| `occurred_at` | ISO 8601 timestamp |

**Rehydration (`rehydrate(data, init)`):** Static method that constructs an event from stored data WITHOUT calling the constructor. Uses `Object.create(prototype)` + `Object.defineProperty` for each field. Preserves all validations.

### 2.2 Command

**Purpose:** Immutable intent to change system state. Can be rejected.

**Properties:** Same envelope as DomainEvent (`id`, `correlationId`, `causationId`, `metadata`, `timestamp`), plus business fields.

**Difference from event:** Commands represent intent (future), events represent facts (past). Commands can be rejected; events cannot.

### 2.3 ValueObject\<T>

**Purpose:** Object defined by its value, not identity. Immutable.

**Properties:**

- `value: T` — the inner value.
- Structural equality (`equals(other)`).
- `hashCode()` — consistent with `equals()`.
- Optional `compareTo(other)` for ordered VOs.
- `validate(value)` hook — invariants checked at construction.
- `with(mutator)` — functional update (returns new instance).

### 2.4 AggregateId

**Purpose:** Typed UUID value object for aggregate identifiers.

**Extends:** `ValueObject<string>`.

**Factories:**

- `generate()` — fresh UUIDv7.
- `fromExisting(uuid)` — parse any valid UUID (any version).
- `fromUuidv7(uuid)` — parse, enforce v7 only.

**Subclassing:** Users create type-safe subclasses (`class AccountId extends AggregateId`) to prevent mixing IDs at compile time.

---

## 3. Event Sourcing Model

### 3.1 Event Store

**Purpose:** Append-only log of all domain events.

**Access patterns:**

1. Append events to a stream (with optimistic concurrency check).
2. Load events from a stream (by stream ID + from version).
3. Load all events globally (by event ID cursor — chronological for UUIDv7).
4. Load by correlation ID (for tracing).
5. Load stream after a specific event ID (for subscription cursors).
6. Manage subscription cursors (create, read, advance, delete, list).

**Stream ID conventions:**

- Per-aggregate: `"BankAccount-01909e3c-..."` (type + UUID).
- Category: `"BankAccount"` (all events of a type).
- Global: `"$all"` (all events).

### 3.2 EventStoreAdapter Interface

```
interface EventStoreAdapter:
  appendToStream(streamId, events[], expectedVersion) → AppendResult
  loadStream(streamId, fromVersion?, maxCount?) → SerializedEvent[]
  loadStreamAfterEvent(streamId, afterEventId?, maxCount?) → SerializedEvent[]
  loadAll(fromEventId?, maxCount?) → SerializedEvent[]
  loadByCorrelationId(correlationId) → SerializedEvent[]
  streamExists(streamId) → boolean
  getStreamVersion(streamId) → int
  getStreamMetadata(streamId) → StreamMetadata | null
  deleteStream(streamId) → void
  subscribe(name, options?) → Subscription
  getSubscription(name) → Subscription | null
  ackSubscription(name, lastSeenEventId) → void
  deleteSubscription(name) → void
  listSubscriptions() → Subscription[]
```

### 3.3 EventStore Wrapper

Wraps `EventStoreAdapter` and handles:

- **Serialization:** `DomainEvent` → `SerializedEvent` (via `event.serialize()`).
- **Deserialization:** `SerializedEvent` → `DomainEvent` (via `EventRegistry.deserialize()`).
- **Upcasters:** Transform old event versions to current version before deserialization.

### 3.4 EventRegistry

Maps `(eventType, version)` → factory function. Handles:

- `register(eventClass, { version, factory? })` — register an event type.
- `registerUpcaster(eventType, fromVersion, toVersion, upcasterFn)` — register a version transform.
- `deserialize(serialized)` — apply upcasters → call factory → return typed event.
- `knows(eventType, version?)` — check if registered.

**Upcaster pipeline:** Events stored as v1.0.0. Latest registered is v1.2.0.
Chain: v1.0.0 →(upcasterA)→ v1.1.0 →(upcasterB)→ v1.2.0. Applied on read.

---

## 4. Command Model

### 4.1 CommandHandler\<C, A>

**Generic params:**

- `C` — command type.
- `A` — aggregate root type.

**Constructor:** `(repo: Repository<A>, eventBus: EventBus)`. Additional dependencies via subclass constructor.

**Abstract:**

- `aggregateType: new(id) => A` — the aggregate class (for DI resolution).
- `handle(command: C): Promise<void>` — execute the command.

**Helper methods (protected):**

- `load(id): Promise<A>` — delegate to repo.
- `loadOrNull(id): Promise<A | null>` — check exists first.
- `save(aggregate): Promise<void>` — delegate to repo.
- `exists(id): Promise<boolean>` — delegate to repo.

**Validation:** Done inside the handler before calling aggregate methods. Throw domain errors for violations.

### 4.2 CommandDispatcher

Resolves handlers by command class and dispatches.

```
interface CommandDispatcher:
  resolve(command) → CommandHandler  // throws if not registered
  dispatch(command) → CommandDispatchResult
  dispatchAll(commands[], { continueOnError? }) → BatchDispatchResult
  canDispatch(command) → boolean
```

### 4.3 CommandRegistry

Maps command classes to handlers. Also maps command type names to classes (for deserialization from queue messages).

---

## 5. Aggregate Root

### 5.1 AggregateRoot\<TId>

**Properties:**

| Property | Type | Visibility | Description |
|---|---|---|---|
| `id` | TId | public readonly | Aggregate identifier |
| `version` | int | public get | Current version (incremented per event applied) |
| `committedVersion` | int | public get | `version - uncommittedCount` (for optimistic concurrency) |
| `hasUncommittedChanges` | boolean | public get | True if uncommitted events exist |

**Core methods:**

| Method | Visibility | Description |
|---|---|---|
| `raise(event)` | protected | Apply event to state + queue for persistence. Enriches with execution context if set. |
| `loadFromHistory(events[], snapshotVersion?)` | public | Replay events from store. Does NOT queue. |
| `getUncommittedChanges()` | public | Return copy of uncommitted events. |
| `markChangesAsCommitted()` | public | Clear uncommitted buffer. |
| `toSnapshot()` | public | Extract business state for snapshot. |
| `restoreSnapshot(data, version)` | public | Restore state from snapshot. |
| `_setExecutionContext(ctx)` | internal | Set correlation/causation IDs for event enrichment. |
| `_clearExecutionContext()` | internal | Clear after save. |

### 5.2 Event Application

**`raise(event)` flow:**

1. If execution context is set → enrich event with `correlationId` + `causationId`.
2. Call `applyEvent(enrichedEvent)` → find `on[EventName]` method → call it → increment version.
3. Push enriched event to uncommitted buffer.

**`applyEvent(event)` flow:**

1. Find method: `this["on" + event.eventName]`.
2. If not found → call `onUnknownEvent(event)` (default: throw `MissingEventHandlerError`).
3. If found → call handler → increment `#version`.

### 5.3 Constructor Rules

- Constructor takes only `id` (and optionally initializes field defaults).
- Constructor MUST NOT raise events (it's reused for rehydration).
- Static factory methods (e.g., `BankAccount.open(id, ownerId)`) create instances and call `raise()`.
- The framework constructs instances via `new AggregateType(id)` for rehydration.

### 5.4 Snapshot Support

**`toSnapshot()`:**

- Extract all own enumerable properties except `id`.
- `#version` and `#uncommitted` are private (excluded automatically).
- Override `toSnapshotState()` for `#private` fields.

**`restoreSnapshot(data, version)`:**

- `Object.assign(this, data)` for public fields.
- Override `restoreSnapshotState(data)` for `#private` fields.
- Set `#version = version`, clear `#uncommitted`.

### 5.5 Execution Context

Links events to commands via tracing IDs. Set by the pipeline before `handle()`, cleared after.

```
ExecutionContext.set({ correlationId: command.correlationId, causationId: command.id })
// → repo.load() sets context on aggregate instance
// → aggregate.raise() enriches events with context
// → repo.save() appends enriched events
ExecutionContext.clear()
```

> **Caveat:** Module-level variable — shared between concurrent requests in same process. For Cloudflare Workers, safe for sequential command processing. For high-concurrency, use eventual consistency or per-request context injection.

---

## 6. Repository Pattern

### 6.1 Repository\<T> Interface

```
interface Repository<T extends AggregateRoot>:
  load(id) → T              // throws if not found
  save(aggregate) → void    // append events + publish + mark committed
  exists(id) → boolean
```

### 6.2 EventSourcedRepository\<T> (framework-provided)

**Constructor:** `(eventStore, eventBus, snapshotStore?, factory, aggregateType, snapshotEvery)`

**Load flow:**

1. Try `snapshotStore.loadLatest(id, type)` — non-critical (fallback to full replay on failure).
2. `factory(id)` — create empty aggregate.
3. If snapshot: `aggregate.restoreSnapshot(data, version)`.
4. `eventStore.loadStream(streamId, fromVersion)` — events after snapshot.
5. `aggregate.loadFromHistory(events, snapshotVersion)`.
6. If no snapshot AND no events → throw `AggregateNotFoundError`.
7. Set execution context if available.

**Save flow:**

1. `changes = aggregate.getUncommittedChanges()`.
2. If empty → return.
3. `eventStore.appendToStream(streamId, changes, aggregate.committedVersion)`.
4. `eventBus.publishAll(changes)`.
5. `aggregate.markChangesAsCommitted()`.
6. If `newVersion % snapshotEvery == 0` → `snapshotStore.save(...)` (non-critical).

### 6.3 StateRepository\<T> (user-implemented)

User implements for state-only persistence mode. NO generic implementation — each aggregate gets its own dedicated table.

Base class `StateRepository<T>` provides:

- `save(aggregate)` — calls user's `saveState()` + `eventBus.publishAll(events)` + `markChangesAsCommitted()`.
- `loadOrNull(id)` — helper.
- User implements: `load(id)`, `saveState(aggregate)`, `exists(id)`.

**State extraction:** Use `aggregate.toSnapshot()` → `{ data, version }`.
**State restoration:** Use `aggregate.restoreSnapshot(data, version)`.

---

## 7. Event Store

### 7.1 Serialization Boundary

`EventStore` wraps `EventStoreAdapter`:

- `appendToStream(streamId, events: DomainEvent[], expectedVersion)` → serialize → delegate to adapter.
- `loadStream(streamId, fromVersion)` → deserialize via `EventRegistry` → return `DomainEvent[]`.

### 7.2 Append Atomicity

| Platform | Mechanism | Limit |
|---|---|---|
| D1 (SQLite) | `db.batch(stmts)` — all-or-nothing | ~1000 statements |
| DynamoDB | `TransactWriteItems` — conditional writes | 25 items per transaction |
| InMemory | Synchronous (single-threaded) | Unlimited |

For DynamoDB, events > 12 per append (1 stream + 2N items) exceed the 25-item limit — split into multiple transactions (non-atomic, documented limitation).

### 7.3 Optimistic Concurrency

**At adapter level:** `appendToStream` checks `expectedVersion === streamVersion` before appending.

**At SQL level:** `UPDATE streams SET stream_version = ? WHERE stream_uuid = ? AND stream_version = ?` — 0 rows affected = conflict.

**At DynamoDB level:** `ConditionExpression: 'stream_version = :expected'` — `TransactionCanceledException` = conflict.

On conflict → throw `OptimisticConcurrencyError` (category: `concurrency`, retryable: `true`).

---

## 8. Snapshot Store

### 8.1 SnapshotStoreAdapter Interface

```
interface SnapshotStoreAdapter:
  save(input: SnapshotInput) → void           // idempotent upsert
  loadLatest(sourceUuid, sourceType) → Snapshot | null
  loadAtVersion(sourceUuid, sourceType, version) → Snapshot | null
  list(sourceUuid, sourceType, options?) → Snapshot[]
  deleteAll(sourceUuid, sourceType) → void
  deleteBefore(sourceUuid, sourceType, version) → int  // count deleted
  count(sourceUuid, sourceType) → int
```

### 8.2 Snapshot Types

```
SnapshotInput: { sourceUuid, sourceVersion, sourceType, data, metadata? }
Snapshot: SnapshotInput + { createdAt }
```

**Primary key:** `(source_uuid, source_version)`.

---

## 9. Saga Store

### 9.1 SagaStoreAdapter Interface

```
interface SagaStoreAdapter:
  save(input: SagaSaveInput) → void               // upsert
  load(sagaType, sagaId) → SagaRecord | null
  loadByCorrelationId(sagaType, correlationId) → SagaRecord | null  // sagaId = correlationId
  complete(sagaType, sagaId) → void                // partial update, idempotent
  fail(sagaType, sagaId, reason) → void           // partial update, idempotent
  loadByStatus(status, options?) → SagaRecord[]   // ordered by updated_at ASC
  loadTimedOut(beforeTime, options?) → SagaRecord[]  // status IN (RUNNING, COMPENSATING)
  delete(sagaType, sagaId) → void
  count(sagaType?, status?) → int
```

### 9.2 Saga Record

```
SagaSaveInput: { sagaId, sagaType, correlationId, state, status, currentStep, failureReason? }
SagaRecord: SagaSaveInput + { startedAt, updatedAt, completedAt? }
```

**Primary key:** `(saga_type, saga_id)`.
`saga_id` = `correlation_id` from the starting event.

---

## 10. Event Bus & Dispatch

### 10.1 EventBus Interface

```
interface EventBus:
  readonly name: string
  publishAll(events: DomainEvent[]) → void
  publish(event: DomainEvent) → void  // convenience: publishAll([event])
```

### 10.2 EventHandler Interface

```
interface EventHandler:
  readonly name: string
  handle(event: DomainEvent) → void  // idempotent — may be called multiple times
```

### 10.3 Implementations

| Implementation | Use Case | Behavior |
|---|---|---|
| **InMemoryEventBus** | Strong consistency | Synchronous dispatch. Fail-fast (throws on first handler error). |
| **CloudflareQueueEventBus** | Eventual (CF) | `queue.sendBatch()` — enqueues SerializedEvent messages. Chunked at 100. |
| **SqsEventBus** | Eventual (AWS) | SQS `SendMessageBatch` — chunked at 10. FIFO support with MessageGroupId. |
| **CronDispatcher** | Safety net | Pull-based. Reads events from event store that weren't dispatched (subscription cursor lag). |

### 10.4 InMemoryEventBus (Strong Consistency)

Implements `SubscribableEventBus`:

- `subscribe(handler)` — register handler (prevents duplicates by name).
- `unsubscribe(handler)` — unregister.
- `publishAll(events)` — fail-fast: throws `EventHandlerError` on first failure.
- `publishAllWithResults(events)` — continue-on-error: collects all results, doesn't throw.

### 10.5 Queue Consumer (Eventual)

Separate worker that consumes from the queue:

1. Receive `MessageBatch<SerializedEvent>`.
2. For each message: `EventRegistry.deserialize(body)` → `DomainEvent`.
3. `inMemoryEventBus.publish(event)` — dispatch to projections/sagas.
4. `message.ack()` — success.
5. On failure: `message.retry({ delaySeconds })` if retryable, else dead-letter + `ack()`.

---

## 11. Command Pipeline

### 11.1 StrongConsistencyPipeline

**Flow:**

1. `ConcurrencyChecker.withRetry()` — retry on `OptimisticConcurrencyError`.
2. Inside retry:
   1. `UnitOfWork.begin()` — start transaction.
   2. `ExecutionContext.set({ correlationId, causationId })`.
   3. `handler.handle(command)` — load aggregate, execute, save.
      - `repo.save()` → `eventStore.appendToStream()` (collected by `TransactionalDB`).
      - `eventBus.publishAll()` (`InMemoryEventBus` — synchronous, collected by `TransactionalDB`).
   4. `ExecutionContext.clear()`.
   5. `UnitOfWork.commit()` — all writes atomic.
   6. On failure: `UnitOfWork.rollback()` — discard all writes.
3. Idempotency: check before, mark after.

### 11.2 EventualConsistencyPipeline

**Flow:**

1. `ConcurrencyChecker.withRetry()`.
2. Inside retry:
   1. `ExecutionContext.set()`.
   2. `handler.handle(command)`.
      - `repo.save()` → `eventStore.appendToStream()` (immediate, atomic on its own).
      - `eventBus.publishAll()` (`QueueEventBus` — enqueue).
   3. `ExecutionContext.clear()`.
3. Idempotency: check before, mark after.

No `UnitOfWork` — event append is atomic on its own. Queue enqueue is separate. If enqueue fails, events are in store — cron catches up.

### 11.3 Per-Command Consistency Override

```
Framework.dispatch(command):
  consistency = commandOverrides.get(command.class) ?? defaultConsistency
  pipeline = consistency == 'strong' ? strongPipeline : eventualPipeline
  return pipeline.dispatch(command)
```

**Two repos per aggregate:**

- `strongRepo` → `InMemoryEventBus` (synchronous dispatch)
- `eventualRepo` → `CloudflareQueueEventBus` (enqueue)

**Two event buses:**

- `strongEventBus` — projections + sagas registered (synchronous within transaction)
- `eventualEventBus` — just enqueues to queue

Projections/sagas registered on `strongEventBus` only — the queue consumer worker has its own `InMemoryEventBus`.

### 11.4 UnitOfWork

```
interface UnitOfWork:
  begin() → void
  commit() → void   // execute all collected writes atomically
  rollback() → void  // discard all collected writes
  isActive → boolean
  execute(work) → T  // convenience: begin → work → commit/rollback
```

**TransactionalDatabase (D1):** Proxy that collects writes (`prepare().run()` → collected, `prepare().all()` → immediate read, `batch()` → collected). On `commit()`, all collected statements executed in one `db.batch()`.

### 11.5 ConcurrencyChecker

```
class ConcurrencyChecker:
  assert(expected, actual, streamId)  // throws OptimisticConcurrencyError on mismatch
  withRetry(operation) → T  // retries on OptimisticConcurrencyError (exponential backoff + jitter)
  computeDelay(attempt) → int
```

Default: 3 retries, 10ms → 20ms → 40ms backoff, ±10% jitter.

---

## 12. Projection System

### 12.1 Projection\<TDb>

**Implements:** `EventHandler`.

**Properties:**

- `name: string` (abstract) — unique projection name.
- `db: TDb` — database for read model updates.
- `processedEvents: ProcessedEventStore` — idempotency tracking.

**`handle(event)` flow:**

1. `alreadyProcessed(event)` → check `processedEvents.exists(name, instanceId, event.id)`.
2. If processed → return (skip).
3. Find `on[EventName]` method via bracket notation.
4. If found → call handler (may throw).
5. If not found → skip (unknown event type — mark as processed to prevent retry).
6. `markProcessed(event)` → insert into `processed_events`.

**Sharding (`ShardProjection`):** Override `getInstanceId(event)` to return per-shard instance ID. Each shard processes only events matching `hash(shardKey) % shardCount`.

**Rebuild hooks:** `onRebuildStart()` (drop tables), `onRebuildComplete()` (create indexes).

### 12.2 Convention-Based Dispatch

No decorators. Method name convention:

- `onAccountOpened(event)` — handles `AccountOpened`.
- `onMoneyDeposited(event)` — handles `MoneyDeposited`.

Dispatch via: `this["on" + event.eventName]`. If not found → silently skip + mark as processed.

### 12.3 ProcessedEventStore

```
interface ProcessedEventStore:
  exists(handlerName, handlerInstance, eventId) → boolean
  markProcessed(handlerName, handlerInstance, eventId) → void  // idempotent
  count(handlerName, handlerInstance?) → int
  deleteByHandler(handlerName, handlerInstance?) → int
  deleteOld(olderThan) → int
```

**Composite key:** `(handler_name, handler_instance, event_id)`.

- Projection: `handler_name` = projectionName, `handler_instance` = projectionName (or shard name).
- Saga: `handler_name` = sagaType, `handler_instance` = sagaId.

---

## 13. Process Manager (Saga) System

### 13.1 ProcessManager\<TState>

**Properties:**

- `sagaId: string` — = `correlationId` from starting event.
- `state: TState` — user-defined state (persists between events).
- `status: SagaStatus` — `RUNNING` | `COMPENSATING` | `COMPLETED` | `FAILED`.
- `currentStep: string | null` — last-executed handler name.
- `failureReason: string | null` — set when `FAILED`.
- `compensations: CompensationStack` — LIFO stack of compensable actions.

**`handle(event)` flow:**

1. If terminal (`COMPLETED`/`FAILED`) → throw `SagaAlreadyCompletedError`.
2. Find handlers via convention (priority: `completedWhen` > `compensateWhen` > `continueWhen` > `startsWhen`).
3. If no handler found:
   - Fresh saga (`currentStep == null`) → throw `SagaNotStartedError`.
   - Running saga → return `[]` (ignored).
4. Validate transition via `SagaStateMachine`.
5. If invalid → throw appropriate error.
6. Execute handler → get commands.
7. Apply transition (update status if needed).

**Convention:**

- `startsWhen[EventName](e)` — starts a new saga.
- `continueWhen[EventName](e)` — continues (intermediate step).
- `compensateWhen[EventName](e)` — compensation (sets `COMPENSATING`).
- `completedWhen[EventName](e)` — completion (sets `COMPLETED`, terminal).

### 13.2 SagaStateMachine

**States:** `INITIAL`, `RUNNING`, `COMPENSATING`, `COMPLETED`, `FAILED`.

```
INITIAL + startsWhen → RUNNING
INITIAL + (other) → SagaNotStartedError

RUNNING + continueWhen → RUNNING (stays)
RUNNING + compensateWhen → COMPENSATING
RUNNING + completedWhen → COMPLETED
RUNNING + startsWhen → SagaAlreadyStartedError

COMPENSATING + continueWhen → COMPENSATING (stays)
COMPENSATING + compensateWhen → COMPENSATING (stays)
COMPENSATING + completedWhen → COMPLETED

COMPLETED + (any) → SagaAlreadyCompletedError
FAILED + (any) → SagaAlreadyCompletedError
```

### 13.3 SagaEventHandler

Wraps `ProcessManager` as `EventHandler`:

1. Idempotency: `processedEvents.exists(sagaType, sagaId, eventId)`.
2. Load saga: `sagaStore.load(sagaType, event.correlationId)`.
3. If not found + `startsWhen` match → create new.
4. If not found + no `startsWhen` match → skip.
5. `saga.handle(event)` → get commands.
6. Dispatch commands BEFORE saving state (if dispatch fails, state NOT saved → retry with old state).
7. Save saga state.
8. Terminal: `sagaStore.complete()` or `sagaStore.fail()`.
9. Mark as processed.

### 13.4 Saga Correlation

`sagaId = event.correlationId`. All events in the causation chain share this ID.

**Correlation propagation:**

- Saga dispatches command with `correlationId = sagaId`, `causationId = event.id`.
- Command handler sets `ExecutionContext` with these values.
- Aggregate's `raise()` enriches events with these values.
- Events flow back to saga via `SagaEventHandler` → loads by `event.correlationId`.

### 13.5 CompensationStack

LIFO stack of compensable actions:

```
registerCompensation(factory: () => Command[], description?) → void
getCompensationCommands() → Command[]  // pops ALL, returns in reverse order
popCompensation() → Command[]  // pops ONE (step-by-step)
```

Factory is called at compensation time (not registration time) — saga state is current. NOT persisted — runtime only, rebuilt on replay.

---

## 14. Idempotency

### 14.1 Command Idempotency

**Table:** `processed_commands` (PK: `command_id`).

```
interface CommandIdempotencyStore:
  exists(commandId) → boolean
  markProcessed(commandId, commandType, handlerName) → void  // idempotent
  getProcessedAt(commandId) → Date | null
  count(handlerName?) → int
  list(handlerName, options?) → ProcessedCommand[]
  deleteOld(olderThan) → int
```

**TTL:** 7-day retention (D1: scheduled cleanup. DynamoDB: native TTL).

**Pipeline integration:** Check before dispatch, mark after successful save. Re-check on each retry.

### 14.2 Projection/Saga Idempotency

**Table:** `processed_events` (PK: `(handler_name, handler_instance, event_id)`).

Already covered in sections 12.3 and 13.3.

**TTL:** 30-day retention.

### 14.3 Idempotent Read Model Updates

Read model SQL should be idempotent:

- `INSERT ... ON CONFLICT DO UPDATE` (upsert).
- `INSERT ... ON CONFLICT DO NOTHING` (insert if not exists).
- For incremental updates (`balance += amount`): use event-sourced read model pattern (record each change as a separate row keyed by `event_id`).

---

## 15. Consistency Modes

### 15.1 Strong Consistency

- `InMemoryEventBus` — synchronous dispatch to projections/sagas.
- `UnitOfWork` — all writes (events + projections + sagas) in one DB transaction.
- Fail-fast: if any handler fails, transaction rolls back.
- Higher latency (waits for all handlers).
- **Best for:** create commands, critical mutations, read-after-write guarantees.

### 15.2 Eventual Consistency

- `CloudflareQueueEventBus` / `SqsEventBus` — enqueue events.
- No `UnitOfWork` — event append is atomic on its own.
- Lower latency (returns immediately after enqueue).
- Projections/sagas update asynchronously via queue consumer.
- Cron dispatcher is the safety net (reads from event store, catches up).
- **Best for:** high-frequency updates, integration events, analytics.

### 15.3 Per-Command Override

```
.consistency('eventual')  // framework default
.command(OpenAccount, Handler, { consistency: 'strong' })  // override
```

Builder creates two repos per aggregate + two event buses + two pipelines. `Framework.dispatch()` routes based on per-command override.

### 15.4 Hybrid Event-Sourced + State-Only

Per aggregate:

- `repository('event_sourced')` — events stored in event store, state replayed.
- `repository('state')` — state stored in dedicated table, events still published for projections/sagas.

Both modes raise domain events via `raise()`. Both publish via `EventBus`. The difference is persistence: event store vs dedicated table.

---

## 16. Concurrency Control

### 16.1 Optimistic Concurrency

Every `appendToStream` receives `expectedVersion`. The adapter checks:

- `stream_version == expectedVersion` → proceed.
- Mismatch → throw `OptimisticConcurrencyError`.

### 16.2 Retry

`ConcurrencyChecker.withRetry(operation)`:

- Retries on `OptimisticConcurrencyError` (not on domain/config errors).
- Exponential backoff: `initialMs * multiplier^attempt`, capped at `maxMs`.
- Jitter: ±10% random offset.
- Default: 3 retries, 10ms → 20ms → 40ms.

On retry: reload aggregate (gets latest version) → re-execute command → re-save.

---

## 17. Compensation Tracking

### 17.1 CompensationStack

LIFO stack on `ProcessManager`:

```ts
// In each continueWhen handler:
this.registerCompensation(
  () => [new RefundMoney(this.state.from, this.state.amount)],
  'refund withdrawal',
);

// In compensateWhen handler:
compensateWhenTransferFailed(e) {
  return this.getCompensationCommands();
  // Returns: [ReverseDeposit, RefundMoney] — step 2 first, step 1 second
}
```

Not persisted. Rebuilt on replay (each handler re-registers).

---

## 18. Subscription & Catch-Up

### 18.1 Subscription Cursor

**Table:** `subscriptions` (PK: `subscription_name`).

```
Subscription: { name, streamId?, lastSeen?, createdAt }
```

- `streamId = null` → `$all` (global stream).
- `streamId = "BankAccount-acc-001"` → per-aggregate stream.
- `streamId = "BankAccount"` → category stream.
- `lastSeen` = last processed event ID (UUIDv7, lexicographically sortable).

### 18.2 SubscriptionManager

```
class SubscriptionManager:
  register(name, options?) → void          // in-memory tracking
  ensureAllSubscriptions() → string[]      // create missing in DB
  get(name) → Subscription | null
  list() → Subscription[]
  advance(name, lastSeenEventId) → void   // idempotent (only forward)
  delete(name) → void
  isCaughtUp(name) → boolean               // loadAll(lastSeen, 1) → 0 events
  areAllCaughtUp() → boolean
  getLagReport() → LagReport               // per-subscription status
  reset(name) → void                       // delete + recreate (for rebuild)
  resetAll() → void
```

### 18.3 CronDispatcher

Pull-based catch-up:

1. `listSubscriptions()` — get all subscription cursors.
2. For each: `loadAll(lastSeen, batchSize)` or `loadStreamAfterEvent(streamId, lastSeen, batchSize)`.
3. For each event: `handler.handle(event)` → `ackSubscription(name, event.id)`.
4. Retryable errors → stop (retry on next run). Non-retryable → skip + advance cursor.
5. `hasMore` flag for pagination.

### 18.4 BackpressureController

Wraps `CronDispatcher` with:

- Time budget (`maxDurationMs` — default 25s for CF Workers).
- Event budget (`maxEventsPerRun` — default 10K).
- Per-subscription pass limit (`maxPassesPerSubscription`).
- Throttle delay between batches.
- Progress reporting (events processed, event rate, remaining per subscription).

---

## 19. Timeout Scheduling

### 19.1 SagaTimeoutScheduler

Runs periodically (Cron Trigger / EventBridge):

1. `sagaStore.loadTimedOut(threshold, { maxCount })` — find sagas with status IN (`RUNNING`, `COMPENSATING`) and `updated_at < threshold`.
2. For each timed-out saga:
   1. Check if saga has `compensateWhenSagaTimeout` handler (via routing table).
   2. If yes → create `SagaTimeout` event → dispatch via `SagaEventHandler.handle()`.
   3. If no → `sagaStore.fail(sagaType, sagaId, 'timeout')`.
3. If timeout handler throws → mark saga as `FAILED`.

### 19.2 SagaTimeout Event

Synthetic event — NOT stored in event store. Created by scheduler, dispatched directly to `SagaEventHandler`.

```
SagaTimeout: extends DomainEvent
  - sagaType: string
  - sagaId: string
  - lastStep: string | null
  - correlationId = sagaId
```

**Handler convention:** `compensateWhenSagaTimeout(e: SagaTimeout)`.

### 19.3 Configurable Timeout

Per saga type — separate `SagaTimeoutScheduler` instances with different `timeoutMs`.

---

## 20. Rebuild System

### 20.1 ProjectionRebuilder

```
rebuild(projection, options?) → RebuildResult
rebuildAll(projections[], options?) → RebuildAllResult
rebuildShards(shardManager, options?) → RebuildAllResult
```

**Flow:**

1. `projection.onRebuildStart()` — user drops/truncates read model tables.
2. `subscriptionManager.reset(name)` — delete + recreate subscription (`lastSeen = null`).
3. `processedEvents.deleteByHandler(name)` — clear idempotency records.
4. Replay: `cronDispatcher.catchUp(name)` repeatedly until `hasMore = false`.
5. `projection.onRebuildComplete()` — user creates indexes, updates stats.

**Options:** `continueOnError`, `resetIdempotency`, `callHooks`, `batchSize`, `maxPasses`.

### 20.2 SagaRebuilder

```
rebuildInstance(sagaCtor, sagaId, options?) → SagaRebuildResult
rebuildType(sagaCtor, options?) → SagaRebuildAllResult
rebuildAll(sagaCtors[], options?) → SagaRebuildAllResult[]
```

**Flow:**

1. `sagaStore.delete(sagaType, sagaId)` — remove corrupted state.
2. `processedEvents.deleteByHandler(sagaType, sagaId)` — clear idempotency.
3. `eventStore.loadByCorrelationId(sagaId)` — all events in chain.
4. Create `SagaEventHandler` with `NullCommandDispatcher` (commands discarded).
5. Replay each event through the handler.
6. Saga state reconstructed. Commands NOT dispatched (already executed).

---

## 21. Dead Letter Queue

### 21.1 DeadLetterStore

```
interface DeadLetterStore:
  write(entry: DeadLetterInput) → void
  list(handlerName, options?) → DeadLetterEntry[]
  get(eventId, handlerName) → DeadLetterEntry | null
  getById(id) → DeadLetterEntry | null
  delete(id) → void
  count(handlerName?) → int
  deleteOld(olderThan) → int
```

**TTL:** 30-day retention (D1: scheduled cleanup. DynamoDB: native TTL).

### 21.2 DeadLetterWriter

Combines `RetryPolicy` + `DeadLetterStore`:

```
executeWithRetry(handlerName, event, operation) → T
```

**Flow:**

1. `retryPolicy.withRetry(operation)` — retry on retryable errors.
2. If all retries exhausted → `deadLetterStore.write({ handlerName, eventId, ... })`.
3. Re-throw (caller acks the message — it's in the DLQ).

### 21.3 RetryPolicy

```
class RetryPolicy:
  shouldRetry(error, attempt) → boolean  // uses shouldRetry() from error hierarchy
  computeDelay(attempt) → int             // exponential backoff + jitter
  withRetry(operation, onAttempt?) → T
```

Default: 3 retries, 100ms → 200ms → 400ms, ±10% jitter.

---

## 22. Error Hierarchy

### 22.1 Categories

| Category | Retryable | HTTP Status | Description |
|---|---|---|---|
| `domain` | No | 400 | Business rule violation — retrying won't help |
| `concurrency` | Yes | 409 | Transient conflict — retry might succeed |
| `infrastructure` | Yes | 500 | Transient failure — retry might succeed |
| `configuration` | No | 500 | Framework misconfiguration — retrying won't help |

### 22.2 Error Codes

**Domain (19 codes):**
`INVALID_EVENT_ID`, `INVALID_CORRELATION_ID`, `INVALID_CAUSATION_ID`, `AGGREGATE_ALREADY_EXISTS`, `AGGREGATE_NOT_FOUND`, `INVALID_AGGREGATE_STATE`, `COMMAND_VALIDATION`, `INVALID_COMMAND_ID`, `INVALID_COMMAND_CORRELATION_ID`, `INVALID_COMMAND_CAUSATION_ID`, `SAGA_ALREADY_COMPLETED`, `SAGA_ALREADY_STARTED`, `SAGA_NOT_STARTED`, `COMPENSATION_FAILED`, `SAGA_TIMEOUT`, `INVALID_SAGA_TRANSITION`, `VALUE_OBJECT_INVALID`, `INCOMPARABLE_VALUE`, `VALUE_OBJECT_CONSTRUCTION_FAILED`.

**Concurrency (3 codes):**
`OPTIMISTIC_CONCURRENCY`, `COMMAND_ALREADY_PROCESSED`, `CONCURRENT_MODIFICATION`.

**Infrastructure (18 codes):**
`EVENT_STORE_ERROR`, `EVENT_APPEND_FAILED`, `EVENT_LOAD_FAILED`, `STREAM_NOT_FOUND`, `SNAPSHOT_STORE_ERROR`, `SNAPSHOT_LOAD_FAILED`, `SNAPSHOT_SAVE_FAILED`, `EVENT_BUS_ERROR`, `EVENT_PUBLISH_FAILED`, `EVENT_HANDLER_FAILED`, `SAGA_STORE_ERROR`, `PROJECTION_ERROR`, `PROJECTION_HANDLER_FAILED`, `PROJECTION_IDEMPOTENCY_VIOLATION`, `QUEUE_ERROR`, `SERIALIZATION_ERROR`, `OPERATION_TIMEOUT`, `DEAD_LETTER`.

**Configuration (11 codes):**
`MISSING_EVENT_HANDLER`, `UNKNOWN_EVENT_TYPE`, `DUPLICATE_EVENT_REGISTRATION`, `EVENT_VERSION_MISMATCH`, `COMMAND_HANDLER_NOT_REGISTERED`, `INVALID_CONFIGURATION`, `MISSING_DEPENDENCY`, `AGGREGATE_NOT_REGISTERED`, `PROJECTION_NOT_REGISTERED`, `SAGA_NOT_REGISTERED`, `CIRCULAR_DEPENDENCY`.

### 22.3 FrameworkError Base

```
abstract class FrameworkError extends Error:
  abstract code: string
  abstract category: ErrorCategory
  get retryable: boolean  // computed from category
  get httpStatus: int     // computed from category
  context: Map<string, any>
  cause: Error?
  toJSON() → object
  toHttpResponse() → { status, body }
  toLogEntry() → object
```

---

## 23. Dependency Injection

### 23.1 Token\<T>

Type-safe dependency key. Generic parameter `T` makes `resolve(token)` return `T`.

### 23.2 Container

```
class Container:
  bind<T>(token, factory, lifetime) → this        // lifetime: singleton | transient | request
  bindSingleton<T>(token, factory) → this
  bindTransient<T>(token, factory) → this
  bindScoped<T>(token, factory) → this
  bindInstance<T>(token, instance) → this           // pre-instantiated singleton
  resolve<T>(token) → T                             // throws MissingDependencyError
  resolveOrNull<T>(token) → T | null                // returns null if not registered
  has<T>(token) → boolean
  createScope() → Container                         // child container for request-scoped deps
```

**Lifetimes:**

- `singleton`: one instance per root container. Cached in root.
- `transient`: new instance every `resolve()`. No caching.
- `request`: one instance per scope (child container). Different scopes get different instances.

**Circular dependency detection:** `resolutionStack` tracks tokens being resolved. Throws `CircularDependencyError` on cycle.

---

## 24. Provider Abstraction

### 24.1 Provider Interface

```
interface Provider:
  readonly type: 'cloudflare' | 'aws' | 'local'

  createEventStoreAdapter() → EventStoreAdapter
  createSnapshotStoreAdapter() → SnapshotStoreAdapter
  createSagaStoreAdapter() → SagaStoreAdapter
  createProcessedEventStore() → ProcessedEventStore
  createCommandIdempotencyStore() → CommandIdempotencyStore
  createDeadLetterStore() → DeadLetterStore
  createEventBus(config: { consistency, transport }) → EventBus
  createUnitOfWork() → UnitOfWork | null
  initialize?() → void  // migrations, table creation
```

### 24.2 Implementations

| Provider | Event Store | Snapshot | Saga | Event Bus (eventual) | UnitOfWork | Init |
|---|---|---|---|---|---|---|
| LocalProvider | InMemory | InMemory | InMemory | InMemoryEventBus | InMemoryUnitOfWork | No-op |
| CloudflareProvider (D1) | D1EventStoreAdapter | D1SnapshotStoreAdapter | D1SagaStoreAdapter | CloudflareQueueEventBus | D1UnitOfWork | D1 migrations |
| AwsProvider (DynamoDB) | DynamoDbEventStoreAdapter | DynamoDbSnapshotStoreAdapter | DynamoDbSagaStoreAdapter | SqsEventBus | null | Table creation |

### 24.3 D1EventStoreAdapter (SQLite)

- `appendToStream`: `db.batch([INSERT events, INSERT stream_events, UPDATE stream_version WHERE version = expected])`.
- `loadStream`: `SELECT ... JOIN stream_events JOIN streams WHERE stream_uuid = ? AND stream_version > ?`.
- `loadAll`: `SELECT ... WHERE event_id > ? ORDER BY event_id` (UUIDv7 sortable).
- Subscriptions: CRUD on `subscriptions` table.

### 24.4 DynamoDbEventStoreAdapter

- `appendToStream`: `TransactWriteItems` (max 25 items — 1 stream update + 2N event items).
- `loadStream`: `Query(stream_events, PK=stream_uuid, SK>fromVersion)` + `BatchGetItem(events)`.
- `loadAll`: `Query(events, GSI=all-events-index, PK="ALL", SK>lastSeen)`.
- Conditional writes: `ConditionExpression: 'stream_version = :expected'`.
- TTL: native DynamoDB TTL on `processed_events`, `processed_commands`, `dead_letters`.

---

## 25. Builder DSL

### 25.1 TaroDomain.builder()

```ts
TaroDomain.builder()
  .useCloudflare({ env, ctx, provider? })        // or .useAws() / .useLocal() / .useProvider()
  .consistency('eventual', { transport: 'cloudflare_queue' })  // default for all commands
  .snapshotEvery(100)                              // global default
  .aggregate(BankAccount, a => a
    .repository('event_sourced')                   // or 'state'
    .snapshotEvery(100)
    .factory(id => new BankAccount(id))
    .events(AccountOpened, MoneyDeposited))
  .command(OpenAccount, OpenAccountHandler, { consistency: 'strong' })  // override
  .command(DepositMoney, DepositMoneyHandler)     // default → eventual
  .command(TransferMoney, (c, consistency) =>     // factory pattern
    new TransferMoneyHandler(c.resolve(...), ...))
  .projection(c => new BalanceProjection(...))     // factory receives container
  .saga(TransferSaga)
  .events({ AccountOpened: { version: '1.0.0' } })
  .validation({ projectionMode: 'strict' })        // runtime validation
  .skipValidation()                                 // or disableValidation()
  .build() → Promise<Framework>
```

### 25.2 Framework (built result)

```
class Framework:
  dispatch(command) → Promise<CommandDispatchResult>  // routes to correct pipeline
  fetchHandler() → (Request) → Promise<Response>     // HTTP → command dispatch
  queueHandler() → (MessageBatch) → Promise<void>      // queue consumer
  scheduledHandler() → (ScheduledEvent) → Promise<void>  // cron + timeout
  getConsistencyFor(commandClass) → ConsistencyMode
  di: Container
  eventStore: EventStore
  eventBus: EventBus
  strongEventBus, eventualEventBus: EventBus
  commands: CommandRegistry
  cron: CronDispatcher | null
```

---

## 26. Worker Entry Points

### 26.1 CloudflareWorker

```
class CloudflareWorker:
  constructor(framework: Framework)
  get export() → { fetch, queue, scheduled }
```

- `fetch`: HTTP POST → command dispatch. Also: `GET /health`, `POST /migrate`.
- `queue`: CF Queue consumer → event dispatch.
- `scheduled`: Cron trigger → cron catch-up + saga timeout.

### 26.2 AwsLambda

```
class AwsLambda:
  constructor(framework: Framework)
  apiHandler: (APIGatewayEvent) → APIGatewayResult
  queueHandler: (SQSEvent) → void
  scheduledHandler: (EventBridgeEvent) → void
```

### 26.3 Project Templates

- `generateCloudflareWorkerTs()` → `worker.ts` content.
- `generateWranglerToml()` → `wrangler.toml` content (D1, Queue, Cron bindings).
- `generateAwsLambdaTs()` → Lambda handler content.
- `generateAwsSamTemplate()` → SAM `template.yaml`.
- `generatePackageJson()`, `generateTsConfig()`.

---

## 27. Database Schema

### 27.1 Tables (10 total)

| # | Table | Purpose | Primary Key |
|---|---|---|---|
| 1 | `events` | Append-only event log | `event_id` (UUIDv7) |
| 2 | `streams` | Stream metadata | `stream_uuid` (UNIQUE) |
| 3 | `stream_events` | Event-to-stream mapping | `(stream_id, stream_version)` |
| 4 | `snapshots` | Aggregate state cache | `(source_uuid, source_version)` |
| 5 | `subscriptions` | Persistent cursor | `subscription_name` (UNIQUE) |
| 6 | `processed_events` | Projection/saga idempotency | `(handler_name, handler_instance, event_id)` |
| 7 | `sagas` | Process manager state | `(saga_type, saga_id)` |
| 8 | `dead_letters` | Failed message quarantine | `id` (auto-increment) |
| 9 | `processed_commands` | Command deduplication | `command_id` |
| 10 | `schema_migrations` | Migration tracking | `(major, minor, patch)` |

### 27.2 D1 (SQLite) Specifics

- Timestamps: `TEXT` (ISO 8601 with ms, `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`).
- Version numbers: `INTEGER`.
- JSON: `TEXT` (no native JSON type).
- `WITHOUT ROWID` on composite-PK tables (saves space, improves index performance).
- `ON CONFLICT DO NOTHING/UPDATE` for idempotent inserts.
- `db.batch(stmts)` for atomic multi-statement execution.

### 27.3 DynamoDB Specifics

- `events`: PK=`event_id`, GSIs for `all-events-index`, `correlation-id-index`, `causation-id-index`, `event-type-index`.
- `streams`: PK=`stream_uuid`, `stream_version` attribute.
- `stream_events`: PK=`stream_uuid`, SK=`stream_version` (number), GSI=`event-id-index`.
- `sagas`: PK=`saga_key` (type#id), GSIs for `status-updated-index`, `correlation-id-index`.
- TTL: `expires_at` attribute (epoch seconds) on `processed_events`, `processed_commands`, `dead_letters`.
- `TransactWriteItems` for atomic appends (max 25 items).

---

## 28. Causation & Correlation

### 28.1 Envelope Fields

Every `DomainEvent` and `Command` carries:

| Field | Purpose |
|---|---|
| `correlationId` | End-to-end trace ID. Shared by all events/commands in one business transaction. |
| `causationId` | What caused this. Points to the ID of the parent command or event. `null` for root. |

### 28.2 Propagation Chain

```
HTTP Request (x-request-id: corr-001)
  → Command: id=cmd-001, correlationId=corr-001, causationId=null
    → Event: id=evt-001, correlationId=corr-001, causationId=cmd-001
      → Saga: sagaId=corr-001
        → Command: id=cmd-002, correlationId=corr-001, causationId=evt-001
          → Event: id=evt-002, correlationId=corr-001, causationId=cmd-002
            → Saga: continueWhen
              → Command: id=cmd-003, correlationId=corr-001, causationId=evt-002
                → Event: id=evt-003, correlationId=corr-001, causationId=cmd-003
                  → Saga: completedWhen → COMPLETED
```

All events share `correlationId = corr-001`. `causationId` links each event to its cause.

### 28.3 CorrelationTrace

Utility for debugging — loads all events by `correlationId` and builds a causation tree:

```
CorrelationTrace.trace(eventStore, correlationId) → CorrelationTraceEntry[]
CorrelationTrace.buildTree(entries) → CausationTreeNode
CorrelationTrace.formatTree(tree) → string  // human-readable
```

---

## 29. Validation

### 29.1 Startup Validation (G6)

`FrameworkValidator` runs during `build()`:

- No duplicate aggregate/command/saga/projection registrations.
- State-mode aggregates have `stateRepositoryFactory`.
- Sagas have at least one `startsWhen` handler.
- Projections have at least one `on[EventName]` handler.
- All referenced event types are registered in `EventRegistry`.

**Errors** → throw `InvalidConfigurationError` with all issues listed.
**Warnings** → logged (non-blocking).

### 29.2 Runtime Validation (G8)

`DispatchGuard` validates at dispatch time:

- `validateCommand(command)` — handler exists.
- `validateEvent(event)` — event type registered.
- `validateProjectionDispatch(projectionName, event)` — projection has handler (configurable: skip/warn/strict).

### 29.3 Type-Level Helpers (G7)

Pure TypeScript types (no runtime):

- `defineEvents(...names)` → typed tuple.
- `ProjectionOf<TEvents>` → optional `on[EventName]?` methods.
- `SagaOf<TEvents>` → optional `startsWhen`/`continueWhen`/`compensateWhen`/`completedWhen` methods.
- `RequiredProjectionOf<TEvents>` → required methods (enforces all).

---

## 30. Glossary

| Term | Definition |
|---|---|
| **Aggregate** | Cluster of domain objects treated as a single unit. Root is the `AggregateRoot`. |
| **AggregateRoot** | Entry point to an aggregate. Only object that can raise domain events. |
| **Append-only** | Events are never modified or deleted. Only new events are appended. |
| **Causation ID** | UUID linking an event to the command/event that caused it. |
| **Command** | Intent to change state. Can be rejected. |
| **Compensation** | Undo logic for a saga step. Executed in reverse order (LIFO). |
| **Consistency (strong)** | All changes atomic within a transaction. Read-after-write guaranteed. |
| **Consistency (eventual)** | Changes propagate asynchronously. Projections may lag briefly. |
| **Correlation ID** | End-to-end trace ID shared by all events in a business transaction. |
| **CQRS** | Command Query Responsibility Segregation. Writes (commands) and reads (queries) separated. |
| **Dead Letter Queue** | Store for events that failed processing after exhausting retries. |
| **Domain Event** | Immutable record of something that happened in the domain. |
| **Event Sourcing** | Persisting state as a sequence of events. State reconstructed by replaying. |
| **Event Store** | Append-only log of all domain events. |
| **Event Bus** | Mechanism for dispatching events to handlers (sync or async). |
| **GSI** | Global Secondary Index (DynamoDB). Used for non-key access patterns. |
| **Idempotency** | Processing the same message twice produces the same result. |
| **Optimistic Concurrency** | Check version before update. If mismatch, throw and retry. |
| **Process Manager** | Long-running coordinator (saga). Reacts to events, dispatches commands. |
| **Projection** | Read model builder. Consumes events, updates query-optimized tables. |
| **Repository** | Abstraction for loading/saving aggregates. |
| **Saga** | Long-running business process coordinated via events and commands. |
| **Snapshot** | Cached aggregate state at a specific version. Speeds up replay. |
| **Stream** | Sequence of events for a specific aggregate. Identified by `stream_uuid`. |
| **Subscription** | Persistent cursor tracking which events a handler has processed. |
| **UnitOfWork** | Transaction boundary. Collects writes, commits atomically. |
| **Upcaster** | Function that transforms old event version to newer version on read. |
| **UUIDv7** | Time-ordered UUID. Lexicographically sortable = chronological. |
| **Value Object** | Object defined by its value, not identity. Immutable. |
