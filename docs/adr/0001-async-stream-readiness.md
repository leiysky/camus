# ADR 0001: Async logical-stream readiness

- Status: Accepted
- Date: 2026-07-14
- Scope: Process-local notification that a logical stream has pending records

## Context

Camus durably stores opaque records and exposes recovery, lazy reads, release,
and reclamation. The application needs a timely way to learn that one logical
stream can be consumed without polling, embedding consumer scheduling in
Camus, or weakening append/release durability boundaries.

A synchronous lifecycle callback was considered and rejected. It did not
cover pending work discovered during open, ran application code on the storage
call stack, and described storage transitions rather than the condition the
consumer actually needs: whether a stream currently has pending records.

## Domain model

- **Storage root** is the aggregate owned by one open `Log`.
- **Logical stream** is an independently identified record namespace inside
  the root.
- **Pending record** is a complete durable record without a durable release.
- **Stream readiness** is a process-local projection: a stream is ready when
  at least one pending record exists in the current authoritative snapshot.
- **Readiness subscriber** is an application task waiting to learn that a
  stream is ready. It has no durable identity or record ownership.
- **Application consumer** is code outside Camus that selects pending records,
  performs external effects, and decides when release is safe.
- **Release** is the durable transition that can make a stream not ready.

Readiness is derived state. It is not written to the manifest and is not a
consumer cursor, lease, record assignment, or acknowledgment.

## Decision

Keep Camus a durable stream buffer. Add only runtime-neutral,
level-triggered readiness observation backed by standard `Waker`
registration:

```text
append[_batch]_to(stream, records)             -> durable locations
Readiness::wait_for(stream)                     -> readiness Future
recovery().pending_records_for_iter(stream)     -> pending metadata/locations
read[_many](locations)                          -> opaque payloads
release_from(stream, record_ids)                -> durable release
```

1. Seed readiness from fully validated recovery during `Log::open`.
2. After a successful append epoch sync and in-memory publication, mark that
   stream ready and wake its waiters.
3. After a successful release manifest sync, recompute readiness for that
   stream.
4. On explicit recovery, replace the complete readiness projection.
5. On `Log` poison or drop, close the projection and wake waiters with
   `ReadinessClosed`.

The cloneable readiness handle creates Futures without borrowing `Log`, so an
application executor can await while the synchronous storage owner keeps
servicing commands. Every waiter for a ready stream is awakened. Camus does
not start a notification thread or async runtime and does not create consumer
identities, cursors, claims, leases, or batch reservations.

An application that wants a bounded batch uses
`pending_records_for_iter(stream).take(limit)` and then `read_many`. This is a
snapshot selection, not an assignment: independent application tasks may
select the same pending record. They coordinate above Camus or tolerate
duplicate effects.

## Invariants

1. After append returns success, every record in that durability epoch remains
   recoverable and pending unless a release marker becomes durable. A release
   error can have an uncertain outcome, so recovery after reopen is authority.
2. A record is never excluded from pending recovery before its release marker
   is durable.
3. `ready(stream) == exists(complete && !released record in stream)` for the
   latest fully published in-memory snapshot.
4. No successful readiness wake is published before the corresponding append
   or release durability boundary.
5. An uncertain storage error closes readiness; recovery after reopen is the
   authority for whether work exists.
6. Waiting observes level state and never transfers ownership, advances a
   cursor, or reserves a record.
7. All waiters for one ready stream may wake and may subsequently observe the
   same pending records.
8. Cancelling a wait does not release or acknowledge any record.
9. Camus guarantees durable recoverability/redelivery until release; the
   application owns execution, retry, idempotency, and downstream delivery.

## Alternatives considered

### Synchronous lifecycle callback

Rejected. It runs user code inline, has no startup replay, and exposes the
wrong abstraction level for consumption.

### Background callback reactor

Rejected. It introduces thread lifecycle, callback panic, queueing,
backpressure, and shutdown behavior without strengthening the storage
contract.

### Polling `pending_records_for`

Retained as a correctness fallback but rejected as the primary notification
mechanism because notification latency and polling load become application
configuration concerns.

### `StreamConsumer` handles and reserved batches

Rejected from the Camus boundary. Exclusive attachments, non-overlapping
batches, retries, fairness, and handoff turn a durable buffer into a consumer
scheduler. Applications may build those policies above pending-record queries
and readiness.

### Competing-consumer or fan-out queue

Rejected from the Camus boundary. Competing consumers require claims or
leases; fan-out requires durable consumer identities and per-consumer release
state. Both materially change the recovery model and on-disk contract.

## Resolved design questions

1. The public guarantee says "durable recoverability/redelivery until release"
   and explicitly does not promise execution by an external consumer.
2. The minimal API waits for a caller-selected stream. `wait_for_any()` is
   deferred until a concrete need demonstrates its selection semantics.
3. `ReadinessClosed` is sufficient for both poison and drop because detached
   waiters take the same action in either case: discard the readiness handle
   and obtain a new one from a successfully reopened `Log`.

## Consequences

- The storage durability and at-least-once recovery contract stays independent
  of async runtimes and application topology.
- Multiple readiness subscribers are safe because notification carries no
  ownership semantics.
- Applications can choose one worker, competing workers, fan-out, or another
  topology, but Camus does not coordinate it.
- Concurrent readers may perform duplicate external effects even without a
  crash. This is permitted by at-least-once staging and must be documented
  clearly rather than hidden behind an implied consumer lease.
- `next_batch` is unnecessary at this layer: iterator `take(limit)` provides a
  bounded ordered snapshot without inventing reservation semantics.

## Interview decision log

### 2026-07-14: Consumer cardinality and delivery guarantee — resolved later

- The initial answer allowed exactly one consumer per logical stream.
- Each record is consumed with at-least-once semantics.
- The one-consumer restriction was reopened later in the interview after
  examining batch and partial-success semantics.

### 2026-07-14: Consumer attachment lifetime — superseded

- Consumer exclusivity is process-local, not persisted in the manifest.
- `acquire_consumer(stream_id)` returns one non-cloneable exclusive handle.
- A second acquisition for the same stream fails while that handle exists.
- Dropping the handle permits handoff to a new consumer.
- Handoff preserves at-least-once behavior because every record without a
  durable release remains pending for the new consumer.
- The later storage-boundary decision removes consumer attachments from Camus.

### 2026-07-14: Consumption unit — superseded

- The consumer requests an ordered batch with an application-supplied limit.
- The API is expected to expose `StreamConsumer::next_batch(limit)` rather than
  only a readiness signal or a single-record `next()` operation.
- Partial-success behavior remains unresolved; in-flight overlap is resolved
  by the decision below.
- The later storage-boundary decision uses the existing pending-record iterator
  plus `take(limit)` and leaves batch execution policy to the application.

### 2026-07-14: In-flight batch cardinality — superseded

- A stream consumer may hold exactly one in-flight batch at a time.
- `next_batch` cannot produce another batch while the current batch remains
  active.
- Resolving or abandoning the batch ends its process-local reservation.
- Abandonment does not release records; every unresolved record becomes
  eligible for at-least-once redelivery.
- Partial-success behavior within the single batch remains unresolved.
- The later storage-boundary decision removes in-flight batch state from Camus.

### 2026-07-14: Multiple consumers reconsidered — resolved

- Multiple consumers may be compatible with record-level at-least-once
  delivery.
- Camus does not model those consumers. Multiple application tasks may observe
  the same pending record, and all readiness subscribers may wake.
- Applications that need disjoint assignment, fan-out, or controlled
  concurrency build that policy above Camus.

### 2026-07-14: Durable-buffer boundary

- Camus is a persistent logical-stream buffer, not a consumer runtime.
- Its at-least-once contribution is durable recoverability of every successful
  append until a durable release; it cannot guarantee that external code runs.
- Readiness is broadcast, process-local observation only. Multiple subscribers
  are allowed and no record ownership is implied.
- Consumer identity, cardinality, assignment, leases, batch reservations,
  retry, partial success, fairness, and fan-out state remain application policy.
- The existing public shape (`wait_for`, pending iterators, `read_many`, and
  `release_from`) already matches this boundary; no `StreamConsumer` or
  `next_batch` API is needed.
- The boundary and precise at-least-once staging wording were accepted.
