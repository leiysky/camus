# Camus glossary

This glossary defines storage-domain language used by the public API,
architecture, operations guide, and ADRs.

## Application consumer

Code outside Camus that selects pending records, reads payloads, performs
external effects, and decides when release is safe. Camus does not represent
its identity, cardinality, cursor, claims, or retry policy.

## At-least-once staging

A successfully appended record remains recoverable and pending until its
release marker is durable. It may be observed and externally applied more than
once, so the application and destination must make repeated effects safe where
duplication matters. Camus does not guarantee that external consumer code runs.

## Complete record

A record frame that belongs to a fully committed durability epoch and passed
recovery validation.

## Logical stream

A stable `StreamId` namespace with independent record IDs, segment sequence,
release state, reclamation eligibility, and rollover policy.

## Pending record

A complete record that has no durable release marker in its logical stream.
Pending is the recoverable at-least-once work set.

## Readiness projection

Process-local derived state maintained by an open `Log`. A logical stream is
ready exactly when the latest published snapshot contains at least one pending
record in that stream. The projection is not persisted.

## Ready stream

A logical stream for which the readiness projection is true. Ready means that
the application can query pending records; it does not mean that a consumer
owns the stream or that a particular record was assigned.

## Release

The durable, stream-scoped declaration that the application no longer needs a
record. A released record is excluded from pending recovery and may contribute
to segment reclamation eligibility.

## Storage owner

The one process and open `Log` holding the root's exclusive `camus.lock` and
serializing state-changing storage operations.

## Readiness subscriber

An application task holding a readiness Future for one stream. Every
subscriber may be awakened when the stream is ready. Cancellation unregisters
its Waker; waiting never assigns or acknowledges records.

## Waker

The standard Rust async notification primitive registered by a pending waiter.
Camus invokes it after publishing a relevant readiness transition; the
application's executor decides when to poll the Future again.

## Poisoned handle

An open `Log` whose storage outcome became uncertain after an I/O, corruption,
codec, or internal-state error. It rejects further storage access and closes
its readiness projection; the root must be reopened and recovered.
