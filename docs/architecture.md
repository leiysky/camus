# Camus architecture

This document defines Camus's on-disk stream, rollover, ordering, recovery,
release, and reclamation contract.

The exact version-1 byte layouts, checksum inputs, manifest schemas, and codec
compatibility boundary are specified separately in
[file-format.md](file-format.md).

## Boundary

Camus is an embedded durable staging log. It understands only four caller
concepts:

- a stable numeric logical stream ID;
- a stable UTF-8 record ID that callers never reuse within that stream;
- opaque metadata bytes; and
- opaque payload bytes.

Release state is keyed by `(stream ID, record ID)` and may outlive the
corresponding payload bytes, so reusing an ID in the same stream would make old
and new release state ambiguous. Record IDs in different streams are
independent.

It does not serialize application objects, route work, perform external
effects, or decide when a record is safe to release. Those policies belong to
the embedding application. Internally, Camus uses write-ahead-log and manifest
techniques, but it is not a database redo/undo log or a complete message queue.

## Append durability

One `append_batch` or `append_batch_to` call is one stream-local durability
epoch:

```text
record frame 1
...
record frame N
epoch commit marker
sync_data
return success
```

Each record frame has a checksummed descriptor, metadata length, payload
length, redundant total length, and payload checksum. The epoch marker binds
the epoch start, frame count, and frame-descriptor checksum. Rotation occurs
before an epoch, so an epoch never crosses segment files.

An epoch never spans streams or segments, and Camus does not provide one atomic
append across multiple streams. Recovery publishes an epoch only after
validating its commit marker. An incomplete or checksum-damaged final epoch in
the active segment is truncated back to its start and synced. A physically
complete checksummed frame with
invalid semantics fails closed, as does damage before a valid suffix, inside a
sealed segment, or in an authoritative header.

Payload bytes are not loaded during recovery. `read` and `read_many` route each
location to its declared stream, validate that the segment is live, revalidate
its authoritative header, read complete record frames positionally, verify
descriptor and metadata checksums, match the supplied location to the
descriptor, and verify each payload checksum. Adjacent frames in one segment
may share a physical read without changing result order.

## Logical streams and rollover

Logical stream IDs map one-to-one to physical shard IDs in version 1. Stream
zero retains the root `segments/` directory; every nonzero stream uses
`streams/stream-<10-digit-id>/`. Each stream has an independent segment
sequence beginning at zero, active segment, record-ID namespace, release set,
size target, and optional age target. Recovery presents streams in numeric ID
order; this order is not a cross-stream append order. Rollover policies are
supplied at open and are not persisted; segment creation times are persisted.

Before writing an epoch, Camus rotates a non-empty active segment when either:

- the projected complete epoch would exceed that stream's `segment_bytes`; or
- the persisted segment creation time is at least `max_segment_age` old.

One epoch is never split, so `segment_bytes` is a target and a single epoch may
exceed it. Empty segments are not repeatedly rotated. `rollover_expired`
performs the same age test for idle streams and batches the resulting rotation
records behind one manifest sync; the embedding application owns the timer.
`rollover` provides an explicit non-empty-stream rotation.

Every rotation first writes and syncs the new segment header and syncs its
directory. Only then does Camus append and sync the manifest rotation record.
An append-triggered epoch is written only after that manifest sync succeeds.

Creation time is Unix time at millisecond precision and is authoritative only
after its manifest rotation, snapshot, or timestamp event is synced. A legacy
active segment without a timestamp receives a conservative current-time
baseline only after every declared segment has passed recovery validation. A
backward wall-clock adjustment delays age rollover until wall time catches up;
it never makes an unsigned age wrap into an immediate rollover.

## At-least-once lifecycle

Within the supported storage envelope, every record in an append call that
returns success remains recoverable and pending unless a release marker becomes
durable. Camus never excludes a record from pending recovery before syncing
that release marker.

The application owns the transition from a pending record to a durable
external effect:

```text
recover/read record
  -> perform external effect
  -> release record
```

If the process crashes after the external effect but before the release record
is synced, Camus returns the record again during recovery. This is
at-least-once staging: Camus guarantees durable recoverability and redelivery
until release, but cannot guarantee that external consumer code runs or that a
destination accepts the record. Camus does not claim exactly-once delivery,
deduplicate external effects, or coordinate a distributed transaction with a
destination.

## Async stream readiness

Each open handle maintains process-local readiness equal to the set of logical
streams with at least one complete, unreleased record. It is initialized from
the fully validated recovery snapshot. A successful append marks its stream
ready and wakes every registered Waker only after the epoch sync and in-memory
publication complete. A successful release recomputes that stream after the
release manifest sync. Explicit recovery replaces the complete readiness set.

`wait_for(stream_id)` is level-triggered. It returns immediately while that
stream remains ready and otherwise registers the caller's Waker. Waiting does
not claim a stream, lease records, choose a consumer, or advance a cursor;
multiple waiters for one stream all wake. Dropping a pending Future unregisters
its Waker. Dropping or poisoning the owning `Log` closes the shared readiness
state and wakes pending Futures with `ReadinessClosed`.

Readiness has no independent durability and never overrides recovery. If an
operation reports an uncertain error after a sync, the handle is poisoned and
waiters are closed instead of being told that the operation succeeded. The
caller reopens the root, whose recovered pending set initializes a new
readiness handle. The implementation uses standard Future/Waker mechanics and
starts neither a background thread nor an async runtime.

## Failed operation outcomes

Success is definitive: the operation's documented durability sync completed.
An I/O error can be observed before or after a durability boundary, so failure
is not proof that an append, rotation, release, removal, or checkpoint is
absent. The open `Log` becomes poisoned after an I/O, corruption, codec, or
internal-state error and rejects further storage access. Callers drop it,
reopen the root, and use recovered state as the authority.

Input errors that are completely validated before storage mutation do not
poison the handle. These include invalid records or locations, duplicate IDs,
unknown streams, and unknown release IDs.

## Release and reclamation

`release` or `release_from` appends one checksummed, stream-scoped manifest
record and syncs it before returning. It is idempotent for an already released
live record in that stream. Unknown IDs and duplicate IDs within one call are
rejected before writing.

Reclamation follows this order:

```text
all records in a sealed segment are released
  -> append manifest segment-removal record(s) and sync once
  -> delete segment file
  -> sync segment directory
  -> checkpoint live release and segment state when useful
```

An active segment is never removed by ordinary `reclaim`. Under explicit
storage pressure, fully released active segments can first be rotated, making
the old segments eligible for the same ordered removal path. Removal records
for all affected streams are synced before any of their segment files are
deleted.

## Manifest

`MANIFEST` is authoritative for stream/segment lifecycle, segment creation
times, and stream-scoped released record IDs.
Directory enumeration is reconciliation input only. A missing declared
segment fails recovery; empty interrupted segment creation can be cleaned up,
but an unmanifested segment containing data fails closed.

Only a structurally incomplete or checksum-damaged final manifest event is
repairable. A complete checksummed event with an unknown field, kind, or
invalid lifecycle transition fails closed. Segment snapshots are valid only
inside a complete checkpoint; the ordinary manifest suffix accepts lifecycle
events, not snapshot records.

Format version 1 supports all `u32` logical stream IDs. Shard fields in segment
headers and manifest events are interpreted as logical stream IDs. A missing
declared stream segment fails closed. A header-only interrupted stream or
segment creation may be removed, but unmanifested stream data fails closed.

Checkpoint compaction writes a complete checksummed temporary manifest, syncs
it, atomically renames it over `MANIFEST`, and syncs the root directory. A stale
temporary checkpoint is removed on open.

## Process model

`Log` is synchronous and requires mutable access for state-changing operations.
An exclusive `camus.lock` prevents two owners from opening the same root.
The handle is not `Sync`; an embedding application serializes access if it
moves the handle behind a mutex.
Callers that need age-timer scheduling, async group commit, retry scheduling,
admission control, leases, or consumer coordination build those policies above
Camus without weakening its durability ordering.

Camus currently supports Unix targets. Its ordering contract assumes a local
filesystem that honors data sync, directory sync, atomic same-directory rename,
and advisory locking. Checksums detect accidental corruption and are not a
cryptographic authenticity mechanism. The supported operational envelope is
defined in [operations.md](operations.md).

## Non-goals

Camus does not provide tables, queries, indexes, mutable records, database
transactions, redo/undo recovery, queue-consumer coordination, exactly-once
delivery, application serialization, networking, or cluster management.
