# Camus usage guide

Camus is an embedded, synchronous persistent buffer for opaque records grouped
into logical streams. It provides durable append, recovery of unreleased
records, lazy payload reads, durable release, storage reclamation, and an async
readiness signal. The application still owns dispatch, retry, backpressure,
downstream effects, and idempotency.

This guide focuses on composing those primitives safely. The byte-level
durability rules are defined in [architecture.md](architecture.md), while
production filesystem and operational requirements are covered by
[operations.md](operations.md).

## API map

| Goal | API |
| --- | --- |
| Open or recover a root | `Log::open(Config)` |
| Append to stream zero | `append`, `append_batch` |
| Append to another stream | `append_to`, `append_batch_to` |
| Wait until a stream has pending work | `readiness`, `wait_for` |
| Enumerate pending metadata and locations | `recovery().pending_records_for_iter` |
| Read and validate payloads | `read`, `read_many` |
| Durably stop replaying completed records | `release`, `release_from` |
| Rotate segments | automatic size/age checks, `rollover_expired`, `rollover` |
| Remove eligible storage | `reclaim`, `reclaim_active_for_storage_pressure` |
| Observe local I/O activity and size | `stats`, `storage_bytes` |

## The lifecycle to preserve

Use Camus in this order:

```text
append successfully
  -> enumerate pending record
  -> read and validate payload
  -> durably apply the external effect
  -> release successfully
  -> reclaim eligible storage
```

Within the supported storage envelope, a successful append remains recoverable
until a release marker becomes durable. A crash after the external effect but
before release causes the record to be returned again. This is the source of
Camus's at-least-once staging behavior.

Never release merely because a record was read or handed to application code.
Release only after the application no longer needs Camus to replay it.

## Open and configure a root

One open `Log` owns the entire root and every stream beneath it. A second open
attempt fails with `Error::RootInUse`. State-changing methods take `&mut self`,
and the application must serialize access to the synchronous handle.

```rust
use camus::{Config, Log, Result, RolloverPolicy, StreamId};
use std::path::Path;
use std::time::Duration;

const UPLOADS: StreamId = StreamId::new(7);

fn open(root: &Path) -> Result<Log> {
    let config = Config::new(root)
        // Default policy for streams without an override.
        .with_segment_bytes(64 * 1024 * 1024)
        .with_max_segment_age(Duration::from_secs(15 * 60))
        // A stream-specific override.
        .with_stream_rollover(
            UPLOADS,
            RolloverPolicy::new(256 * 1024 * 1024)
                .with_max_segment_age(Duration::from_secs(60 * 60)),
        );

    Log::open(config)
}
```

Rollover policy is open-time configuration, not durable manifest state. Supply
the intended default and per-stream overrides again on every reopen.

Opening performs recovery before returning. `log.recovery()` already contains
the validated snapshot; callers do not need to call `recover()` immediately
after `open`.

## Append records

Stream zero is available through `append` and `append_batch`. Use the
stream-aware methods for other stable `StreamId` values:

```rust
use camus::{Log, Record, Result, StreamId};

fn stage(log: &mut Log, stream: StreamId) -> Result<()> {
    log.append_batch_to(
        stream,
        &[
            Record::new("record-100", b"first payload".as_slice())
                .with_metadata(b"opaque metadata".as_slice()),
            Record::new("record-101", b"second payload".as_slice()),
        ],
    )?;
    Ok(())
}
```

One `append_batch_to` call is one durability epoch in exactly one stream.
Recovery never publishes a partial epoch: success confirms that every record
is durable, while recovery after an interrupted or failed call publishes the
whole committed epoch or none of it. One epoch never spans streams, and Camus
does not provide an atomic append across streams.

Streams have independent segment sequences, record-ID namespaces, release
state, and rollover policies. The same record ID may appear in different
streams, but an ID must never be reused within one stream, including after
release and reclamation. Stream IDs and record IDs are application-owned stable
identifiers.

Metadata and payload bytes are opaque. Camus stores and validates them but does
not serialize or interpret application objects.

## Enumerate and drain a bounded batch

Recovery keeps metadata and physical locations in memory while payloads remain
on disk. Select a bounded snapshot with iterator `take`, then use `read_many`
to fetch and validate payloads in the same order as the supplied locations.

The following helper treats each record independently. It releases only those
whose external effect succeeded and leaves every failed record pending:

```rust
use camus::{Log, Result, StreamId};

fn drain_once<E>(
    log: &mut Log,
    stream: StreamId,
    limit: usize,
    mut deliver: impl FnMut(&str, &[u8], &[u8]) -> std::result::Result<(), E>,
) -> Result<usize> {
    if limit == 0 {
        return Ok(0);
    }

    let records = log
        .recovery()
        .pending_records_for_iter(stream)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();

    let locations = records
        .iter()
        .map(|record| record.location.clone())
        .collect::<Vec<_>>();
    let payloads = log.read_many(&locations)?;

    let mut completed = Vec::new();
    for (record, payload) in records.iter().zip(&payloads) {
        if deliver(
            &record.meta.record_id,
            record.meta.metadata.as_ref(),
            payload.as_ref(),
        )
        .is_ok()
        {
            completed.push(record.meta.record_id.as_str());
        }
    }

    let completed_count = completed.len();
    if completed_count != 0 {
        log.release_from(stream, completed)?;
    }
    Ok(completed_count)
}
```

This helper does not reserve records. Another application task using the same
snapshot may select the same records. Camus deliberately leaves worker
coordination above the storage layer.

If application semantics require strict in-stream effect ordering, stop at the
first failed effect and release only the successful prefix. If records are
independent, releasing any successful subset is valid. Camus does not choose
that policy.

## Wait for a stream without polling

`Readiness::wait_for(stream)` returns a runtime-neutral Future. The readiness
handle is cloneable and detached from the `Log` borrow, so an application task
may await it while the synchronous storage owner continues processing storage
commands.

```rust
use camus::{Readiness, Result, StreamId};

async fn wait_for_pending(readiness: Readiness, stream: StreamId) -> Result<StreamId> {
    readiness.wait_for(stream).await?;
    Ok(stream)
}
```

The recommended ownership shape is:

```text
async readiness tasks ── notify/command ──> one synchronous Log owner
                                             ├─ enumerate pending records
producers ───────────── append command ──────┤
                                             ├─ read payloads
                                             ├─ release completed IDs
application scheduler ─ maintenance command ─└─ rollover/reclaim
```

Readiness has condition-variable semantics rather than event-queue semantics:

- it is **level-triggered**: a wait completes immediately while the stream has
  at least one pending record;
- it is initialized from recovery, so work present before the wait is not
  missed;
- an append wakes waiters only after the epoch is durable and published in the
  in-memory recovery snapshot;
- all waiters for the stream may wake; no waiter owns or reserves a record;
- dropping a pending Future cancels only that wait; and
- dropping or poisoning `Log` completes outstanding waits with
  `Error::ReadinessClosed`.

One call to `wait_for` is one Future completion. After a wake, tell the `Log`
owner to enumerate and process pending records. Do not immediately loop back
to `wait_for` while the stream is still pending: the next wait will complete
immediately and can create a busy loop. Rearm the wait after the application
has attempted to drain the stream or otherwise coordinated the next attempt.

`Readiness::is_ready` is useful as an observation, but recovery's pending set
remains the source of truth for which records to read and release.

## Multiple readiness subscribers

Any number of application tasks may wait for the same stream. They all observe
the same level state and may all wake for one append. This is safe because a
wake carries no record ownership.

If several tasks independently enumerate pending records, they may perform the
same external effect concurrently. That still fits at-least-once staging, but
the application must either coordinate those tasks or make the effect safe to
repeat. Camus does not implement competing-consumer assignment or fan-out
consumer state.

## Restart and replay

Reopen the same root with the same intended rollover configuration. Opening
validates the manifest and declared segments, repairs only an incomplete active
tail where permitted, and populates the recovery snapshot.

```rust
use camus::{Log, StreamId};

fn pending_ids(log: &Log, stream: StreamId) -> Vec<String> {
    log.recovery()
        .pending_records_for_iter(stream)
        .map(|record| record.meta.record_id.clone())
        .collect()
}
```

Every complete record without a durable release is returned again, including a
record whose external effect may already have succeeded before a crash. Use a
stable idempotency key at the destination when duplicates matter; the Camus
record ID is commonly suitable when its scope matches the destination's
deduplication scope.

Do not use `Log::recover()` to repair a poisoned handle. A poisoned handle may
have an uncertain manifest/segment relationship. Drop it, reopen the root, and
use the new recovery snapshot as authority.

## Release records safely

`release_from(stream, ids)` validates the complete request, then writes and
syncs one stream-scoped release event when at least one supplied ID is not yet
released. Repeating a release for records whose release state is still retained
returns success without writing another event. Released records disappear from
pending recovery and may make their sealed segment reclaimable.

Release only IDs whose external effects are durably represented elsewhere. A
successful effect followed by a failed or interrupted release can be replayed.
If a storage-class release error poisons the handle, do not assume the release
is absent: it may have crossed its durability boundary before the error was
observed. Reopen and inspect the pending set.

Never reuse a record ID within the same stream. Release is lifecycle state, not
permission to recycle an identifier.

## Rollover and reclamation

`segment_bytes` is a target, not a hard maximum. Camus checks projected size
before each append, but one durability epoch is never split and may exceed the
configured target.

Age rollover is checked before append. Idle streams need an application-owned
timer to call `rollover_expired()`:

```rust
use camus::{Log, Result};

fn maintenance(log: &mut Log) -> Result<()> {
    let _rotated_streams = log.rollover_expired()?;
    let _reclaimed = log.reclaim()?;
    Ok(())
}
```

Camus starts no timer or background maintenance thread. Call `rollover(stream)`
when the application needs an explicit boundary unrelated to configured size
or age.

Ordinary `reclaim()` removes fully released **sealed** segments. It never
rotates an active segment solely to delete it. Under explicit storage pressure,
`reclaim_active_for_storage_pressure()` may rotate fully released active
segments first. Prefer the limit-aware variants when the application enforces
a storage budget or filesystem free-space reserve.

## Handle errors by outcome class

Input and lookup errors such as `InvalidRecord`, `DuplicateRecord`,
`UnknownRecord`, `UnknownStream`, and `InvalidLocation` are validated without
making the handle unusable.

I/O, corruption, codec, and internal-state failures poison the open handle
because the operation's durable outcome may be uncertain. When
`log.is_poisoned()` is true:

1. stop using that handle;
2. drop it to release files, the root lock, and readiness waiters;
3. reopen the root; and
4. reconcile work from the recovered pending set.

An errored append may appear after reopen, and an errored release may already
have removed a record from the pending set. Never infer durable absence from
the error alone.

Corruption fails closed. Follow the forensic and restore guidance in
[operations.md](operations.md#failure-handling) instead of deleting files or
tails to make the root open.

## Common mistakes

- **Treating readiness as assignment.** A wake says the stream is pending; it
  does not choose a worker or reserve records.
- **Busy-looping on level readiness.** A new wait completes immediately until
  the stream has no pending records.
- **Releasing before downstream durability.** This changes a recoverable retry
  into possible application-level loss.
- **Reusing a record ID.** IDs are lifetime-unique within a stream, even after
  release or reclamation.
- **Assuming batches span streams.** Append durability epochs and release state
  are stream-local; there is no cross-stream transaction or total order.
- **Expecting automatic age rollover or reclamation.** The application owns the
  maintenance schedule.
- **Retrying through a poisoned handle.** Reopen and trust recovered state.
- **Copying a live root file by file.** Back up a closed root or use an atomic
  filesystem snapshot of the entire root.

## Production checklist

- Validate the filesystem's sync, rename, directory-sync, and advisory-lock
  behavior described in [operations.md](operations.md#supported-environment).
- Keep one serialized `Log` owner per root.
- Use stable stream IDs and never-reused per-stream record IDs.
- Make downstream effects idempotent where duplicates matter.
- Bound drain batches to control memory and downstream pressure.
- Schedule idle age rollover and reclamation explicitly.
- Monitor storage bytes, filesystem headroom, pending work, append/release
  errors, poisoned-handle transitions, and repaired tails.
- Treat recovery after reopen as authoritative after every uncertain error.
