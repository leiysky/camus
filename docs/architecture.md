# Camus architecture

This document is authoritative for Camus's execution, durability, ordering,
recovery, release, and reclamation invariants. The exact format-v1 byte layouts,
checksum inputs, and codecs are specified in [file-format.md](file-format.md).
The public project and API boundary is specified in the
[README](../README.md).

## Storage boundary

Camus is an embedded persistent buffer. It understands only:

- one filesystem storage root;
- caller-selected numeric logical stream IDs;
- opaque metadata and payload bytes; and
- Camus-assigned record identities.

It does not interpret application schemas, route records, schedule consumers,
perform external effects, or decide when an effect is safe to release. A
logical stream is a namespace for identity, ordering, pending state, and
release. It is not a physical shard, directory, segment set, I/O lane,
subscription, or consumer.

Physical format v1 uses one shared root-wide data log. Records from different
logical streams may be interleaved in the same physical segment. Rollover and
reclamation operate on those segments without exposing their placement to the
public API.

## Root and logical identity

Creating a root generates a random 128-bit `RootId` and publishes an immutable
`ROOT` superblock. Every data segment, manifest log, and checkpoint repeats
that identity. A mismatched artifact is corruption; Camus never silently joins
files copied from another root.

`StreamId` is a caller-selected `u64` scoped by the root. Every value, including
zero, is ordinary. Constructing `Log::stream(id)` is local and creates no
durable object. The first complete recoverable append epoch makes that stream
durable-known.

Camus allocates a stream-local `u64` sequence for every record. The first
recoverable record in a stream has sequence zero. Each append epoch receives a
contiguous interval in record input order when it reaches the reactor's FIFO
execution head. Sequence allocation never wraps; once sequence `u64::MAX` is
durable, later appends to that stream return `SequenceExhausted` before
admission.

Public `RecordId` is the opaque fixed-width identity
`{ RootId, StreamId, Sequence }`. It survives recovery and physical
reclamation and is never reused in the root. The sequence component defines
logical record order internally, but the token does not expose ordering,
physical addressing, or a consumer cursor.

A stream's inclusive sequence high-water remains durable for the lifetime of
the root. It can be derived from extant complete epochs, a checkpoint, or a
`SegmentRemoved` manifest frame. Before deleting the last physical evidence
for a sequence, Camus publishes the necessary high-water advance. Consequently
the initial API has no stream delete, reset, or ID-reuse operation.
`known_streams()` includes every durable-known stream, even if empty after
reclamation, and excludes handles that never completed an append.

Retained high-water metadata counts toward root capacity and grows with the
number of streams ever made durable-known. Workloads that create an unbounded
number of one-use streams are outside v1's intended boundary; Camus does not
silently garbage-collect identity state.

Application idempotency keys belong in opaque metadata. Camus does not
deduplicate append retries. Retrying after an unknown append outcome may create
another record with another Camus ID.

## Runtime and root ownership

`Log::open` acquires an exclusive root lock and performs recovery through a
runtime backend. If the caller does not configure an `Arc<dyn Runtime>`, Camus
lazily initializes one private process-wide Tokio runtime and retains it until
process exit. A custom backend supplies async task spawning, finite blocking
work, and deadline sleeping without leaking executor-specific types into the
storage API.

Each open root starts one async reactor task. `Log` and `Stream` values are
cheap, cloneable, thread-safe clients of that reactor. The reactor owns:

- the bounded command queue and operation admission;
- logical and physical in-memory state;
- foreground FIFO scheduling and group selection;
- capacity reservations and blocked-append wakeups;
- the age-rollover timer and automatic maintenance; and
- root shutdown.

Filesystem calls execute as finite jobs through the runtime's blocking-work
facility. At most one such storage job executes for a root at a time. The
reactor itself never occupies a blocking worker as a permanent loop. Separate
roots may execute storage jobs concurrently.

`Log::stream`, `Stream::id`, `known_streams`, root health, and root or stream
statistics read only reactor-maintained memory and are synchronous. They never
refresh state from disk. Root statistics separate durable storage state,
reactor pressure, caller-observed operations, commit groups, maintenance, and
recovery; stream statistics expose logical pending state. Every operation that
may wait for storage, durability, capacity, or a timer is async.

## Admission, cancellation, and scheduling

Operation admission is the ownership boundary. Before a command enters the
executable queue, Camus validates its immediate input and reserves every
required queue, memory, and append-capacity resource. A capacity-blocked append
and a read waiting for pending work remain outside the queue.

Cancellation has two distinct meanings:

- before admission, dropping the Future releases provisional resources and
  guarantees that the operation will not execute;
- after admission, the reactor owns the operation through its outcome, and
  dropping the Future only abandons the result.

The latter rule is necessary because a caller cannot safely cancel filesystem
work that may already have crossed a durability barrier. Reopen and recovery
are authoritative after an unobserved result.

Admitted foreground commands execute in FIFO admission order. Queue-slot
admission is starvation-free among operations whose other resource conditions
are satisfied. An append waiting for root capacity is not eligible and cannot
block admission of a read or release.

The reactor may group only consecutive commit units of the same kind at the
queue head:

- one append call is one append durability epoch;
- one release call's newly pending subset is one release unit;
- append epochs group only with append epochs;
- release units group only with release units; and
- a read, a different command kind, or either configured group limit ends the
  group.

Group selection never scans past an intervening command. It adds no deliberate
linger interval: an idle reactor starts the queue head immediately, while
concurrent arrivals during a write or sync become candidates for the next
group. `max_commit_units` bounds completion fan-out and CPU work;
`max_commit_bytes` bounds one blocking write burst. Grouping shares a
durability barrier but never merges the recovery atomicity of its units.

Automatic maintenance is outside the foreground FIFO and normally lower
priority. It is promoted when sealing, checkpointing, or reclamation is needed
for blocked capacity to make progress. One automatic reclamation storage job
selects at most four segments. A completed batch yields back to the reactor
before another low-priority batch can start, so newly queued foreground work is
not held behind an unbounded segment scan. Continued reclamation is
level-triggered from current storage state rather than an edge notification, so
an idle reactor drains every remaining batch without losing maintenance work.

## Append epochs and durability

`Stream::append(record)` creates a one-record epoch. `append_batch(records)`
creates one non-empty multi-record epoch; Camus never splits a call. Input
metadata and payload are owned immutable byte buffers. Submission transfers
their ownership without a hidden deep copy.

Every root has a hard configurable `max_epoch_bytes` that covers the complete
encoded epoch: its header, all record descriptors, metadata, payloads, and
commit. A request that exceeds it returns `EpochTooLarge` before admission.
This bound also limits a single record, so v1 needs no separate record-size
setting.

When an epoch reaches the execution head, the reactor assigns its contiguous
sequence interval and encodes:

```text
epoch header
record descriptor + metadata + payload
...
epoch commit
```

The commit binds the stream, sequence interval, exact byte boundaries, and
ordered record descriptors. Recovery publishes every record in a valid epoch
or none of them.

For an existing active segment, one append commit group follows this order:

```text
write every complete epoch, including every record and epoch commit
  -> sync_data the active segment once for the group
  -> publish the records and sequence high-waters in memory
  -> complete every covered append with success
```

Each epoch has exactly one covering data durability barrier. Several epochs
may share that barrier. No append reports success before the barrier succeeds.

Every epoch and complete append group stays in one segment. Group selection is
therefore also bounded by remaining segment space. If another epoch would not
fit, the current non-empty group is synced and the next epoch starts a later
group. If the queue-head epoch itself cannot fit the current segment, Camus
seals the segment before writing that epoch elsewhere. A durability barrier
never has to sync two data files for one append group.

## Self-published segment creation

Camus has either one non-empty active segment or no active segment. It never
creates a header-only successor during rollover.

When append work arrives with no active segment, the reactor allocates the next
root-wide physical segment ID and writes a temporary file in `segments/`. That
file contains:

```text
segment header
first complete append commit group
optional seal footer when the segment is immediately full
```

The reactor then performs:

```text
sync_data temporary segment
  -> atomic rename to the canonical segment name
  -> sync the segment directory
  -> publish append success
```

The canonical file and its validated header are the creation record. There is
no `SegmentCreated` manifest frame. A temporary file left before rename is
discardable. If a crash occurs after rename but before directory sync, the
canonical file may or may not survive and no caller had yet received success;
either result is valid. A surviving complete file is recovered.

## Size and age rollover

Configured `segment_bytes` is a hard final-file bound including the 48-byte
header and possible 48-byte seal footer. Append admission always leaves room
for the footer; usable epoch space must be at least `max_epoch_bytes`. A short
segment is sealed at its actual end and is neither padded nor charged up to the
configured ceiling.

Optional `max_segment_age` is a soft operational deadline measured from the
wall-clock creation baseline stored in the segment header with its first
records. It is disabled when not configured. Reopen applies the current
configured age to the persisted baseline; it does not reset age. Elapsed time
uses saturating subtraction, so a backward
clock adjustment delays rollover and a forward adjustment may make it due
immediately.

The reactor checks age at every finite work boundary and owns a timer that can
seal a non-empty segment while append traffic is idle. It never interrupts an
in-flight blocking job, so the setting is not a real-time SLA or record
retention policy. Sustained append traffic cannot postpone the check
indefinitely. Capacity pressure may also seal a fully released active segment
so it can be reclaimed.

## Two-stage segment seal

Sealing is ordered across data and manifest paths:

```text
stop assigning appends to the segment
  -> write the complete seal footer
  -> sync_data the segment
  -> append SegmentSealed to MANIFEST.log
  -> sync_data MANIFEST.log
```

The footer binds the final file length, epoch count, and ordered structural
digest. If rollover is known while writing the final append group, the footer
may be included in that group's data sync. Sealing an already durable idle
segment needs its own data sync.

A valid footer makes the file physically immutable. Only the synced
`SegmentSealed` frame makes it logically sealed and eligible for reclamation.
Recovery treats a complete footer without its manifest publication as an
interrupted second stage and durably publishes the missing frame before open
returns. The reverse state is corruption: if authoritative manifest state says
sealed but the footer is missing, malformed, or inconsistent, recovery fails
closed.

## Waiting reads

`Stream::read(limits)` is both readiness wait and bounded data read. Camus has
no separate record-readiness callback, subscription, `wait_for`, claim, or
cursor API.

If the stream has no pending record, the Future waits outside command
admission. Once work is visible it admits one read command. A concurrent
release may remove that work before execution; in that case the operation
returns to waiting instead of producing an empty snapshot.

A read selects the longest fitting prefix of the stream's logical pending
sequence subject to hard `max_records` and payload `max_bytes` limits. Released
gaps are skipped. Camus does not skip the earliest pending record merely to fit
smaller later records. If that first record alone exceeds the byte limit, the
operation returns its ID, required bytes, and configured bytes in a typed
non-poisoning error.

The storage job resolves internal physical locations and may coalesce adjacent
reads without changing logical result order. Before returning, it verifies the
metadata and payload checksum of every selected record. Any mismatch fails the
entire operation without a partial snapshot and poisons the root as
authoritative corruption.

The returned snapshot owns its records. It is observation only: it does not
reserve, hide, or mutate them. Any number of stream handles may receive the
same record concurrently or after restart.

Within one stream, snapshot order is input order within an epoch followed by
the reactor serialization order of later epochs. Released records disappear
without reordering the remainder. Camus exposes no cross-stream order and no
consumer execution or completion order.

## Exact release and at-least-once handoff

`Stream::release(ids)` validates that every token belongs to the open root and
selected stream before admission. An in-scope sequence greater than the
stream's durable high-water is unknown and rejects the complete request.
Duplicates and IDs at or below the high-water that are already non-pending are
successful no-ops, including records whose data has been reclaimed.

Camus sorts and deduplicates a valid request, removes existing no-ops, and
encodes the remaining pending sequences as maximally coalesced ranges. That
subset is one atomic release unit. An empty subset succeeds immediately with
no I/O. Every root has a hard configurable `max_release_records`; a larger
request returns `ReleaseTooLarge` and is never split implicitly.

One release commit group follows this order:

```text
append every complete Release frame to MANIFEST.log
  -> sync_data MANIFEST.log once for the group
  -> exclude each released subset from the in-memory pending view
  -> complete every covered release with success
```

Camus never excludes a record from recovery or a pending read before the
covering manifest sync. Different release calls may share that sync without
merging their all-or-none recovery units.

This is Camus's at-least-once boundary: an append that reports success remains
pending and recoverable until some release reports success. The application
makes its downstream effect durable before release. A crash before release is
durable exposes the record again. Camus does not guarantee that consumer code
runs, that a particular handle sees a record, or that separate subscribers
each receive a copy.

## Manifest state and compaction

Mutable control state uses two canonical root files:

- `MANIFEST.chk` is one complete compact checkpoint; and
- `MANIFEST.log` is the append-only suffix after that checkpoint.

The checkpoint contains durable stream high-waters, the next physical segment
ID, extant segment lifecycle and footer state, and exact per-segment release
state. The log has exactly three mutation kinds: `Release`, `SegmentSealed`,
and `SegmentRemoved`.

Every manifest frame carries a strictly increasing root-wide `manifest_seq`.
The checkpoint stores `last_applied_seq`, and the log header stores its base
sequence. Recovery validates an old duplicate prefix at or below the
checkpoint fence but does not apply it twice; every newer frame must be a
contiguous, conflict-free suffix. A gap, reversal, or conflicting duplicate
fails closed.

Normal consecutive release or lifecycle frames may share one manifest-log
sync. Each frame remains a separate atomic replay unit.

Compaction is serialized with all other root work and follows this order:

```text
write a complete MANIFEST.chk.tmp
  -> sync_data the temporary checkpoint
  -> atomic rename over MANIFEST.chk
  -> sync the root directory
  -> write and sync MANIFEST.log.tmp with the new sequence fence
  -> atomic rename over MANIFEST.log
  -> sync the root directory
```

The checkpoint is published before the old log is replaced. A crash between
those publications leaves a new checkpoint with an older duplicate log
prefix, which sequence fencing makes safe to validate and skip.

Checkpoint compaction starts only after every manifest-published physical
deletion has finished and the segment directory is synced. Removed-segment
tombstones can then disappear; retained stream high-waters and the next
segment ID prevent identity reuse.

## Root capacity and admission

Capacity applies only to the complete root:

```rust
Capacity::Unbounded
Capacity::Bounded { total_bytes, when_full: FullPolicy::Block }
Capacity::Bounded { total_bytes, when_full: FullPolicy::RejectNew }
```

Camus has no per-stream capacity, quota, overflow policy, or fairness promise.
Applications requiring tenant isolation use separate roots or impose their own
logical admission before Camus.

Accounted storage is the exact encoded length of canonical and transactional
Camus format files: the root superblock, data segments, checkpoint, manifest
log, and temporary rewrite/create files. Framing, released bytes awaiting
reclamation, and durable metadata all count. Filesystem allocation blocks,
unwritten segment tail, and observed device free space do not.

A bounded root preserves this invariant for every append admission:

```text
projected_file_bytes
  + checkpoint_rewrite_reserve
  + largest_manifest_group_reserve
  + active_segment_footer_reserve
  <= configured_total_capacity
```

- `checkpoint_rewrite_reserve` is the complete next-checkpoint upper bound for
  the current topology using worst-case bitmap release state;
- `largest_manifest_group_reserve` is the largest valid next bounded release
  group, seal frame, or four-frame removal batch derived from configuration
  and current segment contents; and
- `active_segment_footer_reserve` is 48 bytes while an active segment exists.

This maintenance headroom is inside `total_bytes`, changes with topology, and
cannot be reduced by configuration. Under pressure, a manifest commit group
may shrink to one frame. Durable release and removal frames may remain as a
recoverable manifest suffix; they do not require an immediate checkpoint
rewrite. The reactor compacts when the manifest log reaches 8 MiB or a
completed mutation consumes reserved maintenance headroom. Removing the last
physical segment also checkpoints durable stream high-waters so a later
missing control peer cannot be mistaken for partial empty-root initialization.

`Block` keeps an append outside the command queue until capacity is admissible.
Release and reclamation remain able to free space. `RejectNew` returns a typed
not-admitted result. An append that can never fit the root's usable capacity
returns `ExceedsCapacity` immediately under either policy; it never waits
forever. No policy silently drops, evicts, or reports success for new data.

Open rejects a bounded configuration if current accounted files plus mandatory
headroom already exceed `total_bytes`. Camus does not reserve filesystem free
blocks. Device-full and quota errors after admission are ordinary uncertain
I/O failures and poison the root.

`RootStats::storage` exposes configured total, actual accounted bytes,
maintenance headroom, and currently data-admissible bytes as an in-memory
snapshot.

## Physical reclamation

A sealed segment becomes eligible only when every one of its physical records
is durably released. Reclamation follows this order:

```text
derive every stream high-water whose last physical evidence is in the segment
  -> append SegmentRemoved with those high-waters to MANIFEST.log
  -> sync_data MANIFEST.log
  -> make the segment absent from authoritative recovered state
  -> delete the canonical segment file
  -> sync the segment directory
```

One reclamation batch may publish several `SegmentRemoved` frames with one
manifest `sync_data`, delete those published segment files, and issue one
directory sync for the batch. This preserves the ordering above for every
segment while sharing durability barriers. Automatic batches are bounded to
four segments; an explicitly requested pass processes the complete eligible
set as consecutive batches within one API call.

The removal frame is authoritative before physical deletion. Recovery may
therefore finish deleting a leftover file after a crash, while absence of a
file removed by a durable frame is valid. A segment missing without such an
authoritative state transition is corruption.

Reclamation is automatic low-priority reactor work. Capacity pressure promotes
it, and may first seal a fully released active segment. `Log::reclaim` requests
and awaits a maintenance pass but is never required for correctness or
eventual capacity progress.

Because physical segments interleave logical streams, one pending record can
pin already released bytes for other streams in the same segment. Format v1
accepts this space amplification and performs no relocation. Future dynamic
sharding may change placement while preserving logical order and IDs.

## Recovery and repair

Open performs fail-closed structural recovery while holding the exclusive root
lock. At a high level it:

1. validates the immutable `ROOT` superblock and obtains the `RootId`;
2. validates the complete manifest checkpoint and manifest-log header and
   frames, including sequence fencing;
3. enumerates canonical root-wide segments and validates every header against
   the root identity and canonical segment ID;
4. structurally scans epochs and footers without reading opaque bodies;
5. reconciles segment files with checkpoint and manifest lifecycle state;
6. completes permitted interrupted seal or deletion work durably;
7. derives stream sequences, applies exact release state, and builds the
   pending and physical-location indexes; and
8. validates configured capacity and publishes the recovered in-memory view.

Recovery validates record descriptors, epoch headers and commits, segment
footers, manifest frames, checkpoint state, checked lengths, digests, and
lifecycle transitions. Only complete epochs enter the recovered logical view
or advance sequence high-water.

Opaque metadata and payload are skipped using validated lengths during normal
open. Their stored checksums are structurally bound by descriptors and epoch
digests, but the bytes themselves are verified when read. A future explicit
scrub may verify all bodies; ordinary open does not.

Repair is intentionally narrow:

- a recognized noncanonical temporary file left before publication may be
  removed;
- an incomplete final epoch or incomplete footer in the one manifest-active
  data segment may be truncated to the preceding complete boundary and the
  repaired file synced;
- an incomplete final manifest-log frame may be truncated and the log synced;
- a complete valid footer missing only `SegmentSealed` publication is completed
  by appending and syncing that frame; and
- a file left behind after a durable `SegmentRemoved` may be deleted and its
  directory synced.

A complete structure with a bad checksum or invalid semantics is corruption,
not a torn tail. Camus also fails closed on an authoritative header error,
checkpoint error, sealed-segment damage, a manifest-active segment contradicted
by manifest state, corruption before any valid suffix, a sequence gap, root-ID
mismatch, or a missing segment not covered by durable removal. Recovery never
skips damaged bytes to salvage a later unit.

## In-memory observability publication

Observability state is scoped to one open session and is not authoritative
file-format state. It is neither written during normal operation nor recovered
as historical telemetry. Recovery counters describe only the work performed by
the current successful `Log::open`.

After a completed storage transition, the reactor publishes root storage,
commit, maintenance, and recovery summaries together under one in-memory view
lock. Only logical streams touched by that transition have their per-stream
view updated; publishing one stream does not rebuild every known stream.
Changes that affect only telemetry do not wake stream-readiness or
capacity-change waiters.

Queue, wait, public-operation, active-job, and optional detailed-timing fields
use independent atomics because they also change outside the storage
publication point. `Log::stats` therefore gives a coherent durable-storage
portion plus concurrently sampled runtime activity, not one global
linearization point across every field.

Public-operation counters follow the caller Future. Dropping a Future records
cancellation, even though an admitted append or release can subsequently
complete. Commit counters follow successful live durability groups and can
therefore advance without a caller-observed success. This distinction mirrors
the cancellation and unknown-outcome contract rather than attempting to infer
application delivery.

Root lifecycle health is published on a separate low-frequency watch channel.
It retains the first failed-closed operation, error classification, durability
outcome, and human-readable detail. Receivers coalesce intermediate states and
never backpressure or keep the root alive. The health channel is not a durable
event log and does not carry stream readiness; `Stream::read` remains the only
readiness-and-data Future.

## Failure and poisoned roots

Expected errors determined before mutation do not poison the root. Examples
include invalid input or configuration, lock contention, closure, capacity
rejection, requests over configured limits, insufficient read limits, token
scope or existence errors, and identifier exhaustion.

Any I/O error after admission poisons the root. Authoritative corruption, lazy
body-checksum failure, or inability of the runtime backend to continue reactor
work does the same. The triggering command receives the specific cause. Every
mutating command that crossed admission treats its durable outcome as unknown;
an error is not proof that its mutation is absent.

After poison, no new queued storage work starts. Queued commands, waiting
reads, and future storage operations return `Poisoned`. Synchronous in-memory
observations remain available and shutdown remains legal. The only recovery is
to close every handle, reopen the root, and trust the recovered bytes. Camus
does not reset or continue a poisoned reactor in place.

## Shutdown

Explicit `shutdown().await` closes operation admission, completes waiters still
outside admission with a closed error, drains every admitted command and the
current blocking job, stops nonessential maintenance, closes storage resources,
and releases the root lock. Concurrent shutdown callers observe the same
completion. Cancelling a shutdown Future abandons only that wait.

Dropping the final `Log` or `Stream` handle asks the configured runtime to drive
the same shutdown in the background. Because drop cannot await lock release, an
immediate reopen may temporarily return `RootInUse`. Applications needing a
deterministic handoff call explicit shutdown.

## Filesystem assumptions and non-goals

The durability ordering assumes a local Unix filesystem that honors:

- file `sync_data` for written bytes;
- directory sync for rename, create, and delete publication;
- atomic rename within one directory; and
- exclusive advisory locking.

Checksums detect accidental corruption and are not authentication. A storage
stack that weakens these operations is outside the supported durability
envelope unless independently validated.

Camus does not provide mutable records, queries, indexes, database
transactions, redo/undo recovery, consumer scheduling, claims, leases,
per-subscriber delivery, exactly-once external effects, networking,
replication, or cluster management.
