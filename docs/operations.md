# Camus operations guide

This guide defines the supported production envelope and the operator response
to capacity, process, integrity, and upgrade events. The byte-level durability
contract remains authoritative in [architecture.md](architecture.md).

## Supported environment

Camus supports Unix targets and assumes a local filesystem with the following
semantics:

- `sync_data` makes previously written file data and required size metadata
  durable;
- directory `sync_all` makes create, rename, and delete operations durable;
- rename within the storage root is atomic;
- advisory exclusive locks coordinate every process that can open the root;
  and
- a successfully synced file is not silently reverted by a lower storage
  layer.

Do not claim the Camus durability contract on NFS, SMB, FUSE, object-backed
mounts, asynchronous replicas, or disk caches that ignore flushes without
validating the complete failure matrix on that exact stack. Keep the manifest
and all segments on the same filesystem.

The embedding process must have durable read/write/create/delete access to the
root and its parent directory. Restrict those permissions because checksums are
not protection against a malicious writer.

Camus requests mode 0700 for directories and 0600 for files it creates; the
process umask may make those modes stricter. Existing root permissions are
preserved so operators can deliberately use a more specific owner/group model.

## Ownership and shutdown

Exactly one open `Log` owns a root, including all logical streams beneath it. A
second owner receives `RootInUse`. All processes must honor the advisory lock;
tools that edit files directly do not. `Log` is not `Sync`; serialize access
when moving it behind an application mutex rather than issuing concurrent reads
and writes against one handle.

Every successful state-changing API call performs its required sync before it
returns. Dropping `Log` therefore does not perform a final commit. Graceful
shutdown should stop new appends, finish in-flight calls, drop the handle, and
only then copy, move, or inspect the root offline.

## Failure handling

Successful calls have definitive outcomes:

- successful `append` or `append_batch` records are durable;
- successful `release` markers are durable; and
- successful reclamation has durably published segment removal before the
  files are deleted.

An input error such as `InvalidRecord`, `DuplicateRecord`, `UnknownRecord`,
`UnknownStream`, or `InvalidLocation` leaves the handle usable. I/O,
corruption, codec, or internal-state errors poison the handle because the
operation may have crossed a durability boundary before the error was
observed.

When `log.is_poisoned()` is true:

1. stop issuing storage operations through that handle;
2. drop the handle so it releases files and the root lock;
3. reopen the same root; and
4. reconcile application work from `recovery().pending_records_iter()` or
   `pending_records_for_iter(stream_id)`.

Do not infer absence from an errored append or release. For example, an append
can be synced and then encounter an injected process failure before the caller
observes success. On reopen, the complete epoch is correctly recovered.

`Corruption` on open is fail-closed. Preserve a forensic copy of the entire
root before attempting manual recovery. Do not delete a manifest, segment, or
tail merely to make the root open: that can turn detectable damage into silent
loss. Restore a known-good closed snapshot or use a separately reviewed repair
tool that understands the exact format version.

## Record identity and delivery

Record IDs must be unique for the lifetime of one logical stream, including
after release and reclamation. The same ID may be used in another stream.
Camus rejects same-stream reuse while either live bytes or a retained release
marker still exists, but manifest compaction can eventually discard both. The
application remains responsible for generating non-repeating IDs within each
stream.

Release only after the external effect is durably represented elsewhere. A
crash between that effect and `release` causes replay, so destination writes or
application-level deduplication must make repeated delivery safe.

Treat this as an at-least-once storage contract, not an application execution
guarantee. Within the supported storage envelope, a successful append remains
recoverable until a release marker is durable. Camus does not ensure that a
consumer task runs or that its destination accepts the record.

### Consumer readiness

Clone `log.readiness()` into application async tasks and call
`wait_for(stream_id).await` for streams they observe. No polling interval or
background thread is involved: a successful durable append wakes the Wakers
registered for that stream. Pending records recovered during open make the
first wait complete immediately.

Treat wakeup as level state, not work assignment. Multiple waiters all wake,
and repeated waits complete immediately until `release_from` leaves the stream
with no pending records. The application still owns dispatch, retry, leases,
backpressure, and idempotency. After readiness, ask the synchronous storage
owner to enumerate `pending_records_for_iter`, read bounded batches, and
release only completed record IDs.

A readiness Future owns its shared handle and does not retain a `Log` borrow,
which permits integration with any executor. Dropping the Future cancels that
wait. Dropping or poisoning `Log` returns `ReadinessClosed` to all outstanding
waiters; discard that readiness handle, reopen the root, and obtain a new one.

## Capacity and reclamation

The configured `segment_bytes` is the default per-stream rotation target, not a
hard record or epoch limit. `with_stream_rollover` overrides it for a selected
stream. A single durability epoch is never split and may create a segment
larger than the target. Size is evaluated immediately before each append.
Rollover policies are open-time configuration rather than manifest state; pass
the intended defaults and per-stream overrides on every reopen. Persisted
segment creation times let a newly supplied age policy evaluate existing
active segments without resetting their age.

`max_segment_age` is optional and accepts positive whole-millisecond values.
Append checks age before writing, while `rollover_expired()` lets the
application check streams that are idle. Schedule that call at the
application's required precision; Camus starts no timer. Empty active segments
do not rotate, even if their creation time is old. A backward system-clock
adjustment delays the trigger until Unix time catches up. Use
`rollover(stream_id)` when policy requires an explicit boundary independent of
size or age.

Record IDs are limited to 16 KiB of UTF-8 bytes, and a record's encoded
ID/metadata envelope, including its 4-byte ID-length field, is limited to 16
MiB. One `release` call is encoded as one atomic manifest record and has the
same 16 MiB metadata ceiling; split an oversized release set into smaller
calls. Payload size is limited by the platform address space and available
storage rather than by `segment_bytes`.

Ordinary `reclaim` examines every stream and removes only fully released sealed
segments. A fully released active segment remains until a later append or age
check rotates it, the caller invokes `rollover`, or the caller explicitly
invokes `reclaim_active_for_storage_pressure`.

The limit-aware reclamation methods require enough observed storage and
filesystem headroom to create a replacement segment header and temporary
manifest checkpoint. Sealed segment deletion can still proceed when checkpoint
compaction lacks headroom; compaction is retried by a later reclamation call.
Free-space checks are preflight observations, not filesystem reservations;
other users of the same filesystem can consume space immediately afterward.

Monitor at least, broken down by logical stream where applicable:

- `storage_bytes()` versus the root's filesystem quota;
- filesystem free bytes;
- pending record count, oldest application timestamp, and age-rollover
  scheduler lag;
- append/release error counts and `is_poisoned()` transitions;
- `Stats::repaired_tails`, which should correspond to understood process or
  power failures; and
- reclaim reports and the number of segment files.

Apply admission control before the filesystem reaches its reserve. Camus does
not start a background reclaimer or reject writes according to an internal
quota.

## Recovery and scaling

Open scans every manifest-declared stream and segment descriptor and retains
recovered record metadata, locations, live IDs, and stream-scoped release IDs
in memory. Payload bytes remain lazy. Benchmark startup time and memory using
the expected number of streams, segments, records, record-ID lengths, and
metadata sizes before selecting a recovery-time objective.

Use `pending_records_iter()` when a borrowed scan is sufficient. The
`pending_records()` convenience method clones every pending record and its
metadata.

`read_many` first groups locations by stream and then by segment, validates the
segment header and each complete frame, and coalesces adjacent frames. Bound
application batch sizes to control temporary read memory and payload lifetime.

## Backup and restore

The simplest consistent backup is a copy made after dropping `Log`. An atomic
filesystem or volume snapshot of the entire root is also acceptable while the
process is running if the snapshot preserves a single point-in-time view of
the manifest and all stream segment directories.

Never restore only `MANIFEST` or only selected segments. Restore the complete
root into a new empty directory, verify permissions and available space, and
open it with the same Camus format compatibility before switching the
application to it.

## Upgrades

On-disk format version 1 is strict and includes logical-stream events and
persisted segment creation times. Unsupported headers fail closed; there is no
implicit format guessing. Roots produced by pre-release builds that lack an
active-segment timestamp receive one conservative, durably recorded baseline
after their complete segment set validates successfully.
Once that timestamp or any nondefault-stream event is written, a pre-release
binary that does not understand those version-1 record kinds will fail closed;
use the closed-root backup for rollback rather than trying to strip events.

Before upgrading:

1. stop the owner and take a complete closed-root backup;
2. verify the new binary against a copy of representative production roots;
3. run the full locked test and fuzz checks for the release; and
4. confirm rollback compatibility before deleting the backup.

If a future release changes its on-disk format, it must document an explicit
forward and rollback procedure rather than silently rewriting authoritative
state on open.
