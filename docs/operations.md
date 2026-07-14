# Camus operations guide

This guide describes the supported deployment envelope and operator response
to capacity, integrity, process, and upgrade events. The normative durability
order is in [architecture.md](architecture.md); exact bytes and recovery rules
are in [file-format.md](file-format.md).

## Supported storage environment

Camus currently supports Unix targets and assumes a local filesystem where:

- `sync_data` durably publishes preceding file data and required size metadata;
- directory `sync_all` durably publishes create, rename, and delete operations;
- rename within the root is atomic; and
- every process that can open a root honors advisory exclusive locks.

Do not claim the durability contract on NFS, SMB, object-backed mounts, FUSE,
asynchronous replicas, or caches that ignore flushes without validating that
stack's complete failure behavior. Keep `ROOT`, both manifest files, and
`segments/` on one filesystem. Restrict direct write access; checksums detect
accidents, not malicious modification.

## Ownership and runtime

One reactor owns all filesystem state for an open root. `Log`, `Stream`, and
their clones are thread-safe clients. Stream handles keep the root alive, so a
program may drop `Log` after constructing the handles it needs.

The default runtime is a lazily initialized private process-wide Tokio runtime.
A custom runtime is an execution contract: failure to spawn the reactor, a
finite blocking job, or a deadline Future is a root progress failure. Benchmark
the custom backend with realistic queue depth and storage latency before
production use.

Prefer explicit `shutdown().await` during controlled service termination. It
drains admitted operations and releases the lock before returning. Final-handle
drop performs background shutdown and is unsuitable as a synchronization
barrier before copying, replacing, or immediately reopening a root.

## Durability and failure response

Successful calls have definitive storage meaning:

- append success means the complete epoch and commit marker were covered by a
  successful data sync;
- release success means the exact release frame was covered by a successful
  manifest sync; and
- reclaim success follows durable removal publication, physical deletion, and
  segment-directory sync.

An I/O error after admission has an unknown durable outcome. An append or
release may be present even when its caller receives an error. Do not retry by
assuming absence. Stop using the poisoned root, close every handle, reopen,
and reconcile from recovered pending records. Application idempotency keys in
opaque metadata should make repeated downstream effects safe.

`Corruption` fails closed. Before manual action:

1. stop the owner and preserve a forensic copy of the complete root;
2. record the exact Camus build, filesystem, kernel, and preceding failure;
3. do not delete a manifest, segment, or tail merely to make open succeed; and
4. restore a known-good complete snapshot or use a separately reviewed repair
   tool that implements the exact format version.

Only the narrowly documented incomplete active tails and interrupted
publication/deletion states are repaired automatically.

## Capacity and pressure

A bounded root preserves space for forward maintenance progress. Monitor:

- `actual_file_bytes` versus configured `total_bytes`;
- `maintenance_headroom_bytes` and `data_admissible_bytes`;
- queue depth, admission waiters, aggregate wait time, and maximum wait;
- pending record and payload bytes root-wide and per logical stream;
- append/release/reclaim errors and poisoned transitions; and
- filesystem quota/free-space alerts independently of Camus capacity.

`Block` is backpressure, not data loss: an append waits outside the reactor
queue while release and reclamation remain eligible. `RejectNew` is useful
when the caller owns retry or spill policy. Neither mode evicts existing
pending data. Camus has no per-stream quota or fairness guarantee; use separate
roots or application admission for tenant isolation.

Device-full and quota failures are ordinary uncertain I/O errors. Leave enough
filesystem headroom beyond Camus's configured total for filesystem metadata,
other processes, and operational tooling.

## Segment lifecycle and space amplification

All streams share one root-wide physical segment sequence. Size rollover is a
hard final-file bound. Optional age rollover is a soft reactor deadline and can
be delayed by a busy executor or slow storage operation.

Reclamation publishes and syncs `SegmentRemoved` before deleting the file, then
syncs `segments/`. Recovery finishes a deletion left after the authoritative
frame. A missing manifest-live segment is corruption.

Because streams may interleave physically, one pending record can pin a whole
segment containing otherwise released records. This is expected format-v1
behavior, not a leak. Model worst-case stream interleaving and record lifetime
when choosing root capacity and segment size.

## Recovery, startup, and memory

Open structurally scans every extant segment descriptor and commit envelope,
but opaque bodies remain lazy until read. Startup time and memory scale with
stream count, physical record count, release state, and segment topology.
Benchmark representative roots rather than extrapolating only from payload
bytes.

Read verifies the descriptor plus metadata and payload checksums for every
selected record. A cold workload therefore moves verification cost from open
to first delivery. Bound `ReadLimits` according to latency and memory targets.

## Backup and restore

The simplest consistent backup is a complete copy after explicit shutdown. A
single-point-in-time filesystem or volume snapshot of the whole root is also
acceptable if it preserves ordered file and directory state. Never restore
only a manifest or selected segments.

Restore into a new empty directory, validate permissions and capacity, open it
with a compatible binary, and verify pending application work before switching
traffic.

## Upgrade policy

The project is pre-release and format v1 is strict. Unsupported magic,
versions, kinds, lengths, ordering, or noncanonical encodings fail closed; an
old reader does not silently ignore new state.

Before an upgrade:

1. explicitly shut down the owner;
2. take a complete closed-root backup;
3. test the new build on a copy of representative roots;
4. run locked debug/release tests, docs, fuzz checks, audit, deny, and package
   verification; and
5. retain the backup until rollback requirements expire.

Any future incompatible format change needs an explicit migration and rollback
procedure. It must not be introduced as an incidental open-time rewrite.
