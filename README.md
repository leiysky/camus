# Camus · /kæmˈuː/

[![crates.io][crates-badge]][crates] [![docs.rs][docs-badge]][docs] [![CI][ci-badge]][ci] [![license][license-badge]][license]

**An embedded persistent buffer with at-least-once storage handoff.**

Camus durably stages opaque records between application code and an external
effect. It is purpose-built for local spools, embedded outboxes, upload
staging, and durable write-behind workloads. Once an append succeeds, the
record remains recoverable until its exact release is durable.

## At a glance

- **Small lifecycle:** `append -> read -> release -> reclaim`.
- **Logical multi-stream:** any number of lightweight stream namespaces share
  efficient root-wide physical storage.
- **Async readiness:** `Stream::read` waits for work and returns a non-empty,
  owned, bounded snapshot; no callback or polling loop is required.
- **Durable progress:** group commit amortizes syncs without weakening the
  recovery outcome of an individual append or release.
- **Explicit pressure:** capacity is unbounded or globally bounded with
  `Block` or `RejectNew`; pending data is never silently evicted.
- **Application-neutral bytes:** metadata and payload remain opaque, and
  physical locations never enter the public API.

Camus deliberately stops at the durable-buffer boundary. It is not a
general-purpose KV database or message broker and provides no arbitrary
queries, mutable records, networking, consumer ownership, retry scheduling,
or exactly-once effects. The embedding application owns delivery policy and
downstream idempotency.

> **Compatibility commitment:** starting with `1.0.0-rc.1`, the public Rust API
> follows Semantic Versioning and format-v1 roots remain readable by later
> compatible releases. Incompatible persistent changes require a new explicit
> format version and migration design. Earlier unpublished development roots
> are outside this compatibility boundary.

## Install

Camus is available as a stable 1.0 release:

```toml
[dependencies]
camus = "1.0.0"
```

The 1.0 production durability target is Linux on a validated local filesystem.
macOS is supported as a development environment; see the
[deployment envelope](#deployment-envelope) before production use.

## Quick start

The intended API composes directly in async application code:

```rust,no_run
use bytes::Bytes;
use camus::{Capacity, Config, FullPolicy, Log, ReadLimits, Record, StreamId};

async fn drain(root: &std::path::Path) -> camus::Result<()> {
    let log = Log::open(Config::new(
        root,
        Capacity::Bounded {
            total_bytes: 1024 * 1024 * 1024,
            when_full: FullPolicy::Block,
        },
    ))
    .await?;

    let uploads = log.stream(StreamId::new(7));
    uploads
        .append(Record {
            metadata: Bytes::from_static(b"content-type: example"),
            payload: Bytes::from_static(b"opaque payload"),
        })
        .await?;

    // read waits when this stream has no pending records. It never returns an
    // empty snapshot merely to signal readiness.
    let snapshot = uploads
        .read(ReadLimits {
            max_records: 128,
            max_bytes: 8 * 1024 * 1024,
        })
        .await?;

    let mut completed = Vec::new();
    for record in snapshot {
        make_downstream_effect_durable(&record).await?;
        completed.push(record.id);
    }
    uploads.release(completed).await?;

    log.shutdown().await
}
```

Runnable, compiled versions of this lifecycle are in [examples](examples/).

## Core contract

- A storage root contains any number of caller-selected logical streams.
- `StreamId` is a root-scoped `u64`; every value is valid and no stream is a
  reserved default.
- A stream is a logical namespace, not a directory, shard, rollover unit, I/O
  lane, consumer, or subscription.
- `append` and `append_batch` transfer owned opaque metadata and payload bytes
  into one recoverable durability epoch.
- Camus allocates a stable opaque 32-byte `RecordId` for each record. Physical
  segment locations never enter the public API.
- `Stream::read` waits asynchronously for pending work and returns a non-empty,
  owned, bounded snapshot. Observation does not claim or remove records.
- `Stream::release` durably removes an exact record subset from the shared
  pending set. It is idempotent for records already released or reclaimed.
- Reclamation is automatic. `Log::reclaim` lets an application explicitly
  request and await the same maintenance when useful.
- Capacity is configured once for the complete root as either unbounded or
  bounded with `Block` or `RejectNew`; Camus has no per-stream quota.

The safe application flow is:

```text
append record
  -> read pending record
  -> make the downstream effect durable
  -> release record
  -> Camus reclaims its physical segment when eligible
```

If a process stops after the downstream effect but before release becomes
durable, recovery returns the record again. Applications must make repeated
effects safe when that matters.

## API overview

Potentially blocking storage work is async. Methods that only construct a
handle or copy reactor-maintained state are synchronous and never perform
hidden filesystem I/O.

```rust
pub enum Capacity {
    Unbounded,
    Bounded {
        total_bytes: u64,
        when_full: FullPolicy,
    },
}

pub enum FullPolicy {
    Block,
    RejectNew,
}

impl Config {
    pub fn new(root: impl Into<PathBuf>, capacity: Capacity) -> Self;

    // Configurable execution bounds include segment bytes, optional segment
    // age, epoch bytes, release record count, commit-group count and bytes,
    // command-queue capacity, optional detailed timing, and an optional
    // Arc<dyn Runtime>.
}

impl Log {
    pub async fn open(config: Config) -> Result<Self>;
    pub fn stream(&self, id: StreamId) -> Stream;
    pub fn known_streams(&self) -> Vec<StreamId>;
    pub fn stats(&self) -> RootStats;
    pub fn health(&self) -> RootHealth;
    pub fn watch_health(&self) -> HealthWatch;
    pub async fn reclaim(&self) -> Result<ReclaimReport>;
    pub async fn shutdown(&self) -> Result<()>;
}

impl HealthWatch {
    pub fn current(&self) -> RootHealth;
    pub async fn changed(&mut self) -> Option<RootHealth>;
}

impl Stream {
    pub fn id(&self) -> StreamId;
    pub fn stats(&self) -> StreamStats;
    pub async fn append(&self, record: Record) -> Result<RecordId>;
    pub async fn append_batch(
        &self,
        records: Vec<Record>,
    ) -> Result<Vec<RecordId>>;
    pub async fn read(&self, limits: ReadLimits) -> Result<PendingSnapshot>;
    pub async fn release(&self, ids: Vec<RecordId>) -> Result<()>;
}
```

`Log`, `Stream`, and their clones are thread-safe clients of one root reactor.
A `Stream` keeps the root alive, so applications may retain only stream
handles after setup. Explicit shutdown closes the shared lifecycle; surviving
handles then return a closed error.

`Record` and `PendingRecord` own metadata and payload as immutable `Bytes`.
Moving a record across the reactor boundary does not cause an undocumented
deep copy. A pending record contains its `RecordId`, metadata, and payload, and
the snapshot can outlive the read Future.

`RecordId` implements copy, equality, and hashing and has a fixed 32-byte
serialization. It is useful for storage identity and release, not ordering,
physical addressing, or application deduplication. Applications put their own
idempotency keys in opaque metadata.

Configuration requires an explicit `Capacity`. Other operational limits have
documented, configurable defaults and may be tuned from benchmarks without
changing format-v1 semantics.

An absent `max_segment_age` disables age rollover. `known_streams` and stats
are concurrent in-memory snapshots; they do not imply a disk refresh. Root
stats separate storage, pressure, logical-operation, durability-group,
maintenance, and recovery state. Detailed pressure stats separate command
queue admission from reactor dispatch time and split finite filesystem jobs by
append, read, release, reclaim, and timer-driven rollover. Health is a separate
low-frequency lifecycle view.

## Read and release semantics

`ReadLimits` contains a hard record-count limit and a hard payload-byte limit.
A read selects the longest fitting prefix of that stream's pending logical
sequence, skipping released gaps but never skipping the earliest pending
record to find smaller later work. Metadata bytes are returned but do not
count toward `max_bytes`; complete record size remains bounded by the root's
epoch limit.

If the earliest record alone exceeds `max_bytes`, read returns a typed error
containing its ID, required payload bytes, and configured bytes. It never
silently violates the limit. Before returning, Camus verifies both metadata
and payload checksums for every selected record. One mismatch fails the whole
read with no partial snapshot and poisons the open root.

When no record is pending, read waits outside reactor admission. After being
woken it admits one bounded storage read. If another handle releases all of
that work first, the operation waits again rather than returning an empty
snapshot.

Multiple handles may read the same pending records concurrently. They do not
represent independent consumers: one durable release removes a record from
future snapshots for every handle on that stream. Applications that need
claims, leases, per-subscriber copies, or ownership coordination build them
above Camus.

One release call is an atomic ensure-not-pending operation for its currently
pending subset. Duplicate IDs and IDs already released or reclaimed are
successful no-ops. Scope errors and unknown future IDs are rejected before
admission. Every root has a configurable hard `max_release_records`; Camus
never silently splits a larger release into weaker atomic units.

## Ordering and durability

Within one stream, records retain input order inside an append batch and the
reactor's serialization order across batches. Released gaps are skipped
without reordering the remainder. Camus exposes no cross-stream order and does
not equate read order with consumer execution or completion order.

Each append request is one independent recovery epoch. Each release request's
newly pending subset is one independent release unit. The reactor may group
consecutive units of the same kind behind one storage sync, but group commit
does not merge their atomic recovery outcomes.

Append success means the epoch's complete record bytes and commit marker were
covered by a successful data durability barrier. Release success means its
manifest frame was covered by a successful manifest durability barrier.
Commands that arrive during a sync join a later group; Camus adds no fixed
linger delay at low load.

## Capacity, rollover, and reclamation

For a bounded root, `Block` waits asynchronously for admissible capacity while
leaving read, release, and maintenance able to run. `RejectNew` returns a typed
not-admitted error. A request that can never fit fails immediately under either
policy. Neither policy evicts pending data or reports false durable success.

Capacity charges exact encoded lengths of Camus format files, including
framing, control metadata, released bytes not yet reclaimed, and transactional
temporary files. It does not model filesystem allocation blocks or reserve the
device's free space. Camus keeps non-configurable dynamic headroom for a
checkpoint rewrite, the largest next manifest frame, and an active segment's
seal footer so release and reclamation can still make progress.

Physical data uses one root-wide segment sequence shared by every logical
stream. Segment size is a hard limit; a complete epoch never crosses a
segment. Optional maximum segment age, disabled by default, is a reactor-driven
soft rollover deadline, not a real-time or retention guarantee. The timer can
seal an idle non-empty segment without an application callback. Camus creates
no empty replacement; the next append creates a segment lazily.

A segment is physically removed only after every record it contains is
released. Interleaving streams therefore means one slow stream may pin
released bytes from another stream. This is an accepted initial tradeoff;
future dynamic sharding or relocation may improve it without changing the
logical API.

Reclamation runs automatically at low priority and is promoted when blocked
appends need capacity. `Log::reclaim` exists for explicit observability and
tests, not for correctness.

## Runtime and performance contract

Camus starts one async reactor task per open root. The reactor owns admission,
the bounded command queue, group selection, timers, and in-memory state. Each
finite filesystem job runs through the runtime's blocking facility, with at
most one storage job executing per root. Different roots may make progress in
parallel.

When no runtime is supplied, Camus lazily creates one process-wide private
Tokio runtime and retains it until process exit. Applications that require
scheduler isolation may configure an `Arc<dyn Runtime>`. Tokio types do not
appear in storage-domain values or Futures.

Admitted foreground operations execute FIFO. Only consecutive same-kind
commit units at the queue head may be grouped; a read or change of kind is a
barrier. Automatic maintenance is lower priority unless capacity progress
requires it.

Cancellation before operation admission is side-effect-free. After admission,
the reactor owns the operation through completion; dropping its Future only
abandons the result. A cancelled append or release may therefore be recovered
as durable even though its caller observed no result.

## Failure model

Expected pre-mutation errors do not poison a root. These include invalid
configuration or input, lock contention, closure, capacity rejection,
requests over configured limits, insufficient read limits, record-ID scope or
existence errors, and exhausted identifiers.

An I/O error after admission, authoritative corruption, lazy body-checksum
failure, or runtime failure poisons the open root. The triggering operation
returns its precise error. A mutating operation reports that its durable
outcome is unknown; failure is not proof that the mutation is absent. Queued
and future storage operations return `Poisoned`, while in-memory stats and
shutdown remain available.

Recovery is close and reopen. The reopened root validates durable bytes and
decides which complete operations survived. Camus has no in-place reset or
continue-after-poison mode.

## Observability boundary

`Log::stats` and `Stream::stats` are synchronous, in-memory snapshots. Root
counters are scoped to one successful open and saturate instead of wrapping.
Base gauges, counters, real wait durations, and recovery duration are always
available. `Config::with_detailed_observability` additionally measures
end-to-end logical calls and storage jobs; it is disabled by default to keep
the fast path lean.

`Log::health` reports `Running`, `ShuttingDown`, `Poisoned`, or `Closed` and
retains the first failed-closed cause. `HealthWatch::changed` provides prompt,
coalescing async notification without owning the root or backpressuring its
reactor. It is not a record-delivery event stream; `Stream::read` remains the
readiness Future.

Returned errors expose a stable low-cardinality `ErrorKind`. Camus does not
choose metric names, automatically label by stream ID, or depend on a logging,
tracing, metrics, or exporter framework. See the
[observability guide](docs/observability.md) for counter semantics and adapter
guidance.

## Use cases and non-goals

Camus fits local spools, embedded outboxes, upload staging, and durable
write-behind buffers where the application wants a small persistent handoff
boundary.

Camus intentionally does not provide:

- application schemas, serialization, filtering, indexes, or mutable records;
- record-delivery callbacks or subscriptions, consumers, claims, leases, or
  retry scheduling;
- per-subscriber delivery or per-stream capacity fairness;
- exactly-once delivery, application deduplication, or distributed
  transactions;
- network protocols, HTTP handlers, service routing, clustering, or
  replication; or
- database redo/undo or an externally visible physical log sequence number.

## Storage layout and compatibility

Logical streams do not have physical directories. Format v1 uses one root
identity, one checkpoint/log pair, and one shared segment directory:

```text
<root>/
  ROOT
  camus.lock
  MANIFEST.chk
  MANIFEST.log
  segments/
    segment-00000000000000000000.log
    ...
```

Every authoritative artifact carries the immutable random 128-bit root ID.
XXH3-64 checksums use the fixed format-v1 seed `CAMUSV1!`; they detect
accidental corruption and are not authentication.

The exact layouts and compatibility boundary are defined in
[docs/file-format.md](docs/file-format.md). Filesystem ordering, recovery,
release, and reclamation invariants are defined in
[docs/architecture.md](docs/architecture.md). Unsupported or ambiguous
authoritative data fails closed.

Release `1.0.0-rc.1` establishes format v1 as the published compatibility
boundary. Earlier unpublished development roots are not migration inputs.
Starting with this candidate, changing a magic, field, checksum range, codec,
or semantic interpretation requires an explicit new format version and
migration design.

## Deployment envelope

Camus 1.0 targets Linux local filesystems that honor file data sync, directory
sync, exclusive advisory locking, and atomic same-directory rename. macOS is a
development environment, not part of the production durability support matrix
or required CI and release qualification. A network, userspace, or layered
filesystem is outside the durability envelope unless its behavior has been
independently validated.

One process owner opens a root at a time. Back up a closed root or use an
atomic filesystem snapshot; do not copy a live root file by file. Checksums do
not protect against an attacker who can rewrite bytes and recompute them.

## Documentation

- [Architecture](docs/architecture.md): execution, durability, recovery,
  release, and reclamation invariants.
- [File format](docs/file-format.md): normative format-v1 byte layouts and
  codecs.
- [Usage guide](docs/usage.md): API composition, capacity, cancellation, and
  shutdown.
- [Operations guide](docs/operations.md): supported deployment and failure
  response.
- [Observability guide](docs/observability.md): snapshots, counter semantics,
  health transitions, and adapter guidance.
- [Benchmark guide](docs/benchmarks.md): reproducible durable-buffer workloads,
  comparison-engine mappings, and regression comparison.
- [Long-running smoke guide](docs/long-running-smoke.md): cyclic capacity
  pressure, latency telemetry, VictoriaMetrics reporting, and pass criteria.
- [Release guide](docs/releasing.md): RC qualification, compatibility gates,
  required repository settings, packaging, publication, and rollback.
- [Runnable examples](examples/README.md): replay, waiting reads, multi-stream
  use, maintenance, and observability.

## Development

Before publishing, run:

```sh
cargo fmt --all --check
cargo fmt --all --check --manifest-path fuzz/Cargo.toml
cargo fmt --all --check --manifest-path benchmarks/Cargo.toml
cargo fmt --all --check --manifest-path smoke/Cargo.toml
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --manifest-path smoke/Cargo.toml --all-targets -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --release --lib --tests
cargo test --locked --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo check --locked --manifest-path fuzz/Cargo.toml
cargo test --locked --manifest-path benchmarks/Cargo.toml --no-default-features --features redb-engine
cargo test --locked --manifest-path smoke/Cargo.toml
cargo audit --deny warnings
cargo audit --deny warnings --file fuzz/Cargo.lock
cargo audit --deny warnings --file benchmarks/Cargo.lock
cargo audit --deny warnings --file smoke/Cargo.lock
cargo deny --locked check -A license-not-encountered licenses sources
cargo deny --locked --manifest-path fuzz/Cargo.toml check licenses sources
cargo deny --locked --manifest-path benchmarks/Cargo.toml --no-default-features --features redb-engine check -A license-not-encountered licenses sources
cargo deny --locked --manifest-path smoke/Cargo.toml check -A license-not-encountered licenses sources
cargo package --locked
cargo publish --dry-run --locked
```

Stable releases and release candidates additionally use the manual-only
[`Release qualification`](docs/releasing.md#manual-release-qualification)
workflow to validate candidate metadata, the applicable public-API baseline,
and tests executed from Cargo's extracted package source. The workflow never
publishes or tags a release.

## License

Camus `1.0.0` and current development are licensed under the
[MIT License](LICENSE-MIT). Published versions through `1.0.0-rc.2` remain
under the [Apache License 2.0](https://github.com/leiysky/camus/blob/v1.0.0-rc.2/LICENSE-APACHE)
included with those artifacts.

[ci]: https://github.com/leiysky/camus/actions/workflows/ci.yml
[ci-badge]: https://github.com/leiysky/camus/actions/workflows/ci.yml/badge.svg?branch=main
[crates]: https://crates.io/crates/camus
[crates-badge]: https://img.shields.io/crates/v/camus.svg
[docs]: https://docs.rs/camus
[docs-badge]: https://docs.rs/camus/badge.svg
[license]: https://github.com/leiysky/camus/blob/main/LICENSE-MIT
[license-badge]: https://img.shields.io/badge/license-MIT-blue.svg
