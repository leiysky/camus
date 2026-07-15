# Camus · /kæmˈuː/

[![crates.io][crates-badge]][crates] [![docs.rs][docs-badge]][docs] [![CI][ci-badge]][ci] [![license][license-badge]][license]

**An embedded persistent buffer with at-least-once storage handoff.**

Camus durably stages opaque records between application code and an external
effect. It is designed for local spools, embedded outboxes, upload staging,
and durable write-behind workloads. Once an append succeeds, the record stays
recoverable until its exact release is durable.

Camus is a storage primitive, not a message broker or general-purpose
database. The embedding application owns delivery policy, worker
coordination, retries, and downstream idempotency.

## Why Camus

- **Small lifecycle:** `append -> read -> release -> reclaim`.
- **Waitable reads:** `Stream::read` waits asynchronously for work and returns
  a non-empty, owned, bounded snapshot; no callback or polling loop is needed.
- **Logical multi-stream:** lightweight stream handles share efficient
  root-wide physical storage without exposing physical locations.
- **Definitive success:** a successful append or release has crossed its
  required durability barrier; group commit amortizes that barrier under load.
- **Explicit pressure:** capacity is unbounded or globally bounded with
  `Block` or `RejectNew`; Camus never silently evicts pending data.
- **Application-neutral bytes:** metadata and payload remain opaque immutable
  bytes with no schema, routing, or serialization policy.

The guarantee is an **at-least-once storage handoff**. If the process stops
after a downstream effect becomes durable but before release becomes durable,
the record is returned again after recovery. Applications must make repeated
effects safe when duplicates matter.

## Install

```toml
[dependencies]
camus = "1.0.0"
```

Camus 1.0's production durability target is Linux on a validated local
filesystem. macOS is supported as a development environment; see
[deployment and compatibility](#deployment-and-compatibility) before
production use.

## Quick start

Potentially blocking storage operations are async. Handle construction and
reactor-maintained observations are synchronous and never hide filesystem
I/O.

```rust,no_run
use camus::{Capacity, Config, FullPolicy, Log, ReadLimits, Record, StreamId};

async fn make_downstream_effect_durable(
    _record: &camus::PendingRecord,
) -> camus::Result<()> {
    // Persist the application-specific effect here.
    Ok(())
}

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
        .append(
            Record::new("opaque payload")
                .with_metadata("idempotency-key=request-42"),
        )
        .await?;

    // Waits while this stream has no pending records.
    let snapshot = uploads
        .read(ReadLimits::new(128, 8 * 1024 * 1024))
        .await?;

    let mut completed = Vec::with_capacity(snapshot.len());
    for record in &snapshot {
        make_downstream_effect_durable(record).await?;
        completed.push(record.id);
    }

    // Release only after the corresponding effects are durable.
    uploads.release(completed).await?;
    log.shutdown().await
}
```

Runnable versions of the lifecycle and its common variations are in
[examples](examples/).

## Programming model

| Concept | Meaning |
| --- | --- |
| `Log` | One exclusively owned storage root and its async reactor |
| `Stream` | A cheap, cloneable handle to a caller-selected logical namespace |
| `Record` | Opaque metadata and payload owned as immutable `Bytes` |
| `RecordId` | A stable opaque 32-byte storage identity used for exact release |
| `PendingSnapshot` | A non-empty owned observation of currently pending records |

A root may contain any number of streams. Every `u64` is a valid `StreamId`;
there is no reserved default stream. Streams are independent logical
sequences, but they are not directories, physical shards, consumers,
subscriptions, claims, or I/O lanes.

`Log`, `Stream`, and their clones are thread-safe clients of the same root
reactor. A stream handle keeps the root alive, so applications may retain only
the handles they need after setup. `RecordId` is for Camus identity and
release, not physical addressing, ordering, or application deduplication.

### Append and ordering

`Stream::append` and `Stream::append_batch` transfer owned records into one
recoverable epoch per call. A successful append means the complete epoch and
its commit marker were covered by a successful data sync.

Records retain input order within a batch and reactor serialization order
across batches on the same stream. Camus exposes no cross-stream ordering.
Consecutive append or release units may share one durability barrier, but
group commit never merges their individual recovery outcomes.

### Wait, read, and release

`Stream::read` is both the readiness and read API. It waits outside reactor
admission while a stream is empty, then returns a non-empty snapshot bounded
by record count and total payload bytes. It selects the longest fitting prefix
of pending stream order and verifies metadata and payload checksums before
returning it.

Reading observes shared pending state; it does not claim or remove records.
Multiple handles may see the same records, and they are not independent
consumers. One durable release removes a record from future reads for every
handle on that stream. Applications that need claims, leases, per-subscriber
delivery, or exclusive workers coordinate them above Camus.

`Stream::release` durably removes an exact set of record IDs. Duplicate IDs and
already released or reclaimed IDs are successful no-ops. A release succeeds
only after its manifest record crosses a durability barrier. Always make the
external effect durable before releasing its record.

### Capacity and reclamation

Capacity is configured once for the whole root:

- `Capacity::Unbounded` imposes no Camus byte budget.
- `FullPolicy::Block` waits asynchronously for capacity while reads, releases,
  and maintenance continue to make progress.
- `FullPolicy::RejectNew` returns a typed admission error so the application
  can retry or spill elsewhere.

Neither bounded policy evicts existing records. Camus capacity accounts for
its exact encoded files and maintenance headroom; it is not a substitute for
filesystem free-space or quota monitoring.

All streams share one physical segment sequence. Segment size is a hard bound,
while optional age rollover is a soft reactor deadline. Reclamation runs
automatically after every record in a segment has been released;
`Log::reclaim` is an optional maintenance barrier, not a correctness
requirement. Because streams may interleave in a segment, a slow stream can
temporarily pin released bytes belonging to another stream.

### Runtime, cancellation, and failure

Camus runs one async reactor per open root and at most one finite storage job
per root. By default it lazily creates one process-wide private Tokio runtime.
Applications that need executor isolation can provide an `Arc<dyn Runtime>`.

Cancellation before admission is side-effect-free. After admission, dropping
an append or release Future abandons only its result; recovery may still find
the operation durable.

Expected validation and admission errors do not poison a root. An I/O error
after admission, authoritative corruption, a body-checksum failure, or a
runtime failure does. A mutating operation then reports an unknown durability
outcome: the error is not proof that the mutation is absent. Close all
handles, reopen the root, and reconcile from recovered pending records.

Use `Log::shutdown` as the synchronization barrier before copying, replacing,
or immediately reopening a root. Final-handle drop starts background shutdown
but is not such a barrier.

### Observability

`Log::stats`, `Stream::stats`, `Log::health`, and `Log::known_streams` return
reactor-maintained in-memory snapshots. `HealthWatch::changed` provides prompt,
coalescing lifecycle notification without turning health into a record event
stream. `Stream::read` remains the data-readiness Future.

Camus exposes structured counters, gauges, duration totals/maxima, health, and
stable low-cardinality error kinds. It deliberately does not choose a metrics,
logging, tracing, or exporter framework. Detailed per-operation and
per-storage-job timing is opt-in to keep the default fast path lean.

## Fit and non-goals

Camus fits a single-process local spool, embedded outbox, upload staging area,
or durable write-behind buffer when the application wants a small persistent
handoff boundary.

Camus intentionally does not provide:

- application schemas, serialization, filtering, indexes, or mutable records;
- callbacks, subscriptions, consumers, claims, leases, or retry scheduling;
- per-subscriber delivery, per-stream quotas, or capacity fairness;
- exactly-once effects, application deduplication, or distributed
  transactions;
- networking, routing, clustering, or replication; or
- database redo/undo and arbitrary key-value queries.

Use separate roots or application-level admission when workloads need hard
tenant isolation. Use a broker when delivery ownership and subscriber state
must be part of the storage product. Use a database when records need mutable
lookup and query semantics.

## Deployment and compatibility

Camus 1.x follows Semantic Versioning. Format v1 is the persistent
compatibility boundary: later compatible releases remain able to read these
roots, while an incompatible layout or interpretation requires an explicit
new format version and migration design. Earlier unpublished development
roots are outside this boundary.

The supported production envelope is a Linux local filesystem that honors
file data sync, directory sync, exclusive advisory locking, and atomic
same-directory rename. Network, userspace, and layered filesystems require
independent validation. One process owner opens a root at a time.

Back up a root after explicit shutdown, or take one atomic snapshot of the
whole root. Do not copy a live root file by file. Checksums detect accidental
corruption; they do not authenticate data against an attacker who can rewrite
bytes and recompute them.

The normative byte layouts and compatibility rules are in the
[file-format specification](docs/file-format.md). Filesystem ordering,
recovery, release, and reclamation invariants are in the
[architecture specification](docs/architecture.md).

## Documentation

- [API reference][docs]: crate overview, core types, and method-level
  contracts.
- [Usage guide](docs/usage.md): API composition, capacity, cancellation, and
  shutdown.
- [Operations guide](docs/operations.md): deployment, pressure, backup, and
  failure response.
- [Architecture](docs/architecture.md): runtime, durability, recovery,
  release, and reclamation invariants.
- [File format](docs/file-format.md): normative format-v1 layouts, checksums,
  codecs, and compatibility.
- [Observability guide](docs/observability.md): snapshot and counter semantics,
  health transitions, and adapter guidance.
- [Benchmark guide](docs/benchmarks.md): reproducible workloads and comparison
  results.
- [Long-running smoke guide](docs/long-running-smoke.md): capacity cycling,
  latency telemetry, and pass criteria.
- [Runnable examples](examples/README.md): replay, waiting reads, multi-stream
  use, maintenance, and observability.
- [Release guide](docs/releasing.md): qualification, packaging, publication,
  and rollback.

## Development

The normal local gate is:

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
```

Fuzz, benchmark, smoke, supply-chain, package, and release qualification are
kept as explicit workflows so normal development stays fast. See the
[release guide](docs/releasing.md) for the complete publication gate.

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
