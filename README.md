# Camus

Camus is an embedded durable staging log for at-least-once pipelines. It gives
an application a local `append -> recover/read -> release -> reclaim` lifecycle
for opaque records without prescribing what the records mean or where they go.

Camus is a storage primitive, not a service or pipeline runtime. It starts no
worker thread, async runtime, network listener, retry loop, or background
reclamation/rollover task. One process owns one storage root through an
exclusive lock.

## Core contract

- `append_batch` and `append_batch_to` commit one stream-local batch as one
  durability epoch with one epoch `sync_data` (a required segment rotation has
  its own prerequisite syncs);
- `pending_records` exposes only complete, unreleased records without loading
  payloads;
- `read` and `read_many` fetch payloads lazily and verify their checksums;
- runtime-neutral `wait_for(stream_id)` futures expose level-triggered stream
  readiness without starting a thread or async runtime;
- `release` and `release_from` durably record that an application no longer
  needs a stream-scoped record; and
- `reclaim` removes fully released sealed segments from every stream in
  manifest order.

Record IDs are stable UTF-8 application keys. Camus assumes callers never
reuse an ID within one logical stream, including after release or reclamation.
The same ID may be used independently in different streams. Metadata and
payloads are opaque byte strings.

The intended delivery flow is:

```text
append record
  -> recover/read record
  -> perform the external effect
  -> release record
  -> reclaim its segment when eligible
```

Within the supported storage envelope, a successfully appended record remains
recoverable and pending until its release marker is durable. If the external
effect succeeds but the process crashes before `release` becomes durable,
recovery returns the record again. This is at-least-once staging, not a
guarantee that application consumer code runs or that downstream delivery
succeeds. Applications must make repeated effects safe when that matters.

## Quick start

The examples below introduce the core lifecycle and logical streams. The
[complete usage guide](docs/usage.md) covers bounded draining, async readiness
integration, multiple subscribers, restart recovery, maintenance, and failure
handling.

```rust
use camus::{Config, Log, Record, Result};

fn stage(root: &std::path::Path) -> Result<()> {
    let mut log = Log::open(Config::new(root))?;

    let locations = log.append_batch(&[
        Record::new("record-1", b"payload-1".as_slice())
            .with_metadata(b"content-type:example".as_slice()),
        Record::new("record-2", b"payload-2".as_slice()),
    ])?;

    assert_eq!(log.read(&locations[0])?, b"payload-1".as_slice());

    // Release only after the records are durably represented elsewhere.
    log.release(["record-1", "record-2"])?;
    log.reclaim()?;
    Ok(())
}
```

Logical streams use stable `u32` identifiers. Stream zero backs the original
`append`, `append_batch`, and `release` methods; the stream-aware methods keep
record IDs, segment sequences, rollover, release, and reclamation isolated:

```rust
use camus::{Config, Log, Record, Result, RolloverPolicy, StreamId};
use std::time::Duration;

fn stage_upload(root: &std::path::Path) -> Result<()> {
    let uploads = StreamId::new(7);
    let config = Config::new(root).with_stream_rollover(
        uploads,
        RolloverPolicy::new(64 * 1024 * 1024)
            .with_max_segment_age(Duration::from_secs(300)),
    );
    let mut log = Log::open(config)?;
    log.append_to(uploads, Record::new("upload-1", b"opaque".as_slice()))?;

    // Call from the application's timer so an idle expired segment rotates.
    log.rollover_expired()?;
    log.release_from(uploads, ["upload-1"])?;
    Ok(())
}
```

Size rollover is checked before every append. Age rollover is checked both
before append and by `rollover_expired`; Camus deliberately does not create a
timer thread. A durability epoch is never split, so the size setting is a
target rather than a hard limit.

On restart, `log.recovery().pending_records()` lists complete, unreleased
records. `release` is durable and idempotent for a live record that is already
released. Physical bytes remain until their sealed segment is reclaimable.
`reclaim_active_for_storage_pressure` may rotate a fully released active
segment before reclaiming it.

## Async stream readiness

`wait_for(stream_id)` returns a runtime-neutral Future. It completes when the
selected stream has at least one complete, unreleased record, including work
found while opening the root. The Future owns shared readiness state rather
than borrowing `Log`, so a storage owner can keep using the synchronous handle
while another application task awaits readiness:

```rust
use camus::{Readiness, Result, StreamId};

async fn wait_for_upload(readiness: Readiness, uploads: StreamId) -> Result<()> {
    readiness.wait_for(uploads).await?;
    // Notify the Log owner to inspect pending_records_for(uploads), read the
    // payloads, perform the external effect, and release_from(uploads, ids).
    Ok(())
}
```

Obtain a detached handle with `log.readiness()`, or create an owned Future
directly with `log.wait_for(stream_id)`. Readiness is level-triggered: waiting
again completes immediately until every record in that stream is released.
All waiters for a ready stream are awakened; Camus does not select a consumer
or assign records. Append wakes waiters only after its epoch is durable and the
in-memory recovery view is updated. Release recomputes readiness after its
manifest record is durable. Dropping or poisoning `Log` wakes outstanding
waiters with `Error::ReadinessClosed`; reopen and wait on the new handle when
an errored operation may have crossed its durability boundary.

If a storage, corruption, codec, or internal-state error makes an operation's
durable outcome uncertain, the open `Log` becomes poisoned. Drop it and reopen
the root; do not keep issuing reads or writes through that handle. An operation
that returned an error may still be present after reopening if its durability
sync completed before the error was observed. Recovery is the source of truth.

## Appropriate uses

Camus fits local spools, durable write-behind buffers, upload staging, and the
storage core of an embedded outbox. The embedding application owns scheduling,
retry policy, backpressure, destination semantics, and idempotency.

Camus intentionally does not provide:

- tables, queries, indexes, updates, or database transactions;
- database redo/undo or an LSN-based recovery protocol;
- leases, acknowledger coordination, or a multi-consumer queue;
- exactly-once delivery, deduplication, or distributed transactions; or
- a network service, cluster membership, routing, or async storage I/O.

## Documentation

- [Usage guide](docs/usage.md): end-to-end API composition and common mistakes.
- [Runnable examples](examples/README.md): restart replay, multi-stream
  draining, async readiness, and maintenance patterns.
- [File format](docs/file-format.md): normative version-1 byte layouts,
  checksums, manifest schemas, repair boundaries, and stable vectors.
- [Architecture](docs/architecture.md): durability, recovery, ordering, and
  on-disk invariants.
- [Operations guide](docs/operations.md): supported filesystems, capacity,
  monitoring, backup, and upgrades.
- [Glossary](docs/glossary.md): storage-domain terminology.
- [Async readiness ADR](docs/adr/0001-async-stream-readiness.md): why readiness
  is broadcast observation rather than consumer assignment.

## Supported deployment envelope

Camus currently builds on Unix targets and is tested as a synchronous local
filesystem component. Its durability contract requires filesystem support for
file data sync, directory sync, exclusive advisory locks, and atomic rename
within one directory. Network filesystems, userspace filesystems, and storage
layers that weaken those operations are outside the supported durability
envelope unless independently validated.

Use one storage root per process owner, keep record IDs unique for the lifetime
of their logical stream, and make downstream effects idempotent. XXH3
checksums detect accidental corruption; they are not authentication and do not
protect against malicious modification. Back up a closed root or use an
atomic filesystem snapshot rather than copying a live directory file by file.

Operational requirements, failure handling, capacity guidance, monitoring, and
upgrade procedures are documented in [docs/operations.md](docs/operations.md).
Report security issues through the private process in
[SECURITY.md](SECURITY.md).

## Storage layout and compatibility

```text
<root>/
  camus.lock
  MANIFEST
  segments/
    segment-00000000000000000000.log
    ...
  streams/
    stream-0000000007/
      segment-00000000000000000000.log
      ...
```

Stream zero retains the original `segments/` layout. Nonzero streams use one
canonical directory each. Camus's on-disk format version 1 includes
stream-scoped manifest events and persisted segment creation times.
Unsupported or corrupt authoritative data fails closed; Camus does not infer
or reinterpret another system's log format.

The exact version-1 encoding is specified in
[docs/file-format.md](docs/file-format.md). The durability and recovery
contract is documented in [docs/architecture.md](docs/architecture.md).

## Development

```sh
cargo fmt --all --check
cargo fmt --all --check --manifest-path fuzz/Cargo.toml
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --release --lib --tests
cargo test --locked --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo check --locked --manifest-path fuzz/Cargo.toml
cargo deny --locked check -A license-not-encountered licenses sources
cargo deny --locked --manifest-path fuzz/Cargo.toml check licenses sources
cargo package --locked
```

The runnable examples are exercised in CI and catalogued in
[examples/README.md](examples/README.md).

Pull requests are checked for a semantic title and run formatting, Clippy,
documentation, release-package, fuzz-target build, and locked test checks.
Tests cover Ubuntu and macOS on the declared Rust toolchain, plus current
stable Rust. Dependency-policy changes also run RustSec audits and license and
source checks. Workflow actions are pinned to full commit SHAs and updated by
Dependabot.

The scheduled workflows run both recovery fuzz targets, audit `Cargo.lock`
plus `fuzz/Cargo.lock` against the RustSec advisory database, and enforce the
repository's dependency license and source policy.

For a local dependency audit, install the workflow-pinned tool with
`cargo install cargo-audit --locked --version 0.22.2`, then run
`cargo audit --deny warnings` and
`cargo audit --deny warnings --file fuzz/Cargo.lock`.

Install the license/source checker with
`cargo install cargo-deny --locked --version 0.20.2`; its policy is in
`deny.toml`.

The recovery fuzzer is documented in [fuzz/README.md](fuzz/README.md).
