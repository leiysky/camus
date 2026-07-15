//! Camus is an embedded persistent buffer that provides at-least-once storage
//! handoff for opaque application records.
//!
//! It stages bytes between application code and an external effect using one
//! small lifecycle:
//!
//! ```text
//! append -> read -> durable external effect -> release -> reclaim
//! ```
//!
//! A successful append remains recoverable until its exact release is durable.
//! If a process stops after the external effect but before release, recovery
//! returns the record again. This is an **at-least-once storage handoff**; the
//! application owns downstream idempotency and delivery policy.
//!
//! Camus is intended for local spools, embedded outboxes, upload staging, and
//! durable write-behind workloads. It is not a message broker or general-purpose
//! key-value database.
//!
//! ## Quick start
//!
//! ```no_run
//! use camus::{Capacity, Config, FullPolicy, Log, ReadLimits, Record, StreamId};
//!
//! # async fn make_effect_durable(_: &camus::PendingRecord) -> camus::Result<()> {
//! #     Ok(())
//! # }
//! async fn drain(root: &std::path::Path) -> camus::Result<()> {
//!     let log = Log::open(Config::new(
//!         root,
//!         Capacity::Bounded {
//!             total_bytes: 1024 * 1024 * 1024,
//!             when_full: FullPolicy::Block,
//!         },
//!     ))
//!     .await?;
//!
//!     let uploads = log.stream(StreamId::new(7));
//!     uploads
//!         .append(
//!             Record::new("opaque payload")
//!                 .with_metadata("idempotency-key=request-42"),
//!         )
//!         .await?;
//!
//!     // `read` waits while this stream has no pending records.
//!     let snapshot = uploads
//!         .read(ReadLimits::new(128, 8 * 1024 * 1024))
//!         .await?;
//!
//!     let mut completed = Vec::with_capacity(snapshot.len());
//!     for record in &snapshot {
//!         make_effect_durable(record).await?;
//!         completed.push(record.id);
//!     }
//!
//!     // Release only after the corresponding effects are durable.
//!     uploads.release(completed).await?;
//!     log.shutdown().await
//! }
//! ```
//!
//! ## API map
//!
//! | Task | API |
//! | --- | --- |
//! | Open or recover a root | [`Log::open`], [`Config`], [`Capacity`] |
//! | Create a logical handle | [`Log::stream`], [`StreamId`] |
//! | Append one durability epoch | [`Stream::append`], [`Stream::append_batch`] |
//! | Wait for and read pending work | [`Stream::read`], [`ReadLimits`] |
//! | Durably remove exact records | [`Stream::release`], [`RecordId`] |
//! | Observe current state | [`Log::stats`], [`Log::health`], [`Log::watch_health`] |
//! | Await one maintenance pass | [`Log::reclaim`] |
//! | Drain and close the root | [`Log::shutdown`] |
//!
//! ## Core semantics
//!
//! - [`Log`] owns one exclusively opened storage root. [`Log::stream`] creates
//!   a cheap, cloneable [`Stream`] handle for any caller-selected [`StreamId`].
//! - Streams are logical namespaces, not physical shards, consumers,
//!   subscriptions, claims, or I/O lanes. Multiple handles observe the same
//!   pending state, and one durable release removes a record for every handle.
//! - [`Stream::read`] is the readiness API. It waits asynchronously when the
//!   stream is empty and returns a non-empty owned [`PendingSnapshot`] bounded
//!   by [`ReadLimits`]. Reading observes records; it does not claim them.
//! - [`Stream::release`] durably removes an exact set of [`RecordId`] values.
//!   Duplicate and already released IDs are successful no-ops.
//! - Capacity is root-wide. [`FullPolicy::Block`] applies async backpressure;
//!   [`FullPolicy::RejectNew`] leaves retry or spill policy to the caller.
//!   Neither policy evicts pending data.
//! - Reclamation is automatic. [`Log::reclaim`] is available when an
//!   application needs to await one maintenance pass.
//!
//! Potentially blocking storage work is async. Handle construction and
//! reactor-maintained observations such as [`Log::stats`] and [`Stream::stats`]
//! are synchronous and perform no hidden filesystem I/O. Camus uses a shared
//! private Tokio runtime by default and accepts a caller-provided [`Runtime`]
//! when executor isolation is required.
//!
//! ## Failure and deployment boundary
//!
//! An I/O error after admission can leave a mutating operation with
//! [`DurabilityOutcome::Unknown`]; the error is not proof that the mutation is
//! absent. The open root then fails closed. Drop or close all handles, reopen
//! it, and reconcile from recovered pending records.
//!
//! Camus 1.0's production durability target is Linux on a validated local
//! filesystem. Use [`Log::shutdown`] before copying, replacing, or immediately
//! reopening a root. The [operations guide] describes the filesystem,
//! capacity, backup, and recovery envelope in detail.
//!
//! ## Further reading
//!
//! - [README]: project positioning, guarantees, and non-goals
//! - [Usage guide]: API composition and common lifecycle patterns
//! - [Operations guide][operations guide]: deployment and failure response
//! - [Architecture]: durability, recovery, release, and reclamation invariants
//! - [File format]: normative format-v1 byte layouts and compatibility
//! - [Runnable examples]: replay, readiness, multi-stream, and observability
//!
//! [README]: https://github.com/leiysky/camus
//! [Usage guide]: https://github.com/leiysky/camus/blob/main/docs/usage.md
//! [operations guide]: https://github.com/leiysky/camus/blob/main/docs/operations.md
//! [Architecture]: https://github.com/leiysky/camus/blob/main/docs/architecture.md
//! [File format]: https://github.com/leiysky/camus/blob/main/docs/file-format.md
//! [Runnable examples]: https://github.com/leiysky/camus/tree/main/examples

#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(not(unix))]
compile_error!("Camus currently supports Unix targets only");

mod api;
mod config;
mod error;
mod format;
mod model;
mod runtime;
mod storage;
#[cfg(test)]
mod test_crash;

pub use api::{HealthWatch, Log, Stream};
pub use config::{
    Capacity, Config, FullPolicy, DEFAULT_COMMAND_QUEUE_CAPACITY, DEFAULT_MAX_COMMIT_BYTES,
    DEFAULT_MAX_COMMIT_UNITS, DEFAULT_MAX_EPOCH_BYTES, DEFAULT_MAX_RELEASE_RECORDS,
    DEFAULT_SEGMENT_BYTES,
};
pub use error::{DurabilityOutcome, Error, ErrorKind, Result};
pub use model::{
    CommitStats, DurationStats, FailureInfo, MaintenanceStats, OperationCounters, OperationKind,
    OperationStats, PendingRecord, PendingSnapshot, PressureStats, ReadLimits, ReclaimReport,
    Record, RecordId, RecoveryStats, RootHealth, RootState, RootStats, StorageJobStats,
    StorageStats, StreamId, StreamStats, WaitStats,
};
pub use runtime::{Runtime, RuntimeError, RuntimeFuture};
