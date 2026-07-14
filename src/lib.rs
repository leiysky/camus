//! Camus is an embedded persistent buffer for opaque application records.
//!
//! It provides an asynchronous `append -> read -> release -> reclaim`
//! lifecycle with an at-least-once storage handoff. Logical streams are
//! application-selected namespaces; they are independent of physical segment
//! placement and do not represent consumers or subscriptions.

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

pub use api::{Log, Stream};
pub use config::{
    Capacity, Config, FullPolicy, DEFAULT_COMMAND_QUEUE_CAPACITY, DEFAULT_MAX_COMMIT_BYTES,
    DEFAULT_MAX_COMMIT_UNITS, DEFAULT_MAX_EPOCH_BYTES, DEFAULT_MAX_RELEASE_RECORDS,
    DEFAULT_SEGMENT_BYTES,
};
pub use error::{DurabilityOutcome, Error, Result};
pub use model::{
    PendingRecord, PendingSnapshot, ReadLimits, ReclaimReport, Record, RecordId, Stats, StreamId,
    StreamStats,
};
pub use runtime::{Runtime, RuntimeError, RuntimeFuture};
