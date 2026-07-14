//! Camus is an embedded durable staging log for at-least-once pipelines.
//!
//! It stores opaque metadata and payload bytes in checksummed, stream-local
//! segment files, commits record batches with one durability sync, recovers
//! complete epochs, applies size/age rollover, and reclaims fully released
//! segments. Runtime-neutral stream-readiness Futures wake application-owned
//! tasks after successful durable appends while recovery remains authoritative.
//! Applications own external effects, timer scheduling, retry policy, and
//! idempotency.
//!
//! # Example
//!
//! ```
//! use camus::{Config, Log, Record};
//!
//! # fn main() -> camus::Result<()> {
//! let directory = tempfile::tempdir()?;
//! let mut log = Log::open(Config::new(directory.path()))?;
//! let location = log.append(
//!     Record::new("record-1", b"payload".as_slice())
//!         .with_metadata(b"opaque metadata".as_slice()),
//! )?;
//!
//! assert_eq!(log.read(&location)?.as_ref(), b"payload");
//! log.release(["record-1"])?;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(not(unix))]
compile_error!("Camus currently supports Unix targets only");

#[cfg(unix)]
mod wal;

#[cfg(unix)]
pub use wal::{
    AppendRecord as Record, FileWal as Log, FileWalConfig as Config, ReclaimReport, RecordMeta,
    RecoveredRecord, RolloverPolicy, StreamId, StreamReadiness as Readiness, WaitForStream,
    WalError as Error, WalLocation as Location, WalRecovery as Recovery, WalResult as Result,
    WalState as State, WalStats as Stats, DEFAULT_SEGMENT_BYTES, DEFAULT_STREAM,
    MAX_RECORD_ID_BYTES,
};
