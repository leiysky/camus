use crate::model::{RecordId, StreamId};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

/// Whether an I/O error can say anything definitive about durable mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurabilityOutcome {
    /// This value carries no mutation-specific uncertainty.
    ///
    /// For errors that do not encode an outcome themselves, callers must also
    /// apply the operation's admission and cancellation contract.
    NotApplicable,
    /// Recovery must decide whether the mutation became durable.
    Unknown,
}

/// Stable low-cardinality classification for a Camus error.
///
/// Applications may use this value as a metric label. Paths, record IDs, and
/// human-readable error strings should remain log or trace fields instead.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Invalid open configuration.
    InvalidConfig,
    /// Exclusive root ownership could not be acquired.
    RootInUse,
    /// Operation admission is closed.
    Closed,
    /// The open root has failed closed.
    Poisoned,
    /// Empty append request.
    EmptyAppend,
    /// Invalid bounded-read limits.
    InvalidReadLimits,
    /// Encoded append epoch exceeds its configured bound.
    EpochTooLarge,
    /// Release request exceeds its configured bound.
    ReleaseTooLarge,
    /// Earliest pending record cannot fit the read byte bound.
    ReadLimitTooSmall,
    /// Record ID belongs to another root or stream.
    RecordIdScopeMismatch,
    /// Record ID is beyond the durable stream high-water.
    UnknownRecordId,
    /// Stream sequence space is exhausted.
    SequenceExhausted,
    /// Physical segment ID space is exhausted.
    SegmentIdExhausted,
    /// Manifest sequence space is exhausted.
    ManifestSequenceExhausted,
    /// Bounded reject policy declined an append.
    RejectedCapacity,
    /// Append can never fit the configured root capacity.
    ExceedsCapacity,
    /// Filesystem operation failure.
    Io,
    /// Authoritative format or lifecycle corruption.
    Corruption,
    /// Runtime progress failure.
    Runtime,
}

impl ErrorKind {
    /// Returns the stable snake-case label for this classification.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::RootInUse => "root_in_use",
            Self::Closed => "closed",
            Self::Poisoned => "poisoned",
            Self::EmptyAppend => "empty_append",
            Self::InvalidReadLimits => "invalid_read_limits",
            Self::EpochTooLarge => "epoch_too_large",
            Self::ReleaseTooLarge => "release_too_large",
            Self::ReadLimitTooSmall => "read_limit_too_small",
            Self::RecordIdScopeMismatch => "record_id_scope_mismatch",
            Self::UnknownRecordId => "unknown_record_id",
            Self::SequenceExhausted => "sequence_exhausted",
            Self::SegmentIdExhausted => "segment_id_exhausted",
            Self::ManifestSequenceExhausted => "manifest_sequence_exhausted",
            Self::RejectedCapacity => "rejected_capacity",
            Self::ExceedsCapacity => "exceeds_capacity",
            Self::Io => "io",
            Self::Corruption => "corruption",
            Self::Runtime => "runtime",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// An error returned by a Camus operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The open configuration violates a required invariant.
    #[error("invalid configuration: {message}")]
    InvalidConfig {
        /// Human-readable invariant failure.
        message: String,
    },

    /// Another owner currently holds the root lock.
    #[error("storage root is already in use: {path}", path = .path.display())]
    RootInUse {
        /// Locked storage root.
        path: PathBuf,
    },

    /// The shared root lifecycle has closed.
    #[error("storage root is closed")]
    Closed,

    /// The open root failed closed and must be reopened.
    #[error("storage root is poisoned; close it and reopen for recovery")]
    Poisoned,

    /// An append batch contained no records.
    #[error("append batch must contain at least one record")]
    EmptyAppend,

    /// A read limit could never produce a record.
    #[error("read limits require max_records greater than zero")]
    InvalidReadLimits,

    /// A complete append epoch exceeded its configured hard bound.
    #[error("encoded epoch is {encoded_bytes} bytes, exceeding the {max_bytes}-byte limit")]
    EpochTooLarge {
        /// Complete encoded epoch bytes.
        encoded_bytes: u64,
        /// Configured maximum epoch bytes.
        max_bytes: u64,
    },

    /// One release request exceeded its configured record-count bound.
    #[error("release contains {records} IDs, exceeding the {max_records}-record limit")]
    ReleaseTooLarge {
        /// Number of IDs after request-level validation.
        records: usize,
        /// Configured maximum IDs per request.
        max_records: usize,
    },

    /// The earliest pending record could not fit the payload-byte bound.
    #[error(
        "earliest pending record {id} needs {required_bytes} payload bytes, exceeding the {max_bytes}-byte read limit"
    )]
    ReadLimitTooSmall {
        /// Earliest pending record.
        id: RecordId,
        /// Payload bytes required for that record.
        required_bytes: u64,
        /// Configured payload-byte limit.
        max_bytes: u64,
    },

    /// A record ID belongs to another root or logical stream.
    #[error("record ID {id} does not belong to stream {expected_stream} in this root")]
    RecordIdScopeMismatch {
        /// Rejected opaque record ID.
        id: RecordId,
        /// Stream on which release was called.
        expected_stream: StreamId,
    },

    /// A well-scoped record ID has never been durable in its stream.
    #[error("record ID {id} is not known in this stream")]
    UnknownRecordId {
        /// Unknown opaque record ID.
        id: RecordId,
    },

    /// A stream has allocated every representable sequence.
    #[error("stream {stream_id} exhausted its record sequence space")]
    SequenceExhausted {
        /// Exhausted logical stream.
        stream_id: StreamId,
    },

    /// The root has allocated every supported physical segment ID.
    #[error("storage root exhausted its physical segment ID space")]
    SegmentIdExhausted,

    /// The root has allocated every supported manifest sequence.
    #[error("storage root exhausted its manifest sequence space")]
    ManifestSequenceExhausted,

    /// Bounded `RejectNew` policy declined an append before admission.
    #[error(
        "bounded root rejected {needed_bytes} projected bytes; {available_bytes} bytes are currently admissible"
    )]
    RejectedCapacity {
        /// Projected bytes required for admission.
        needed_bytes: u64,
        /// Bytes currently admissible for data.
        available_bytes: u64,
    },

    /// An append can never fit within the bounded root.
    #[error(
        "append requires {needed_bytes} projected bytes and can never fit the {total_bytes}-byte root capacity"
    )]
    ExceedsCapacity {
        /// Minimum projected encoded bytes required.
        needed_bytes: u64,
        /// Configured total root capacity.
        total_bytes: u64,
    },

    /// A filesystem operation failed.
    #[error(
        "{operation} failed for {path} (durable outcome: {outcome:?}): {source}",
        path = .path.display()
    )]
    Io {
        /// Short operation description.
        operation: &'static str,
        /// Artifact or directory involved.
        path: PathBuf,
        /// Whether mutation durability is unknown.
        outcome: DurabilityOutcome,
        /// Underlying operating-system error.
        #[source]
        source: io::Error,
    },

    /// Authoritative format or lifecycle state is corrupt.
    #[error("corruption in {path} at byte {offset}: {message}", path = .path.display())]
    Corruption {
        /// Corrupt artifact.
        path: PathBuf,
        /// First byte associated with the failure.
        offset: u64,
        /// Validation failure.
        message: String,
    },

    /// The configured runtime could not continue Camus work.
    #[error("runtime backend failure: {message}")]
    Runtime {
        /// Runtime failure description.
        message: String,
    },
}

impl Error {
    /// Returns a stable low-cardinality classification for this error.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::InvalidConfig { .. } => ErrorKind::InvalidConfig,
            Self::RootInUse { .. } => ErrorKind::RootInUse,
            Self::Closed => ErrorKind::Closed,
            Self::Poisoned => ErrorKind::Poisoned,
            Self::EmptyAppend => ErrorKind::EmptyAppend,
            Self::InvalidReadLimits => ErrorKind::InvalidReadLimits,
            Self::EpochTooLarge { .. } => ErrorKind::EpochTooLarge,
            Self::ReleaseTooLarge { .. } => ErrorKind::ReleaseTooLarge,
            Self::ReadLimitTooSmall { .. } => ErrorKind::ReadLimitTooSmall,
            Self::RecordIdScopeMismatch { .. } => ErrorKind::RecordIdScopeMismatch,
            Self::UnknownRecordId { .. } => ErrorKind::UnknownRecordId,
            Self::SequenceExhausted { .. } => ErrorKind::SequenceExhausted,
            Self::SegmentIdExhausted => ErrorKind::SegmentIdExhausted,
            Self::ManifestSequenceExhausted => ErrorKind::ManifestSequenceExhausted,
            Self::RejectedCapacity { .. } => ErrorKind::RejectedCapacity,
            Self::ExceedsCapacity { .. } => ErrorKind::ExceedsCapacity,
            Self::Io { .. } => ErrorKind::Io,
            Self::Corruption { .. } => ErrorKind::Corruption,
            Self::Runtime { .. } => ErrorKind::Runtime,
        }
    }

    /// Returns the durability knowledge carried by this error.
    ///
    /// Only filesystem errors currently carry a mutation-specific outcome.
    /// Other errors report `NotApplicable`; operation context remains
    /// available separately through the returned error or root health.
    #[must_use]
    pub const fn durability_outcome(&self) -> DurabilityOutcome {
        match self {
            Self::Io { outcome, .. } => *outcome,
            _ => DurabilityOutcome::NotApplicable,
        }
    }

    pub(crate) fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig {
            message: message.into(),
        }
    }

    pub(crate) fn io(
        operation: &'static str,
        path: impl AsRef<Path>,
        outcome: DurabilityOutcome,
        source: io::Error,
    ) -> Self {
        Self::Io {
            operation,
            path: path.as_ref().to_path_buf(),
            outcome,
            source,
        }
    }

    pub(crate) fn corruption(
        path: impl AsRef<Path>,
        offset: u64,
        message: impl Into<String>,
    ) -> Self {
        Self::Corruption {
            path: path.as_ref().to_path_buf(),
            offset,
            message: message.into(),
        }
    }

    pub(crate) const fn poisons_root(&self) -> bool {
        matches!(
            self,
            Self::Io { .. } | Self::Corruption { .. } | Self::Runtime { .. }
        )
    }

    pub(crate) fn copy_nonpoisoning(&self) -> Option<Self> {
        match self {
            Self::InvalidConfig { message } => Some(Self::InvalidConfig {
                message: message.clone(),
            }),
            Self::RootInUse { path } => Some(Self::RootInUse { path: path.clone() }),
            Self::Closed => Some(Self::Closed),
            Self::Poisoned => Some(Self::Poisoned),
            Self::EmptyAppend => Some(Self::EmptyAppend),
            Self::InvalidReadLimits => Some(Self::InvalidReadLimits),
            Self::EpochTooLarge {
                encoded_bytes,
                max_bytes,
            } => Some(Self::EpochTooLarge {
                encoded_bytes: *encoded_bytes,
                max_bytes: *max_bytes,
            }),
            Self::ReleaseTooLarge {
                records,
                max_records,
            } => Some(Self::ReleaseTooLarge {
                records: *records,
                max_records: *max_records,
            }),
            Self::ReadLimitTooSmall {
                id,
                required_bytes,
                max_bytes,
            } => Some(Self::ReadLimitTooSmall {
                id: *id,
                required_bytes: *required_bytes,
                max_bytes: *max_bytes,
            }),
            Self::RecordIdScopeMismatch {
                id,
                expected_stream,
            } => Some(Self::RecordIdScopeMismatch {
                id: *id,
                expected_stream: *expected_stream,
            }),
            Self::UnknownRecordId { id } => Some(Self::UnknownRecordId { id: *id }),
            Self::SequenceExhausted { stream_id } => Some(Self::SequenceExhausted {
                stream_id: *stream_id,
            }),
            Self::SegmentIdExhausted => Some(Self::SegmentIdExhausted),
            Self::ManifestSequenceExhausted => Some(Self::ManifestSequenceExhausted),
            Self::RejectedCapacity {
                needed_bytes,
                available_bytes,
            } => Some(Self::RejectedCapacity {
                needed_bytes: *needed_bytes,
                available_bytes: *available_bytes,
            }),
            Self::ExceedsCapacity {
                needed_bytes,
                total_bytes,
            } => Some(Self::ExceedsCapacity {
                needed_bytes: *needed_bytes,
                total_bytes: *total_bytes,
            }),
            Self::Io { .. } | Self::Corruption { .. } | Self::Runtime { .. } => None,
        }
    }
}

/// The result type returned by Camus operations.
pub type Result<T> = std::result::Result<T, Error>;
