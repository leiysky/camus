use crate::config::FullPolicy;
use crate::error::{DurabilityOutcome, ErrorKind};
use bytes::Bytes;
use std::fmt;
use std::ops::Deref;
use std::time::Duration;

/// A caller-selected logical stream namespace inside one storage root.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct StreamId(u64);

impl StreamId {
    /// Creates a logical stream identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the caller-selected numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for StreamId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The immutable identity stored in one Camus root superblock.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RootId([u8; 16]);

impl RootId {
    pub(crate) const LEN: usize = 16;

    pub(crate) fn random() -> std::io::Result<Self> {
        let mut bytes = [0_u8; Self::LEN];
        getrandom::fill(&mut bytes).map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn from_bytes(bytes: [u8; Self::LEN]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn to_bytes(self) -> [u8; Self::LEN] {
        self.0
    }
}

impl fmt::Debug for RootId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RootId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// A stable opaque record identity allocated by Camus.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RecordId([u8; Self::BYTE_LEN]);

impl RecordId {
    /// The fixed serialized size of every record ID.
    pub const BYTE_LEN: usize = 32;

    /// Reconstructs an opaque ID from its fixed serialization.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::BYTE_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns the fixed serialization of this ID.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; Self::BYTE_LEN] {
        self.0
    }

    /// Borrows the fixed serialization of this ID.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::BYTE_LEN] {
        &self.0
    }

    pub(crate) fn from_parts(root_id: RootId, stream_id: StreamId, sequence: u64) -> Self {
        let mut bytes = [0_u8; Self::BYTE_LEN];
        bytes[..16].copy_from_slice(&root_id.to_bytes());
        bytes[16..24].copy_from_slice(&stream_id.get().to_le_bytes());
        bytes[24..32].copy_from_slice(&sequence.to_le_bytes());
        Self(bytes)
    }

    pub(crate) fn root_id(self) -> RootId {
        let mut bytes = [0_u8; RootId::LEN];
        bytes.copy_from_slice(&self.0[..16]);
        RootId::from_bytes(bytes)
    }

    pub(crate) fn stream_id(self) -> StreamId {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&self.0[16..24]);
        StreamId::new(u64::from_le_bytes(bytes))
    }

    pub(crate) fn sequence(self) -> u64 {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&self.0[24..32]);
        u64::from_le_bytes(bytes)
    }
}

impl fmt::Debug for RecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecordId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

/// Opaque metadata and payload transferred into one append operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Record {
    /// Application-owned opaque metadata bytes.
    pub metadata: Bytes,
    /// Application-owned opaque payload bytes.
    pub payload: Bytes,
}

impl Record {
    /// Creates a record with empty metadata.
    #[must_use]
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            metadata: Bytes::new(),
            payload: payload.into(),
        }
    }

    /// Replaces the opaque metadata bytes.
    #[must_use]
    pub fn with_metadata(mut self, metadata: impl Into<Bytes>) -> Self {
        self.metadata = metadata.into();
        self
    }
}

/// One owned pending record returned by a stream read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingRecord {
    /// Stable storage identity used for release.
    pub id: RecordId,
    /// Verified opaque metadata bytes.
    pub metadata: Bytes,
    /// Verified opaque payload bytes.
    pub payload: Bytes,
}

/// Hard bounds for one waiting stream read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadLimits {
    /// Maximum number of records returned.
    pub max_records: usize,
    /// Maximum sum of returned payload bytes.
    pub max_bytes: u64,
}

impl ReadLimits {
    /// Creates explicit record-count and payload-byte limits.
    #[must_use]
    pub const fn new(max_records: usize, max_bytes: u64) -> Self {
        Self {
            max_records,
            max_bytes,
        }
    }
}

/// A non-empty owned observation of pending records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingSnapshot(Vec<PendingRecord>);

impl PendingSnapshot {
    pub(crate) fn new(records: Vec<PendingRecord>) -> Self {
        debug_assert!(!records.is_empty());
        Self(records)
    }

    /// Returns the number of records in this snapshot.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the snapshot is empty.
    ///
    /// Successful `Stream::read` calls always return `false`; this method is
    /// provided for normal collection ergonomics.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Borrows the returned records.
    #[must_use]
    pub fn records(&self) -> &[PendingRecord] {
        &self.0
    }
}

impl Deref for PendingSnapshot {
    type Target = [PendingRecord];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for PendingSnapshot {
    type Item = PendingRecord;
    type IntoIter = std::vec::IntoIter<PendingRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a PendingSnapshot {
    type Item = &'a PendingRecord;
    type IntoIter = std::slice::Iter<'a, PendingRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A count, total, and maximum for monotonic elapsed-time observations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct DurationStats {
    /// Number of elapsed-time observations included in this summary.
    pub observations: u64,
    /// Saturating sum of all observed durations.
    pub total: Duration,
    /// Longest observed duration.
    pub max: Duration,
}

/// Current durable and physical aggregate state for one root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct StorageStats {
    /// Configured root capacity in bytes, or `None` for an unbounded root.
    pub configured_capacity_bytes: Option<u64>,
    /// Configured bounded-root full policy, or `None` when unbounded.
    pub full_policy: Option<FullPolicy>,
    /// Number of logical streams that have durable identity.
    pub durable_streams: u64,
    /// Number of records currently pending across all streams.
    pub pending_records: u64,
    /// Sum of payload bytes currently pending across all streams.
    pub pending_payload_bytes: u64,
    /// Exact encoded bytes currently charged to the root.
    pub actual_file_bytes: u64,
    /// Dynamic capacity reserved for maintenance progress.
    pub maintenance_headroom_bytes: u64,
    /// Bytes currently admissible for data, or `None` for an unbounded root.
    pub data_admissible_bytes: Option<u64>,
    /// Number of extant physical data segments.
    pub live_segments: u64,
    /// Number of live segments with a durable footer.
    pub sealed_segments: u64,
    /// Number of sealed segments currently eligible for reclamation.
    pub reclaimable_segments: u64,
    /// Exact segment bytes currently eligible for reclamation.
    pub reclaimable_bytes: u64,
}

/// Current waiters plus saturating session totals for one wait category.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct WaitStats {
    /// Number of operations currently waiting in this category.
    pub current: usize,
    /// Number of waits begun since this root was opened.
    pub waits: u64,
    /// Aggregate and maximum wait duration.
    pub elapsed: DurationStats,
}

/// Reactor admission and backpressure state for one open root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct PressureStats {
    /// Configured maximum number of admitted commands buffered for this root.
    pub command_queue_capacity: usize,
    /// Number of commands admitted but not complete.
    pub queue_depth: usize,
    /// Number of filesystem jobs currently executing for this root.
    pub active_storage_jobs: usize,
    /// Number of commands admitted since this root was opened.
    pub admitted_commands: u64,
    /// Scheduling plus filesystem duration of completed storage jobs when enabled.
    pub storage_job_elapsed: DurationStats,
    /// Time spent waiting for a command-queue permit.
    pub queue_wait: WaitStats,
    /// Time waiting for a stream to become readable.
    pub readiness_wait: WaitStats,
    /// Time blocked appends spent waiting for capacity to change.
    pub capacity_wait: WaitStats,
}

/// Saturating session counters for one logical public operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct OperationCounters {
    /// Calls begun since this root was opened.
    pub started: u64,
    /// Calls that returned success to their caller.
    pub succeeded: u64,
    /// Calls that returned an error to their caller.
    pub failed: u64,
    /// Calls whose Future was dropped before returning an outcome.
    pub cancelled: u64,
    /// Records supplied to or returned by successful calls.
    ///
    /// For release this includes duplicate and already-released IDs. Use
    /// `CommitStats::release_records` for the number that changed durable
    /// pending state.
    pub records: u64,
    /// Payload bytes supplied to successful appends or returned by reads.
    ///
    /// Release and reclaim calls contribute zero.
    pub payload_bytes: u64,
    /// End-to-end call durations when detailed timing is enabled.
    pub elapsed: DurationStats,
}

/// Logical-operation activity for one open root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct OperationStats {
    /// Append and append-batch calls.
    pub append: OperationCounters,
    /// Waiting bounded-read calls.
    pub read: OperationCounters,
    /// Exact durable-release calls.
    pub release: OperationCounters,
    /// Explicit reclamation calls.
    pub reclaim: OperationCounters,
}

/// Aggregate durability-group activity for one open root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct CommitStats {
    /// Successful append durability groups.
    pub append_groups: u64,
    /// Append API units included in successful groups.
    pub append_units: u64,
    /// Records included in successful append groups.
    pub append_records: u64,
    /// Encoded bytes included in successful append groups.
    pub append_encoded_bytes: u64,
    /// Largest successful append group in API units.
    pub max_append_units: u64,
    /// Largest successful append group in encoded bytes.
    pub max_append_encoded_bytes: u64,
    /// Successful release durability groups.
    pub release_groups: u64,
    /// Release API units that contributed a durable frame.
    pub release_units: u64,
    /// Unique records included in successful release groups.
    pub release_records: u64,
    /// Encoded manifest bytes included in successful release groups.
    pub release_encoded_bytes: u64,
    /// Largest successful release group in API units.
    pub max_release_units: u64,
    /// Largest successful release group in encoded bytes.
    pub max_release_encoded_bytes: u64,
}

/// Successful physical maintenance activity for one open root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct MaintenanceStats {
    /// Low-priority or capacity-promoted reclamation passes.
    pub automatic_reclaim_passes: u64,
    /// Reclamation passes requested through `Log::reclaim`.
    pub explicit_reclaim_passes: u64,
    /// Segment seals caused by the configured hard size boundary.
    pub size_rollovers: u64,
    /// Segment seals caused by the configured soft age boundary.
    pub age_rollovers: u64,
    /// Fully released active segments sealed to make reclamation possible.
    pub reclaim_rollovers: u64,
    /// Physical segments successfully deleted and directory-synced.
    pub reclaimed_segments: u64,
    /// Exact segment bytes successfully deleted and directory-synced.
    pub reclaimed_bytes: u64,
    /// Successful complete manifest checkpoint rewrites.
    pub manifest_compactions: u64,
}

/// Work performed while successfully opening and recovering the current root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct RecoveryStats {
    /// Complete physical manifest frames structurally scanned.
    pub manifest_frames_scanned: u64,
    /// Physical data segments structurally scanned.
    pub segments_scanned: u64,
    /// Complete append epochs structurally scanned.
    pub epochs_scanned: u64,
    /// Record descriptors indexed during recovery.
    pub records_scanned: u64,
    /// Incomplete active segment tails repaired under the format rules.
    pub repaired_active_tails: u64,
    /// Incomplete manifest-log tails repaired under the format rules.
    pub repaired_manifest_tails: u64,
    /// Complete segment footers published into the manifest during recovery.
    pub completed_segment_seals: u64,
    /// Durable segment deletions completed during recovery.
    pub completed_segment_deletions: u64,
    /// Stale root or segment temporary files removed during recovery.
    pub removed_temporary_files: u64,
    /// End-to-end duration of the successful storage open.
    pub elapsed: Duration,
}

/// A synchronous in-memory snapshot of root-wide operational state.
///
/// Durable storage fields describe one coherent completed storage transition.
/// Concurrent pressure and operation counters are sampled independently and
/// may advance while the snapshot is being assembled.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct RootStats {
    /// Whether end-to-end logical-call and storage-job timing is enabled.
    pub detailed_timings: bool,
    /// Durable and physical aggregate state.
    pub storage: StorageStats,
    /// Reactor queue and wait state.
    pub pressure: PressureStats,
    /// Logical public-operation activity for this open session.
    pub operations: OperationStats,
    /// Durability-group activity for this open session.
    pub commits: CommitStats,
    /// Successful maintenance activity for this open session.
    pub maintenance: MaintenanceStats,
    /// Work performed by the successful current open.
    pub recovery: RecoveryStats,
}

/// Lifecycle state of one open root.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum RootState {
    /// The root accepts new operations.
    #[default]
    Running,
    /// New admission is closed while admitted work drains.
    ShuttingDown,
    /// Storage or runtime execution failed closed and requires reopen.
    Poisoned,
    /// The reactor and every storage job have finished.
    Closed,
}

impl RootState {
    /// Returns the stable snake-case label for this lifecycle state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::ShuttingDown => "shutting_down",
            Self::Poisoned => "poisoned",
            Self::Closed => "closed",
        }
    }
}

impl fmt::Display for RootState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Low-cardinality operation context for a failed-closed transition.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum OperationKind {
    /// Append durability work.
    Append,
    /// Pending-record verification and read work.
    Read,
    /// Durable release work.
    Release,
    /// Automatic or explicit reclamation work.
    Reclaim,
    /// Reactor-driven age rollover work outside a foreground append.
    SegmentRollover,
    /// Publication of a completed storage state into the in-memory view.
    StatePublication,
    /// Root reactor or runtime progress.
    Reactor,
}

impl OperationKind {
    /// Returns the stable snake-case label for this operation context.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Read => "read",
            Self::Release => "release",
            Self::Reclaim => "reclaim",
            Self::SegmentRollover => "segment_rollover",
            Self::StatePublication => "state_publication",
            Self::Reactor => "reactor",
        }
    }
}

impl fmt::Display for OperationKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Structured detail retained for the first failed-closed transition.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct FailureInfo {
    /// Operation that encountered the root-poisoning failure.
    pub operation: OperationKind,
    /// Stable low-cardinality error classification.
    pub error_kind: ErrorKind,
    /// Whether an admitted mutation may nevertheless be durable.
    ///
    /// Camus reports `Unknown` conservatively when runtime failure prevents a
    /// more precise result.
    pub durability_outcome: DurabilityOutcome,
    /// Human-readable detail intended for logs, never metric labels.
    pub message: String,
}

/// Current lifecycle and first failed-closed cause for one open root.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct RootHealth {
    /// Monotonic in-memory lifecycle generation for this open session.
    pub generation: u64,
    /// Current root lifecycle state.
    pub state: RootState,
    /// First root-poisoning failure, retained through shutdown.
    pub failure: Option<FailureInfo>,
}

/// A synchronous in-memory snapshot of one logical stream.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StreamStats {
    /// Whether the stream has ever completed a recoverable append.
    pub durable_known: bool,
    /// Number of records currently pending in the stream.
    pub pending_records: u64,
    /// Sum of payload bytes currently pending in the stream.
    pub pending_payload_bytes: u64,
}

/// Work completed by one explicit reclamation request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReclaimReport {
    /// Number of physical segment files removed.
    pub segments: u64,
    /// Exact encoded segment bytes removed.
    pub bytes: u64,
}

impl ReclaimReport {
    /// Returns whether the request removed no segment bytes.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.segments == 0 && self.bytes == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_id_has_fixed_root_stream_sequence_encoding() {
        let root = RootId::from_bytes([0x11; 16]);
        let stream = StreamId::new(0x0203_0405_0607_0809);
        let id = RecordId::from_parts(root, stream, 0x0a0b_0c0d_0e0f_1011);

        assert_eq!(&id.as_bytes()[..16], &[0x11; 16]);
        assert_eq!(id.root_id(), root);
        assert_eq!(id.stream_id(), stream);
        assert_eq!(id.sequence(), 0x0a0b_0c0d_0e0f_1011);
        assert_eq!(RecordId::from_bytes(id.to_bytes()), id);
    }
}
