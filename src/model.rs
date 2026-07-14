use crate::config::FullPolicy;
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

/// A synchronous in-memory snapshot of root-wide operational state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Stats {
    /// Configured root capacity in bytes, or `None` for an unbounded root.
    pub configured_capacity_bytes: Option<u64>,
    /// Configured bounded-root full policy, or `None` when unbounded.
    pub full_policy: Option<FullPolicy>,
    /// Number of logical streams that have durable identity.
    pub durable_streams: usize,
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
    /// Number of commands currently admitted but not complete.
    pub queue_depth: usize,
    /// Number of operations currently waiting before admission.
    pub admission_waiters: usize,
    /// Number of operations admitted since this root was opened.
    pub admitted_operations: u64,
    /// Aggregate time operations have spent waiting for admission.
    pub total_admission_wait: Duration,
    /// Longest observed wait before admission.
    pub max_admission_wait: Duration,
    /// Whether storage execution has failed closed for this open root.
    pub poisoned: bool,
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
