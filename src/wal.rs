//! Camus's segmented, checksummed storage implementation.
//!
//! Records are durable only after their complete epoch and epoch marker have
//! been written by one `sync_data`. Recovery returns committed epochs, repairs
//! an incomplete active tail, and fails closed on corruption before the tail.
use bytes::Bytes;
use fs2::FileExt as Fs2FileExt;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::future::Future;
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use xxhash_rust::xxh3::Xxh3;

const SEGMENT_MAGIC: &[u8; 8] = b"CAMSEG01";
const MANIFEST_MAGIC: &[u8; 8] = b"CAMMAN01";
const RECORD_MAGIC: &[u8; 8] = b"CAMREC01";
const MANIFEST_RECORD_MAGIC: &[u8; 8] = b"CAMMRC01";
const SEGMENT_FORMAT_VERSION: u16 = 1;
const MANIFEST_FORMAT_VERSION: u16 = 1;
pub(crate) const FILE_HEADER_LEN: u64 = 32;
const SEGMENT_RECORD_PREFIX_LEN: u64 = 48;
const MANIFEST_RECORD_PREFIX_LEN: u64 = 32;
const RECORD_KIND: u8 = 1;
const EPOCH_COMMIT_KIND: u8 = 2;
const RELEASE_KIND: u8 = 1;
const SEGMENT_ROTATION_KIND: u8 = 2;
const SEGMENT_REMOVAL_KIND: u8 = 3;
const SEGMENT_SNAPSHOT_KIND: u8 = 4;
const SEGMENT_TIMESTAMP_KIND: u8 = 5;
const STREAM_RELEASE_KIND: u8 = 6;
const EPOCH_COMMIT_METADATA_LEN: u64 = 24;
const MAX_METADATA_LEN: u64 = 16 * 1024 * 1024;
const MAX_SEGMENT_IDS_PER_REMOVAL_RECORD: usize = 64 * 1024;
/// Maximum UTF-8 byte length accepted for a record ID.
pub const MAX_RECORD_ID_BYTES: usize = 16 * 1024;
const HASH_BUFFER_LEN: usize = 64 * 1024;

/// Default target size at which a non-empty active segment is rotated.
pub const DEFAULT_SEGMENT_BYTES: u64 = 128 * 1024 * 1024;

/// Stable identifier for one logical stream inside a storage root.
///
/// Each stream has its own segment sequence, record-ID namespace, release
/// state, and rollover policy. Stream zero is used by the compatibility APIs
/// such as [`FileWal::append`] and [`FileWal::release`].
#[repr(transparent)]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct StreamId(u32);

impl StreamId {
    /// Creates a stream identifier from its durable numeric value.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the durable numeric value stored in segment and manifest data.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for StreamId {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Logical stream used by APIs that predate multi-stream support.
pub const DEFAULT_STREAM: StreamId = StreamId::new(0);

/// Per-stream segment rollover policy.
///
/// The size is a target rather than a hard limit because one durability epoch
/// is never split. Age is checked before append and by
/// [`FileWal::rollover_expired`]; Camus does not start a timer thread.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RolloverPolicy {
    /// Target size at which a non-empty active segment is rotated.
    pub segment_bytes: u64,
    /// Maximum age of a non-empty active segment, measured with persisted Unix
    /// time at whole-millisecond precision. `None` disables age rollover.
    pub max_segment_age: Option<Duration>,
}

impl RolloverPolicy {
    /// Creates a size-only rollover policy.
    pub const fn new(segment_bytes: u64) -> Self {
        Self {
            segment_bytes,
            max_segment_age: None,
        }
    }

    /// Adds an age target to this policy.
    #[must_use]
    pub const fn with_max_segment_age(mut self, max_segment_age: Duration) -> Self {
        self.max_segment_age = Some(max_segment_age);
        self
    }
}

impl Default for RolloverPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_SEGMENT_BYTES)
    }
}

/// Per-root failpoints used by deterministic crash-window tests. Keeping them
/// on `WalRoot` avoids global test state and lets the normal test suite run in
/// parallel without timing or serialization assumptions.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TestFailPoint {
    EpochFramesWritten,
    EpochMarkerWritten,
    EpochSynced,
    SegmentCreated,
    RotationManifestWritten,
    RotationManifestSynced,
    ReleaseManifestWritten,
    ReleaseManifestSynced,
    RemovalManifestWritten,
    RemovalManifestSynced,
    SegmentsDeleted,
    CompactionSynced,
    CompactionRenamed,
}

/// Errors returned by Camus storage operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum WalError {
    /// An operating-system I/O operation failed.
    #[error("Camus I/O error: {0}")]
    Io(#[from] io::Error),
    /// Internal manifest metadata could not be encoded.
    #[error("Camus metadata encoding error: {0}")]
    Codec(#[from] serde_json::Error),
    /// Checksummed or authoritative on-disk state is corrupt.
    #[error("corrupt Camus storage at {path}:{offset}: {message}")]
    Corruption {
        /// File or directory in which corruption was detected.
        path: PathBuf,
        /// Byte offset of the damaged structure, or zero for directory state.
        offset: u64,
        /// Human-readable corruption detail.
        message: String,
    },
    /// An append attempted to reuse a live or retained record ID.
    #[error("record id already exists: {0}")]
    DuplicateRecord(String),
    /// A release referenced an ID unknown to the selected logical stream.
    #[error("record does not exist in Camus: {0}")]
    UnknownRecord(String),
    /// An operation referenced a logical stream not declared by the manifest.
    #[error("logical stream does not exist in Camus: {0}")]
    UnknownStream(StreamId),
    /// A record or release request violated an input bound.
    #[error("invalid record: {0}")]
    InvalidRecord(String),
    /// A supplied record location is malformed, stale, or mismatched.
    #[error("invalid record location: {0}")]
    InvalidLocation(String),
    /// Configuration or authoritative lifecycle state is inconsistent.
    #[error("invalid Camus configuration: {0}")]
    InvalidConfig(String),
    /// Another process or handle already owns the storage root lock.
    #[error("Camus storage root is already open: {0}")]
    RootInUse(PathBuf),
    /// A readiness future's owning `Log` was dropped or became poisoned.
    #[error("Camus stream readiness closed because its Log was dropped or poisoned")]
    ReadinessClosed,
    /// A previous failure had an uncertain durable outcome; reopen is required.
    #[error(
        "Camus log cannot be reused after an uncertain storage failure; drop it and reopen the root"
    )]
    Poisoned,
}

impl WalError {
    fn invalidates_open_log(&self) -> bool {
        matches!(
            self,
            Self::Io(_) | Self::Codec(_) | Self::Corruption { .. } | Self::InvalidConfig(_)
        )
    }
}

/// Result type returned by Camus APIs.
pub type WalResult<T> = Result<T, WalError>;

/// Configuration used to open one storage root.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct FileWalConfig {
    /// Directory containing the lock, manifest, and logical-stream segments.
    pub root: PathBuf,
    /// Target segment size. Must exceed 32 bytes; a single durability epoch
    /// may exceed this value.
    pub segment_bytes: u64,
    /// Default maximum segment age for streams without an explicit override.
    /// `None` disables age rollover.
    pub max_segment_age: Option<Duration>,
    stream_rollover: BTreeMap<StreamId, RolloverPolicy>,
}

impl FileWalConfig {
    /// Creates a configuration using [`DEFAULT_SEGMENT_BYTES`].
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            max_segment_age: None,
            stream_rollover: BTreeMap::new(),
        }
    }

    /// Sets the target segment rotation size. [`FileWal::open`] rejects values
    /// of 32 bytes or less.
    #[must_use]
    pub fn with_segment_bytes(mut self, segment_bytes: u64) -> Self {
        self.segment_bytes = segment_bytes;
        self
    }

    /// Sets the default maximum age for non-empty active segments. The value
    /// must be at least one whole millisecond.
    #[must_use]
    pub fn with_max_segment_age(mut self, max_segment_age: Duration) -> Self {
        self.max_segment_age = Some(max_segment_age);
        self
    }

    /// Overrides size and age rollover for one logical stream.
    #[must_use]
    pub fn with_stream_rollover(mut self, stream_id: StreamId, policy: RolloverPolicy) -> Self {
        self.stream_rollover.insert(stream_id, policy);
        self
    }

    /// Returns the effective rollover policy for a logical stream.
    pub fn rollover_policy(&self, stream_id: StreamId) -> RolloverPolicy {
        self.stream_rollover
            .get(&stream_id)
            .copied()
            .unwrap_or(RolloverPolicy {
                segment_bytes: self.segment_bytes,
                max_segment_age: self.max_segment_age,
            })
    }
}

/// One staged record. Metadata and payload are opaque to Camus.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct AppendRecord {
    /// Stable UTF-8 application ID. IDs must never be reused in one stream.
    pub record_id: String,
    /// Opaque metadata stored inline with the record descriptor. Its encoded
    /// envelope together with the record ID is limited to 16 MiB.
    pub metadata: Bytes,
    /// Opaque payload read lazily from its segment.
    pub payload: Bytes,
}

impl AppendRecord {
    /// Creates a record with empty metadata.
    pub fn new(record_id: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            record_id: record_id.into(),
            metadata: Bytes::new(),
            payload: payload.into(),
        }
    }

    /// Attaches opaque metadata to the record.
    #[must_use]
    pub fn with_metadata(mut self, metadata: impl Into<Bytes>) -> Self {
        self.metadata = metadata.into();
        self
    }

    /// Returns the non-payload metadata persisted for recovery.
    pub fn meta(&self) -> RecordMeta {
        RecordMeta {
            record_id: self.record_id.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

/// Record identity and opaque metadata returned during recovery.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordMeta {
    /// Stable application record ID within its logical stream.
    pub record_id: String,
    /// Opaque caller-provided metadata.
    pub metadata: Bytes,
}

fn encode_record_meta(meta: &RecordMeta) -> WalResult<Vec<u8>> {
    validate_record_id(&meta.record_id)?;
    let encoded_len = 4_usize
        .checked_add(meta.record_id.len())
        .and_then(|len| len.checked_add(meta.metadata.len()))
        .ok_or_else(|| WalError::InvalidRecord("metadata length overflow".into()))?;
    if encoded_len as u64 > MAX_METADATA_LEN {
        return Err(WalError::InvalidRecord(format!(
            "record id and metadata exceed {MAX_METADATA_LEN} bytes"
        )));
    }
    let id_len = u32::try_from(meta.record_id.len())
        .map_err(|_| WalError::InvalidRecord("record id is too large".into()))?;
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(&id_len.to_le_bytes());
    encoded.extend_from_slice(meta.record_id.as_bytes());
    encoded.extend_from_slice(&meta.metadata);
    Ok(encoded)
}

fn decode_record_meta(encoded: &[u8]) -> Result<RecordMeta, String> {
    let id_len_bytes = encoded
        .get(..4)
        .ok_or_else(|| "record metadata is missing its id length".to_string())?;
    let id_len = u32::from_le_bytes(id_len_bytes.try_into().unwrap()) as usize;
    if id_len == 0 || id_len > MAX_RECORD_ID_BYTES {
        return Err("record id length is outside the supported range".into());
    }
    let id_end = 4_usize
        .checked_add(id_len)
        .filter(|end| *end <= encoded.len())
        .ok_or_else(|| "record metadata contains a truncated id".to_string())?;
    let record_id = std::str::from_utf8(&encoded[4..id_end])
        .map_err(|_| "record id is not valid UTF-8".to_string())?
        .to_owned();
    Ok(RecordMeta {
        record_id,
        metadata: Bytes::copy_from_slice(&encoded[id_end..]),
    })
}

fn validate_record_id(record_id: &str) -> WalResult<()> {
    if record_id.is_empty() {
        return Err(WalError::InvalidRecord(
            "record id must not be empty".into(),
        ));
    }
    if record_id.len() > MAX_RECORD_ID_BYTES {
        return Err(WalError::InvalidRecord(format!(
            "record id exceeds {MAX_RECORD_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_release_metadata_len(metadata_len: usize) -> WalResult<()> {
    if metadata_len as u64 > MAX_METADATA_LEN {
        return Err(WalError::InvalidRecord(format!(
            "release request metadata exceeds {MAX_METADATA_LEN} bytes; split the request"
        )));
    }
    Ok(())
}

fn validate_append_batch(records: &[AppendRecord]) -> WalResult<()> {
    let mut seen = HashSet::with_capacity(records.len());
    let mut epoch_len = segment_record_len(EPOCH_COMMIT_METADATA_LEN, 0)
        .map_err(|_| WalError::InvalidRecord("durability epoch length overflow".into()))?;
    for record in records {
        if !seen.insert(record.record_id.as_str()) {
            return Err(WalError::DuplicateRecord(record.record_id.clone()));
        }
        let metadata = encode_record_meta(&record.meta())?;
        let frame_len = segment_record_len(metadata.len() as u64, record.payload.len() as u64)
            .map_err(|_| WalError::InvalidRecord("record length overflow".into()))?;
        epoch_len = epoch_len
            .checked_add(frame_len)
            .ok_or_else(|| WalError::InvalidRecord("durability epoch is too large".into()))?;
    }
    Ok(())
}

fn duration_to_millis(duration: Duration) -> WalResult<u64> {
    if !duration.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(WalError::InvalidConfig(
            "max_segment_age must use whole-millisecond precision".into(),
        ));
    }
    let millis = u64::try_from(duration.as_millis()).map_err(|_| {
        WalError::InvalidConfig("max_segment_age exceeds the supported range".into())
    })?;
    if millis == 0 {
        return Err(WalError::InvalidConfig(
            "max_segment_age must be at least one millisecond".into(),
        ));
    }
    Ok(millis)
}

fn validate_rollover_policy(stream_id: StreamId, policy: RolloverPolicy) -> WalResult<()> {
    if policy.segment_bytes <= FILE_HEADER_LEN {
        return Err(WalError::InvalidConfig(format!(
            "segment_bytes for stream {stream_id} must be greater than {FILE_HEADER_LEN}"
        )));
    }
    if let Some(max_age) = policy.max_segment_age {
        duration_to_millis(max_age)?;
    }
    Ok(())
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn validate_manifest_release_ids(path: &Path, offset: u64, record_ids: &[String]) -> WalResult<()> {
    if record_ids.is_empty() {
        return Err(corruption(path, offset, "release record is empty"));
    }
    let mut seen = HashSet::with_capacity(record_ids.len());
    for record_id in record_ids {
        validate_record_id(record_id)
            .map_err(|error| corruption(path, offset, error.to_string()))?;
        if !seen.insert(record_id) {
            return Err(corruption(
                path,
                offset,
                format!("release repeats record id {record_id}"),
            ));
        }
    }
    Ok(())
}

/// Physical location of one record payload.
///
/// Locations may be serialized but are valid only for the storage root and
/// segment lifecycle that produced them. Camus revalidates all fields on read.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalLocation {
    /// Logical stream containing the segment. Missing values in locations
    /// serialized by older Camus releases decode as [`DEFAULT_STREAM`].
    #[serde(default)]
    pub stream_id: StreamId,
    /// Segment file sequence number.
    pub segment_id: u64,
    /// Byte offset at which the complete record frame begins.
    pub frame_offset: u64,
    /// Total descriptor, metadata, and payload byte length.
    pub frame_len: u64,
    /// Byte offset at which the payload begins.
    pub payload_offset: u64,
    /// Logical payload byte length.
    pub payload_len: u64,
    /// XXH3 checksum stored in the record descriptor.
    pub payload_checksum: u64,
}

/// One complete record discovered during segment recovery.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredRecord {
    /// Logical stream containing this record.
    pub stream_id: StreamId,
    /// Recovered record identity and opaque metadata.
    pub meta: RecordMeta,
    /// Validated physical location used for lazy payload reads.
    pub location: WalLocation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseV1 {
    pub record_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamReleaseV1 {
    stream_id: u32,
    record_ids: Vec<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum SegmentLifecycle {
    Active,
    Sealed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SegmentRotationV1 {
    shard_id: u32,
    previous_segment_id: Option<u64>,
    new_segment_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at_unix_millis: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SegmentRemovalV1 {
    shard_id: u32,
    segment_ids: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SegmentSnapshotV1 {
    shard_id: u32,
    segment_id: u64,
    lifecycle: SegmentLifecycle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at_unix_millis: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SegmentTimestampV1 {
    shard_id: u32,
    segment_id: u64,
    created_at_unix_millis: u64,
}

/// Authoritative release and segment lifecycle state reconstructed from the
/// manifest.
#[derive(Clone, Debug, Default)]
pub struct WalState {
    released_record_ids: BTreeSet<String>,
    stream_released_record_ids: BTreeMap<u32, BTreeSet<String>>,
    segments: BTreeMap<u32, BTreeMap<u64, SegmentLifecycle>>,
    segment_created_at_unix_millis: BTreeMap<u32, BTreeMap<u64, u64>>,
}

impl WalState {
    /// Returns default-stream record IDs with a durable release marker still
    /// retained in the manifest checkpoint.
    pub fn released_record_ids(&self) -> &BTreeSet<String> {
        &self.released_record_ids
    }

    /// Returns retained release IDs for one logical stream, if any. Stream
    /// zero is also available through [`Self::released_record_ids`].
    pub fn released_record_ids_for(&self, stream_id: StreamId) -> Option<&BTreeSet<String>> {
        if stream_id == DEFAULT_STREAM {
            Some(&self.released_record_ids)
        } else {
            self.stream_released_record_ids.get(&stream_id.get())
        }
    }

    /// Returns whether one stream-scoped record ID has a durable release.
    pub fn is_released(&self, stream_id: StreamId, record_id: &str) -> bool {
        self.released_record_ids_for(stream_id)
            .is_some_and(|released| released.contains(record_id))
    }

    /// Iterates logical streams declared by the authoritative manifest.
    pub fn stream_ids(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.segments.keys().copied().map(StreamId::new)
    }

    fn segments(&self, shard_id: u32) -> BTreeMap<u64, SegmentLifecycle> {
        self.segments.get(&shard_id).cloned().unwrap_or_default()
    }

    fn active_segment(&self, shard_id: u32) -> WalResult<Option<u64>> {
        let mut active = self
            .segments
            .get(&shard_id)
            .into_iter()
            .flat_map(|segments| segments.iter())
            .filter_map(|(segment_id, lifecycle)| {
                (*lifecycle == SegmentLifecycle::Active).then_some(*segment_id)
            });
        let active_segment = active.next();
        if active.next().is_some() {
            return Err(WalError::InvalidConfig(format!(
                "manifest contains multiple active segments for shard {shard_id}"
            )));
        }
        Ok(active_segment)
    }

    fn active_segment_created_at(&self, shard_id: u32) -> WalResult<Option<u64>> {
        Ok(self.active_segment(shard_id)?.and_then(|segment_id| {
            self.segment_created_at_unix_millis
                .get(&shard_id)
                .and_then(|segments| segments.get(&segment_id))
                .copied()
        }))
    }

    fn validate_segments(&self) -> WalResult<()> {
        for shard_id in self.segments.keys() {
            let active = self.active_segment(*shard_id)?;
            let last = self
                .segments
                .get(shard_id)
                .and_then(|segments| segments.last_key_value().map(|(segment_id, _)| *segment_id));
            if active.is_none() || active != last {
                return Err(WalError::InvalidConfig(format!(
                    "manifest active segment is not the newest segment for shard {shard_id}"
                )));
            }
        }
        for (shard_id, timestamps) in &self.segment_created_at_unix_millis {
            let Some(segments) = self.segments.get(shard_id) else {
                return Err(WalError::InvalidConfig(format!(
                    "manifest timestamps reference missing shard {shard_id}"
                )));
            };
            if let Some(segment_id) = timestamps
                .keys()
                .find(|segment_id| !segments.contains_key(segment_id))
            {
                return Err(WalError::InvalidConfig(format!(
                    "manifest timestamp references missing segment {segment_id} for shard {shard_id}"
                )));
            }
        }
        if self.stream_released_record_ids.contains_key(&0) {
            return Err(WalError::InvalidConfig(
                "manifest stores default-stream releases in the extended namespace".into(),
            ));
        }
        if !self.released_record_ids.is_empty()
            && !self.segments.contains_key(&DEFAULT_STREAM.get())
        {
            return Err(WalError::InvalidConfig(
                "manifest default-stream releases have no declared stream".into(),
            ));
        }
        if let Some(stream_id) = self
            .stream_released_record_ids
            .keys()
            .find(|stream_id| !self.segments.contains_key(stream_id))
        {
            return Err(WalError::InvalidConfig(format!(
                "manifest releases reference missing stream {stream_id}"
            )));
        }
        Ok(())
    }

    fn apply_rotation(&mut self, rotation: &SegmentRotationV1) -> WalResult<()> {
        let segments = self.segments.entry(rotation.shard_id).or_default();
        match rotation.previous_segment_id {
            None => {
                if !segments.is_empty() || rotation.new_segment_id != 0 {
                    return Err(WalError::InvalidConfig(format!(
                        "manifest cannot initialize shard {} at segment {}",
                        rotation.shard_id, rotation.new_segment_id
                    )));
                }
            }
            Some(previous_segment_id) => {
                if previous_segment_id.checked_add(1) != Some(rotation.new_segment_id)
                    || segments.get(&previous_segment_id) != Some(&SegmentLifecycle::Active)
                {
                    return Err(WalError::InvalidConfig(format!(
                        "manifest rotation for shard {} does not follow active segment {previous_segment_id}",
                        rotation.shard_id
                    )));
                }
                segments.insert(previous_segment_id, SegmentLifecycle::Sealed);
            }
        }
        if segments
            .insert(rotation.new_segment_id, SegmentLifecycle::Active)
            .is_some()
        {
            return Err(WalError::InvalidConfig(format!(
                "manifest segment {} already exists for shard {}",
                rotation.new_segment_id, rotation.shard_id
            )));
        }
        if let Some(created_at) = rotation.created_at_unix_millis {
            self.segment_created_at_unix_millis
                .entry(rotation.shard_id)
                .or_default()
                .insert(rotation.new_segment_id, created_at);
        }
        Ok(())
    }

    fn apply_removal(&mut self, removal: &SegmentRemovalV1) -> WalResult<()> {
        if removal.segment_ids.is_empty() {
            return Err(WalError::InvalidConfig(
                "manifest segment removal is empty".into(),
            ));
        }
        let segments = self.segments.get_mut(&removal.shard_id).ok_or_else(|| {
            WalError::InvalidConfig(format!(
                "manifest removal references missing shard {}",
                removal.shard_id
            ))
        })?;
        let mut seen = HashSet::with_capacity(removal.segment_ids.len());
        for segment_id in &removal.segment_ids {
            if !seen.insert(*segment_id) || !segments.contains_key(segment_id) {
                return Err(WalError::InvalidConfig(format!(
                    "manifest cannot remove segment {segment_id} from shard {}",
                    removal.shard_id
                )));
            }
        }
        if removal
            .segment_ids
            .iter()
            .any(|segment_id| segments.get(segment_id) != Some(&SegmentLifecycle::Sealed))
        {
            return Err(WalError::InvalidConfig(format!(
                "manifest cannot remove an active segment from shard {}",
                removal.shard_id
            )));
        }
        for segment_id in &removal.segment_ids {
            segments.remove(segment_id);
            if let Some(timestamps) = self
                .segment_created_at_unix_millis
                .get_mut(&removal.shard_id)
            {
                timestamps.remove(segment_id);
            }
        }
        Ok(())
    }

    fn apply_snapshot(&mut self, snapshot: &SegmentSnapshotV1) -> WalResult<()> {
        if self
            .segments
            .entry(snapshot.shard_id)
            .or_default()
            .insert(snapshot.segment_id, snapshot.lifecycle)
            .is_some()
        {
            return Err(WalError::InvalidConfig(format!(
                "manifest snapshot repeats segment {} for shard {}",
                snapshot.segment_id, snapshot.shard_id
            )));
        }
        if let Some(created_at) = snapshot.created_at_unix_millis {
            self.segment_created_at_unix_millis
                .entry(snapshot.shard_id)
                .or_default()
                .insert(snapshot.segment_id, created_at);
        }
        Ok(())
    }

    fn apply_timestamp(&mut self, timestamp: &SegmentTimestampV1) -> WalResult<()> {
        if !self
            .segments
            .get(&timestamp.shard_id)
            .is_some_and(|segments| segments.contains_key(&timestamp.segment_id))
        {
            return Err(WalError::InvalidConfig(format!(
                "manifest timestamp references missing segment {} for shard {}",
                timestamp.segment_id, timestamp.shard_id
            )));
        }
        if self
            .segment_created_at_unix_millis
            .entry(timestamp.shard_id)
            .or_default()
            .insert(timestamp.segment_id, timestamp.created_at_unix_millis)
            .is_some()
        {
            return Err(WalError::InvalidConfig(format!(
                "manifest repeats timestamp for segment {} of shard {}",
                timestamp.segment_id, timestamp.shard_id
            )));
        }
        Ok(())
    }
}

/// Complete physical records and manifest state found during recovery.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct WalRecovery {
    /// All complete record frames still present in the segment set. Payloads
    /// are deliberately absent and are read from their recorded locations only
    /// when the caller needs them.
    pub records: Vec<RecoveredRecord>,
    /// Authoritative release and segment lifecycle state.
    pub state: WalState,
}

impl WalRecovery {
    /// Iterates complete records that do not have a durable release marker.
    pub fn pending_records_iter(&self) -> impl Iterator<Item = &RecoveredRecord> {
        self.records.iter().filter(|record| {
            !self
                .state
                .is_released(record.stream_id, &record.meta.record_id)
        })
    }

    /// Iterates complete, unreleased records in one logical stream.
    pub fn pending_records_for_iter(
        &self,
        stream_id: StreamId,
    ) -> impl Iterator<Item = &RecoveredRecord> {
        self.pending_records_iter()
            .filter(move |record| record.stream_id == stream_id)
    }

    /// Clones all complete records that do not have a durable release marker.
    /// Prefer [`Self::pending_records_iter`] when a borrowed view is enough.
    pub fn pending_records(&self) -> Vec<RecoveredRecord> {
        self.pending_records_iter().cloned().collect()
    }

    /// Clones complete, unreleased records in one logical stream.
    pub fn pending_records_for(&self, stream_id: StreamId) -> Vec<RecoveredRecord> {
        self.pending_records_for_iter(stream_id).cloned().collect()
    }
}

/// Cumulative I/O and recovery counters for an open log handle.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalStats {
    /// Successful epoch `sync_data` calls.
    pub epoch_syncs: u64,
    /// Record frames durably appended, excluding epoch markers.
    pub record_frames: u64,
    /// Physical record-frame bytes written, excluding file headers.
    pub bytes: u64,
    /// Logical payload bytes durably appended.
    pub payload_bytes: u64,
    /// Segment-header durability syncs.
    pub segment_header_syncs: u64,
    /// Manifest-header or checkpoint-file durability syncs.
    pub manifest_header_syncs: u64,
    /// Manifest event or checkpoint durability syncs.
    pub manifest_syncs: u64,
    /// Parent-directory durability syncs.
    pub directory_syncs: u64,
    /// Active segment or manifest tails truncated and synced during recovery.
    pub repaired_tails: u64,
    /// Successful non-empty batch read operations.
    pub read_calls: u64,
    /// Segment files opened by successful batch reads.
    pub read_segment_opens: u64,
    /// Contiguous frame ranges read from segment files.
    pub read_ranges: u64,
    /// Record frames returned by successful batch reads.
    pub read_frames: u64,
    /// Physical bytes read across complete record-frame ranges. Adjacent
    /// frames may be coalesced into one positional read.
    pub read_frame_bytes: u64,
    /// Logical payload bytes returned by successful batch reads.
    pub read_payload_bytes: u64,
}

impl WalStats {
    fn accumulate(&mut self, other: Self) {
        self.epoch_syncs = self.epoch_syncs.saturating_add(other.epoch_syncs);
        self.record_frames = self.record_frames.saturating_add(other.record_frames);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.payload_bytes = self.payload_bytes.saturating_add(other.payload_bytes);
        self.segment_header_syncs = self
            .segment_header_syncs
            .saturating_add(other.segment_header_syncs);
        self.manifest_header_syncs = self
            .manifest_header_syncs
            .saturating_add(other.manifest_header_syncs);
        self.manifest_syncs = self.manifest_syncs.saturating_add(other.manifest_syncs);
        self.directory_syncs = self.directory_syncs.saturating_add(other.directory_syncs);
        self.repaired_tails = self.repaired_tails.saturating_add(other.repaired_tails);
        self.read_calls = self.read_calls.saturating_add(other.read_calls);
        self.read_segment_opens = self
            .read_segment_opens
            .saturating_add(other.read_segment_opens);
        self.read_ranges = self.read_ranges.saturating_add(other.read_ranges);
        self.read_frames = self.read_frames.saturating_add(other.read_frames);
        self.read_frame_bytes = self.read_frame_bytes.saturating_add(other.read_frame_bytes);
        self.read_payload_bytes = self
            .read_payload_bytes
            .saturating_add(other.read_payload_bytes);
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct WalReadStats {
    calls: u64,
    segment_opens: u64,
    ranges: u64,
    frames: u64,
    frame_bytes: u64,
    payload_bytes: u64,
}

/// Storage removed by a reclamation call.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReclaimReport {
    /// Number of sealed segment files removed.
    pub segments: u64,
    /// Segment and manifest bytes removed.
    pub bytes: u64,
}

/// Cloneable, runtime-neutral handle for waiting until one logical stream has
/// at least one complete, unreleased record.
#[derive(Clone)]
pub struct StreamReadiness {
    shared: Arc<StreamReadinessShared>,
}

struct StreamReadinessShared {
    state: Mutex<StreamReadinessState>,
}

#[derive(Default)]
struct StreamReadinessState {
    ready: BTreeSet<StreamId>,
    waiters: BTreeMap<StreamId, BTreeMap<u64, Waker>>,
    next_waiter_id: u64,
    closed: bool,
}

/// Future returned by [`StreamReadiness::wait_for`].
///
/// The future owns its readiness handle rather than borrowing a [`FileWal`],
/// so it may be moved to an application runtime while the storage owner keeps
/// using the synchronous log handle.
#[must_use = "futures do nothing unless polled or awaited"]
pub struct WaitForStream {
    shared: Arc<StreamReadinessShared>,
    stream_id: StreamId,
    waiter_id: Option<u64>,
    completion: Option<WaitCompletion>,
}

#[derive(Clone, Copy)]
enum WaitCompletion {
    Ready,
    Closed,
}

impl fmt::Debug for StreamReadiness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamReadiness")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for WaitForStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WaitForStream")
            .field("stream_id", &self.stream_id)
            .finish_non_exhaustive()
    }
}

impl StreamReadiness {
    fn new(ready: impl IntoIterator<Item = StreamId>) -> Self {
        Self {
            shared: Arc::new(StreamReadinessShared {
                state: Mutex::new(StreamReadinessState {
                    ready: ready.into_iter().collect(),
                    ..StreamReadinessState::default()
                }),
            }),
        }
    }

    /// Returns a future that completes when `stream_id` has at least one
    /// complete, unreleased record.
    ///
    /// This is level-triggered: it completes immediately while the stream is
    /// already consumable. Multiple waiters for one stream are all awakened;
    /// Camus does not assign records or coordinate consumers.
    ///
    /// ```
    /// use camus::{Readiness, Result, StreamId};
    ///
    /// async fn wait_for_work(readiness: Readiness, stream: StreamId) -> Result<()> {
    ///     readiness.wait_for(stream).await
    /// }
    /// ```
    pub fn wait_for(&self, stream_id: StreamId) -> WaitForStream {
        WaitForStream {
            shared: Arc::clone(&self.shared),
            stream_id,
            waiter_id: None,
            completion: None,
        }
    }

    /// Returns whether the latest published in-memory state marks the stream
    /// as having at least one complete, unreleased record.
    pub fn is_ready(&self, stream_id: StreamId) -> bool {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .ready
            .contains(&stream_id)
    }

    fn update_stream(&self, stream_id: StreamId, ready: bool) {
        let wakers = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.closed {
                return;
            }
            if ready {
                state.ready.insert(stream_id);
                state
                    .waiters
                    .remove(&stream_id)
                    .map(BTreeMap::into_values)
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
            } else {
                state.ready.remove(&stream_id);
                Vec::new()
            }
        };
        for waker in wakers {
            waker.wake();
        }
    }

    fn refresh(&self, ready: impl IntoIterator<Item = StreamId>) {
        let wakers = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.closed {
                return;
            }
            state.ready = ready.into_iter().collect();
            let ready_streams = state.ready.iter().copied().collect::<Vec<_>>();
            let mut wakers = Vec::new();
            for stream_id in ready_streams {
                if let Some(stream_waiters) = state.waiters.remove(&stream_id) {
                    wakers.extend(stream_waiters.into_values());
                }
            }
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
    }

    fn close(&self) {
        let wakers = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.closed {
                return;
            }
            state.closed = true;
            state.ready.clear();
            std::mem::take(&mut state.waiters)
                .into_values()
                .flat_map(BTreeMap::into_values)
                .collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

impl WaitForStream {
    /// Returns the logical stream this future is waiting for.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }
}

impl Future for WaitForStream {
    type Output = WalResult<()>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(completion) = self.completion {
            return Poll::Ready(match completion {
                WaitCompletion::Ready => Ok(()),
                WaitCompletion::Closed => Err(WalError::ReadinessClosed),
            });
        }

        let shared = Arc::clone(&self.shared);
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            remove_waiter(&mut state, self.stream_id, self.waiter_id.take());
            self.completion = Some(WaitCompletion::Closed);
            return Poll::Ready(Err(WalError::ReadinessClosed));
        }
        if state.ready.contains(&self.stream_id) {
            remove_waiter(&mut state, self.stream_id, self.waiter_id.take());
            self.completion = Some(WaitCompletion::Ready);
            return Poll::Ready(Ok(()));
        }

        let waiter_id = match self.waiter_id {
            Some(waiter_id) => waiter_id,
            None => {
                let waiter_id = allocate_waiter_id(&mut state, self.stream_id);
                self.waiter_id = Some(waiter_id);
                waiter_id
            }
        };
        let stream_waiters = state.waiters.entry(self.stream_id).or_default();
        if !stream_waiters
            .get(&waiter_id)
            .is_some_and(|waker| waker.will_wake(context.waker()))
        {
            stream_waiters.insert(waiter_id, context.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for WaitForStream {
    fn drop(&mut self) {
        if self.completion.is_some() {
            return;
        }
        let Some(waiter_id) = self.waiter_id.take() else {
            return;
        };
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        remove_waiter(&mut state, self.stream_id, Some(waiter_id));
    }
}

fn allocate_waiter_id(state: &mut StreamReadinessState, stream_id: StreamId) -> u64 {
    loop {
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        if state.next_waiter_id != 0
            && !state
                .waiters
                .get(&stream_id)
                .is_some_and(|waiters| waiters.contains_key(&state.next_waiter_id))
        {
            return state.next_waiter_id;
        }
    }
}

fn remove_waiter(state: &mut StreamReadinessState, stream_id: StreamId, waiter_id: Option<u64>) {
    let Some(waiter_id) = waiter_id else {
        return;
    };
    let Some(stream_waiters) = state.waiters.get_mut(&stream_id) else {
        return;
    };
    stream_waiters.remove(&waiter_id);
    if stream_waiters.is_empty() {
        state.waiters.remove(&stream_id);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ManifestWrite {
    pub(crate) added_records: usize,
    pub(crate) added_bytes: u64,
}

/// Synchronous owner of one Camus storage root.
///
/// State-changing operations require mutable access. An exclusive file lock
/// prevents a second handle or process from opening the same root.
pub struct FileWal {
    shards: BTreeMap<StreamId, ShardSegments>,
    default_rollover: RolloverPolicy,
    stream_rollover: BTreeMap<StreamId, RolloverPolicy>,
    manifest: LocalManifest,
    recovery: WalRecovery,
    readiness: StreamReadiness,
    poisoned: Cell<bool>,
    // Declared last so the process lease outlives both file-owning components.
    root: WalRoot,
}

/// Owns the process-wide storage root lease independently from file handles.
#[derive(Clone)]
pub(crate) struct WalRoot {
    guard: Arc<WalRootGuard>,
}

struct WalRootGuard {
    path: PathBuf,
    lock: File,
    stats: WalStats,
    #[cfg(test)]
    failpoint: Mutex<Option<TestFailPoint>>,
}

impl Drop for WalRootGuard {
    fn drop(&mut self) {
        // End the lease before descriptor destruction so an immediate
        // in-process reopen cannot observe a stale lock.
        let _ = Fs2FileExt::unlock(&self.lock);
    }
}

/// All mutable state for one append segment shard. Manifest lifecycle state is
/// owned separately so segment and manifest durability remain explicit.
pub(crate) struct ShardSegments {
    _root: WalRoot,
    shard_id: u32,
    segment_bytes: u64,
    max_segment_age_millis: Option<u64>,
    active_created_at_unix_millis: u64,
    directory: PathBuf,
    active: Option<SegmentWriter>,
    segment_lifecycle: BTreeMap<u64, SegmentLifecycle>,
    segment_records: BTreeMap<u64, Vec<String>>,
    record_ids: HashSet<String>,
    physical_bytes: u64,
    stats: WalStats,
    read_stats: Cell<WalReadStats>,
}

struct SegmentWriter {
    id: u64,
    file: File,
    len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EpochCommit {
    epoch_start: u64,
    frame_count: u64,
    descriptors_checksum: u64,
}

impl EpochCommit {
    fn encode(self) -> [u8; EPOCH_COMMIT_METADATA_LEN as usize] {
        let mut encoded = [0_u8; EPOCH_COMMIT_METADATA_LEN as usize];
        encoded[..8].copy_from_slice(&self.epoch_start.to_le_bytes());
        encoded[8..16].copy_from_slice(&self.frame_count.to_le_bytes());
        encoded[16..24].copy_from_slice(&self.descriptors_checksum.to_le_bytes());
        encoded
    }

    fn decode(encoded: &[u8]) -> Result<Self, &'static str> {
        if encoded.len() != EPOCH_COMMIT_METADATA_LEN as usize {
            return Err("invalid epoch commit length");
        }
        Ok(Self {
            epoch_start: u64::from_le_bytes(encoded[..8].try_into().unwrap()),
            frame_count: u64::from_le_bytes(encoded[8..16].try_into().unwrap()),
            descriptors_checksum: u64::from_le_bytes(encoded[16..24].try_into().unwrap()),
        })
    }
}

struct SegmentScan {
    active: Option<SegmentWriter>,
    records: Vec<RecoveredRecord>,
    segment_records: BTreeMap<u64, Vec<String>>,
    record_ids: HashSet<String>,
}

pub(crate) struct ReclaimedSegments {
    pub(crate) report: ReclaimReport,
    pub(crate) segment_ids: Vec<u64>,
}

impl WalRoot {
    fn acquire(path: PathBuf) -> WalResult<Self> {
        let directory_syncs = create_directories_durably(&path)?;
        let lock_path = path.join("camus.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&lock_path)?;
        match Fs2FileExt::try_lock_exclusive(&lock) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Err(WalError::RootInUse(lock_path));
            }
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            guard: Arc::new(WalRootGuard {
                path,
                lock,
                stats: WalStats {
                    directory_syncs,
                    ..WalStats::default()
                },
                #[cfg(test)]
                failpoint: Mutex::new(None),
            }),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.guard.path
    }

    pub(crate) fn available_space(&self) -> io::Result<u64> {
        fs2::available_space(&self.guard.path)
    }

    pub(crate) fn stats(&self) -> WalStats {
        self.guard.stats
    }

    #[cfg(test)]
    fn arm_failpoint(&self, failpoint: TestFailPoint) {
        let mut armed = self.guard.failpoint.lock().unwrap();
        assert!(
            armed.replace(failpoint).is_none(),
            "a failpoint is already armed"
        );
    }

    #[cfg(test)]
    fn fail_if_armed(&self, failpoint: TestFailPoint) -> io::Result<()> {
        let mut armed = self.guard.failpoint.lock().unwrap();
        if *armed == Some(failpoint) {
            *armed = None;
            return Err(io::Error::other(format!(
                "injected crash after {failpoint:?}"
            )));
        }
        Ok(())
    }
}

impl ShardSegments {
    fn open(
        root: WalRoot,
        shard_id: u32,
        policy: RolloverPolicy,
        active_created_at_unix_millis: u64,
        segments: BTreeMap<u64, SegmentLifecycle>,
        mut stats: WalStats,
    ) -> WalResult<(Self, Vec<RecoveredRecord>)> {
        let directory = shard_directory(root.path(), shard_id);
        stats.directory_syncs = stats
            .directory_syncs
            .saturating_add(create_directories_durably(&directory)?);
        let scan = scan_segments(shard_id, &directory, &segments, &mut stats)?;
        let physical_bytes = measure_segment_bytes(&directory)?;
        Ok((
            Self {
                _root: root,
                shard_id,
                segment_bytes: policy.segment_bytes,
                max_segment_age_millis: policy
                    .max_segment_age
                    .map(duration_to_millis)
                    .transpose()?,
                active_created_at_unix_millis,
                directory,
                active: scan.active,
                segment_lifecycle: segments,
                segment_records: scan.segment_records,
                record_ids: scan.record_ids,
                physical_bytes,
                stats,
                read_stats: Cell::new(WalReadStats::default()),
            },
            scan.records,
        ))
    }

    pub(crate) fn append_epoch_with_manifest<E, F>(
        &mut self,
        batches: &[AppendRecord],
        now_unix_millis: u64,
        mut publish_rotation: F,
    ) -> Result<Vec<WalLocation>, E>
    where
        E: From<WalError>,
        F: FnMut(u32, u64, u64, u64) -> Result<(), E>,
    {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let mut seen = HashSet::with_capacity(batches.len());
        let mut prepared = Vec::with_capacity(batches.len());
        let mut append_bytes = 0_u64;
        let mut descriptors = Xxh3::new();
        for batch in batches {
            if self.record_ids.contains(&batch.record_id) || !seen.insert(batch.record_id.as_str())
            {
                return Err(E::from(WalError::DuplicateRecord(batch.record_id.clone())));
            }
            let meta = batch.meta();
            let encoded = encode_record_meta(&meta).map_err(E::from)?;
            let payload_checksum = xxhash_rust::xxh3::xxh3_64(&batch.payload);
            let prefix = segment_record_prefix(
                RECORD_KIND,
                &encoded,
                batch.payload.len() as u64,
                payload_checksum,
            )
            .map_err(E::from)?;
            let frame_len = segment_record_len(encoded.len() as u64, batch.payload.len() as u64)
                .map_err(E::from)?;
            append_bytes = append_bytes.checked_add(frame_len).ok_or_else(|| {
                E::from(WalError::InvalidConfig(
                    "durability epoch is too large".into(),
                ))
            })?;
            descriptors.update(&prefix);
            prepared.push((meta, encoded, prefix, frame_len, payload_checksum));
        }
        let epoch_marker_len = segment_record_len(EPOCH_COMMIT_METADATA_LEN, 0).map_err(E::from)?;
        let epoch_len = append_bytes.checked_add(epoch_marker_len).ok_or_else(|| {
            E::from(WalError::InvalidConfig(
                "durability epoch is too large".into(),
            ))
        })?;

        let active_len = self
            .active
            .as_ref()
            .ok_or_else(|| {
                WalError::InvalidConfig(format!("shard {} has no active segment", self.shard_id))
            })
            .map_err(E::from)?
            .len;
        let projected_len = active_len
            .checked_add(epoch_len)
            .ok_or_else(|| E::from(WalError::InvalidConfig("segment length overflow".into())))?;
        let age_due = self.max_segment_age_millis.is_some_and(|max_age| {
            now_unix_millis.saturating_sub(self.active_created_at_unix_millis) >= max_age
        });
        if active_len > FILE_HEADER_LEN && (projected_len > self.segment_bytes || age_due) {
            let (previous_segment_id, new_segment_id) =
                self.rotate(now_unix_millis).map_err(E::from)?;
            publish_rotation(
                self.shard_id,
                previous_segment_id,
                new_segment_id,
                now_unix_millis,
            )?;
        }

        let active = self
            .active
            .as_mut()
            .ok_or_else(|| {
                WalError::InvalidConfig(format!("shard {} has no active segment", self.shard_id))
            })
            .map_err(E::from)?;
        let physical_bytes = self
            .physical_bytes
            .checked_add(epoch_len)
            .ok_or_else(|| E::from(WalError::InvalidConfig("WAL size overflow".into())))?;
        let mut locations = Vec::with_capacity(batches.len());
        let epoch_start = active.len;
        let mut next_offset = active.len;
        for (batch, (_, metadata, prefix, frame_len, payload_checksum)) in
            batches.iter().zip(prepared.iter())
        {
            active
                .file
                .write_all(prefix)
                .map_err(WalError::from)
                .map_err(E::from)?;
            active
                .file
                .write_all(metadata)
                .map_err(WalError::from)
                .map_err(E::from)?;
            active
                .file
                .write_all(&batch.payload)
                .map_err(WalError::from)
                .map_err(E::from)?;

            let payload_offset = next_offset + SEGMENT_RECORD_PREFIX_LEN + metadata.len() as u64;
            locations.push(WalLocation {
                stream_id: StreamId::new(self.shard_id),
                segment_id: active.id,
                frame_offset: next_offset,
                frame_len: *frame_len,
                payload_offset,
                payload_len: batch.payload.len() as u64,
                payload_checksum: *payload_checksum,
            });
            next_offset += frame_len;
        }

        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::EpochFramesWritten)
            .map_err(WalError::from)
            .map_err(E::from)?;

        let marker = EpochCommit {
            epoch_start,
            frame_count: batches.len() as u64,
            descriptors_checksum: descriptors.digest(),
        }
        .encode();
        let marker_prefix =
            segment_record_prefix(EPOCH_COMMIT_KIND, &marker, 0, 0).map_err(E::from)?;
        active
            .file
            .write_all(&marker_prefix)
            .map_err(WalError::from)
            .map_err(E::from)?;
        active
            .file
            .write_all(&marker)
            .map_err(WalError::from)
            .map_err(E::from)?;

        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::EpochMarkerWritten)
            .map_err(WalError::from)
            .map_err(E::from)?;
        next_offset = next_offset
            .checked_add(epoch_marker_len)
            .ok_or_else(|| E::from(WalError::InvalidConfig("segment length overflow".into())))?;

        // This is the only durability sync for every append in the epoch.
        active
            .file
            .sync_data()
            .map_err(WalError::from)
            .map_err(E::from)?;

        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::EpochSynced)
            .map_err(WalError::from)
            .map_err(E::from)?;
        active.len = next_offset;
        self.physical_bytes = physical_bytes;
        self.stats.epoch_syncs += 1;
        self.stats.record_frames += batches.len() as u64;
        self.stats.bytes += epoch_len;
        self.stats.payload_bytes += batches
            .iter()
            .map(|batch| batch.payload.len() as u64)
            .sum::<u64>();

        let ids = self.segment_records.entry(active.id).or_default();
        for (meta, _, _, _, _) in prepared {
            ids.push(meta.record_id.clone());
            self.record_ids.insert(meta.record_id);
        }
        Ok(locations)
    }

    pub(crate) fn read_many(&self, locations: &[WalLocation]) -> WalResult<Vec<Bytes>> {
        if locations.is_empty() {
            return Ok(Vec::new());
        }

        let mut by_segment: BTreeMap<u64, Vec<(usize, &WalLocation)>> = BTreeMap::new();
        let mut read_stats = WalReadStats {
            calls: 1,
            frames: locations.len() as u64,
            payload_bytes: locations.iter().fold(0_u64, |bytes, location| {
                bytes.saturating_add(location.payload_len)
            }),
            ..WalReadStats::default()
        };
        for (input_index, location) in locations.iter().enumerate() {
            validate_location_bounds(location)?;
            if !self.segment_lifecycle.contains_key(&location.segment_id) {
                return Err(WalError::InvalidLocation(format!(
                    "segment {} is not live",
                    location.segment_id
                )));
            }
            by_segment
                .entry(location.segment_id)
                .or_default()
                .push((input_index, location));
        }

        let mut payloads = vec![None; locations.len()];
        for (segment_id, mut segment_locations) in by_segment {
            segment_locations.sort_by_key(|(_, location)| location.frame_offset);
            let path = segment_path(&self.directory, segment_id);
            let file = File::open(&path)?;
            let file_len = file.metadata()?.len();
            validate_file_header(
                &file,
                &path,
                SEGMENT_MAGIC,
                SEGMENT_FORMAT_VERSION,
                self.shard_id,
                segment_id,
            )?;
            read_stats.segment_opens += 1;
            for (_, location) in &segment_locations {
                let frame_end = location_frame_end(location)?;
                if frame_end > file_len {
                    return Err(WalError::InvalidLocation(
                        "frame extends beyond the segment".into(),
                    ));
                }
            }

            let mut range_begin = 0;
            while range_begin < segment_locations.len() {
                let range_start = segment_locations[range_begin].1.frame_offset;
                let mut range_end = location_frame_end(segment_locations[range_begin].1)?;
                let mut range_limit = range_begin + 1;
                while range_limit < segment_locations.len()
                    && segment_locations[range_limit].1.frame_offset == range_end
                {
                    range_end = location_frame_end(segment_locations[range_limit].1)?;
                    range_limit += 1;
                }

                let range_len = usize::try_from(range_end - range_start).map_err(|_| {
                    WalError::InvalidLocation("WAL read range is too large to address".into())
                })?;
                let mut range = vec![0_u8; range_len];
                read_exact_at(&file, &mut range, range_start)?;
                let range = Bytes::from(range);
                read_stats.ranges = read_stats.ranges.saturating_add(1);
                read_stats.frame_bytes = read_stats.frame_bytes.saturating_add(range.len() as u64);

                for (input_index, location) in &segment_locations[range_begin..range_limit] {
                    payloads[*input_index] = Some(validated_payload_from_range(
                        &path,
                        location,
                        range_start,
                        &range,
                    )?);
                }
                range_begin = range_limit;
            }
        }

        let previous = self.read_stats.get();
        self.read_stats.set(WalReadStats {
            calls: previous.calls.saturating_add(read_stats.calls),
            segment_opens: previous
                .segment_opens
                .saturating_add(read_stats.segment_opens),
            ranges: previous.ranges.saturating_add(read_stats.ranges),
            frames: previous.frames.saturating_add(read_stats.frames),
            frame_bytes: previous.frame_bytes.saturating_add(read_stats.frame_bytes),
            payload_bytes: previous
                .payload_bytes
                .saturating_add(read_stats.payload_bytes),
        });

        payloads
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                WalError::InvalidConfig("validated WAL read did not produce every payload".into())
            })
    }

    fn recover(&mut self) -> WalResult<Vec<RecoveredRecord>> {
        let scan = scan_segments(
            self.shard_id,
            &self.directory,
            &self.segment_lifecycle,
            &mut self.stats,
        )?;
        self.active = scan.active;
        self.segment_records = scan.segment_records;
        self.record_ids = scan.record_ids;
        self.physical_bytes = measure_segment_bytes(&self.directory)?;
        Ok(scan.records)
    }

    pub(crate) fn reclaimable_segments(&self, state: &WalState) -> Vec<u64> {
        let active_segment_id = self.active.as_ref().map(|active| active.id);
        self.segment_records
            .iter()
            .filter(|(segment_id, record_ids)| {
                Some(**segment_id) != active_segment_id
                    && record_ids
                        .iter()
                        .all(|record_id| state.is_released(StreamId::new(self.shard_id), record_id))
            })
            .map(|(segment_id, _)| *segment_id)
            .collect()
    }

    pub(crate) fn remove_segments(&mut self, segment_ids: &[u64]) -> WalResult<ReclaimedSegments> {
        let mut seen = HashSet::with_capacity(segment_ids.len());
        for segment_id in segment_ids {
            if !seen.insert(*segment_id) {
                return Err(WalError::InvalidConfig(format!(
                    "segment removal repeats segment {segment_id}"
                )));
            }
            if self.segment_lifecycle.get(segment_id) != Some(&SegmentLifecycle::Sealed) {
                return Err(WalError::InvalidConfig(format!(
                    "cannot remove unsealed segment {segment_id} from shard {}",
                    self.shard_id
                )));
            }
        }
        let bytes = segment_ids.iter().try_fold(0_u64, |total, segment_id| {
            let path = segment_path(&self.directory, *segment_id);
            total
                .checked_add(fs::metadata(path)?.len())
                .ok_or_else(|| WalError::InvalidConfig("WAL size overflow".into()))
        })?;
        let physical_bytes = self
            .physical_bytes
            .checked_sub(bytes)
            .ok_or_else(|| WalError::InvalidConfig("WAL size underflow".into()))?;

        for segment_id in segment_ids {
            let path = segment_path(&self.directory, *segment_id);
            fs::remove_file(path)?;
            if let Some(ids) = self.segment_records.remove(segment_id) {
                for record_id in ids {
                    self.record_ids.remove(&record_id);
                }
            }
            self.segment_lifecycle.remove(segment_id);
        }
        if !segment_ids.is_empty() {
            #[cfg(test)]
            self._root.fail_if_armed(TestFailPoint::SegmentsDeleted)?;
            sync_directory(&self.directory)?;
            self.stats.directory_syncs += 1;
        }
        self.physical_bytes = physical_bytes;
        let segments = u64::try_from(segment_ids.len())
            .map_err(|_| WalError::InvalidConfig("segment count overflow".into()))?;
        Ok(ReclaimedSegments {
            report: ReclaimReport { segments, bytes },
            segment_ids: segment_ids.to_vec(),
        })
    }

    pub(crate) fn active_is_fully_released(&self, state: &WalState) -> bool {
        self.active
            .as_ref()
            .and_then(|active| self.segment_records.get(&active.id))
            .is_some_and(|record_ids| {
                !record_ids.is_empty()
                    && record_ids
                        .iter()
                        .all(|record_id| state.is_released(StreamId::new(self.shard_id), record_id))
            })
    }

    pub(crate) fn active_is_nonempty(&self) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.len > FILE_HEADER_LEN)
    }

    pub(crate) fn active_age_is_due(&self, now_unix_millis: u64) -> bool {
        self.active_is_nonempty()
            && self.max_segment_age_millis.is_some_and(|max_age| {
                now_unix_millis.saturating_sub(self.active_created_at_unix_millis) >= max_age
            })
    }

    pub(crate) fn next_rotation_ids(&self) -> WalResult<(u64, u64)> {
        let previous_id = self
            .active
            .as_ref()
            .ok_or_else(|| {
                WalError::InvalidConfig(format!("shard {} has no active segment", self.shard_id))
            })?
            .id;
        let next_id = previous_id
            .checked_add(1)
            .ok_or_else(|| WalError::InvalidConfig("segment id exhausted".into()))?;
        Ok((previous_id, next_id))
    }

    pub(crate) fn rotate(&mut self, created_at_unix_millis: u64) -> WalResult<(u64, u64)> {
        let (previous_id, next_id) = self.next_rotation_ids()?;
        let physical_bytes = self
            .physical_bytes
            .checked_add(FILE_HEADER_LEN)
            .ok_or_else(|| WalError::InvalidConfig("WAL size overflow".into()))?;
        let active = create_segment(&self.directory, self.shard_id, next_id, &mut self.stats)?;
        #[cfg(test)]
        self._root.fail_if_armed(TestFailPoint::SegmentCreated)?;
        self.active = Some(active);
        self.active_created_at_unix_millis = created_at_unix_millis;
        self.physical_bytes = physical_bytes;
        if let Some(previous) = self.segment_lifecycle.get_mut(&previous_id) {
            *previous = SegmentLifecycle::Sealed;
        }
        self.segment_lifecycle
            .insert(next_id, SegmentLifecycle::Active);
        self.segment_records.entry(next_id).or_default();
        Ok((previous_id, next_id))
    }

    pub(crate) fn stats(&self) -> WalStats {
        let mut stats = self.stats;
        let reads = self.read_stats.get();
        stats.read_calls = reads.calls;
        stats.read_segment_opens = reads.segment_opens;
        stats.read_ranges = reads.ranges;
        stats.read_frames = reads.frames;
        stats.read_frame_bytes = reads.frame_bytes;
        stats.read_payload_bytes = reads.payload_bytes;
        stats
    }

    pub(crate) fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub(crate) fn storage_bytes(&self) -> u64 {
        self.physical_bytes
    }

    pub(crate) fn live_record_ids(&self) -> &HashSet<String> {
        &self.record_ids
    }
}

impl FileWal {
    /// Opens or creates a storage root, acquires its exclusive lock, repairs
    /// only incomplete active tails, and reconstructs recovery state.
    pub fn open(config: FileWalConfig) -> WalResult<Self> {
        let FileWalConfig {
            root: root_path,
            segment_bytes,
            max_segment_age,
            stream_rollover,
        } = config;
        let default_rollover = RolloverPolicy {
            segment_bytes,
            max_segment_age,
        };
        validate_rollover_policy(DEFAULT_STREAM, default_rollover)?;
        for (stream_id, policy) in &stream_rollover {
            validate_rollover_policy(*stream_id, *policy)?;
        }

        let root = WalRoot::acquire(root_path)?;
        let mut manifest = LocalManifest::open(root.clone())?;
        let now = now_unix_millis();

        let mut stream_ids = discover_stream_directories(root.path())?;
        stream_ids.extend(manifest.state.stream_ids().map(StreamId::get));
        stream_ids.insert(DEFAULT_STREAM.get());
        let mut opening_stats = WalStats::default();
        for shard_id in stream_ids {
            let directory = shard_directory(root.path(), shard_id);
            opening_stats.directory_syncs = opening_stats
                .directory_syncs
                .saturating_add(create_directories_durably(&directory)?);
            let expected = manifest
                .state()
                .segments(shard_id)
                .keys()
                .copied()
                .collect();
            reconcile_segment_directory(&directory, expected, true, &mut opening_stats)?;
        }

        if manifest.state().segments(DEFAULT_STREAM.get()).is_empty() {
            let directory = shard_directory(root.path(), DEFAULT_STREAM.get());
            drop(create_segment(
                &directory,
                DEFAULT_STREAM.get(),
                0,
                &mut opening_stats,
            )?);
            manifest.append_segment_rotations(&[SegmentRotationV1 {
                shard_id: DEFAULT_STREAM.get(),
                previous_segment_id: None,
                new_segment_id: 0,
                created_at_unix_millis: Some(now),
            }])?;
        }

        let declared_streams = manifest.state.stream_ids().collect::<Vec<_>>();
        let mut missing_timestamps = Vec::new();
        for stream_id in &declared_streams {
            if manifest
                .state
                .active_segment_created_at(stream_id.get())?
                .is_none()
            {
                let segment_id =
                    manifest
                        .state
                        .active_segment(stream_id.get())?
                        .ok_or_else(|| {
                            WalError::InvalidConfig(format!(
                                "logical stream {stream_id} has no active segment"
                            ))
                        })?;
                missing_timestamps.push(SegmentTimestampV1 {
                    shard_id: stream_id.get(),
                    segment_id,
                    created_at_unix_millis: now,
                });
            }
        }

        let mut shards = BTreeMap::new();
        let mut records = Vec::new();
        for stream_id in declared_streams {
            let policy = stream_rollover
                .get(&stream_id)
                .copied()
                .unwrap_or(default_rollover);
            let created_at = manifest
                .state
                .active_segment_created_at(stream_id.get())?
                .unwrap_or(now);
            let stats = if stream_id == DEFAULT_STREAM {
                std::mem::take(&mut opening_stats)
            } else {
                WalStats::default()
            };
            let (shard, mut recovered) = ShardSegments::open(
                root.clone(),
                stream_id.get(),
                policy,
                created_at,
                manifest.state().segments(stream_id.get()),
                stats,
            )?;
            records.append(&mut recovered);
            shards.insert(stream_id, shard);
        }
        // Backfill only after every authoritative segment has been validated,
        // so opening a corrupt legacy root does not first mutate its manifest.
        manifest.append_segment_timestamps(&missing_timestamps)?;

        let live_record_ids = all_live_record_ids(shards.values());
        if manifest.needs_compaction(&live_record_ids)? {
            manifest.mark_compaction_pending();
        }
        let recovery = WalRecovery {
            records,
            state: manifest.state.clone(),
        };
        let readiness = StreamReadiness::new(
            recovery
                .pending_records_iter()
                .map(|record| record.stream_id)
                .collect::<BTreeSet<_>>(),
        );
        Ok(Self {
            shards,
            default_rollover,
            stream_rollover,
            manifest,
            recovery,
            readiness,
            poisoned: Cell::new(false),
            root,
        })
    }

    /// Returns the last fully recovered in-memory snapshot. If
    /// [`Self::is_poisoned`] is true, this snapshot is informational only and
    /// the storage root must be reopened before doing more work.
    pub fn recovery(&self) -> &WalRecovery {
        &self.recovery
    }

    /// Returns a cloneable handle for async stream-readiness waits.
    ///
    /// The handle is independent of the `Log` borrow and can be moved into an
    /// async task. Its waits return [`WalError::ReadinessClosed`] after this
    /// `Log` is dropped or poisoned.
    #[must_use]
    pub fn readiness(&self) -> StreamReadiness {
        self.readiness.clone()
    }

    /// Returns a runtime-neutral future that completes when `stream_id` has at
    /// least one complete, unreleased record.
    ///
    /// The returned future owns a readiness handle and does not keep this `Log`
    /// borrowed. It can therefore be awaited by an application task while the
    /// synchronous storage owner continues appending and releasing records.
    pub fn wait_for(&self, stream_id: StreamId) -> WaitForStream {
        self.readiness.wait_for(stream_id)
    }

    /// Whether an I/O, corruption, codec, or internal-state failure made the
    /// outcome of a storage operation uncertain. A poisoned log deliberately
    /// rejects further storage access; drop it and call [`Self::open`] again.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.get()
    }

    /// Appends one record and returns after its epoch is durable.
    pub fn append(&mut self, record: AppendRecord) -> WalResult<WalLocation> {
        self.append_to(DEFAULT_STREAM, record)
    }

    /// Appends one record to a logical stream and returns after its epoch is
    /// durable. A valid first append durably initializes the stream before its
    /// record epoch is written.
    pub fn append_to(
        &mut self,
        stream_id: StreamId,
        record: AppendRecord,
    ) -> WalResult<WalLocation> {
        self.append_batch_to(stream_id, std::slice::from_ref(&record))?
            .pop()
            .ok_or_else(|| WalError::InvalidRecord("append produced no location".into()))
    }

    /// Appends a batch as one durability epoch backed by one `sync_data`.
    /// Recovery returns either the entire batch or none of it.
    pub fn append_batch(&mut self, batches: &[AppendRecord]) -> WalResult<Vec<WalLocation>> {
        self.append_batch_to(DEFAULT_STREAM, batches)
    }

    /// Appends one durability epoch to a logical stream. Record IDs are unique
    /// within the stream rather than across the entire storage root.
    pub fn append_batch_to(
        &mut self,
        stream_id: StreamId,
        batches: &[AppendRecord],
    ) -> WalResult<Vec<WalLocation>> {
        self.append_batch_to_at(stream_id, batches, now_unix_millis())
    }

    fn append_batch_to_at(
        &mut self,
        stream_id: StreamId,
        batches: &[AppendRecord],
        now_unix_millis: u64,
    ) -> WalResult<Vec<WalLocation>> {
        self.ensure_healthy()?;
        validate_append_batch(batches)?;
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        for batch in batches {
            if self
                .shards
                .get(&stream_id)
                .is_some_and(|shard| shard.live_record_ids().contains(&batch.record_id))
                || self.manifest.state.is_released(stream_id, &batch.record_id)
            {
                return Err(WalError::DuplicateRecord(batch.record_id.clone()));
            }
        }

        let result = (|| {
            self.ensure_stream(stream_id, now_unix_millis)?;
            let manifest = &mut self.manifest;
            self.shards
                .get_mut(&stream_id)
                .ok_or(WalError::UnknownStream(stream_id))?
                .append_epoch_with_manifest(
                    batches,
                    now_unix_millis,
                    |shard_id, previous, next, created_at| {
                        manifest
                            .record_segment_rotation(shard_id, previous, next, created_at)
                            .map(|_| ())
                    },
                )
        })();
        let locations = self.finish_operation(result)?;
        for (batch, location) in batches.iter().zip(locations.iter().cloned()) {
            self.recovery.records.push(RecoveredRecord {
                stream_id,
                meta: batch.meta(),
                location,
            });
        }
        self.recovery.state = self.manifest.state.clone();
        self.readiness.update_stream(stream_id, true);
        Ok(locations)
    }

    /// Reads and validates one payload at a location returned by Camus.
    pub fn read(&self, location: &WalLocation) -> WalResult<Bytes> {
        self.read_many(std::slice::from_ref(location))?
            .pop()
            .ok_or_else(|| WalError::InvalidLocation("batch read returned no payload".into()))
    }

    /// Reads record payloads in input order while opening each segment once.
    /// Adjacent record frames are fetched with one positional read, then each
    /// frame is independently validated before its payload is returned.
    pub fn read_many(&self, locations: &[WalLocation]) -> WalResult<Vec<Bytes>> {
        self.ensure_healthy()?;
        let result = (|| {
            let mut by_stream: BTreeMap<StreamId, Vec<(usize, &WalLocation)>> = BTreeMap::new();
            for (index, location) in locations.iter().enumerate() {
                if !self.shards.contains_key(&location.stream_id) {
                    return Err(WalError::InvalidLocation(format!(
                        "logical stream {} is not live",
                        location.stream_id
                    )));
                }
                by_stream
                    .entry(location.stream_id)
                    .or_default()
                    .push((index, location));
            }

            let mut payloads = vec![None; locations.len()];
            for (stream_id, indexed_locations) in by_stream {
                let stream_locations = indexed_locations
                    .iter()
                    .map(|(_, location)| (*location).clone())
                    .collect::<Vec<_>>();
                let stream_payloads = self
                    .shards
                    .get(&stream_id)
                    .ok_or_else(|| {
                        WalError::InvalidLocation(format!("logical stream {stream_id} is not live"))
                    })?
                    .read_many(&stream_locations)?;
                for ((index, _), payload) in indexed_locations.into_iter().zip(stream_payloads) {
                    payloads[index] = Some(payload);
                }
            }
            payloads
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    WalError::InvalidConfig(
                        "validated WAL read did not produce every payload".into(),
                    )
                })
        })();
        self.finish_operation(result)
    }

    /// Rescans the currently open manifest and segment set.
    ///
    /// This is not a recovery path for a poisoned handle because a failed
    /// operation may have changed the manifest/segment relationship. Drop and
    /// reopen a poisoned handle instead.
    pub fn recover(&mut self) -> WalResult<WalRecovery> {
        self.ensure_healthy()?;
        let result = (|| {
            self.manifest.recover()?;
            let mut records = Vec::new();
            for shard in self.shards.values_mut() {
                records.extend(shard.recover()?);
            }
            self.recovery = WalRecovery {
                records,
                state: self.manifest.state.clone(),
            };
            let live_record_ids = all_live_record_ids(self.shards.values());
            if self.manifest.needs_compaction(&live_record_ids)? {
                self.manifest.mark_compaction_pending();
            } else {
                self.manifest.clear_compaction_pending();
            }
            Ok(self.recovery.clone())
        })();
        let recovery = self.finish_operation(result)?;
        self.readiness.refresh(self.pending_stream_ids());
        Ok(recovery)
    }

    /// Durably marks records as no longer needed by the caller. Released
    /// records are excluded from `pending_records` and become reclaimable once
    /// every record in their sealed segment has been released. One call is one
    /// atomic manifest record with a 16 MiB encoded metadata ceiling; split a
    /// request that returns [`WalError::InvalidRecord`].
    pub fn release<I, S>(&mut self, record_ids: I) -> WalResult<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.release_from(DEFAULT_STREAM, record_ids)
    }

    /// Durably releases record IDs in one logical stream. The same record ID
    /// may remain live in another stream.
    pub fn release_from<I, S>(&mut self, stream_id: StreamId, record_ids: I) -> WalResult<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.ensure_healthy()?;
        let shard = self
            .shards
            .get(&stream_id)
            .ok_or(WalError::UnknownStream(stream_id))?;
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        for record_id in record_ids {
            let record_id = record_id.as_ref().to_owned();
            validate_record_id(&record_id)?;
            if !seen.insert(record_id.clone()) {
                return Err(WalError::InvalidRecord(format!(
                    "release repeats record id {record_id}"
                )));
            }
            if !shard.live_record_ids().contains(&record_id)
                && !self.manifest.state.is_released(stream_id, &record_id)
            {
                return Err(WalError::UnknownRecord(record_id));
            }
            ids.push(record_id);
        }
        let result = self.manifest.append_release(stream_id, ids).map(|_| ());
        self.finish_operation(result)?;
        self.recovery.state = self.manifest.state.clone();
        let ready = self
            .recovery
            .pending_records_for_iter(stream_id)
            .next()
            .is_some();
        self.readiness.update_stream(stream_id, ready);
        Ok(())
    }

    /// Returns the last fully published manifest state. A poisoned log must be
    /// reopened before this snapshot is used to make new lifecycle decisions.
    pub fn state(&self) -> &WalState {
        &self.manifest.state
    }

    /// Iterates logical streams currently declared by the manifest.
    pub fn streams(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.shards.keys().copied()
    }

    /// Manually rotates one non-empty logical stream. Returns `false` for an
    /// empty active segment, avoiding chains of empty segment files.
    pub fn rollover(&mut self, stream_id: StreamId) -> WalResult<bool> {
        self.ensure_healthy()?;
        let now = now_unix_millis();
        let result = (|| {
            let shard = self
                .shards
                .get_mut(&stream_id)
                .ok_or(WalError::UnknownStream(stream_id))?;
            if !shard.active_is_nonempty() {
                return Ok(false);
            }
            let (previous, next) = shard.rotate(now)?;
            self.manifest
                .record_segment_rotation(stream_id.get(), previous, next, now)?;
            Ok(true)
        })();
        let rotated = self.finish_operation(result)?;
        self.recovery.state = self.manifest.state.clone();
        Ok(rotated)
    }

    /// Rotates every non-empty stream whose configured age has elapsed.
    ///
    /// Applications call this method from their own scheduler to rotate idle
    /// streams; append operations perform the same age check automatically.
    /// The returned stream IDs were durably rotated by this call.
    pub fn rollover_expired(&mut self) -> WalResult<Vec<StreamId>> {
        self.rollover_expired_at(now_unix_millis())
    }

    fn rollover_expired_at(&mut self, now_unix_millis: u64) -> WalResult<Vec<StreamId>> {
        self.ensure_healthy()?;
        let due = self
            .shards
            .iter()
            .filter_map(|(stream_id, shard)| {
                shard
                    .active_age_is_due(now_unix_millis)
                    .then_some(*stream_id)
            })
            .collect::<Vec<_>>();
        let result = (|| {
            let mut rotations = Vec::with_capacity(due.len());
            for stream_id in &due {
                let shard = self
                    .shards
                    .get_mut(stream_id)
                    .ok_or(WalError::UnknownStream(*stream_id))?;
                let (previous, next) = shard.rotate(now_unix_millis)?;
                rotations.push(SegmentRotationV1 {
                    shard_id: stream_id.get(),
                    previous_segment_id: Some(previous),
                    new_segment_id: next,
                    created_at_unix_millis: Some(now_unix_millis),
                });
            }
            self.manifest.append_segment_rotations(&rotations)?;
            Ok(())
        })();
        self.finish_operation(result)?;
        self.recovery.state = self.manifest.state.clone();
        Ok(due)
    }

    /// Removes every fully released sealed segment and compacts the manifest
    /// when the compacted checkpoint is smaller.
    pub fn reclaim(&mut self) -> WalResult<ReclaimReport> {
        self.reclaim_with_limits(u64::MAX, 0)
    }

    /// Reclaims fully released sealed segments while limiting temporary space
    /// used for manifest compaction.
    ///
    /// `storage_budget_bytes` is the maximum combined segment/manifest size
    /// permitted while creating the temporary checkpoint.
    /// `min_filesystem_free_bytes` reserves filesystem headroom. Eligible
    /// sealed segment deletion does not require extra headroom and still runs
    /// when compaction must be deferred.
    pub fn reclaim_with_limits(
        &mut self,
        storage_budget_bytes: u64,
        min_filesystem_free_bytes: u64,
    ) -> WalResult<ReclaimReport> {
        self.ensure_healthy()?;
        let result = (|| {
            let removals = self
                .shards
                .iter()
                .filter_map(|(stream_id, shard)| {
                    let segment_ids = shard.reclaimable_segments(&self.manifest.state);
                    (!segment_ids.is_empty()).then_some((*stream_id, segment_ids))
                })
                .collect::<Vec<_>>();
            self.manifest.record_segment_removals(
                &removals
                    .iter()
                    .map(|(stream_id, segment_ids)| SegmentRemovalV1 {
                        shard_id: stream_id.get(),
                        segment_ids: segment_ids.clone(),
                    })
                    .collect::<Vec<_>>(),
            )?;

            let mut removed = BTreeSet::new();
            let mut report = ReclaimReport::default();
            for (stream_id, segment_ids) in removals {
                let reclaimed = self
                    .shards
                    .get_mut(&stream_id)
                    .ok_or(WalError::UnknownStream(stream_id))?
                    .remove_segments(&segment_ids)?;
                report.segments = report.segments.saturating_add(reclaimed.report.segments);
                report.bytes = report.bytes.saturating_add(reclaimed.report.bytes);
                removed.extend(
                    reclaimed
                        .segment_ids
                        .into_iter()
                        .map(|segment_id| (stream_id, segment_id)),
                );
            }
            if !removed.is_empty() {
                self.recovery.records.retain(|record| {
                    !removed.contains(&(record.stream_id, record.location.segment_id))
                });
                self.manifest.mark_compaction_pending();
            }
            if self.manifest.compaction_pending() {
                let all_live_record_ids = all_live_record_ids(self.shards.values());
                let budget_headroom =
                    storage_budget_bytes.saturating_sub(self.storage_bytes_unchecked()?);
                let filesystem_headroom = self
                    .root
                    .available_space()?
                    .saturating_sub(min_filesystem_free_bytes);
                let compaction_headroom = budget_headroom.min(filesystem_headroom);
                if let Some(compacted_bytes) = self
                    .manifest
                    .compact_to_live_records(&all_live_record_ids, compaction_headroom)?
                {
                    report.bytes = report.bytes.saturating_add(compacted_bytes);
                    self.manifest.clear_compaction_pending();
                }
            }
            self.recovery.state = self.manifest.state.clone();
            Ok(report)
        })();
        self.finish_operation(result)
    }

    /// Rotates a fully released active segment, then runs ordinary reclamation.
    pub fn reclaim_active_for_storage_pressure(&mut self) -> WalResult<ReclaimReport> {
        self.reclaim_active_for_storage_pressure_with_limits(u64::MAX, 0)
    }

    /// Rotates and reclaims a fully released active segment only when a new
    /// segment header fits both the storage budget and free-space reserve.
    pub fn reclaim_active_for_storage_pressure_with_limits(
        &mut self,
        storage_budget_bytes: u64,
        min_filesystem_free_bytes: u64,
    ) -> WalResult<ReclaimReport> {
        self.ensure_healthy()?;
        let now = now_unix_millis();
        let result = (|| {
            let mut filesystem_headroom = self
                .root
                .available_space()?
                .saturating_sub(min_filesystem_free_bytes);
            let mut storage_bytes = self.storage_bytes_unchecked()?;
            let candidates = self
                .shards
                .iter()
                .filter_map(|(stream_id, shard)| {
                    shard
                        .active_is_fully_released(&self.manifest.state)
                        .then_some(*stream_id)
                })
                .collect::<Vec<_>>();
            for stream_id in candidates {
                let (previous, next) = self
                    .shards
                    .get(&stream_id)
                    .ok_or(WalError::UnknownStream(stream_id))?
                    .next_rotation_ids()?;
                let rotation_metadata = serde_json::to_vec(&SegmentRotationV1 {
                    shard_id: stream_id.get(),
                    previous_segment_id: Some(previous),
                    new_segment_id: next,
                    created_at_unix_millis: Some(now),
                })?;
                let required_bytes = FILE_HEADER_LEN
                    .checked_add(manifest_record_len(rotation_metadata.len() as u64)?)
                    .ok_or_else(|| WalError::InvalidConfig("WAL size overflow".into()))?;
                let has_storage_headroom = storage_bytes
                    .checked_add(required_bytes)
                    .is_some_and(|bytes| bytes <= storage_budget_bytes);
                if filesystem_headroom < required_bytes || !has_storage_headroom {
                    continue;
                }
                let shard = self
                    .shards
                    .get_mut(&stream_id)
                    .ok_or(WalError::UnknownStream(stream_id))?;
                let rotated = shard.rotate(now)?;
                if rotated != (previous, next) {
                    return Err(WalError::InvalidConfig(
                        "stream rotation IDs changed during storage preflight".into(),
                    ));
                }
                self.manifest
                    .record_segment_rotation(stream_id.get(), previous, next, now)?;
                filesystem_headroom = filesystem_headroom.saturating_sub(required_bytes);
                storage_bytes = storage_bytes.saturating_add(required_bytes);
            }
            Ok(())
        })();
        self.finish_operation(result)?;
        self.recovery.state = self.manifest.state.clone();
        self.reclaim_with_limits(storage_budget_bytes, min_filesystem_free_bytes)
    }

    /// Returns current manifest plus canonical segment file bytes.
    pub fn storage_bytes(&self) -> WalResult<u64> {
        self.ensure_healthy()?;
        let result = self.storage_bytes_unchecked();
        self.finish_operation(result)
    }

    /// Returns cumulative counters for the lifetime of this open handle.
    pub fn stats(&self) -> WalStats {
        let mut stats = self.root.stats();
        for shard in self.shards.values() {
            stats.accumulate(shard.stats());
        }
        stats.accumulate(self.manifest.stats());
        stats
    }

    fn policy_for(&self, stream_id: StreamId) -> RolloverPolicy {
        self.stream_rollover
            .get(&stream_id)
            .copied()
            .unwrap_or(self.default_rollover)
    }

    fn ensure_stream(&mut self, stream_id: StreamId, created_at_unix_millis: u64) -> WalResult<()> {
        if self.shards.contains_key(&stream_id) {
            return Ok(());
        }
        let policy = self.policy_for(stream_id);
        let directory = shard_directory(self.root.path(), stream_id.get());
        let mut stats = WalStats {
            directory_syncs: create_directories_durably(&directory)?,
            ..WalStats::default()
        };
        reconcile_segment_directory(&directory, BTreeSet::new(), true, &mut stats)?;
        drop(create_segment(&directory, stream_id.get(), 0, &mut stats)?);
        #[cfg(test)]
        self.root.fail_if_armed(TestFailPoint::SegmentCreated)?;
        self.manifest
            .append_segment_rotations(&[SegmentRotationV1 {
                shard_id: stream_id.get(),
                previous_segment_id: None,
                new_segment_id: 0,
                created_at_unix_millis: Some(created_at_unix_millis),
            }])?;
        let (shard, recovered) = ShardSegments::open(
            self.root.clone(),
            stream_id.get(),
            policy,
            created_at_unix_millis,
            self.manifest.state().segments(stream_id.get()),
            stats,
        )?;
        if !recovered.is_empty() {
            return Err(WalError::InvalidConfig(format!(
                "new logical stream {stream_id} recovered unexpected records"
            )));
        }
        self.shards.insert(stream_id, shard);
        self.recovery.state = self.manifest.state.clone();
        Ok(())
    }

    fn storage_bytes_unchecked(&self) -> WalResult<u64> {
        self.shards
            .values()
            .try_fold(self.manifest.storage_bytes(), |bytes, shard| {
                bytes
                    .checked_add(shard.storage_bytes())
                    .ok_or_else(|| WalError::InvalidConfig("WAL size overflow".into()))
            })
    }

    fn pending_stream_ids(&self) -> BTreeSet<StreamId> {
        self.recovery
            .pending_records_iter()
            .map(|record| record.stream_id)
            .collect()
    }

    fn ensure_healthy(&self) -> WalResult<()> {
        if self.poisoned.get() {
            Err(WalError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn finish_operation<T>(&self, result: WalResult<T>) -> WalResult<T> {
        if result
            .as_ref()
            .is_err_and(|error| error.invalidates_open_log())
        {
            self.poisoned.set(true);
            self.readiness.close();
        }
        result
    }
}

impl Drop for FileWal {
    fn drop(&mut self) {
        self.readiness.close();
    }
}

/// Owns the storage root's authoritative lifecycle state.
pub(crate) struct LocalManifest {
    _root: WalRoot,
    path: PathBuf,
    file: File,
    len: u64,
    state: WalState,
    compaction_pending: bool,
    stats: WalStats,
}

impl LocalManifest {
    fn open(root: WalRoot) -> WalResult<Self> {
        let path = root.path().join("MANIFEST");
        let creating = root.path().join("MANIFEST.create");
        let mut stats = WalStats::default();
        if !path.exists() {
            match fs::remove_file(&creating) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let mut file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .mode(0o600)
                .open(&creating)?;
            file.write_all(&file_header(MANIFEST_MAGIC, MANIFEST_FORMAT_VERSION, 0, 0))?;
            file.sync_data()?;
            stats.manifest_header_syncs += 1;
            fs::rename(&creating, &path)?;
            sync_directory(root.path())?;
            stats.directory_syncs += 1;
        } else {
            match fs::remove_file(&creating) {
                Ok(()) => {
                    sync_directory(root.path())?;
                    stats.directory_syncs += 1;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut manifest = Self {
            _root: root,
            path,
            file,
            len: 0,
            state: WalState::default(),
            compaction_pending: false,
            stats,
        };
        manifest.recover()?;
        manifest.remove_stale_compaction()?;
        Ok(manifest)
    }

    fn remove_stale_compaction(&mut self) -> WalResult<()> {
        let temporary = self.path.with_extension("compact");
        match fs::remove_file(temporary) {
            Ok(()) => {
                let parent = self.path.parent().ok_or_else(|| {
                    WalError::InvalidConfig("manifest has no parent directory".into())
                })?;
                sync_directory(parent)?;
                self.stats.directory_syncs += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn recover(&mut self) -> WalResult<()> {
        self.state = WalState::default();
        self.len = self.file.metadata()?.len();
        let header = read_file_header(
            &self.file,
            &self.path,
            MANIFEST_MAGIC,
            MANIFEST_FORMAT_VERSION,
        )?;
        let checkpoint_records = if header.owner == 0 {
            if header.sequence != 0 {
                return Err(corruption(&self.path, 0, "invalid manifest log header"));
            }
            None
        } else {
            Some(header.owner - 1)
        };
        let mut checkpoint = WalState::default();
        let mut checkpoint_seen = 0_u32;
        let mut checkpoint_descriptors = Xxh3::new();
        if checkpoint_records == Some(0) && checkpoint_descriptors.digest() != header.sequence {
            return Err(corruption(
                &self.path,
                FILE_HEADER_LEN,
                "manifest checkpoint descriptor checksum mismatch",
            ));
        }

        let mut offset = FILE_HEADER_LEN;
        while offset < self.len {
            let checkpoint_incomplete =
                checkpoint_records.is_some_and(|records| checkpoint_seen < records);
            let remaining = self.len - offset;
            if remaining < MANIFEST_RECORD_PREFIX_LEN {
                if checkpoint_incomplete {
                    return Err(corruption(
                        &self.path,
                        offset,
                        "incomplete manifest checkpoint",
                    ));
                }
                self.repair_tail(offset)?;
                break;
            }
            let mut prefix = [0_u8; MANIFEST_RECORD_PREFIX_LEN as usize];
            read_exact_at(&self.file, &mut prefix, offset)?;
            let parsed = match parse_manifest_record_prefix(&prefix) {
                Ok(parsed) => parsed,
                Err(message) => {
                    self.repair_or_fail(!checkpoint_incomplete, offset, &prefix, message)?;
                    break;
                }
            };
            if parsed.metadata_len > MAX_METADATA_LEN {
                self.repair_or_fail(
                    !checkpoint_incomplete,
                    offset,
                    &prefix,
                    "invalid manifest record lengths",
                )?;
                break;
            }
            let end = match offset.checked_add(parsed.frame_len) {
                Some(end) => end,
                None => {
                    self.repair_or_fail(
                        !checkpoint_incomplete,
                        offset,
                        &prefix,
                        "manifest record length overflow",
                    )?;
                    break;
                }
            };
            if end > self.len {
                self.repair_or_fail(
                    !checkpoint_incomplete,
                    offset,
                    &prefix,
                    "incomplete manifest record",
                )?;
                break;
            }
            let mut metadata = vec![0_u8; parsed.metadata_len as usize];
            read_exact_at(
                &self.file,
                &mut metadata,
                offset + MANIFEST_RECORD_PREFIX_LEN,
            )?;
            let mut checksum = Xxh3::new();
            checksum.update(&prefix[..24]);
            checksum.update(&metadata);
            if checksum.digest() != parsed.metadata_checksum {
                if checkpoint_incomplete {
                    return Err(corruption(
                        &self.path,
                        offset,
                        "manifest checkpoint checksum mismatch",
                    ));
                }
                if end == self.len
                    && !has_valid_manifest_record_after(&self.file, offset, self.len)?
                {
                    self.repair_tail(offset)?;
                    break;
                }
                return Err(corruption(
                    &self.path,
                    offset,
                    "checksum mismatch before manifest tail",
                ));
            }
            if checkpoint_incomplete
                && !matches!(
                    parsed.kind,
                    RELEASE_KIND | STREAM_RELEASE_KIND | SEGMENT_SNAPSHOT_KIND
                )
            {
                return Err(corruption(
                    &self.path,
                    offset,
                    "manifest checkpoint contains an event record",
                ));
            }
            if !checkpoint_incomplete && parsed.kind == SEGMENT_SNAPSHOT_KIND {
                return Err(corruption(
                    &self.path,
                    offset,
                    "segment snapshot appears outside a manifest checkpoint",
                ));
            }
            let state = if checkpoint_incomplete {
                &mut checkpoint
            } else {
                &mut self.state
            };
            Self::apply_record(state, &self.path, parsed.kind, &metadata, offset)?;
            if checkpoint_incomplete {
                checkpoint_descriptors.update(&prefix);
                checkpoint_seen += 1;
                if checkpoint_records == Some(checkpoint_seen) {
                    if checkpoint_descriptors.digest() != header.sequence {
                        return Err(corruption(
                            &self.path,
                            offset,
                            "manifest checkpoint descriptor checksum mismatch",
                        ));
                    }
                    checkpoint
                        .validate_segments()
                        .map_err(|error| corruption(&self.path, offset, error.to_string()))?;
                    self.state = std::mem::take(&mut checkpoint);
                }
            }
            offset = end;
        }
        if checkpoint_records.is_some_and(|records| checkpoint_seen < records) {
            return Err(corruption(
                &self.path,
                offset,
                "incomplete manifest checkpoint",
            ));
        }
        self.len = offset;
        self.state
            .validate_segments()
            .map_err(|error| corruption(&self.path, offset, error.to_string()))?;
        self.file.seek(SeekFrom::Start(self.len))?;
        Ok(())
    }

    fn append_release(
        &mut self,
        stream_id: StreamId,
        record_ids: Vec<String>,
    ) -> WalResult<ManifestWrite> {
        let added = record_ids
            .iter()
            .filter(|record_id| !self.state.is_released(stream_id, record_id))
            .cloned()
            .collect::<Vec<_>>();
        if added.is_empty() {
            return Ok(ManifestWrite::default());
        }

        let (kind, metadata) = if stream_id == DEFAULT_STREAM {
            (
                RELEASE_KIND,
                serde_json::to_vec(&ReleaseV1 {
                    record_ids: added.clone(),
                })?,
            )
        } else {
            (
                STREAM_RELEASE_KIND,
                serde_json::to_vec(&StreamReleaseV1 {
                    stream_id: stream_id.get(),
                    record_ids: added.clone(),
                })?,
            )
        };
        validate_release_metadata_len(metadata.len())?;
        let mut encoded = Vec::new();
        encode_manifest_record(&mut encoded, kind, &metadata)?;
        let write = ManifestWrite {
            added_records: 1,
            added_bytes: encoded.len() as u64,
        };
        let new_len = self
            .len
            .checked_add(write.added_bytes)
            .ok_or_else(|| WalError::InvalidConfig("manifest length overflow".into()))?;
        self.file.write_all(&encoded)?;
        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::ReleaseManifestWritten)?;
        self.file.sync_data()?;
        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::ReleaseManifestSynced)?;
        self.len = new_len;
        if stream_id == DEFAULT_STREAM {
            self.state.released_record_ids.extend(added);
        } else {
            self.state
                .stream_released_record_ids
                .entry(stream_id.get())
                .or_default()
                .extend(added);
        }
        self.stats.manifest_syncs += 1;
        Ok(write)
    }

    fn append_segment_rotations(
        &mut self,
        rotations: &[SegmentRotationV1],
    ) -> WalResult<ManifestWrite> {
        if rotations.is_empty() {
            return Ok(ManifestWrite::default());
        }
        let mut state = self.state.clone();
        let mut encoded = Vec::new();
        for rotation in rotations {
            state.apply_rotation(rotation)?;
            encode_manifest_record(
                &mut encoded,
                SEGMENT_ROTATION_KIND,
                &serde_json::to_vec(rotation)?,
            )?;
        }
        let write = ManifestWrite {
            added_records: rotations.len(),
            added_bytes: encoded.len() as u64,
        };
        let new_len = self
            .len
            .checked_add(write.added_bytes)
            .ok_or_else(|| WalError::InvalidConfig("manifest length overflow".into()))?;
        self.file.write_all(&encoded)?;
        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::RotationManifestWritten)?;
        self.file.sync_data()?;
        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::RotationManifestSynced)?;
        self.len = new_len;
        self.state = state;
        self.stats.manifest_syncs += 1;
        Ok(write)
    }

    pub(crate) fn record_segment_rotation(
        &mut self,
        shard_id: u32,
        previous_segment_id: u64,
        new_segment_id: u64,
        created_at_unix_millis: u64,
    ) -> WalResult<ManifestWrite> {
        self.append_segment_rotations(&[SegmentRotationV1 {
            shard_id,
            previous_segment_id: Some(previous_segment_id),
            new_segment_id,
            created_at_unix_millis: Some(created_at_unix_millis),
        }])
    }

    fn append_segment_timestamps(
        &mut self,
        timestamps: &[SegmentTimestampV1],
    ) -> WalResult<ManifestWrite> {
        if timestamps.is_empty() {
            return Ok(ManifestWrite::default());
        }
        let mut state = self.state.clone();
        let mut encoded = Vec::new();
        for timestamp in timestamps {
            state.apply_timestamp(timestamp)?;
            encode_manifest_record(
                &mut encoded,
                SEGMENT_TIMESTAMP_KIND,
                &serde_json::to_vec(timestamp)?,
            )?;
        }
        let write = ManifestWrite {
            added_records: timestamps.len(),
            added_bytes: encoded.len() as u64,
        };
        let new_len = self
            .len
            .checked_add(write.added_bytes)
            .ok_or_else(|| WalError::InvalidConfig("manifest length overflow".into()))?;
        self.file.write_all(&encoded)?;
        self.file.sync_data()?;
        self.len = new_len;
        self.state = state;
        self.stats.manifest_syncs += 1;
        Ok(write)
    }

    fn append_segment_removals(
        &mut self,
        removals: &[SegmentRemovalV1],
    ) -> WalResult<ManifestWrite> {
        if removals.is_empty() {
            return Ok(ManifestWrite::default());
        }
        let mut state = self.state.clone();
        let mut encoded = Vec::new();
        let mut added_records = 0_usize;
        for removal in removals {
            state.apply_removal(removal)?;
            let (removal_bytes, removal_records) = encode_segment_removal_records(removal)?;
            encoded.extend_from_slice(&removal_bytes);
            added_records = added_records
                .checked_add(removal_records)
                .ok_or_else(|| WalError::InvalidConfig("segment removal is too large".into()))?;
        }
        let write = ManifestWrite {
            added_records,
            added_bytes: encoded.len() as u64,
        };
        let new_len = self
            .len
            .checked_add(write.added_bytes)
            .ok_or_else(|| WalError::InvalidConfig("manifest length overflow".into()))?;
        self.file.write_all(&encoded)?;
        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::RemovalManifestWritten)?;
        self.file.sync_data()?;
        #[cfg(test)]
        self._root
            .fail_if_armed(TestFailPoint::RemovalManifestSynced)?;
        self.len = new_len;
        self.state = state;
        self.stats.manifest_syncs += 1;
        Ok(write)
    }

    fn record_segment_removals(
        &mut self,
        removals: &[SegmentRemovalV1],
    ) -> WalResult<ManifestWrite> {
        self.append_segment_removals(removals)
    }

    pub(crate) fn compact_to_live_records(
        &mut self,
        all_live_record_ids: &LiveRecordIds,
        max_temporary_bytes: u64,
    ) -> WalResult<Option<u64>> {
        let checkpoint = self.checkpoint(all_live_record_ids);
        let compacted_len = manifest_checkpoint_len(&checkpoint)?;
        if compacted_len >= self.len {
            return Ok(Some(0));
        }
        if compacted_len > max_temporary_bytes {
            return Ok(None);
        }

        let temporary = self.path.with_extension("compact");
        match fs::remove_file(&temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        let (checkpoint_records, checkpoint_checksum) =
            manifest_checkpoint_descriptor(&checkpoint)?;
        let checkpoint_header = checkpoint_records
            .checked_add(1)
            .ok_or_else(|| WalError::InvalidConfig("manifest checkpoint is too large".into()))?;
        file.write_all(&file_header(
            MANIFEST_MAGIC,
            MANIFEST_FORMAT_VERSION,
            checkpoint_header,
            checkpoint_checksum,
        ))?;
        let mut len = FILE_HEADER_LEN;
        visit_manifest_checkpoint(&checkpoint, |kind, metadata| {
            len = write_manifest_record(&mut file, kind, metadata, len)?;
            Ok(())
        })?;
        file.sync_data()?;
        self.stats.manifest_header_syncs += 1;
        self.stats.manifest_syncs += 1;
        #[cfg(test)]
        self._root.fail_if_armed(TestFailPoint::CompactionSynced)?;
        fs::rename(&temporary, &self.path)?;
        #[cfg(test)]
        self._root.fail_if_armed(TestFailPoint::CompactionRenamed)?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| WalError::InvalidConfig("manifest has no parent directory".into()))?;
        sync_directory(parent)?;
        self.stats.directory_syncs += 1;

        let old_len = self.len;
        file.seek(SeekFrom::Start(len))?;
        self.file = file;
        self.len = len;
        self.state = checkpoint;
        Ok(Some(old_len.saturating_sub(len)))
    }

    fn checkpoint(&self, all_live_record_ids: &LiveRecordIds) -> WalState {
        let released_record_ids = self
            .state
            .released_record_ids
            .iter()
            .filter(|record_id| {
                all_live_record_ids
                    .get(&DEFAULT_STREAM.get())
                    .is_some_and(|live| live.contains(*record_id))
            })
            .cloned()
            .collect();
        let stream_released_record_ids = self
            .state
            .stream_released_record_ids
            .iter()
            .filter_map(|(stream_id, released)| {
                let live = all_live_record_ids.get(stream_id)?;
                let retained = released
                    .iter()
                    .filter(|record_id| live.contains(*record_id))
                    .cloned()
                    .collect::<BTreeSet<_>>();
                (!retained.is_empty()).then_some((*stream_id, retained))
            })
            .collect();
        WalState {
            released_record_ids,
            stream_released_record_ids,
            segments: self.state.segments.clone(),
            segment_created_at_unix_millis: self.state.segment_created_at_unix_millis.clone(),
        }
    }

    fn needs_compaction(&self, all_live_record_ids: &LiveRecordIds) -> WalResult<bool> {
        Ok(manifest_checkpoint_len(&self.checkpoint(all_live_record_ids))? < self.len)
    }

    fn apply_record(
        state: &mut WalState,
        path: &Path,
        kind: u8,
        metadata: &[u8],
        offset: u64,
    ) -> WalResult<()> {
        match kind {
            RELEASE_KIND => {
                let release: ReleaseV1 = serde_json::from_slice(metadata).map_err(|error| {
                    corruption(path, offset, format!("invalid release record: {error}"))
                })?;
                if !state.segments.contains_key(&DEFAULT_STREAM.get()) {
                    return Err(corruption(
                        path,
                        offset,
                        "release record precedes default-stream initialization",
                    ));
                }
                validate_manifest_release_ids(path, offset, &release.record_ids)?;
                for record_id in release.record_ids {
                    state.released_record_ids.insert(record_id);
                }
            }
            STREAM_RELEASE_KIND => {
                let release: StreamReleaseV1 =
                    serde_json::from_slice(metadata).map_err(|error| {
                        corruption(
                            path,
                            offset,
                            format!("invalid stream release record: {error}"),
                        )
                    })?;
                if release.stream_id == DEFAULT_STREAM.get() {
                    return Err(corruption(
                        path,
                        offset,
                        "extended release record uses the default stream",
                    ));
                }
                if !state.segments.contains_key(&release.stream_id) {
                    return Err(corruption(
                        path,
                        offset,
                        format!(
                            "release record precedes logical stream {} initialization",
                            release.stream_id
                        ),
                    ));
                }
                validate_manifest_release_ids(path, offset, &release.record_ids)?;
                state
                    .stream_released_record_ids
                    .entry(release.stream_id)
                    .or_default()
                    .extend(release.record_ids);
            }
            SEGMENT_ROTATION_KIND => {
                let rotation: SegmentRotationV1 =
                    serde_json::from_slice(metadata).map_err(|error| {
                        corruption(path, offset, format!("invalid segment rotation: {error}"))
                    })?;
                state
                    .apply_rotation(&rotation)
                    .map_err(|error| corruption(path, offset, error.to_string()))?;
            }
            SEGMENT_REMOVAL_KIND => {
                let removal: SegmentRemovalV1 =
                    serde_json::from_slice(metadata).map_err(|error| {
                        corruption(path, offset, format!("invalid segment removal: {error}"))
                    })?;
                state
                    .apply_removal(&removal)
                    .map_err(|error| corruption(path, offset, error.to_string()))?;
            }
            SEGMENT_SNAPSHOT_KIND => {
                let snapshot: SegmentSnapshotV1 =
                    serde_json::from_slice(metadata).map_err(|error| {
                        corruption(path, offset, format!("invalid segment snapshot: {error}"))
                    })?;
                state
                    .apply_snapshot(&snapshot)
                    .map_err(|error| corruption(path, offset, error.to_string()))?;
            }
            SEGMENT_TIMESTAMP_KIND => {
                let timestamp: SegmentTimestampV1 =
                    serde_json::from_slice(metadata).map_err(|error| {
                        corruption(path, offset, format!("invalid segment timestamp: {error}"))
                    })?;
                state
                    .apply_timestamp(&timestamp)
                    .map_err(|error| corruption(path, offset, error.to_string()))?;
            }
            _ => {
                return Err(corruption(
                    path,
                    offset,
                    format!("unknown manifest record kind {kind}"),
                ));
            }
        }
        Ok(())
    }

    fn repair_or_fail(
        &mut self,
        repairable: bool,
        offset: u64,
        prefix: &[u8; MANIFEST_RECORD_PREFIX_LEN as usize],
        message: impl Into<String>,
    ) -> WalResult<()> {
        if repairable
            && manifest_record_can_be_final_frame(prefix, offset, self.len)
            && !has_valid_manifest_record_after(&self.file, offset, self.len)?
        {
            self.repair_tail(offset)
        } else {
            Err(corruption(&self.path, offset, message))
        }
    }

    fn repair_tail(&mut self, offset: u64) -> WalResult<()> {
        self.file.set_len(offset)?;
        self.file.sync_data()?;
        self.len = offset;
        self.stats.repaired_tails += 1;
        Ok(())
    }

    pub(crate) fn state(&self) -> &WalState {
        &self.state
    }

    pub(crate) fn storage_bytes(&self) -> u64 {
        self.len
    }

    pub(crate) fn stats(&self) -> WalStats {
        self.stats
    }

    pub(crate) fn mark_compaction_pending(&mut self) {
        self.compaction_pending = true;
    }

    pub(crate) fn clear_compaction_pending(&mut self) {
        self.compaction_pending = false;
    }

    pub(crate) fn compaction_pending(&self) -> bool {
        self.compaction_pending
    }
}

fn manifest_checkpoint_len(checkpoint: &WalState) -> WalResult<u64> {
    let mut length = FILE_HEADER_LEN;
    visit_manifest_checkpoint(checkpoint, |_, metadata| {
        length = length
            .checked_add(manifest_record_len(metadata.len() as u64)?)
            .ok_or_else(|| WalError::InvalidConfig("manifest length overflow".into()))?;
        Ok(())
    })?;
    Ok(length)
}

fn manifest_checkpoint_descriptor(checkpoint: &WalState) -> WalResult<(u32, u64)> {
    let mut records = 0_u32;
    let mut descriptors = Xxh3::new();
    visit_manifest_checkpoint(checkpoint, |kind, metadata| {
        records = records
            .checked_add(1)
            .ok_or_else(|| WalError::InvalidConfig("manifest checkpoint is too large".into()))?;
        descriptors.update(&manifest_record_prefix(kind, metadata)?);
        Ok(())
    })?;
    Ok((records, descriptors.digest()))
}

fn visit_manifest_checkpoint(
    checkpoint: &WalState,
    mut visit: impl FnMut(u8, &[u8]) -> WalResult<()>,
) -> WalResult<()> {
    for (shard_id, segments) in &checkpoint.segments {
        for (segment_id, lifecycle) in segments {
            let metadata = serde_json::to_vec(&SegmentSnapshotV1 {
                shard_id: *shard_id,
                segment_id: *segment_id,
                lifecycle: *lifecycle,
                created_at_unix_millis: checkpoint
                    .segment_created_at_unix_millis
                    .get(shard_id)
                    .and_then(|timestamps| timestamps.get(segment_id))
                    .copied(),
            })?;
            visit(SEGMENT_SNAPSHOT_KIND, &metadata)?;
        }
    }
    for record_id in &checkpoint.released_record_ids {
        visit(
            RELEASE_KIND,
            &serde_json::to_vec(&ReleaseV1 {
                record_ids: vec![record_id.clone()],
            })?,
        )?;
    }
    for (stream_id, record_ids) in &checkpoint.stream_released_record_ids {
        for record_id in record_ids {
            visit(
                STREAM_RELEASE_KIND,
                &serde_json::to_vec(&StreamReleaseV1 {
                    stream_id: *stream_id,
                    record_ids: vec![record_id.clone()],
                })?,
            )?;
        }
    }
    Ok(())
}

fn encode_manifest_record(output: &mut Vec<u8>, kind: u8, metadata: &[u8]) -> WalResult<()> {
    if metadata.len() as u64 > MAX_METADATA_LEN {
        return Err(WalError::InvalidConfig(format!(
            "manifest record metadata exceeds {MAX_METADATA_LEN} bytes"
        )));
    }
    let prefix = manifest_record_prefix(kind, metadata)?;
    output.extend_from_slice(&prefix);
    output.extend_from_slice(metadata);
    Ok(())
}

fn encode_segment_removal_records(removal: &SegmentRemovalV1) -> WalResult<(Vec<u8>, usize)> {
    let mut encoded = Vec::new();
    let mut records = 0_usize;
    for segment_ids in removal
        .segment_ids
        .chunks(MAX_SEGMENT_IDS_PER_REMOVAL_RECORD)
    {
        let metadata = serde_json::to_vec(&SegmentRemovalV1 {
            shard_id: removal.shard_id,
            segment_ids: segment_ids.to_vec(),
        })?;
        encode_manifest_record(&mut encoded, SEGMENT_REMOVAL_KIND, &metadata)?;
        records = records
            .checked_add(1)
            .ok_or_else(|| WalError::InvalidConfig("segment removal is too large".into()))?;
    }
    Ok((encoded, records))
}

fn write_manifest_record(
    file: &mut File,
    kind: u8,
    metadata: &[u8],
    offset: u64,
) -> WalResult<u64> {
    let mut encoded = Vec::new();
    encode_manifest_record(&mut encoded, kind, metadata)?;
    file.write_all(&encoded)?;
    offset
        .checked_add(encoded.len() as u64)
        .ok_or_else(|| WalError::InvalidConfig("manifest length overflow".into()))
}

fn scan_segments(
    shard_id: u32,
    segment_dir: &Path,
    segments: &BTreeMap<u64, SegmentLifecycle>,
    stats: &mut WalStats,
) -> WalResult<SegmentScan> {
    let active_id = segments
        .iter()
        .find_map(|(segment_id, lifecycle)| {
            (*lifecycle == SegmentLifecycle::Active).then_some(*segment_id)
        })
        .ok_or_else(|| {
            WalError::InvalidConfig(format!(
                "active shard {shard_id} has no active manifest segment"
            ))
        })?;

    let mut records = Vec::new();
    let mut segment_records = BTreeMap::new();
    let mut record_ids = HashSet::new();
    for (segment_id, lifecycle) in segments {
        let path = segment_path(segment_dir, *segment_id);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        validate_file_header(
            &file,
            &path,
            SEGMENT_MAGIC,
            SEGMENT_FORMAT_VERSION,
            shard_id,
            *segment_id,
        )?;
        let recovered = scan_segment(
            &file,
            &path,
            StreamId::new(shard_id),
            *segment_id,
            *lifecycle == SegmentLifecycle::Active,
            stats,
        )?;
        let mut ids_in_segment = Vec::with_capacity(recovered.len());
        for append in recovered {
            if !record_ids.insert(append.meta.record_id.clone()) {
                return Err(corruption(
                    &path,
                    append.location.frame_offset,
                    format!("duplicate record id {}", append.meta.record_id),
                ));
            }
            ids_in_segment.push(append.meta.record_id.clone());
            records.push(append);
        }
        segment_records.insert(*segment_id, ids_in_segment);
    }

    let active_path = segment_path(segment_dir, active_id);
    let mut active_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(active_path)?;
    let active_len = active_file.metadata()?.len();
    active_file.seek(SeekFrom::Start(active_len))?;
    let active = Some(SegmentWriter {
        id: active_id,
        file: active_file,
        len: active_len,
    });
    Ok(SegmentScan {
        active,
        records,
        segment_records,
        record_ids,
    })
}

fn scan_segment(
    file: &File,
    path: &Path,
    stream_id: StreamId,
    segment_id: u64,
    is_tail_segment: bool,
    stats: &mut WalStats,
) -> WalResult<Vec<RecoveredRecord>> {
    let mut records = Vec::new();
    let mut epoch_appends = Vec::new();
    let mut descriptors = Xxh3::new();
    let file_len = file.metadata()?.len();
    let mut offset = FILE_HEADER_LEN;
    let mut epoch_start = FILE_HEADER_LEN;
    while offset < file_len {
        if file_len - offset < SEGMENT_RECORD_PREFIX_LEN {
            repair_segment_tail(file, path, epoch_start, is_tail_segment, stats)?;
            break;
        }
        let mut prefix = [0_u8; SEGMENT_RECORD_PREFIX_LEN as usize];
        read_exact_at(file, &mut prefix, offset)?;
        let parsed = match parse_segment_record_prefix(&prefix) {
            Ok(parsed) => parsed,
            Err(message) => {
                repair_segment_epoch(
                    file,
                    path,
                    epoch_start,
                    offset,
                    file_len,
                    is_tail_segment,
                    message,
                    stats,
                )?;
                break;
            }
        };
        if parsed.metadata_len > MAX_METADATA_LEN {
            repair_segment_epoch(
                file,
                path,
                epoch_start,
                offset,
                file_len,
                is_tail_segment,
                "append metadata is unreasonably large",
                stats,
            )?;
            break;
        }
        let Some(end) = offset.checked_add(parsed.frame_len) else {
            repair_segment_epoch(
                file,
                path,
                epoch_start,
                offset,
                file_len,
                is_tail_segment,
                "append record length overflow",
                stats,
            )?;
            break;
        };
        if end > file_len {
            repair_segment_epoch(
                file,
                path,
                epoch_start,
                offset,
                file_len,
                is_tail_segment,
                "incomplete record frame",
                stats,
            )?;
            break;
        }

        let mut metadata = vec![0_u8; parsed.metadata_len as usize];
        read_exact_at(file, &mut metadata, offset + SEGMENT_RECORD_PREFIX_LEN)?;
        let mut checksum = Xxh3::new();
        checksum.update(&prefix[..40]);
        checksum.update(&metadata);
        if checksum.digest() != parsed.metadata_checksum {
            repair_segment_epoch(
                file,
                path,
                epoch_start,
                offset,
                file_len,
                is_tail_segment,
                "append metadata checksum mismatch",
                stats,
            )?;
            break;
        }

        match parsed.kind {
            RECORD_KIND => {
                let meta = decode_record_meta(&metadata).map_err(|error| {
                    corruption(path, offset, format!("invalid record metadata: {error}"))
                })?;
                let payload_offset = offset + SEGMENT_RECORD_PREFIX_LEN + parsed.metadata_len;
                epoch_appends.push(RecoveredRecord {
                    stream_id,
                    meta,
                    location: WalLocation {
                        stream_id,
                        segment_id,
                        frame_offset: offset,
                        frame_len: end - offset,
                        payload_offset,
                        payload_len: parsed.payload_len,
                        payload_checksum: parsed.payload_checksum,
                    },
                });
                descriptors.update(&prefix);
            }
            EPOCH_COMMIT_KIND => {
                let commit = EpochCommit::decode(&metadata).and_then(|commit| {
                    if parsed.payload_len != 0 || parsed.payload_checksum != 0 {
                        return Err("epoch commit contains a payload");
                    }
                    if commit.epoch_start != epoch_start {
                        return Err("epoch commit start does not match pending frames");
                    }
                    if commit.frame_count != epoch_appends.len() as u64 || commit.frame_count == 0 {
                        return Err("epoch commit frame count does not match pending frames");
                    }
                    if commit.descriptors_checksum != descriptors.digest() {
                        return Err("epoch commit descriptor checksum mismatch");
                    }
                    Ok(commit)
                });
                if let Err(message) = commit {
                    return Err(corruption(path, offset, message));
                }
                records.append(&mut epoch_appends);
                descriptors = Xxh3::new();
                epoch_start = end;
            }
            kind => {
                return Err(corruption(
                    path,
                    offset,
                    format!("unknown segment record kind {kind}"),
                ));
            }
        }
        offset = end;
    }
    if offset == file_len && !epoch_appends.is_empty() {
        repair_segment_tail(file, path, epoch_start, is_tail_segment, stats)?;
    }
    Ok(records)
}

// Recovery keeps every physical boundary visible at the corruption decision.
#[allow(clippy::too_many_arguments)]
fn repair_segment_epoch(
    file: &File,
    path: &Path,
    epoch_start: u64,
    damaged_offset: u64,
    file_len: u64,
    is_tail_segment: bool,
    message: impl Into<String>,
    stats: &mut WalStats,
) -> WalResult<()> {
    if !is_tail_segment || has_valid_epoch_commit_after(file, damaged_offset, file_len)? {
        return Err(corruption(path, damaged_offset, message));
    }
    repair_segment_tail(file, path, epoch_start, true, stats)
}

fn repair_segment_tail(
    file: &File,
    path: &Path,
    offset: u64,
    is_tail_segment: bool,
    stats: &mut WalStats,
) -> WalResult<()> {
    if !is_tail_segment {
        return Err(corruption(path, offset, "incomplete non-tail frame"));
    }
    file.set_len(offset)?;
    file.sync_data()?;
    stats.repaired_tails += 1;
    Ok(())
}

fn create_segment(
    segment_dir: &Path,
    shard_id: u32,
    segment_id: u64,
    stats: &mut WalStats,
) -> WalResult<SegmentWriter> {
    let path = segment_path(segment_dir, segment_id);
    if path.exists() {
        return Err(WalError::InvalidConfig(format!(
            "segment {segment_id} already exists"
        )));
    }
    let temporary = segment_temporary_path(segment_dir, segment_id);
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&file_header(
        SEGMENT_MAGIC,
        SEGMENT_FORMAT_VERSION,
        shard_id,
        segment_id,
    ))?;
    file.sync_data()?;
    stats.segment_header_syncs += 1;
    fs::rename(&temporary, &path)?;
    sync_directory(segment_dir)?;
    stats.directory_syncs += 1;
    file.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
    Ok(SegmentWriter {
        id: segment_id,
        file,
        len: FILE_HEADER_LEN,
    })
}

fn file_header(
    magic: &[u8; 8],
    version: u16,
    owner: u32,
    sequence: u64,
) -> [u8; FILE_HEADER_LEN as usize] {
    let mut header = [0_u8; FILE_HEADER_LEN as usize];
    header[..8].copy_from_slice(magic);
    header[8..10].copy_from_slice(&version.to_le_bytes());
    header[10..12].copy_from_slice(&(FILE_HEADER_LEN as u16).to_le_bytes());
    header[12..16].copy_from_slice(&owner.to_le_bytes());
    header[16..24].copy_from_slice(&sequence.to_le_bytes());
    let mut checksum = Xxh3::new();
    checksum.update(&header[..24]);
    header[24..32].copy_from_slice(&checksum.digest().to_le_bytes());
    header
}

struct FileHeader {
    owner: u32,
    sequence: u64,
}

fn read_file_header(
    file: &File,
    path: &Path,
    magic: &[u8; 8],
    expected_version: u16,
) -> WalResult<FileHeader> {
    if file.metadata()?.len() < FILE_HEADER_LEN {
        return Err(corruption(path, 0, "incomplete file header"));
    }
    let mut header = [0_u8; FILE_HEADER_LEN as usize];
    read_exact_at(file, &mut header, 0)?;
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    let header_len = u16::from_le_bytes(header[10..12].try_into().unwrap());
    let owner = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let sequence = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let actual_checksum = u64::from_le_bytes(header[24..32].try_into().unwrap());
    let mut checksum = Xxh3::new();
    checksum.update(&header[..24]);
    if &header[..8] != magic
        || header_len as u64 != FILE_HEADER_LEN
        || checksum.digest() != actual_checksum
    {
        return Err(corruption(path, 0, "invalid file header"));
    }
    if version != expected_version {
        return Err(corruption(
            path,
            0,
            format!("unsupported storage wire version {version}; expected {expected_version}"),
        ));
    }
    Ok(FileHeader { owner, sequence })
}

fn validate_file_header(
    file: &File,
    path: &Path,
    magic: &[u8; 8],
    expected_version: u16,
    owner: u32,
    sequence: u64,
) -> WalResult<()> {
    let header = read_file_header(file, path, magic, expected_version)?;
    if header.owner != owner || header.sequence != sequence {
        return Err(corruption(path, 0, "invalid file header"));
    }
    Ok(())
}

struct ParsedManifestRecordPrefix {
    kind: u8,
    metadata_len: u64,
    frame_len: u64,
    metadata_checksum: u64,
}

fn manifest_record_prefix(
    kind: u8,
    metadata: &[u8],
) -> WalResult<[u8; MANIFEST_RECORD_PREFIX_LEN as usize]> {
    let metadata_len = u32::try_from(metadata.len())
        .map_err(|_| WalError::InvalidConfig("manifest record is too large".into()))?;
    let mut prefix = [0_u8; MANIFEST_RECORD_PREFIX_LEN as usize];
    prefix[..8].copy_from_slice(MANIFEST_RECORD_MAGIC);
    prefix[8..10].copy_from_slice(&MANIFEST_FORMAT_VERSION.to_le_bytes());
    prefix[10] = kind;
    prefix[12..16].copy_from_slice(&metadata_len.to_le_bytes());
    let frame_len = MANIFEST_RECORD_PREFIX_LEN
        .checked_add(metadata_len as u64)
        .ok_or_else(|| WalError::InvalidConfig("manifest record length overflow".into()))?;
    prefix[16..24].copy_from_slice(&frame_len.to_le_bytes());
    let mut checksum = Xxh3::new();
    checksum.update(&prefix[..24]);
    checksum.update(metadata);
    prefix[24..32].copy_from_slice(&checksum.digest().to_le_bytes());
    Ok(prefix)
}

fn parse_manifest_record_prefix(
    prefix: &[u8; MANIFEST_RECORD_PREFIX_LEN as usize],
) -> Result<ParsedManifestRecordPrefix, &'static str> {
    if &prefix[..8] != MANIFEST_RECORD_MAGIC {
        return Err("invalid record magic");
    }
    if u16::from_le_bytes(prefix[8..10].try_into().unwrap()) != MANIFEST_FORMAT_VERSION {
        return Err("unsupported record version");
    }
    if prefix[11] != 0 {
        return Err("unsupported record flags");
    }
    let metadata_len = u32::from_le_bytes(prefix[12..16].try_into().unwrap()) as u64;
    let frame_len = u64::from_le_bytes(prefix[16..24].try_into().unwrap());
    let expected_len = MANIFEST_RECORD_PREFIX_LEN
        .checked_add(metadata_len)
        .ok_or("record length overflow")?;
    if frame_len != expected_len {
        return Err("inconsistent record lengths");
    }
    Ok(ParsedManifestRecordPrefix {
        kind: prefix[10],
        metadata_len,
        frame_len,
        metadata_checksum: u64::from_le_bytes(prefix[24..32].try_into().unwrap()),
    })
}

fn manifest_record_len(metadata_len: u64) -> WalResult<u64> {
    MANIFEST_RECORD_PREFIX_LEN
        .checked_add(metadata_len)
        .ok_or_else(|| WalError::InvalidConfig("record length overflow".into()))
}

fn manifest_record_can_be_final_frame(
    prefix: &[u8; MANIFEST_RECORD_PREFIX_LEN as usize],
    offset: u64,
    file_len: u64,
) -> bool {
    let metadata_len = u32::from_le_bytes(prefix[12..16].try_into().unwrap()) as u64;
    let stored_frame_len = u64::from_le_bytes(prefix[16..24].try_into().unwrap());
    let Some(derived_frame_len) = manifest_record_len(metadata_len).ok() else {
        return false;
    };
    let stored_end = offset.checked_add(stored_frame_len);
    let derived_end = offset.checked_add(derived_frame_len);
    if stored_frame_len == derived_frame_len {
        return derived_end.is_some_and(|end| end >= file_len);
    }
    // Component lengths and their redundant total let recovery recognize a
    // physically complete final frame when either representation is corrupt.
    // For an earlier frame, both legitimate endpoints precede another frame.
    stored_end == Some(file_len) || derived_end == Some(file_len)
}

struct ParsedSegmentRecordPrefix {
    kind: u8,
    metadata_len: u64,
    payload_len: u64,
    frame_len: u64,
    payload_checksum: u64,
    metadata_checksum: u64,
}

fn segment_record_prefix(
    kind: u8,
    metadata: &[u8],
    payload_len: u64,
    payload_checksum: u64,
) -> WalResult<[u8; SEGMENT_RECORD_PREFIX_LEN as usize]> {
    let metadata_len = u32::try_from(metadata.len())
        .map_err(|_| WalError::InvalidConfig("append metadata is too large".into()))?;
    let frame_len = segment_record_len(metadata_len as u64, payload_len)?;
    let mut prefix = [0_u8; SEGMENT_RECORD_PREFIX_LEN as usize];
    prefix[..8].copy_from_slice(RECORD_MAGIC);
    prefix[8..10].copy_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
    prefix[10] = kind;
    prefix[12..16].copy_from_slice(&metadata_len.to_le_bytes());
    prefix[16..24].copy_from_slice(&payload_len.to_le_bytes());
    prefix[24..32].copy_from_slice(&frame_len.to_le_bytes());
    prefix[32..40].copy_from_slice(&payload_checksum.to_le_bytes());
    let mut checksum = Xxh3::new();
    checksum.update(&prefix[..40]);
    checksum.update(metadata);
    prefix[40..48].copy_from_slice(&checksum.digest().to_le_bytes());
    Ok(prefix)
}

fn parse_segment_record_prefix(
    prefix: &[u8; SEGMENT_RECORD_PREFIX_LEN as usize],
) -> Result<ParsedSegmentRecordPrefix, &'static str> {
    if &prefix[..8] != RECORD_MAGIC {
        return Err("invalid record magic");
    }
    if u16::from_le_bytes(prefix[8..10].try_into().unwrap()) != SEGMENT_FORMAT_VERSION {
        return Err("unsupported record version");
    }
    if prefix[11] != 0 {
        return Err("unsupported record flags");
    }
    let metadata_len = u32::from_le_bytes(prefix[12..16].try_into().unwrap()) as u64;
    let payload_len = u64::from_le_bytes(prefix[16..24].try_into().unwrap());
    let frame_len = u64::from_le_bytes(prefix[24..32].try_into().unwrap());
    let expected_len = SEGMENT_RECORD_PREFIX_LEN
        .checked_add(metadata_len)
        .and_then(|length| length.checked_add(payload_len))
        .ok_or("record length overflow")?;
    if frame_len != expected_len {
        return Err("inconsistent record lengths");
    }
    Ok(ParsedSegmentRecordPrefix {
        kind: prefix[10],
        metadata_len,
        payload_len,
        frame_len,
        payload_checksum: u64::from_le_bytes(prefix[32..40].try_into().unwrap()),
        metadata_checksum: u64::from_le_bytes(prefix[40..48].try_into().unwrap()),
    })
}

fn segment_record_len(metadata_len: u64, payload_len: u64) -> WalResult<u64> {
    SEGMENT_RECORD_PREFIX_LEN
        .checked_add(metadata_len)
        .and_then(|length| length.checked_add(payload_len))
        .ok_or_else(|| WalError::InvalidConfig("record length overflow".into()))
}

// A repairable tail cannot have a complete checksummed record after the
// damaged offset. The physical bytes after that record may themselves be torn.
fn has_valid_record_after(
    file: &File,
    damaged_offset: u64,
    file_len: u64,
    magic: &[u8; 8],
    validate: fn(&File, u64, u64) -> WalResult<bool>,
) -> WalResult<bool> {
    let mut search_offset = damaged_offset.saturating_add(1);
    let mut buffer = [0_u8; HASH_BUFFER_LEN + 7];
    while file_len.saturating_sub(search_offset) >= RECORD_MAGIC.len() as u64 {
        let count = usize::try_from(
            file_len
                .saturating_sub(search_offset)
                .min(buffer.len() as u64),
        )
        .map_err(|_| WalError::InvalidConfig("corruption scan range is too large".into()))?;
        read_exact_at(file, &mut buffer[..count], search_offset)?;
        for index in 0..=count - RECORD_MAGIC.len() {
            if &buffer[index..index + magic.len()] == magic {
                let candidate = search_offset + index as u64;
                if validate(file, candidate, file_len)? {
                    return Ok(true);
                }
            }
        }
        if count < RECORD_MAGIC.len() {
            break;
        }
        search_offset += (count - (RECORD_MAGIC.len() - 1)) as u64;
    }
    Ok(false)
}

fn has_valid_manifest_record_after(
    file: &File,
    damaged_offset: u64,
    file_len: u64,
) -> WalResult<bool> {
    has_valid_record_after(
        file,
        damaged_offset,
        file_len,
        MANIFEST_RECORD_MAGIC,
        valid_manifest_record_at,
    )
}

fn valid_manifest_record_at(file: &File, offset: u64, file_len: u64) -> WalResult<bool> {
    if file_len.saturating_sub(offset) < MANIFEST_RECORD_PREFIX_LEN {
        return Ok(false);
    }
    let mut prefix = [0_u8; MANIFEST_RECORD_PREFIX_LEN as usize];
    read_exact_at(file, &mut prefix, offset)?;
    let parsed = match parse_manifest_record_prefix(&prefix) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(false),
    };
    if !matches!(
        parsed.kind,
        RELEASE_KIND
            | SEGMENT_ROTATION_KIND
            | SEGMENT_REMOVAL_KIND
            | SEGMENT_SNAPSHOT_KIND
            | SEGMENT_TIMESTAMP_KIND
            | STREAM_RELEASE_KIND
    ) || parsed.metadata_len > MAX_METADATA_LEN
    {
        return Ok(false);
    }
    let Some(end) = offset.checked_add(parsed.frame_len) else {
        return Ok(false);
    };
    if end > file_len {
        return Ok(false);
    }

    let mut metadata = vec![0_u8; parsed.metadata_len as usize];
    read_exact_at(file, &mut metadata, offset + MANIFEST_RECORD_PREFIX_LEN)?;
    let mut checksum = Xxh3::new();
    checksum.update(&prefix[..24]);
    checksum.update(&metadata);
    if checksum.digest() != parsed.metadata_checksum {
        return Ok(false);
    }
    Ok(match parsed.kind {
        RELEASE_KIND => serde_json::from_slice::<ReleaseV1>(&metadata).is_ok(),
        SEGMENT_ROTATION_KIND => serde_json::from_slice::<SegmentRotationV1>(&metadata).is_ok(),
        SEGMENT_REMOVAL_KIND => serde_json::from_slice::<SegmentRemovalV1>(&metadata).is_ok(),
        SEGMENT_SNAPSHOT_KIND => serde_json::from_slice::<SegmentSnapshotV1>(&metadata).is_ok(),
        SEGMENT_TIMESTAMP_KIND => serde_json::from_slice::<SegmentTimestampV1>(&metadata).is_ok(),
        STREAM_RELEASE_KIND => serde_json::from_slice::<StreamReleaseV1>(&metadata).is_ok(),
        _ => false,
    })
}

fn has_valid_epoch_commit_after(
    file: &File,
    damaged_offset: u64,
    file_len: u64,
) -> WalResult<bool> {
    has_valid_record_after(
        file,
        damaged_offset,
        file_len,
        RECORD_MAGIC,
        valid_epoch_commit_at,
    )
}

fn valid_epoch_commit_at(file: &File, offset: u64, file_len: u64) -> WalResult<bool> {
    if file_len.saturating_sub(offset) < SEGMENT_RECORD_PREFIX_LEN {
        return Ok(false);
    }
    let mut prefix = [0_u8; SEGMENT_RECORD_PREFIX_LEN as usize];
    read_exact_at(file, &mut prefix, offset)?;
    let parsed = match parse_segment_record_prefix(&prefix) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(false),
    };
    if parsed.kind != EPOCH_COMMIT_KIND
        || parsed.metadata_len != EPOCH_COMMIT_METADATA_LEN
        || parsed.payload_len != 0
        || parsed.payload_checksum != 0
    {
        return Ok(false);
    }
    let Some(end) = offset.checked_add(parsed.frame_len) else {
        return Ok(false);
    };
    if end > file_len {
        return Ok(false);
    }
    let mut metadata = [0_u8; EPOCH_COMMIT_METADATA_LEN as usize];
    read_exact_at(file, &mut metadata, offset + SEGMENT_RECORD_PREFIX_LEN)?;
    let mut checksum = Xxh3::new();
    checksum.update(&prefix[..40]);
    checksum.update(&metadata);
    Ok(checksum.digest() == parsed.metadata_checksum && EpochCommit::decode(&metadata).is_ok())
}

fn segment_path(segment_dir: &Path, segment_id: u64) -> PathBuf {
    segment_dir.join(format!("segment-{segment_id:020}.log"))
}

fn shard_directory(root: &Path, shard_id: u32) -> PathBuf {
    if shard_id == DEFAULT_STREAM.get() {
        root.join("segments")
    } else {
        root.join("streams").join(format!("stream-{shard_id:010}"))
    }
}

fn discover_stream_directories(root: &Path) -> WalResult<BTreeSet<u32>> {
    let streams = root.join("streams");
    if !streams.exists() {
        return Ok(BTreeSet::new());
    }
    let mut stream_ids = BTreeSet::new();
    for entry in fs::read_dir(&streams)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(stream_id) = parse_stream_directory_name(&entry.file_name()) else {
            continue;
        };
        if stream_id != DEFAULT_STREAM.get() && entry.path() == shard_directory(root, stream_id) {
            stream_ids.insert(stream_id);
        }
    }
    Ok(stream_ids)
}

fn parse_stream_directory_name(name: &std::ffi::OsStr) -> Option<u32> {
    let name = name.to_str()?;
    let digits = name.strip_prefix("stream-")?;
    if digits.len() != 10 {
        return None;
    }
    digits.parse().ok()
}

fn reconcile_segment_directory(
    segment_dir: &Path,
    expected: BTreeSet<u64>,
    protect_unmanifested_tail: bool,
    stats: &mut WalStats,
) -> WalResult<()> {
    let first_unmanifested_tail = expected
        .last()
        .and_then(|segment_id| segment_id.checked_add(1))
        .unwrap_or(0);
    let mut found = BTreeSet::new();
    let mut removed = false;
    for entry in fs::read_dir(segment_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if parse_segment_temporary_name(&entry.file_name()).is_some() {
            fs::remove_file(entry.path())?;
            removed = true;
            continue;
        }
        let Some(segment_id) = parse_segment_name(&entry.file_name()) else {
            continue;
        };
        let canonical = segment_path(segment_dir, segment_id);
        if !expected.contains(&segment_id) || entry.path() != canonical {
            if protect_unmanifested_tail
                && entry.path() == canonical
                && segment_id >= first_unmanifested_tail
                && entry.metadata()?.len() > FILE_HEADER_LEN
            {
                return Err(corruption(
                    &canonical,
                    FILE_HEADER_LEN,
                    "unmanifested tail segment contains data",
                ));
            }
            fs::remove_file(entry.path())?;
            removed = true;
        } else {
            found.insert(segment_id);
        }
    }
    if removed {
        sync_directory(segment_dir)?;
        stats.directory_syncs += 1;
    }
    if let Some(missing) = expected.difference(&found).next() {
        return Err(corruption(
            segment_dir,
            0,
            format!("manifest segment {missing} is missing"),
        ));
    }
    Ok(())
}

fn segment_temporary_path(segment_dir: &Path, segment_id: u64) -> PathBuf {
    segment_dir.join(format!("segment-{segment_id:020}.log.tmp"))
}

type LiveRecordIds = BTreeMap<u32, HashSet<String>>;

fn all_live_record_ids<'a>(shards: impl IntoIterator<Item = &'a ShardSegments>) -> LiveRecordIds {
    shards
        .into_iter()
        .map(|shard| (shard.shard_id(), shard.live_record_ids().clone()))
        .collect()
}

fn parse_segment_name(name: &std::ffi::OsStr) -> Option<u64> {
    let name = name.to_str()?;
    name.strip_prefix("segment-")?
        .strip_suffix(".log")?
        .parse()
        .ok()
}

fn parse_segment_temporary_name(name: &std::ffi::OsStr) -> Option<u64> {
    let name = name.to_str()?;
    name.strip_prefix("segment-")?
        .strip_suffix(".log.tmp")?
        .parse()
        .ok()
}

fn validate_location_bounds(location: &WalLocation) -> WalResult<()> {
    if location.frame_offset < FILE_HEADER_LEN || location.frame_len < SEGMENT_RECORD_PREFIX_LEN {
        return Err(WalError::InvalidLocation(
            "frame lies inside the segment header or is too short".into(),
        ));
    }
    let minimum_payload_offset = location
        .frame_offset
        .checked_add(SEGMENT_RECORD_PREFIX_LEN)
        .ok_or_else(|| WalError::InvalidLocation("frame offset overflow".into()))?;
    let frame_end = location_frame_end(location)?;
    let payload_end = location_payload_end(location)?;
    if location.payload_offset < minimum_payload_offset || payload_end != frame_end {
        return Err(WalError::InvalidLocation(
            "payload lies outside its frame".into(),
        ));
    }
    Ok(())
}

fn location_frame_end(location: &WalLocation) -> WalResult<u64> {
    location
        .frame_offset
        .checked_add(location.frame_len)
        .ok_or_else(|| WalError::InvalidLocation("frame offset overflow".into()))
}

fn location_payload_end(location: &WalLocation) -> WalResult<u64> {
    location
        .payload_offset
        .checked_add(location.payload_len)
        .ok_or_else(|| WalError::InvalidLocation("payload offset overflow".into()))
}

fn validated_payload_from_range(
    path: &Path,
    location: &WalLocation,
    range_start: u64,
    range: &Bytes,
) -> WalResult<Bytes> {
    let relative_frame_offset = location
        .frame_offset
        .checked_sub(range_start)
        .and_then(|offset| usize::try_from(offset).ok())
        .ok_or_else(|| WalError::InvalidLocation("frame offset is outside read range".into()))?;
    let prefix_end = relative_frame_offset
        .checked_add(SEGMENT_RECORD_PREFIX_LEN as usize)
        .ok_or_else(|| WalError::InvalidLocation("frame prefix range overflow".into()))?;
    let prefix: &[u8; SEGMENT_RECORD_PREFIX_LEN as usize] = range
        .get(relative_frame_offset..prefix_end)
        .and_then(|prefix| prefix.try_into().ok())
        .ok_or_else(|| WalError::InvalidLocation("frame prefix is outside read range".into()))?;
    let parsed = parse_segment_record_prefix(prefix)
        .map_err(|message| corruption(path, location.frame_offset, message))?;
    if parsed.kind != RECORD_KIND {
        return Err(corruption(
            path,
            location.frame_offset,
            "location does not reference a record frame",
        ));
    }
    if parsed.metadata_len > MAX_METADATA_LEN {
        return Err(corruption(
            path,
            location.frame_offset,
            "record metadata is unreasonably large",
        ));
    }
    let metadata_len = usize::try_from(parsed.metadata_len)
        .map_err(|_| WalError::InvalidLocation("metadata is too large to address".into()))?;
    let metadata_end = prefix_end
        .checked_add(metadata_len)
        .ok_or_else(|| WalError::InvalidLocation("metadata range overflow".into()))?;
    let metadata = range
        .get(prefix_end..metadata_end)
        .ok_or_else(|| WalError::InvalidLocation("metadata is outside read range".into()))?;
    let mut metadata_checksum = Xxh3::new();
    metadata_checksum.update(&prefix[..40]);
    metadata_checksum.update(metadata);
    if metadata_checksum.digest() != parsed.metadata_checksum {
        return Err(corruption(
            path,
            location.frame_offset,
            "checksum mismatch while reading record metadata",
        ));
    }
    decode_record_meta(metadata).map_err(|message| {
        corruption(
            path,
            location.frame_offset,
            format!("invalid record metadata while reading payload: {message}"),
        )
    })?;

    let expected_payload_offset = location
        .frame_offset
        .checked_add(SEGMENT_RECORD_PREFIX_LEN)
        .and_then(|offset| offset.checked_add(parsed.metadata_len))
        .ok_or_else(|| WalError::InvalidLocation("payload offset overflow".into()))?;
    if parsed.frame_len != location.frame_len
        || parsed.payload_len != location.payload_len
        || parsed.payload_checksum != location.payload_checksum
        || expected_payload_offset != location.payload_offset
    {
        return Err(WalError::InvalidLocation(
            "location does not match its record descriptor".into(),
        ));
    }

    let relative_offset = metadata_end;
    let payload_len = usize::try_from(location.payload_len)
        .map_err(|_| WalError::InvalidLocation("payload is too large to address".into()))?;
    let relative_end = relative_offset
        .checked_add(payload_len)
        .ok_or_else(|| WalError::InvalidLocation("payload range overflow".into()))?;
    let payload = range
        .get(relative_offset..relative_end)
        .ok_or_else(|| WalError::InvalidLocation("payload is outside read range".into()))?;
    if xxhash_rust::xxh3::xxh3_64(payload) != location.payload_checksum {
        return Err(corruption(
            path,
            location.frame_offset,
            "checksum mismatch while reading append payload",
        ));
    }
    Ok(range.slice(relative_offset..relative_end))
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        let read = file.read_at(buffer, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short positional read",
            ));
        }
        offset += read as u64;
        buffer = &mut buffer[read..];
    }
    Ok(())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn measure_segment_bytes(segment_dir: &Path) -> io::Result<u64> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(segment_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() && parse_segment_name(&entry.file_name()).is_some() {
            bytes = bytes
                .checked_add(entry.metadata()?.len())
                .ok_or_else(|| io::Error::other("WAL size overflow"))?;
        }
    }
    Ok(bytes)
}

fn create_directories_durably(path: &Path) -> io::Result<u64> {
    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.exists() {
        missing.push(cursor.to_path_buf());
        cursor = cursor
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
    }

    let mut syncs = 0_u64;
    for directory in missing.into_iter().rev() {
        match DirBuilder::new().mode(0o700).create(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        let parent = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(parent)?;
        syncs += 1;
    }
    Ok(syncs)
}

fn corruption(path: &Path, offset: u64, message: impl Into<String>) -> WalError {
    WalError::Corruption {
        path: path.to_path_buf(),
        offset,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests;
