use crate::model::{RootId, StreamId};
use std::collections::BTreeMap;
use std::fmt;
use xxhash_rust::xxh3::{xxh3_64_with_seed, Xxh3};

pub(crate) const FORMAT_SEED: u64 = u64::from_le_bytes(*b"CAMUSV1!");

pub(crate) const ROOT_SUPERBLOCK_LEN: u64 = 40;
pub(crate) const SEGMENT_HEADER_LEN: u64 = 48;
pub(crate) const RECORD_DESCRIPTOR_LEN: u64 = 40;
pub(crate) const EPOCH_HEADER_LEN: u64 = 48;
pub(crate) const EPOCH_COMMIT_LEN: u64 = 40;
pub(crate) const SEGMENT_FOOTER_LEN: u64 = 48;
pub(crate) const MANIFEST_LOG_HEADER_LEN: u64 = 40;
pub(crate) const MANIFEST_FRAME_HEADER_LEN: u64 = 48;
pub(crate) const CHECKPOINT_HEADER_LEN: u64 = 56;

pub(crate) const ROOT_MAGIC: &[u8; 8] = b"CAMROOT1";
pub(crate) const SEGMENT_MAGIC: &[u8; 8] = b"CAMSEG01";
pub(crate) const EPOCH_MAGIC: &[u8; 8] = b"CAMEPH01";
pub(crate) const EPOCH_COMMIT_MAGIC: &[u8; 8] = b"CAMCMT01";
pub(crate) const SEGMENT_FOOTER_MAGIC: &[u8; 8] = b"CAMSEA01";
pub(crate) const MANIFEST_LOG_MAGIC: &[u8; 8] = b"CAMLOG01";
pub(crate) const MANIFEST_FRAME_MAGIC: &[u8; 8] = b"CAMCTL01";
pub(crate) const CHECKPOINT_MAGIC: &[u8; 8] = b"CAMCHK01";

const FORMAT_VERSION: u64 = 1;
const RELEASE_KIND: u64 = 1;
const SEGMENT_SEALED_KIND: u64 = 2;
const SEGMENT_REMOVED_KIND: u64 = 3;
const ACTIVE_LIFECYCLE: u64 = 1;
const SEALED_LIFECYCLE: u64 = 2;
const RANGE_ENCODING: u64 = 1;
const BITMAP_ENCODING: u64 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FormatError {
    message: String,
}

impl FormatError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for FormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for FormatError {}

type FormatResult<T> = std::result::Result<T, FormatError>;

pub(crate) fn checksum(bytes: &[u8]) -> u64 {
    xxh3_64_with_seed(bytes, FORMAT_SEED)
}

pub(crate) fn digest<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    let mut hasher = Xxh3::with_seed(FORMAT_SEED);
    for part in parts {
        hasher.update(part);
    }
    hasher.digest()
}

fn exact<const N: usize>(bytes: &[u8], name: &str) -> FormatResult<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| FormatError::new(format!("{name} must be exactly {N} bytes")))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut encoded = [0_u8; 8];
    encoded.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(encoded)
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn checked_len(base: u64, count: u64, unit: u64, name: &str) -> FormatResult<u64> {
    count
        .checked_mul(unit)
        .and_then(|bytes| base.checked_add(bytes))
        .ok_or_else(|| FormatError::new(format!("{name} length overflows u64")))
}

fn usize_len(value: u64, name: &str) -> FormatResult<usize> {
    usize::try_from(value)
        .map_err(|_| FormatError::new(format!("{name} does not fit this platform's usize")))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RootSuperblock {
    pub(crate) root_id: RootId,
}

impl RootSuperblock {
    pub(crate) fn encode(self) -> [u8; ROOT_SUPERBLOCK_LEN as usize] {
        let mut bytes = [0_u8; ROOT_SUPERBLOCK_LEN as usize];
        bytes[..8].copy_from_slice(ROOT_MAGIC);
        write_u64(&mut bytes, 8, FORMAT_VERSION);
        bytes[16..32].copy_from_slice(&self.root_id.to_bytes());
        let value = checksum(&bytes[..32]);
        write_u64(&mut bytes, 32, value);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        let bytes = exact::<{ ROOT_SUPERBLOCK_LEN as usize }>(bytes, "ROOT superblock")?;
        if &bytes[..8] != ROOT_MAGIC {
            return Err(FormatError::new("invalid ROOT magic"));
        }
        if read_u64(&bytes, 8) != FORMAT_VERSION {
            return Err(FormatError::new("unsupported ROOT format version"));
        }
        if read_u64(&bytes, 32) != checksum(&bytes[..32]) {
            return Err(FormatError::new("ROOT checksum mismatch"));
        }
        let mut root_id = [0_u8; RootId::LEN];
        root_id.copy_from_slice(&bytes[16..32]);
        Ok(Self {
            root_id: RootId::from_bytes(root_id),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentHeader {
    pub(crate) root_id: RootId,
    pub(crate) segment_id: u64,
    pub(crate) created_at_unix_millis: u64,
}

impl SegmentHeader {
    pub(crate) fn encode(self) -> FormatResult<[u8; SEGMENT_HEADER_LEN as usize]> {
        if self.segment_id == u64::MAX {
            return Err(FormatError::new("u64::MAX is not a valid segment ID"));
        }
        let mut bytes = [0_u8; SEGMENT_HEADER_LEN as usize];
        bytes[..8].copy_from_slice(SEGMENT_MAGIC);
        bytes[8..24].copy_from_slice(&self.root_id.to_bytes());
        write_u64(&mut bytes, 24, self.segment_id);
        write_u64(&mut bytes, 32, self.created_at_unix_millis);
        let value = checksum(&bytes[..40]);
        write_u64(&mut bytes, 40, value);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        let bytes = exact::<{ SEGMENT_HEADER_LEN as usize }>(bytes, "segment header")?;
        if &bytes[..8] != SEGMENT_MAGIC {
            return Err(FormatError::new("invalid segment header magic"));
        }
        if read_u64(&bytes, 40) != checksum(&bytes[..40]) {
            return Err(FormatError::new("segment header checksum mismatch"));
        }
        let segment_id = read_u64(&bytes, 24);
        if segment_id == u64::MAX {
            return Err(FormatError::new("u64::MAX is not a valid segment ID"));
        }
        let mut root_id = [0_u8; RootId::LEN];
        root_id.copy_from_slice(&bytes[8..24]);
        Ok(Self {
            root_id: RootId::from_bytes(root_id),
            segment_id,
            created_at_unix_millis: read_u64(&bytes, 32),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordDescriptor {
    pub(crate) metadata_len: u64,
    pub(crate) payload_len: u64,
    pub(crate) metadata_checksum: u64,
    pub(crate) payload_checksum: u64,
}

impl RecordDescriptor {
    pub(crate) fn new(metadata: &[u8], payload: &[u8]) -> FormatResult<Self> {
        Ok(Self {
            metadata_len: u64::try_from(metadata.len())
                .map_err(|_| FormatError::new("metadata length does not fit u64"))?,
            payload_len: u64::try_from(payload.len())
                .map_err(|_| FormatError::new("payload length does not fit u64"))?,
            metadata_checksum: checksum(metadata),
            payload_checksum: checksum(payload),
        })
    }

    pub(crate) fn encoded_len(self) -> FormatResult<u64> {
        RECORD_DESCRIPTOR_LEN
            .checked_add(self.metadata_len)
            .and_then(|bytes| bytes.checked_add(self.payload_len))
            .ok_or_else(|| FormatError::new("record length overflows u64"))
    }

    pub(crate) fn encode(self) -> [u8; RECORD_DESCRIPTOR_LEN as usize] {
        let mut bytes = [0_u8; RECORD_DESCRIPTOR_LEN as usize];
        write_u64(&mut bytes, 0, self.metadata_len);
        write_u64(&mut bytes, 8, self.payload_len);
        write_u64(&mut bytes, 16, self.metadata_checksum);
        write_u64(&mut bytes, 24, self.payload_checksum);
        let value = checksum(&bytes[..32]);
        write_u64(&mut bytes, 32, value);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        let bytes = exact::<{ RECORD_DESCRIPTOR_LEN as usize }>(bytes, "record descriptor")?;
        if read_u64(&bytes, 32) != checksum(&bytes[..32]) {
            return Err(FormatError::new("record descriptor checksum mismatch"));
        }
        let descriptor = Self {
            metadata_len: read_u64(&bytes, 0),
            payload_len: read_u64(&bytes, 8),
            metadata_checksum: read_u64(&bytes, 16),
            payload_checksum: read_u64(&bytes, 24),
        };
        descriptor.encoded_len()?;
        Ok(descriptor)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EpochHeader {
    pub(crate) stream_id: StreamId,
    pub(crate) first_sequence: u64,
    pub(crate) record_count: u64,
    pub(crate) records_bytes: u64,
}

impl EpochHeader {
    pub(crate) fn validate(self) -> FormatResult<()> {
        if self.record_count == 0 {
            return Err(FormatError::new("epoch record_count must be nonzero"));
        }
        self.first_sequence
            .checked_add(self.record_count - 1)
            .ok_or_else(|| FormatError::new("epoch sequence interval overflows u64"))?;
        let minimum_records_bytes = self
            .record_count
            .checked_mul(RECORD_DESCRIPTOR_LEN)
            .ok_or_else(|| FormatError::new("minimum epoch records length overflows u64"))?;
        if self.records_bytes < minimum_records_bytes {
            return Err(FormatError::new(
                "epoch records_bytes is smaller than its record descriptors",
            ));
        }
        Ok(())
    }

    pub(crate) fn encode(self) -> FormatResult<[u8; EPOCH_HEADER_LEN as usize]> {
        self.validate()?;
        let mut bytes = [0_u8; EPOCH_HEADER_LEN as usize];
        bytes[..8].copy_from_slice(EPOCH_MAGIC);
        write_u64(&mut bytes, 8, self.stream_id.get());
        write_u64(&mut bytes, 16, self.first_sequence);
        write_u64(&mut bytes, 24, self.record_count);
        write_u64(&mut bytes, 32, self.records_bytes);
        let value = checksum(&bytes[..40]);
        write_u64(&mut bytes, 40, value);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        let bytes = exact::<{ EPOCH_HEADER_LEN as usize }>(bytes, "epoch header")?;
        if &bytes[..8] != EPOCH_MAGIC {
            return Err(FormatError::new("invalid epoch header magic"));
        }
        if read_u64(&bytes, 40) != checksum(&bytes[..40]) {
            return Err(FormatError::new("epoch header checksum mismatch"));
        }
        let header = Self {
            stream_id: StreamId::new(read_u64(&bytes, 8)),
            first_sequence: read_u64(&bytes, 16),
            record_count: read_u64(&bytes, 24),
            records_bytes: read_u64(&bytes, 32),
        };
        header.validate()?;
        Ok(header)
    }
}

pub(crate) fn epoch_digest(
    header: &[u8; EPOCH_HEADER_LEN as usize],
    descriptors: &[[u8; RECORD_DESCRIPTOR_LEN as usize]],
) -> u64 {
    digest(
        std::iter::once(header.as_slice())
            .chain(descriptors.iter().map(|descriptor| descriptor.as_slice())),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EpochCommit {
    pub(crate) epoch_start: u64,
    pub(crate) epoch_bytes: u64,
    pub(crate) epoch_digest: u64,
}

impl EpochCommit {
    pub(crate) fn encode(self) -> [u8; EPOCH_COMMIT_LEN as usize] {
        let mut bytes = [0_u8; EPOCH_COMMIT_LEN as usize];
        bytes[..8].copy_from_slice(EPOCH_COMMIT_MAGIC);
        write_u64(&mut bytes, 8, self.epoch_start);
        write_u64(&mut bytes, 16, self.epoch_bytes);
        write_u64(&mut bytes, 24, self.epoch_digest);
        let value = checksum(&bytes[..32]);
        write_u64(&mut bytes, 32, value);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        let bytes = exact::<{ EPOCH_COMMIT_LEN as usize }>(bytes, "epoch commit")?;
        if &bytes[..8] != EPOCH_COMMIT_MAGIC {
            return Err(FormatError::new("invalid epoch commit magic"));
        }
        if read_u64(&bytes, 32) != checksum(&bytes[..32]) {
            return Err(FormatError::new("epoch commit checksum mismatch"));
        }
        Ok(Self {
            epoch_start: read_u64(&bytes, 8),
            epoch_bytes: read_u64(&bytes, 16),
            epoch_digest: read_u64(&bytes, 24),
        })
    }
}

pub(crate) fn segment_digest(
    header: &[u8; SEGMENT_HEADER_LEN as usize],
    commits: &[[u8; EPOCH_COMMIT_LEN as usize]],
) -> u64 {
    digest(std::iter::once(header.as_slice()).chain(commits.iter().map(|commit| commit.as_slice())))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentFooter {
    pub(crate) segment_id: u64,
    pub(crate) segment_bytes: u64,
    pub(crate) epoch_count: u64,
    pub(crate) segment_digest: u64,
}

impl SegmentFooter {
    pub(crate) fn validate(self) -> FormatResult<()> {
        if self.segment_id == u64::MAX {
            return Err(FormatError::new("u64::MAX is not a valid segment ID"));
        }
        if self.epoch_count == 0 {
            return Err(FormatError::new(
                "sealed segment epoch_count must be nonzero",
            ));
        }
        Ok(())
    }

    pub(crate) fn encode(self) -> FormatResult<[u8; SEGMENT_FOOTER_LEN as usize]> {
        self.validate()?;
        let mut bytes = [0_u8; SEGMENT_FOOTER_LEN as usize];
        bytes[..8].copy_from_slice(SEGMENT_FOOTER_MAGIC);
        write_u64(&mut bytes, 8, self.segment_id);
        write_u64(&mut bytes, 16, self.segment_bytes);
        write_u64(&mut bytes, 24, self.epoch_count);
        write_u64(&mut bytes, 32, self.segment_digest);
        let value = checksum(&bytes[..40]);
        write_u64(&mut bytes, 40, value);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        let bytes = exact::<{ SEGMENT_FOOTER_LEN as usize }>(bytes, "segment footer")?;
        if &bytes[..8] != SEGMENT_FOOTER_MAGIC {
            return Err(FormatError::new("invalid segment footer magic"));
        }
        if read_u64(&bytes, 40) != checksum(&bytes[..40]) {
            return Err(FormatError::new("segment footer checksum mismatch"));
        }
        let footer = Self {
            segment_id: read_u64(&bytes, 8),
            segment_bytes: read_u64(&bytes, 16),
            epoch_count: read_u64(&bytes, 24),
            segment_digest: read_u64(&bytes, 32),
        };
        footer.validate()?;
        Ok(footer)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManifestLogHeader {
    pub(crate) root_id: RootId,
    pub(crate) base_seq: u64,
}

impl ManifestLogHeader {
    pub(crate) fn encode(self) -> [u8; MANIFEST_LOG_HEADER_LEN as usize] {
        let mut bytes = [0_u8; MANIFEST_LOG_HEADER_LEN as usize];
        bytes[..8].copy_from_slice(MANIFEST_LOG_MAGIC);
        bytes[8..24].copy_from_slice(&self.root_id.to_bytes());
        write_u64(&mut bytes, 24, self.base_seq);
        let value = checksum(&bytes[..32]);
        write_u64(&mut bytes, 32, value);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        let bytes = exact::<{ MANIFEST_LOG_HEADER_LEN as usize }>(bytes, "manifest log header")?;
        if &bytes[..8] != MANIFEST_LOG_MAGIC {
            return Err(FormatError::new("invalid manifest log header magic"));
        }
        if read_u64(&bytes, 32) != checksum(&bytes[..32]) {
            return Err(FormatError::new("manifest log header checksum mismatch"));
        }
        let mut root_id = [0_u8; RootId::LEN];
        root_id.copy_from_slice(&bytes[8..24]);
        Ok(Self {
            root_id: RootId::from_bytes(root_id),
            base_seq: read_u64(&bytes, 24),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SequenceRange {
    pub(crate) start: u64,
    pub(crate) len: u64,
}

impl SequenceRange {
    pub(crate) fn end(self) -> FormatResult<u64> {
        if self.len == 0 {
            return Err(FormatError::new("range length must be nonzero"));
        }
        self.start
            .checked_add(self.len - 1)
            .ok_or_else(|| FormatError::new("range end overflows u64"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReleaseBody {
    pub(crate) stream_id: StreamId,
    pub(crate) ranges: Vec<SequenceRange>,
}

impl ReleaseBody {
    pub(crate) fn released_count(&self) -> FormatResult<u64> {
        validate_ranges(&self.ranges, false)?;
        self.ranges.iter().try_fold(0_u64, |total, range| {
            total
                .checked_add(range.len)
                .ok_or_else(|| FormatError::new("released_count overflows u64"))
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentSealedBody {
    pub(crate) segment_id: u64,
    pub(crate) segment_bytes: u64,
    pub(crate) epoch_count: u64,
    pub(crate) segment_digest: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StreamHighwater {
    pub(crate) stream_id: StreamId,
    pub(crate) sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentRemovedBody {
    pub(crate) segment_id: u64,
    pub(crate) highwaters: Vec<StreamHighwater>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ManifestBody {
    Release(ReleaseBody),
    SegmentSealed(SegmentSealedBody),
    SegmentRemoved(SegmentRemovedBody),
}

impl ManifestBody {
    fn kind(&self) -> u64 {
        match self {
            Self::Release(_) => RELEASE_KIND,
            Self::SegmentSealed(_) => SEGMENT_SEALED_KIND,
            Self::SegmentRemoved(_) => SEGMENT_REMOVED_KIND,
        }
    }

    fn encode(&self) -> FormatResult<Vec<u8>> {
        match self {
            Self::Release(release) => {
                let count = release.released_count()?;
                let range_count = u64::try_from(release.ranges.len())
                    .map_err(|_| FormatError::new("release range count does not fit u64"))?;
                let body_len = checked_len(24, range_count, 16, "Release body")?;
                let mut bytes = vec![0_u8; usize_len(body_len, "Release body length")?];
                write_u64(&mut bytes, 0, release.stream_id.get());
                write_u64(&mut bytes, 8, count);
                write_u64(&mut bytes, 16, range_count);
                for (index, range) in release.ranges.iter().enumerate() {
                    let offset = 24 + index * 16;
                    write_u64(&mut bytes, offset, range.start);
                    write_u64(&mut bytes, offset + 8, range.len);
                }
                Ok(bytes)
            }
            Self::SegmentSealed(sealed) => {
                if sealed.segment_id == u64::MAX || sealed.epoch_count == 0 {
                    return Err(FormatError::new("invalid SegmentSealed body"));
                }
                let mut bytes = vec![0_u8; 32];
                write_u64(&mut bytes, 0, sealed.segment_id);
                write_u64(&mut bytes, 8, sealed.segment_bytes);
                write_u64(&mut bytes, 16, sealed.epoch_count);
                write_u64(&mut bytes, 24, sealed.segment_digest);
                Ok(bytes)
            }
            Self::SegmentRemoved(removed) => {
                if removed.segment_id == u64::MAX {
                    return Err(FormatError::new("invalid SegmentRemoved segment ID"));
                }
                validate_highwaters(&removed.highwaters)?;
                let count = u64::try_from(removed.highwaters.len())
                    .map_err(|_| FormatError::new("high-water count does not fit u64"))?;
                let body_len = checked_len(16, count, 16, "SegmentRemoved body")?;
                let mut bytes = vec![0_u8; usize_len(body_len, "SegmentRemoved body length")?];
                write_u64(&mut bytes, 0, removed.segment_id);
                write_u64(&mut bytes, 8, count);
                for (index, highwater) in removed.highwaters.iter().enumerate() {
                    let offset = 16 + index * 16;
                    write_u64(&mut bytes, offset, highwater.stream_id.get());
                    write_u64(&mut bytes, offset + 8, highwater.sequence);
                }
                Ok(bytes)
            }
        }
    }

    fn decode(kind: u64, bytes: &[u8]) -> FormatResult<Self> {
        match kind {
            RELEASE_KIND => {
                if bytes.len() < 24 {
                    return Err(FormatError::new("Release body is shorter than 24 bytes"));
                }
                let released_count = read_u64(bytes, 8);
                let range_count = read_u64(bytes, 16);
                if released_count == 0 || range_count == 0 {
                    return Err(FormatError::new(
                        "Release counts must both be greater than zero",
                    ));
                }
                let expected = checked_len(24, range_count, 16, "Release body")?;
                if usize_len(expected, "Release body length")? != bytes.len() {
                    return Err(FormatError::new("Release body length mismatch"));
                }
                let range_count = usize_len(range_count, "range count")?;
                let mut ranges = Vec::with_capacity(range_count);
                for index in 0..range_count {
                    let offset = 24 + index * 16;
                    ranges.push(SequenceRange {
                        start: read_u64(bytes, offset),
                        len: read_u64(bytes, offset + 8),
                    });
                }
                let release = ReleaseBody {
                    stream_id: StreamId::new(read_u64(bytes, 0)),
                    ranges,
                };
                if release.released_count()? != released_count {
                    return Err(FormatError::new("Release released_count mismatch"));
                }
                Ok(Self::Release(release))
            }
            SEGMENT_SEALED_KIND => {
                if bytes.len() != 32 {
                    return Err(FormatError::new(
                        "SegmentSealed body must be exactly 32 bytes",
                    ));
                }
                let body = SegmentSealedBody {
                    segment_id: read_u64(bytes, 0),
                    segment_bytes: read_u64(bytes, 8),
                    epoch_count: read_u64(bytes, 16),
                    segment_digest: read_u64(bytes, 24),
                };
                if body.segment_id == u64::MAX || body.epoch_count == 0 {
                    return Err(FormatError::new("invalid SegmentSealed body"));
                }
                Ok(Self::SegmentSealed(body))
            }
            SEGMENT_REMOVED_KIND => {
                if bytes.len() < 16 {
                    return Err(FormatError::new(
                        "SegmentRemoved body is shorter than 16 bytes",
                    ));
                }
                let segment_id = read_u64(bytes, 0);
                if segment_id == u64::MAX {
                    return Err(FormatError::new("invalid SegmentRemoved segment ID"));
                }
                let count = read_u64(bytes, 8);
                let expected = checked_len(16, count, 16, "SegmentRemoved body")?;
                if usize_len(expected, "SegmentRemoved body length")? != bytes.len() {
                    return Err(FormatError::new("SegmentRemoved body length mismatch"));
                }
                let count = usize_len(count, "high-water count")?;
                let mut highwaters = Vec::with_capacity(count);
                for index in 0..count {
                    let offset = 16 + index * 16;
                    highwaters.push(StreamHighwater {
                        stream_id: StreamId::new(read_u64(bytes, offset)),
                        sequence: read_u64(bytes, offset + 8),
                    });
                }
                validate_highwaters(&highwaters)?;
                Ok(Self::SegmentRemoved(SegmentRemovedBody {
                    segment_id,
                    highwaters,
                }))
            }
            _ => Err(FormatError::new("unknown manifest frame kind")),
        }
    }
}

fn validate_ranges(ranges: &[SequenceRange], allow_empty: bool) -> FormatResult<()> {
    if ranges.is_empty() && !allow_empty {
        return Err(FormatError::new("range set must be nonempty"));
    }
    let mut previous_end: Option<u64> = None;
    for range in ranges {
        let end = range.end()?;
        if let Some(previous) = previous_end {
            let minimum = previous
                .checked_add(2)
                .ok_or_else(|| FormatError::new("range cannot follow u64::MAX"))?;
            if range.start < minimum {
                return Err(FormatError::new(
                    "ranges must be sorted, disjoint, and maximally coalesced",
                ));
            }
        }
        previous_end = Some(end);
    }
    Ok(())
}

fn validate_highwaters(highwaters: &[StreamHighwater]) -> FormatResult<()> {
    let mut previous = None;
    for highwater in highwaters {
        if previous.is_some_and(|value| highwater.stream_id <= value) {
            return Err(FormatError::new(
                "high-water stream IDs must be strictly increasing",
            ));
        }
        previous = Some(highwater.stream_id);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestFrame {
    pub(crate) manifest_seq: u64,
    pub(crate) body: ManifestBody,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManifestFrameHeader {
    pub(crate) manifest_seq: u64,
    pub(crate) kind: u64,
    pub(crate) body_len: u64,
    pub(crate) body_checksum: u64,
}

impl ManifestFrameHeader {
    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        let bytes =
            exact::<{ MANIFEST_FRAME_HEADER_LEN as usize }>(bytes, "manifest frame header")?;
        if &bytes[..8] != MANIFEST_FRAME_MAGIC {
            return Err(FormatError::new("invalid manifest frame magic"));
        }
        if read_u64(&bytes, 40) != checksum(&bytes[..40]) {
            return Err(FormatError::new("manifest frame header checksum mismatch"));
        }
        let header = Self {
            manifest_seq: read_u64(&bytes, 8),
            kind: read_u64(&bytes, 16),
            body_len: read_u64(&bytes, 24),
            body_checksum: read_u64(&bytes, 32),
        };
        if header.manifest_seq == 0 {
            return Err(FormatError::new("manifest sequence must be nonzero"));
        }
        if !matches!(
            header.kind,
            RELEASE_KIND | SEGMENT_SEALED_KIND | SEGMENT_REMOVED_KIND
        ) {
            return Err(FormatError::new("unknown manifest frame kind"));
        }
        Ok(header)
    }
}

impl ManifestFrame {
    #[cfg(test)]
    pub(crate) fn encode(&self) -> FormatResult<Vec<u8>> {
        Self::encode_parts(self.manifest_seq, &self.body)
    }

    pub(crate) fn encode_parts(manifest_seq: u64, body: &ManifestBody) -> FormatResult<Vec<u8>> {
        if manifest_seq == 0 {
            return Err(FormatError::new("manifest sequence must be nonzero"));
        }
        let encoded_body = body.encode()?;
        let body_len = u64::try_from(encoded_body.len())
            .map_err(|_| FormatError::new("manifest body length does not fit u64"))?;
        let total_len = MANIFEST_FRAME_HEADER_LEN
            .checked_add(body_len)
            .ok_or_else(|| FormatError::new("manifest frame length overflows u64"))?;
        let mut bytes = Vec::with_capacity(usize_len(total_len, "manifest frame length")?);
        let mut header = [0_u8; MANIFEST_FRAME_HEADER_LEN as usize];
        header[..8].copy_from_slice(MANIFEST_FRAME_MAGIC);
        write_u64(&mut header, 8, manifest_seq);
        write_u64(&mut header, 16, body.kind());
        write_u64(&mut header, 24, body_len);
        write_u64(&mut header, 32, checksum(&encoded_body));
        let value = checksum(&header[..40]);
        write_u64(&mut header, 40, value);
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&encoded_body);
        Ok(bytes)
    }

    pub(crate) fn decode(header: ManifestFrameHeader, body: &[u8]) -> FormatResult<Self> {
        if usize_len(header.body_len, "manifest body length")? != body.len() {
            return Err(FormatError::new("manifest frame body length mismatch"));
        }
        if checksum(body) != header.body_checksum {
            return Err(FormatError::new("manifest frame body checksum mismatch"));
        }
        Ok(Self {
            manifest_seq: header.manifest_seq,
            body: ManifestBody::decode(header.kind, body)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentLifecycle {
    Active,
    Sealed(SegmentFooter),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReleaseEncoding {
    Ranges(Vec<SequenceRange>),
    Bitmap(Vec<u64>),
}

impl ReleaseEncoding {
    pub(crate) fn from_flags(flags: &[bool]) -> Self {
        let ranges = ordinal_ranges(flags);
        if ranges.is_empty() {
            return Self::Ranges(Vec::new());
        }
        let bitmap = bitmap_words(flags);
        if ranges.len().saturating_mul(16) <= bitmap.len().saturating_mul(8) {
            Self::Ranges(ranges)
        } else {
            Self::Bitmap(bitmap)
        }
    }

    pub(crate) fn released_count(&self) -> FormatResult<u64> {
        match self {
            Self::Ranges(ranges) => ranges.iter().try_fold(0_u64, |total, range| {
                range.end()?;
                total
                    .checked_add(range.len)
                    .ok_or_else(|| FormatError::new("released_count overflows u64"))
            }),
            Self::Bitmap(words) => words.iter().try_fold(0_u64, |total, word| {
                total
                    .checked_add(u64::from(word.count_ones()))
                    .ok_or_else(|| FormatError::new("released_count overflows u64"))
            }),
        }
    }

    fn validate_canonical(&self, record_count: u64) -> FormatResult<u64> {
        if record_count == 0 {
            return Err(FormatError::new("record count must be nonzero"));
        }
        let bitmap_word_count = record_count.div_ceil(64);
        let bitmap_bytes = bitmap_word_count
            .checked_mul(8)
            .ok_or_else(|| FormatError::new("bitmap byte length overflows u64"))?;
        match self {
            Self::Ranges(ranges) => {
                validate_ranges(ranges, true)?;
                for range in ranges {
                    if range.end()? >= record_count {
                        return Err(FormatError::new("release range exceeds record count"));
                    }
                }
                let released_count = self.released_count()?;
                let range_count = u64::try_from(ranges.len())
                    .map_err(|_| FormatError::new("range count does not fit u64"))?;
                let range_bytes = range_count
                    .checked_mul(16)
                    .ok_or_else(|| FormatError::new("range byte length overflows u64"))?;
                if !ranges.is_empty() && range_bytes > bitmap_bytes {
                    return Err(FormatError::new(
                        "checkpoint release encoding is not canonical",
                    ));
                }
                Ok(released_count)
            }
            Self::Bitmap(words) => {
                if usize_len(bitmap_word_count, "bitmap word count")? != words.len() {
                    return Err(FormatError::new("bitmap word count mismatch"));
                }
                let final_bits = record_count % 64;
                if final_bits != 0 {
                    let valid_mask = u64::MAX >> (64 - final_bits);
                    if words.last().is_some_and(|word| word & !valid_mask != 0) {
                        return Err(FormatError::new("bitmap has nonzero unused high bits"));
                    }
                }

                let released_count = self.released_count()?;
                if released_count == 0 {
                    return Err(FormatError::new(
                        "checkpoint release encoding is not canonical",
                    ));
                }
                let mut range_count = 0_u64;
                let mut previous_one = false;
                for word in words {
                    let mut starts = *word & !(*word << 1);
                    if previous_one {
                        starts &= !1;
                    }
                    range_count = range_count
                        .checked_add(u64::from(starts.count_ones()))
                        .ok_or_else(|| FormatError::new("range count overflows u64"))?;
                    previous_one = word & (1_u64 << 63) != 0;
                }
                let range_bytes = range_count
                    .checked_mul(16)
                    .ok_or_else(|| FormatError::new("range byte length overflows u64"))?;
                if range_bytes <= bitmap_bytes {
                    return Err(FormatError::new(
                        "checkpoint release encoding is not canonical",
                    ));
                }
                Ok(released_count)
            }
        }
    }

    pub(crate) fn to_flags(&self, record_count: u64) -> FormatResult<Vec<bool>> {
        let count = usize_len(record_count, "record count")?;
        let mut flags = vec![false; count];
        match self {
            Self::Ranges(ranges) => {
                validate_ranges(ranges, true)?;
                for range in ranges {
                    let end = range.end()?;
                    if end >= record_count {
                        return Err(FormatError::new("release range exceeds record count"));
                    }
                    let start = usize_len(range.start, "range start")?;
                    let end = usize_len(end, "range end")?;
                    flags[start..=end].fill(true);
                }
            }
            Self::Bitmap(words) => {
                let expected = record_count.div_ceil(64);
                if usize_len(expected, "bitmap word count")? != words.len() {
                    return Err(FormatError::new("bitmap word count mismatch"));
                }
                for (word_index, word) in words.iter().copied().enumerate() {
                    for bit in 0..64 {
                        let ordinal = word_index * 64 + bit;
                        if ordinal >= count {
                            if word & (1_u64 << bit) != 0 {
                                return Err(FormatError::new(
                                    "bitmap has nonzero unused high bits",
                                ));
                            }
                        } else if word & (1_u64 << bit) != 0 {
                            flags[ordinal] = true;
                        }
                    }
                }
            }
        }
        Ok(flags)
    }
}

fn ordinal_ranges(flags: &[bool]) -> Vec<SequenceRange> {
    let mut ranges = Vec::new();
    let mut index = 0;
    while index < flags.len() {
        if !flags[index] {
            index += 1;
            continue;
        }
        let start = index;
        while index < flags.len() && flags[index] {
            index += 1;
        }
        ranges.push(SequenceRange {
            start: start as u64,
            len: (index - start) as u64,
        });
    }
    ranges
}

fn bitmap_words(flags: &[bool]) -> Vec<u64> {
    let mut words = vec![0_u64; flags.len().div_ceil(64)];
    for (index, released) in flags.iter().copied().enumerate() {
        if released {
            words[index / 64] |= 1_u64 << (index % 64);
        }
    }
    words
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointSegment {
    pub(crate) lifecycle: SegmentLifecycle,
    pub(crate) record_count: u64,
    pub(crate) releases: ReleaseEncoding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Checkpoint {
    pub(crate) root_id: RootId,
    pub(crate) last_applied_seq: u64,
    pub(crate) next_segment_id: u64,
    pub(crate) stream_highwaters: BTreeMap<StreamId, u64>,
    pub(crate) segments: BTreeMap<u64, CheckpointSegment>,
}

impl Checkpoint {
    pub(crate) fn encode(&self) -> FormatResult<Vec<u8>> {
        self.validate()?;
        let stream_count = u64::try_from(self.stream_highwaters.len())
            .map_err(|_| FormatError::new("stream count does not fit u64"))?;
        let segment_count = u64::try_from(self.segments.len())
            .map_err(|_| FormatError::new("segment count does not fit u64"))?;
        let mut body = Vec::new();
        body.extend_from_slice(&self.next_segment_id.to_le_bytes());
        body.extend_from_slice(&stream_count.to_le_bytes());
        body.extend_from_slice(&segment_count.to_le_bytes());

        for (stream_id, highwater) in &self.stream_highwaters {
            body.extend_from_slice(&stream_id.get().to_le_bytes());
            body.extend_from_slice(&highwater.to_le_bytes());
        }

        for (segment_id, segment) in &self.segments {
            let released_count = segment.releases.released_count()?;
            let mut fixed = [0_u8; 72];
            write_u64(&mut fixed, 0, *segment_id);
            match segment.lifecycle {
                SegmentLifecycle::Active => write_u64(&mut fixed, 8, ACTIVE_LIFECYCLE),
                SegmentLifecycle::Sealed(footer) => {
                    write_u64(&mut fixed, 8, SEALED_LIFECYCLE);
                    write_u64(&mut fixed, 16, footer.segment_bytes);
                    write_u64(&mut fixed, 24, footer.epoch_count);
                    write_u64(&mut fixed, 32, footer.segment_digest);
                }
            }
            write_u64(&mut fixed, 40, segment.record_count);
            write_u64(&mut fixed, 48, released_count);
            match &segment.releases {
                ReleaseEncoding::Ranges(ranges) => {
                    write_u64(&mut fixed, 56, RANGE_ENCODING);
                    write_u64(
                        &mut fixed,
                        64,
                        u64::try_from(ranges.len())
                            .map_err(|_| FormatError::new("range count does not fit u64"))?,
                    );
                }
                ReleaseEncoding::Bitmap(words) => {
                    write_u64(&mut fixed, 56, BITMAP_ENCODING);
                    write_u64(
                        &mut fixed,
                        64,
                        u64::try_from(words.len())
                            .map_err(|_| FormatError::new("bitmap count does not fit u64"))?,
                    );
                }
            }
            body.extend_from_slice(&fixed);
            match &segment.releases {
                ReleaseEncoding::Ranges(ranges) => {
                    for range in ranges {
                        body.extend_from_slice(&range.start.to_le_bytes());
                        body.extend_from_slice(&range.len.to_le_bytes());
                    }
                }
                ReleaseEncoding::Bitmap(words) => {
                    for word in words {
                        body.extend_from_slice(&word.to_le_bytes());
                    }
                }
            }
        }

        let body_len = u64::try_from(body.len())
            .map_err(|_| FormatError::new("checkpoint body length does not fit u64"))?;
        let total_len = CHECKPOINT_HEADER_LEN
            .checked_add(body_len)
            .ok_or_else(|| FormatError::new("checkpoint length overflows u64"))?;
        let mut output = Vec::with_capacity(usize_len(total_len, "checkpoint length")?);
        let mut header = [0_u8; CHECKPOINT_HEADER_LEN as usize];
        header[..8].copy_from_slice(CHECKPOINT_MAGIC);
        header[8..24].copy_from_slice(&self.root_id.to_bytes());
        write_u64(&mut header, 24, self.last_applied_seq);
        write_u64(&mut header, 32, body_len);
        write_u64(&mut header, 40, checksum(&body));
        let value = checksum(&header[..48]);
        write_u64(&mut header, 48, value);
        output.extend_from_slice(&header);
        output.extend_from_slice(&body);
        Ok(output)
    }

    pub(crate) fn decode(bytes: &[u8]) -> FormatResult<Self> {
        if bytes.len() < CHECKPOINT_HEADER_LEN as usize {
            return Err(FormatError::new("checkpoint is shorter than its header"));
        }
        let header = &bytes[..CHECKPOINT_HEADER_LEN as usize];
        if &header[..8] != CHECKPOINT_MAGIC {
            return Err(FormatError::new("invalid checkpoint magic"));
        }
        if read_u64(header, 48) != checksum(&header[..48]) {
            return Err(FormatError::new("checkpoint header checksum mismatch"));
        }
        let body_len = read_u64(header, 32);
        let expected = CHECKPOINT_HEADER_LEN
            .checked_add(body_len)
            .ok_or_else(|| FormatError::new("checkpoint length overflows u64"))?;
        if usize_len(expected, "checkpoint length")? != bytes.len() {
            return Err(FormatError::new("checkpoint body length mismatch"));
        }
        let body = &bytes[CHECKPOINT_HEADER_LEN as usize..];
        if checksum(body) != read_u64(header, 40) {
            return Err(FormatError::new("checkpoint body checksum mismatch"));
        }
        if body.len() < 24 {
            return Err(FormatError::new(
                "checkpoint body is shorter than its prefix",
            ));
        }

        let mut root_id = [0_u8; RootId::LEN];
        root_id.copy_from_slice(&header[8..24]);
        let next_segment_id = read_u64(body, 0);
        let stream_count = usize_len(read_u64(body, 8), "stream count")?;
        let segment_count = usize_len(read_u64(body, 16), "segment count")?;
        let stream_bytes = stream_count
            .checked_mul(16)
            .ok_or_else(|| FormatError::new("stream entries overflow usize"))?;
        let mut offset = 24_usize
            .checked_add(stream_bytes)
            .ok_or_else(|| FormatError::new("checkpoint stream boundary overflows usize"))?;
        if offset > body.len() {
            return Err(FormatError::new("checkpoint stream entries exceed body"));
        }

        let mut stream_highwaters = BTreeMap::new();
        let mut previous_stream = None;
        for index in 0..stream_count {
            let entry = 24 + index * 16;
            let stream_id = StreamId::new(read_u64(body, entry));
            if previous_stream.is_some_and(|previous| stream_id <= previous) {
                return Err(FormatError::new(
                    "checkpoint stream IDs must be strictly increasing",
                ));
            }
            previous_stream = Some(stream_id);
            stream_highwaters.insert(stream_id, read_u64(body, entry + 8));
        }

        let mut segments = BTreeMap::new();
        let mut previous_segment = None;
        let mut active_segment = None;
        for _ in 0..segment_count {
            let fixed_end = offset
                .checked_add(72)
                .ok_or_else(|| FormatError::new("checkpoint segment entry overflows usize"))?;
            if fixed_end > body.len() {
                return Err(FormatError::new(
                    "checkpoint segment fixed fields exceed body",
                ));
            }
            let fixed = &body[offset..fixed_end];
            offset = fixed_end;
            let segment_id = read_u64(fixed, 0);
            if segment_id == u64::MAX {
                return Err(FormatError::new("u64::MAX is not a valid segment ID"));
            }
            if previous_segment.is_some_and(|previous| segment_id <= previous) {
                return Err(FormatError::new(
                    "checkpoint segment IDs must be strictly increasing",
                ));
            }
            previous_segment = Some(segment_id);

            let lifecycle = match read_u64(fixed, 8) {
                ACTIVE_LIFECYCLE => {
                    if read_u64(fixed, 16) != 0
                        || read_u64(fixed, 24) != 0
                        || read_u64(fixed, 32) != 0
                    {
                        return Err(FormatError::new(
                            "active checkpoint segment has nonzero footer fields",
                        ));
                    }
                    if active_segment.replace(segment_id).is_some() {
                        return Err(FormatError::new(
                            "checkpoint contains more than one active segment",
                        ));
                    }
                    SegmentLifecycle::Active
                }
                SEALED_LIFECYCLE => {
                    let footer = SegmentFooter {
                        segment_id,
                        segment_bytes: read_u64(fixed, 16),
                        epoch_count: read_u64(fixed, 24),
                        segment_digest: read_u64(fixed, 32),
                    };
                    footer.validate()?;
                    SegmentLifecycle::Sealed(footer)
                }
                _ => return Err(FormatError::new("unknown checkpoint lifecycle")),
            };

            let record_count = read_u64(fixed, 40);
            if record_count == 0 {
                return Err(FormatError::new(
                    "checkpoint segment record_count must be nonzero",
                ));
            }
            let released_count = read_u64(fixed, 48);
            if released_count > record_count {
                return Err(FormatError::new(
                    "checkpoint released_count exceeds record_count",
                ));
            }
            let encoding = read_u64(fixed, 56);
            let unit_count = usize_len(read_u64(fixed, 64), "release encoding unit count")?;
            let releases = match encoding {
                RANGE_ENCODING => {
                    let bytes_len = unit_count
                        .checked_mul(16)
                        .ok_or_else(|| FormatError::new("range bytes overflow usize"))?;
                    let units_end = offset
                        .checked_add(bytes_len)
                        .ok_or_else(|| FormatError::new("range boundary overflows usize"))?;
                    if units_end > body.len() {
                        return Err(FormatError::new("range units exceed checkpoint body"));
                    }
                    let mut ranges = Vec::with_capacity(unit_count);
                    for index in 0..unit_count {
                        let entry = offset + index * 16;
                        ranges.push(SequenceRange {
                            start: read_u64(body, entry),
                            len: read_u64(body, entry + 8),
                        });
                    }
                    offset = units_end;
                    ReleaseEncoding::Ranges(ranges)
                }
                BITMAP_ENCODING => {
                    let bytes_len = unit_count
                        .checked_mul(8)
                        .ok_or_else(|| FormatError::new("bitmap bytes overflow usize"))?;
                    let units_end = offset
                        .checked_add(bytes_len)
                        .ok_or_else(|| FormatError::new("bitmap boundary overflows usize"))?;
                    if units_end > body.len() {
                        return Err(FormatError::new("bitmap units exceed checkpoint body"));
                    }
                    let mut words = Vec::with_capacity(unit_count);
                    for index in 0..unit_count {
                        words.push(read_u64(body, offset + index * 8));
                    }
                    offset = units_end;
                    ReleaseEncoding::Bitmap(words)
                }
                _ => return Err(FormatError::new("unknown checkpoint release encoding")),
            };

            let actual_released = releases.validate_canonical(record_count)?;
            if actual_released != released_count {
                return Err(FormatError::new(
                    "checkpoint release encoding count mismatch",
                ));
            }
            segments.insert(
                segment_id,
                CheckpointSegment {
                    lifecycle,
                    record_count,
                    releases,
                },
            );
        }

        if offset != body.len() {
            return Err(FormatError::new("checkpoint body has trailing bytes"));
        }
        if let Some(active) = active_segment {
            if segments.last_key_value().map(|(id, _)| *id) != Some(active) {
                return Err(FormatError::new(
                    "active checkpoint segment must have greatest extant ID",
                ));
            }
        }
        if segments
            .last_key_value()
            .is_some_and(|(id, _)| next_segment_id <= *id)
        {
            return Err(FormatError::new(
                "next_segment_id must exceed every extant segment ID",
            ));
        }

        let checkpoint = Self {
            root_id: RootId::from_bytes(root_id),
            last_applied_seq: read_u64(header, 24),
            next_segment_id,
            stream_highwaters,
            segments,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    fn validate(&self) -> FormatResult<()> {
        if let Some((&greatest, _)) = self.segments.last_key_value() {
            if self.next_segment_id <= greatest {
                return Err(FormatError::new(
                    "next_segment_id must exceed every extant segment ID",
                ));
            }
        }
        let active = self
            .segments
            .iter()
            .filter(|(_, segment)| matches!(segment.lifecycle, SegmentLifecycle::Active))
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        if active.len() > 1 {
            return Err(FormatError::new(
                "checkpoint contains more than one active segment",
            ));
        }
        if active.first().is_some_and(|id| {
            self.segments.last_key_value().map(|(greatest, _)| greatest) != Some(id)
        }) {
            return Err(FormatError::new(
                "active checkpoint segment must have greatest extant ID",
            ));
        }
        for (segment_id, segment) in &self.segments {
            if *segment_id == u64::MAX || segment.record_count == 0 {
                return Err(FormatError::new("invalid checkpoint segment identity"));
            }
            if let SegmentLifecycle::Sealed(footer) = segment.lifecycle {
                if footer.segment_id != *segment_id {
                    return Err(FormatError::new(
                        "checkpoint footer segment ID does not match entry",
                    ));
                }
                footer.validate()?;
            }
            segment.releases.validate_canonical(segment.record_count)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root_id() -> RootId {
        RootId::from_bytes(*b"0123456789abcdef")
    }

    #[test]
    fn fixed_structures_round_trip_and_have_specified_lengths() {
        let root = RootSuperblock { root_id: root_id() };
        assert_eq!(RootSuperblock::decode(&root.encode()).unwrap(), root);

        let segment = SegmentHeader {
            root_id: root_id(),
            segment_id: 7,
            created_at_unix_millis: 42,
        };
        assert_eq!(
            SegmentHeader::decode(&segment.encode().unwrap()).unwrap(),
            segment
        );

        let descriptor = RecordDescriptor::new(b"meta", b"payload").unwrap();
        assert_eq!(
            RecordDescriptor::decode(&descriptor.encode()).unwrap(),
            descriptor
        );

        let epoch = EpochHeader {
            stream_id: StreamId::new(9),
            first_sequence: 11,
            record_count: 1,
            records_bytes: descriptor.encoded_len().unwrap(),
        };
        assert_eq!(
            EpochHeader::decode(&epoch.encode().unwrap()).unwrap(),
            epoch
        );

        let commit = EpochCommit {
            epoch_start: 48,
            epoch_bytes: 48 + descriptor.encoded_len().unwrap() + 40,
            epoch_digest: 17,
        };
        assert_eq!(EpochCommit::decode(&commit.encode()).unwrap(), commit);

        let footer = SegmentFooter {
            segment_id: 7,
            segment_bytes: 4096,
            epoch_count: 3,
            segment_digest: 19,
        };
        assert_eq!(
            SegmentFooter::decode(&footer.encode().unwrap()).unwrap(),
            footer
        );

        let log = ManifestLogHeader {
            root_id: root_id(),
            base_seq: 1,
        };
        assert_eq!(ManifestLogHeader::decode(&log.encode()).unwrap(), log);
    }

    #[test]
    fn manifest_frames_round_trip_canonical_binary_bodies() {
        let bodies = [
            ManifestBody::Release(ReleaseBody {
                stream_id: StreamId::new(4),
                ranges: vec![
                    SequenceRange { start: 1, len: 2 },
                    SequenceRange { start: 7, len: 1 },
                ],
            }),
            ManifestBody::SegmentSealed(SegmentSealedBody {
                segment_id: 2,
                segment_bytes: 100,
                epoch_count: 3,
                segment_digest: 4,
            }),
            ManifestBody::SegmentRemoved(SegmentRemovedBody {
                segment_id: 2,
                highwaters: vec![StreamHighwater {
                    stream_id: StreamId::new(4),
                    sequence: 8,
                }],
            }),
        ];

        for (index, body) in bodies.into_iter().enumerate() {
            let frame = ManifestFrame {
                manifest_seq: (index + 1) as u64,
                body,
            };
            let encoded = frame.encode().unwrap();
            let header =
                ManifestFrameHeader::decode(&encoded[..MANIFEST_FRAME_HEADER_LEN as usize])
                    .unwrap();
            let decoded =
                ManifestFrame::decode(header, &encoded[MANIFEST_FRAME_HEADER_LEN as usize..])
                    .unwrap();
            assert_eq!(decoded, frame);
        }
    }

    #[test]
    fn checkpoint_uses_ranges_on_tie_and_bitmap_when_dense() {
        let sparse_flags = [true, false, false, false, false, false, false, false];
        assert!(matches!(
            ReleaseEncoding::from_flags(&sparse_flags),
            ReleaseEncoding::Bitmap(_)
        ));

        let tie_flags = [true; 128];
        assert!(matches!(
            ReleaseEncoding::from_flags(&tie_flags),
            ReleaseEncoding::Ranges(_)
        ));

        let mut streams = BTreeMap::new();
        streams.insert(StreamId::new(4), 127);
        let mut segments = BTreeMap::new();
        segments.insert(
            0,
            CheckpointSegment {
                lifecycle: SegmentLifecycle::Active,
                record_count: 128,
                releases: ReleaseEncoding::from_flags(&tie_flags),
            },
        );
        let checkpoint = Checkpoint {
            root_id: root_id(),
            last_applied_seq: 9,
            next_segment_id: 1,
            stream_highwaters: streams,
            segments,
        };
        let encoded = checkpoint.encode().unwrap();
        assert_eq!(Checkpoint::decode(&encoded).unwrap(), checkpoint);
    }

    #[test]
    fn corruption_of_any_fixed_checksum_is_rejected() {
        let mut encoded = RootSuperblock { root_id: root_id() }.encode();
        encoded[16] ^= 1;
        assert!(RootSuperblock::decode(&encoded).is_err());

        let frame = ManifestFrame {
            manifest_seq: 1,
            body: ManifestBody::SegmentRemoved(SegmentRemovedBody {
                segment_id: 0,
                highwaters: Vec::new(),
            }),
        };
        let mut encoded = frame.encode().unwrap();
        encoded[MANIFEST_FRAME_HEADER_LEN as usize] ^= 1;
        let header =
            ManifestFrameHeader::decode(&encoded[..MANIFEST_FRAME_HEADER_LEN as usize]).unwrap();
        assert!(
            ManifestFrame::decode(header, &encoded[MANIFEST_FRAME_HEADER_LEN as usize..]).is_err()
        );
    }

    #[test]
    fn epoch_header_rejects_impossible_descriptor_count_before_allocation() {
        let mut bytes = [0_u8; EPOCH_HEADER_LEN as usize];
        bytes[..8].copy_from_slice(EPOCH_MAGIC);
        write_u64(&mut bytes, 8, 1);
        write_u64(&mut bytes, 16, 0);
        write_u64(&mut bytes, 24, 2);
        write_u64(&mut bytes, 32, 2 * RECORD_DESCRIPTOR_LEN - 1);
        let value = checksum(&bytes[..40]);
        write_u64(&mut bytes, 40, value);

        assert!(EpochHeader::decode(&bytes).is_err());
    }

    #[test]
    fn checkpoint_release_validation_is_bounded_and_counts_cross_word_runs() {
        assert_eq!(
            ReleaseEncoding::Ranges(Vec::new())
                .validate_canonical(u64::MAX)
                .unwrap(),
            0
        );
        assert!(ReleaseEncoding::Bitmap(vec![1_u64 << 63, 1])
            .validate_canonical(128)
            .is_err());
    }

    #[test]
    fn format_seed_is_the_documented_camus_v1_value() {
        assert_eq!(FORMAT_SEED, 0x2131_5653_554d_4143);
    }
}
