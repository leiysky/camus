use super::files::{
    read_exact_at, segment_path, segment_temporary_path, sync_directory, write_all_vectored,
};
use crate::error::{DurabilityOutcome, Error, Result};
use crate::format::{
    checksum, epoch_digest, segment_digest, EpochCommit, EpochHeader, RecordDescriptor,
    SegmentFooter, SegmentHeader, EPOCH_COMMIT_LEN, EPOCH_HEADER_LEN, RECORD_DESCRIPTOR_LEN,
    SEGMENT_FOOTER_LEN, SEGMENT_FOOTER_MAGIC, SEGMENT_HEADER_LEN,
};
use crate::model::{PendingRecord, Record, RecordId, RootId, StreamId};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
#[cfg(test)]
use std::io;
use std::io::{IoSlice, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub(super) struct StoredRecord {
    pub(super) stream_id: StreamId,
    pub(super) sequence: u64,
    pub(super) descriptor_offset: u64,
    pub(super) metadata_offset: u64,
    pub(super) metadata_len: u64,
    pub(super) metadata_checksum: u64,
    pub(super) payload_offset: u64,
    pub(super) payload_len: u64,
    pub(super) payload_checksum: u64,
    pub(super) released: bool,
}

#[derive(Clone, Debug)]
pub(super) struct EpochBoundary {
    pub(super) record_count: u64,
    pub(super) commit: [u8; EPOCH_COMMIT_LEN as usize],
}

pub(super) struct Segment {
    pub(super) id: u64,
    pub(super) path: PathBuf,
    pub(super) file: File,
    pub(super) header: SegmentHeader,
    pub(super) header_bytes: [u8; SEGMENT_HEADER_LEN as usize],
    pub(super) file_len: u64,
    pub(super) records: Vec<StoredRecord>,
    unique_streams: usize,
    pub(super) unreleased_records: u64,
    pub(super) epochs: Vec<EpochBoundary>,
    pub(super) footer: Option<SegmentFooter>,
    pub(super) repaired_tail: bool,
}

pub(super) struct PreparedEpoch {
    pub(super) stream_id: StreamId,
    pub(super) first_sequence: u64,
    pub(super) encoded_bytes: u64,
    header: [u8; EPOCH_HEADER_LEN as usize],
    descriptors: Vec<[u8; RECORD_DESCRIPTOR_LEN as usize]>,
    records: Vec<Record>,
}

struct WrittenEpoch {
    records: Vec<StoredRecord>,
    commit: [u8; EPOCH_COMMIT_LEN as usize],
    end_offset: u64,
}

struct PendingEpochWrite {
    epoch: PreparedEpoch,
    records: Vec<StoredRecord>,
    commit: [u8; EPOCH_COMMIT_LEN as usize],
    end_offset: u64,
}

// Bound shared read backing buffers so retaining one returned body cannot pin
// an arbitrarily large batch. A single larger record still forms one span.
const MAX_COALESCED_READ_BYTES: u64 = 1024 * 1024;

enum ScanEpoch {
    Complete {
        records: Vec<StoredRecord>,
        commit: [u8; EPOCH_COMMIT_LEN as usize],
        end_offset: u64,
    },
    Incomplete,
}

impl PreparedEpoch {
    pub(super) fn new(
        stream_id: StreamId,
        first_sequence: u64,
        records: Vec<Record>,
    ) -> Result<Self> {
        if records.is_empty() {
            return Err(Error::EmptyAppend);
        }
        let record_count = u64::try_from(records.len())
            .map_err(|_| Error::invalid_config("append record count does not fit u64"))?;
        first_sequence
            .checked_add(record_count - 1)
            .ok_or(Error::SequenceExhausted { stream_id })?;

        let mut descriptors = Vec::with_capacity(records.len());
        let mut records_bytes = 0_u64;
        for record in &records {
            let descriptor = RecordDescriptor::new(&record.metadata, &record.payload)
                .map_err(|error| Error::invalid_config(error.to_string()))?;
            records_bytes = records_bytes
                .checked_add(
                    descriptor
                        .encoded_len()
                        .map_err(|error| Error::invalid_config(error.to_string()))?,
                )
                .ok_or_else(|| Error::invalid_config("encoded epoch length overflowed"))?;
            descriptors.push(descriptor.encode());
        }
        let header = EpochHeader {
            stream_id,
            first_sequence,
            record_count,
            records_bytes,
        }
        .encode()
        .map_err(|error| Error::invalid_config(error.to_string()))?;
        let encoded_bytes = EPOCH_HEADER_LEN
            .checked_add(records_bytes)
            .and_then(|bytes| bytes.checked_add(EPOCH_COMMIT_LEN))
            .ok_or_else(|| Error::invalid_config("encoded epoch length overflowed"))?;
        Ok(Self {
            stream_id,
            first_sequence,
            encoded_bytes,
            header,
            descriptors,
            records,
        })
    }

    pub(super) fn record_count(&self) -> usize {
        self.records.len()
    }
}

impl Segment {
    pub(super) fn scan(
        path: PathBuf,
        expected_root: RootId,
        expected_segment_id: u64,
        allow_active_tail_repair: bool,
        checkpoint_record_count: Option<u64>,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| {
                Error::io(
                    "open data segment",
                    &path,
                    DurabilityOutcome::NotApplicable,
                    error,
                )
            })?;
        let mut file_len = file
            .metadata()
            .map_err(|error| {
                Error::io(
                    "read segment metadata",
                    &path,
                    DurabilityOutcome::NotApplicable,
                    error,
                )
            })?
            .len();
        if file_len < SEGMENT_HEADER_LEN {
            return Err(Error::corruption(
                &path,
                0,
                "segment is shorter than its authoritative header",
            ));
        }
        let mut header_bytes = [0_u8; SEGMENT_HEADER_LEN as usize];
        read_exact_at(&file, &path, &mut header_bytes, 0)?;
        let header = SegmentHeader::decode(&header_bytes)
            .map_err(|error| Error::corruption(&path, 0, error.to_string()))?;
        if header.root_id != expected_root {
            return Err(Error::corruption(
                &path,
                8,
                "segment root ID does not match ROOT",
            ));
        }
        if header.segment_id != expected_segment_id {
            return Err(Error::corruption(
                &path,
                24,
                "segment ID does not match canonical file name",
            ));
        }

        let mut records = Vec::new();
        let mut epochs = Vec::new();
        let mut footer = None;
        let mut repaired_tail = false;
        let mut offset = SEGMENT_HEADER_LEN;
        while offset < file_len {
            let remaining = file_len - offset;
            if remaining < EPOCH_HEADER_LEN {
                if !tail_repair_allowed(allow_active_tail_repair, checkpoint_record_count, &epochs)
                {
                    return Err(Error::corruption(
                        &path,
                        offset,
                        "incomplete segment tail is not repairable",
                    ));
                }
                repair_tail(&mut file, &path, offset)?;
                file_len = offset;
                repaired_tail = true;
                break;
            }

            let mut prefix = [0_u8; EPOCH_HEADER_LEN as usize];
            read_exact_at(&file, &path, &mut prefix, offset)?;
            if &prefix[..8] == SEGMENT_FOOTER_MAGIC {
                if remaining != SEGMENT_FOOTER_LEN {
                    return Err(Error::corruption(
                        &path,
                        offset,
                        "segment footer is not the final 48 bytes",
                    ));
                }
                let decoded = SegmentFooter::decode(&prefix)
                    .map_err(|error| Error::corruption(&path, offset, error.to_string()))?;
                footer = Some(decoded);
                break;
            }

            match scan_epoch(&file, &path, offset, file_len)? {
                ScanEpoch::Complete {
                    records: epoch_records,
                    commit,
                    end_offset,
                } => {
                    records.extend(epoch_records);
                    epochs.push(EpochBoundary {
                        record_count: u64::try_from(records.len()).map_err(|_| {
                            Error::corruption(
                                &path,
                                offset,
                                "segment record count does not fit u64",
                            )
                        })?,
                        commit,
                    });
                    offset = end_offset;
                }
                ScanEpoch::Incomplete => {
                    if !tail_repair_allowed(
                        allow_active_tail_repair,
                        checkpoint_record_count,
                        &epochs,
                    ) {
                        return Err(Error::corruption(
                            &path,
                            offset,
                            "incomplete first or sealed epoch is not repairable",
                        ));
                    }
                    repair_tail(&mut file, &path, offset)?;
                    file_len = offset;
                    repaired_tail = true;
                    break;
                }
            }
        }

        if epochs.is_empty() {
            return Err(Error::corruption(
                &path,
                SEGMENT_HEADER_LEN,
                "canonical segment contains no complete epoch",
            ));
        }
        let mut stream_sequences = BTreeMap::new();
        for record in &records {
            if let Some(previous) = stream_sequences.insert(record.stream_id, record.sequence) {
                let expected = previous.checked_add(1).ok_or_else(|| {
                    Error::corruption(
                        &path,
                        record.descriptor_offset,
                        "record follows an exhausted stream sequence",
                    )
                })?;
                if record.sequence != expected {
                    return Err(Error::corruption(
                        &path,
                        record.descriptor_offset,
                        format!(
                            "stream sequence {} follows {previous} inside one segment",
                            record.sequence
                        ),
                    ));
                }
            }
        }
        if let Some(seal) = footer {
            if seal.segment_id != expected_segment_id {
                return Err(Error::corruption(
                    &path,
                    file_len - SEGMENT_FOOTER_LEN + 8,
                    "footer segment ID does not match header",
                ));
            }
            if seal.segment_bytes != file_len {
                return Err(Error::corruption(
                    &path,
                    file_len - SEGMENT_FOOTER_LEN + 16,
                    "footer segment length does not match file",
                ));
            }
            if seal.epoch_count
                != u64::try_from(epochs.len()).map_err(|_| {
                    Error::corruption(&path, file_len, "segment epoch count does not fit u64")
                })?
            {
                return Err(Error::corruption(
                    &path,
                    file_len - SEGMENT_FOOTER_LEN + 24,
                    "footer epoch count does not match segment",
                ));
            }
            let commits = epochs.iter().map(|epoch| epoch.commit).collect::<Vec<_>>();
            if seal.segment_digest != segment_digest(&header_bytes, &commits) {
                return Err(Error::corruption(
                    &path,
                    file_len - SEGMENT_FOOTER_LEN + 32,
                    "footer segment digest mismatch",
                ));
            }
        }

        file.seek(SeekFrom::End(0)).map_err(|error| {
            Error::io(
                "seek data segment",
                &path,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })?;
        let unreleased_records = u64::try_from(records.len())
            .map_err(|_| Error::corruption(&path, 0, "segment record count does not fit u64"))?;
        let unique_streams = stream_sequences.len();
        Ok(Self {
            id: expected_segment_id,
            path,
            file,
            header,
            header_bytes,
            file_len,
            records,
            unique_streams,
            unreleased_records,
            epochs,
            footer,
            repaired_tail,
        })
    }

    pub(super) fn create(
        directory: &Path,
        root_id: RootId,
        segment_id: u64,
        created_at_unix_millis: u64,
        epochs: Vec<PreparedEpoch>,
    ) -> Result<(Self, Vec<Vec<RecordId>>)> {
        if epochs.is_empty() {
            return Err(Error::invalid_config(
                "segment creation requires a first append group",
            ));
        }
        let temporary = segment_temporary_path(directory, segment_id);
        let canonical = segment_path(directory, segment_id);
        let header = SegmentHeader {
            root_id,
            segment_id,
            created_at_unix_millis,
        };
        let header_bytes = header
            .encode()
            .map_err(|error| Error::corruption(&temporary, 0, error.to_string()))?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                Error::io(
                    "create temporary segment",
                    &temporary,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
        file.write_all(&header_bytes).map_err(|error| {
            Error::io(
                "write segment header",
                &temporary,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;

        let mut offset = SEGMENT_HEADER_LEN;
        let mut records = Vec::new();
        let mut stream_ids = BTreeSet::new();
        let mut boundaries = Vec::new();
        let mut ids = Vec::with_capacity(epochs.len());
        for written in write_epoch_group(&mut file, &temporary, offset, epochs)? {
            stream_ids.extend(written.records.iter().map(|record| record.stream_id));
            ids.push(
                written
                    .records
                    .iter()
                    .map(|record| RecordId::from_parts(root_id, record.stream_id, record.sequence))
                    .collect(),
            );
            offset = written.end_offset;
            records.extend(written.records);
            boundaries.push(EpochBoundary {
                record_count: u64::try_from(records.len()).map_err(|_| {
                    Error::corruption(&temporary, offset, "segment record count does not fit u64")
                })?,
                commit: written.commit,
            });
        }
        #[cfg(test)]
        crate::test_crash::inject_io("segment.create.sync_data").map_err(|error| {
            Error::io(
                "sync temporary segment",
                &temporary,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        file.sync_data().map_err(|error| {
            Error::io(
                "sync temporary segment",
                &temporary,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        #[cfg(test)]
        crate::test_crash::hit("segment.create.after_data_sync");
        #[cfg(test)]
        crate::test_crash::inject_io("segment.create.rename").map_err(|error| {
            Error::io(
                "publish data segment",
                &canonical,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        fs::rename(&temporary, &canonical).map_err(|error| {
            Error::io(
                "publish data segment",
                &canonical,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        #[cfg(test)]
        crate::test_crash::hit("segment.create.after_rename");
        #[cfg(test)]
        crate::test_crash::inject_io("segment.create.directory_sync").map_err(|error| {
            Error::io(
                "sync segment directory",
                directory,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        sync_directory(directory, DurabilityOutcome::Unknown)?;
        #[cfg(test)]
        crate::test_crash::hit("segment.create.after_directory_sync");

        let unreleased_records = u64::try_from(records.len()).map_err(|_| {
            Error::corruption(&canonical, 0, "segment record count does not fit u64")
        })?;
        Ok((
            Self {
                id: segment_id,
                path: canonical,
                file,
                header,
                header_bytes,
                file_len: offset,
                records,
                unique_streams: stream_ids.len(),
                unreleased_records,
                epochs: boundaries,
                footer: None,
                repaired_tail: false,
            },
            ids,
        ))
    }

    pub(super) fn append(
        &mut self,
        root_id: RootId,
        epochs: Vec<PreparedEpoch>,
        new_streams: usize,
    ) -> Result<Vec<Vec<RecordId>>> {
        let unique_streams = self
            .unique_streams
            .checked_add(new_streams)
            .ok_or_else(|| {
                Error::corruption(&self.path, self.file_len, "segment stream count overflow")
            })?;
        if self.footer.is_some() {
            return Err(Error::corruption(
                &self.path,
                self.file_len,
                "cannot append to a physically sealed segment",
            ));
        }
        self.file.seek(SeekFrom::End(0)).map_err(|error| {
            Error::io(
                "seek active segment",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        let mut offset = self.file_len;
        let written_epochs = write_epoch_group(&mut self.file, &self.path, offset, epochs)?;
        offset = written_epochs
            .last()
            .map_or(offset, |written| written.end_offset);
        #[cfg(test)]
        crate::test_crash::inject_io("segment.append.sync_data").map_err(|error| {
            Error::io(
                "sync append group",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        self.file.sync_data().map_err(|error| {
            Error::io(
                "sync append group",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        #[cfg(test)]
        crate::test_crash::hit("segment.append.after_data_sync");

        self.unique_streams = unique_streams;
        let mut ids = Vec::with_capacity(written_epochs.len());
        for written in written_epochs {
            self.unreleased_records = self
                .unreleased_records
                .checked_add(u64::try_from(written.records.len()).map_err(|_| {
                    Error::corruption(&self.path, offset, "record count does not fit u64")
                })?)
                .ok_or_else(|| {
                    Error::corruption(&self.path, offset, "unreleased record count overflow")
                })?;
            ids.push(
                written
                    .records
                    .iter()
                    .map(|record| RecordId::from_parts(root_id, record.stream_id, record.sequence))
                    .collect(),
            );
            self.records.extend(written.records);
            self.epochs.push(EpochBoundary {
                record_count: u64::try_from(self.records.len()).map_err(|_| {
                    Error::corruption(&self.path, offset, "segment record count does not fit u64")
                })?,
                commit: written.commit,
            });
        }
        self.file_len = offset;
        Ok(ids)
    }

    pub(super) fn refresh_unreleased_records(&mut self) -> Result<()> {
        self.unreleased_records = u64::try_from(
            self.records
                .iter()
                .filter(|record| !record.released)
                .count(),
        )
        .map_err(|_| Error::corruption(&self.path, 0, "record count does not fit u64"))?;
        Ok(())
    }

    pub(super) fn seal_data(&mut self) -> Result<SegmentFooter> {
        if let Some(footer) = self.footer {
            return Ok(footer);
        }
        let commits = self
            .epochs
            .iter()
            .map(|epoch| epoch.commit)
            .collect::<Vec<_>>();
        let footer = SegmentFooter {
            segment_id: self.id,
            segment_bytes: self
                .file_len
                .checked_add(SEGMENT_FOOTER_LEN)
                .ok_or_else(|| {
                    Error::corruption(&self.path, self.file_len, "footer length overflow")
                })?,
            epoch_count: u64::try_from(self.epochs.len()).map_err(|_| {
                Error::corruption(&self.path, self.file_len, "epoch count does not fit u64")
            })?,
            segment_digest: segment_digest(&self.header_bytes, &commits),
        };
        let bytes = footer
            .encode()
            .map_err(|error| Error::corruption(&self.path, self.file_len, error.to_string()))?;
        self.file.seek(SeekFrom::End(0)).map_err(|error| {
            Error::io(
                "seek active segment for seal",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        #[cfg(test)]
        if let Some(error) = crate::test_crash::injected_io_error("segment.footer.short_write") {
            self.file
                .write_all(&bytes[..bytes.len().div_ceil(2)])
                .map_err(|error| {
                    Error::io(
                        "write segment footer",
                        &self.path,
                        DurabilityOutcome::Unknown,
                        error,
                    )
                })?;
            return Err(Error::io(
                "write segment footer",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            ));
        }
        self.file.write_all(&bytes).map_err(|error| {
            Error::io(
                "write segment footer",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        #[cfg(test)]
        crate::test_crash::inject_io("segment.seal.sync_data").map_err(|error| {
            Error::io(
                "sync sealed segment",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        self.file.sync_data().map_err(|error| {
            Error::io(
                "sync sealed segment",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        #[cfg(test)]
        crate::test_crash::hit("segment.seal.after_data_sync");
        self.file_len = footer.segment_bytes;
        self.footer = Some(footer);
        Ok(footer)
    }

    pub(super) fn read_records(
        &self,
        root_id: RootId,
        records: &[StoredRecord],
    ) -> Result<Vec<PendingRecord>> {
        let mut output = Vec::with_capacity(records.len());
        let mut first = 0;
        while first < records.len() {
            let (end, span_end) =
                contiguous_record_span(&self.path, records, first, MAX_COALESCED_READ_BYTES)?;
            let span_start = records[first].descriptor_offset;
            let span_len = span_end.checked_sub(span_start).ok_or_else(|| {
                Error::corruption(&self.path, span_start, "record span underflow")
            })?;
            let span_len = usize::try_from(span_len).map_err(|_| {
                Error::corruption(&self.path, span_start, "record span does not fit usize")
            })?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(span_len)
                .map_err(|error| Error::Runtime {
                    message: format!(
                        "cannot reserve {span_len} coalesced record bytes from {}: {error}",
                        self.path.display()
                    ),
                })?;
            bytes.resize(span_len, 0);
            read_exact_at(&self.file, &self.path, &mut bytes, span_start)?;
            let bytes = Bytes::from(bytes);

            for record in &records[first..end] {
                let descriptor_range = span_range(
                    &self.path,
                    span_start,
                    bytes.len(),
                    record.descriptor_offset,
                    RECORD_DESCRIPTOR_LEN,
                    "record descriptor",
                )?;
                let descriptor =
                    RecordDescriptor::decode(&bytes[descriptor_range]).map_err(|error| {
                        Error::corruption(&self.path, record.descriptor_offset, error.to_string())
                    })?;
                if descriptor.metadata_len != record.metadata_len
                    || descriptor.payload_len != record.payload_len
                    || descriptor.metadata_checksum != record.metadata_checksum
                    || descriptor.payload_checksum != record.payload_checksum
                {
                    return Err(Error::corruption(
                        &self.path,
                        record.descriptor_offset,
                        "record descriptor changed after recovery",
                    ));
                }

                let metadata_range = span_range(
                    &self.path,
                    span_start,
                    bytes.len(),
                    record.metadata_offset,
                    record.metadata_len,
                    "record metadata",
                )?;
                let payload_range = span_range(
                    &self.path,
                    span_start,
                    bytes.len(),
                    record.payload_offset,
                    record.payload_len,
                    "record payload",
                )?;
                if checksum(&bytes[metadata_range.clone()]) != record.metadata_checksum {
                    return Err(Error::corruption(
                        &self.path,
                        record.metadata_offset,
                        "record metadata checksum mismatch",
                    ));
                }
                if checksum(&bytes[payload_range.clone()]) != record.payload_checksum {
                    return Err(Error::corruption(
                        &self.path,
                        record.payload_offset,
                        "record payload checksum mismatch",
                    ));
                }
                let metadata = if metadata_range.is_empty() {
                    Bytes::new()
                } else {
                    bytes.slice(metadata_range)
                };
                let payload = if payload_range.is_empty() {
                    Bytes::new()
                } else {
                    bytes.slice(payload_range)
                };
                output.push(PendingRecord {
                    id: RecordId::from_parts(root_id, record.stream_id, record.sequence),
                    metadata,
                    payload,
                });
            }
            first = end;
        }
        Ok(output)
    }

    pub(super) fn has_checkpoint_boundary(&self, record_count: u64) -> bool {
        self.epochs
            .iter()
            .any(|epoch| epoch.record_count == record_count)
    }

    pub(super) fn unique_stream_count(&self) -> usize {
        self.unique_streams
    }
}

fn tail_repair_allowed(
    allow_active_tail_repair: bool,
    checkpoint_record_count: Option<u64>,
    complete_epochs: &[EpochBoundary],
) -> bool {
    if !allow_active_tail_repair || complete_epochs.is_empty() {
        return false;
    }
    checkpoint_record_count.is_none_or(|baseline| {
        complete_epochs
            .iter()
            .any(|epoch| epoch.record_count == baseline)
    })
}

fn contiguous_record_span(
    path: &Path,
    records: &[StoredRecord],
    first: usize,
    max_bytes: u64,
) -> Result<(usize, u64)> {
    let span_start = records[first].descriptor_offset;
    let mut end = first + 1;
    let mut span_end = record_end(path, &records[first])?;
    while end < records.len() && records[end].descriptor_offset == span_end {
        let next_end = record_end(path, &records[end])?;
        let projected = next_end.checked_sub(span_start).ok_or_else(|| {
            Error::corruption(path, span_start, "coalesced record span underflow")
        })?;
        if projected > max_bytes {
            break;
        }
        span_end = next_end;
        end += 1;
    }
    Ok((end, span_end))
}

fn record_end(path: &Path, record: &StoredRecord) -> Result<u64> {
    let metadata_offset = record
        .descriptor_offset
        .checked_add(RECORD_DESCRIPTOR_LEN)
        .ok_or_else(|| {
            Error::corruption(path, record.descriptor_offset, "metadata offset overflow")
        })?;
    if metadata_offset != record.metadata_offset {
        return Err(Error::corruption(
            path,
            record.descriptor_offset,
            "stored metadata offset is not canonical",
        ));
    }
    let payload_offset = metadata_offset
        .checked_add(record.metadata_len)
        .ok_or_else(|| {
            Error::corruption(path, record.metadata_offset, "payload offset overflow")
        })?;
    if payload_offset != record.payload_offset {
        return Err(Error::corruption(
            path,
            record.metadata_offset,
            "stored payload offset is not canonical",
        ));
    }
    payload_offset
        .checked_add(record.payload_len)
        .ok_or_else(|| Error::corruption(path, record.payload_offset, "record end overflow"))
}

fn span_range(
    path: &Path,
    span_start: u64,
    span_len: usize,
    offset: u64,
    length: u64,
    name: &str,
) -> Result<std::ops::Range<usize>> {
    let relative = offset
        .checked_sub(span_start)
        .ok_or_else(|| Error::corruption(path, offset, format!("{name} precedes read span")))?;
    let end = relative
        .checked_add(length)
        .ok_or_else(|| Error::corruption(path, offset, format!("{name} range overflow")))?;
    let start = usize::try_from(relative).map_err(|_| {
        Error::corruption(path, offset, format!("{name} offset does not fit usize"))
    })?;
    let end = usize::try_from(end)
        .map_err(|_| Error::corruption(path, offset, format!("{name} end does not fit usize")))?;
    if end > span_len {
        return Err(Error::corruption(
            path,
            offset,
            format!("{name} exceeds read span"),
        ));
    }
    Ok(start..end)
}

fn scan_epoch(file: &File, path: &Path, start: u64, file_len: u64) -> Result<ScanEpoch> {
    let mut header_bytes = [0_u8; EPOCH_HEADER_LEN as usize];
    read_exact_at(file, path, &mut header_bytes, start)?;
    let header = EpochHeader::decode(&header_bytes)
        .map_err(|error| Error::corruption(path, start, error.to_string()))?;
    let epoch_bytes = EPOCH_HEADER_LEN
        .checked_add(header.records_bytes)
        .and_then(|bytes| bytes.checked_add(EPOCH_COMMIT_LEN))
        .ok_or_else(|| Error::corruption(path, start, "epoch length overflow"))?;
    let epoch_end = start
        .checked_add(epoch_bytes)
        .ok_or_else(|| Error::corruption(path, start, "epoch end overflow"))?;
    if epoch_end > file_len {
        return Ok(ScanEpoch::Incomplete);
    }
    let count = usize::try_from(header.record_count).map_err(|_| {
        Error::corruption(
            path,
            start.saturating_add(24),
            "epoch record_count does not fit usize",
        )
    })?;
    let mut records = Vec::new();
    records.try_reserve(count).map_err(|error| {
        Error::corruption(
            path,
            start.saturating_add(24),
            format!("cannot reserve recovered record index: {error}"),
        )
    })?;
    let mut descriptors = Vec::new();
    descriptors.try_reserve(count).map_err(|error| {
        Error::corruption(
            path,
            start.saturating_add(24),
            format!("cannot reserve recovered descriptor index: {error}"),
        )
    })?;
    let mut offset = start
        .checked_add(EPOCH_HEADER_LEN)
        .ok_or_else(|| Error::corruption(path, start, "epoch header end overflow"))?;
    let records_start = offset;
    for index in 0..count {
        if file_len.saturating_sub(offset) < RECORD_DESCRIPTOR_LEN {
            return Ok(ScanEpoch::Incomplete);
        }
        let mut bytes = [0_u8; RECORD_DESCRIPTOR_LEN as usize];
        read_exact_at(file, path, &mut bytes, offset)?;
        let descriptor = RecordDescriptor::decode(&bytes)
            .map_err(|error| Error::corruption(path, offset, error.to_string()))?;
        let metadata_offset = offset
            .checked_add(RECORD_DESCRIPTOR_LEN)
            .ok_or_else(|| Error::corruption(path, offset, "metadata offset overflow"))?;
        let payload_offset = metadata_offset
            .checked_add(descriptor.metadata_len)
            .ok_or_else(|| Error::corruption(path, offset, "payload offset overflow"))?;
        let end = payload_offset
            .checked_add(descriptor.payload_len)
            .ok_or_else(|| Error::corruption(path, offset, "record end overflow"))?;
        if end > file_len {
            return Ok(ScanEpoch::Incomplete);
        }
        let index = u64::try_from(index)
            .map_err(|_| Error::corruption(path, offset, "record ordinal does not fit u64"))?;
        records.push(StoredRecord {
            stream_id: header.stream_id,
            sequence: header
                .first_sequence
                .checked_add(index)
                .ok_or_else(|| Error::corruption(path, offset, "record sequence overflows u64"))?,
            descriptor_offset: offset,
            metadata_offset,
            metadata_len: descriptor.metadata_len,
            metadata_checksum: descriptor.metadata_checksum,
            payload_offset,
            payload_len: descriptor.payload_len,
            payload_checksum: descriptor.payload_checksum,
            released: false,
        });
        descriptors.push(bytes);
        offset = end;
    }
    if offset - records_start != header.records_bytes {
        return Err(Error::corruption(
            path,
            start.saturating_add(32),
            "epoch records_bytes does not match decoded records",
        ));
    }
    if file_len.saturating_sub(offset) < EPOCH_COMMIT_LEN {
        return Ok(ScanEpoch::Incomplete);
    }
    let mut commit_bytes = [0_u8; EPOCH_COMMIT_LEN as usize];
    read_exact_at(file, path, &mut commit_bytes, offset)?;
    let commit = EpochCommit::decode(&commit_bytes)
        .map_err(|error| Error::corruption(path, offset, error.to_string()))?;
    let expected_bytes = EPOCH_HEADER_LEN
        .checked_add(header.records_bytes)
        .and_then(|bytes| bytes.checked_add(EPOCH_COMMIT_LEN))
        .ok_or_else(|| Error::corruption(path, start, "epoch length overflow"))?;
    if commit.epoch_start != start || commit.epoch_bytes != expected_bytes {
        return Err(Error::corruption(
            path,
            offset,
            "epoch commit boundary mismatch",
        ));
    }
    if commit.epoch_digest != epoch_digest(&header_bytes, &descriptors) {
        return Err(Error::corruption(
            path,
            offset.saturating_add(24),
            "epoch descriptor digest mismatch",
        ));
    }
    let end_offset = offset
        .checked_add(EPOCH_COMMIT_LEN)
        .ok_or_else(|| Error::corruption(path, offset, "epoch end overflow"))?;
    Ok(ScanEpoch::Complete {
        records,
        commit: commit_bytes,
        end_offset,
    })
}

fn write_epoch_group<W: Write>(
    file: &mut W,
    path: &Path,
    start: u64,
    epochs: Vec<PreparedEpoch>,
) -> Result<Vec<WrittenEpoch>> {
    if epochs.is_empty() {
        return Ok(Vec::new());
    }
    let mut offset = start;
    let mut pending = Vec::with_capacity(epochs.len());
    for epoch in epochs {
        let write = prepare_epoch_write(path, offset, epoch)?;
        offset = write.end_offset;
        pending.push(write);
    }

    #[cfg(test)]
    if let Some(error) = crate::test_crash::injected_io_error("segment.epoch.short_write") {
        let first = pending
            .first()
            .expect("segment writes contain at least one epoch");
        file.write_all(&first.epoch.header[..first.epoch.header.len().div_ceil(2)])
            .map_err(|error| {
                Error::io(
                    "write epoch header",
                    path,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
        return Err(Error::io(
            "write epoch header",
            path,
            DurabilityOutcome::Unknown,
            error,
        ));
    }

    let slice_capacity = pending.iter().fold(0_usize, |total, write| {
        total.saturating_add(
            write
                .epoch
                .records
                .len()
                .saturating_mul(3)
                .saturating_add(2),
        )
    });
    let mut slices = Vec::with_capacity(slice_capacity);
    for write in &pending {
        slices.push(IoSlice::new(&write.epoch.header));
        for (record, descriptor) in write.epoch.records.iter().zip(&write.epoch.descriptors) {
            slices.push(IoSlice::new(descriptor));
            if !record.metadata.is_empty() {
                slices.push(IoSlice::new(&record.metadata));
            }
            if !record.payload.is_empty() {
                slices.push(IoSlice::new(&record.payload));
            }
        }
        slices.push(IoSlice::new(&write.commit));
    }
    write_all_vectored(
        file,
        path,
        &mut slices,
        "write epoch",
        DurabilityOutcome::Unknown,
    )?;
    drop(slices);

    Ok(pending
        .into_iter()
        .map(|write| WrittenEpoch {
            records: write.records,
            commit: write.commit,
            end_offset: write.end_offset,
        })
        .collect())
}

fn prepare_epoch_write(path: &Path, start: u64, epoch: PreparedEpoch) -> Result<PendingEpochWrite> {
    let commit = EpochCommit {
        epoch_start: start,
        epoch_bytes: epoch.encoded_bytes,
        epoch_digest: epoch_digest(&epoch.header, &epoch.descriptors),
    }
    .encode();

    let mut offset = start
        .checked_add(EPOCH_HEADER_LEN)
        .ok_or_else(|| Error::corruption(path, start, "epoch header end overflow"))?;
    let mut stored = Vec::with_capacity(epoch.records.len());
    for (index, descriptor_bytes) in epoch.descriptors.iter().enumerate() {
        let descriptor = RecordDescriptor::decode(descriptor_bytes)
            .map_err(|error| Error::corruption(path, offset, error.to_string()))?;
        let metadata_offset = offset
            .checked_add(RECORD_DESCRIPTOR_LEN)
            .ok_or_else(|| Error::corruption(path, offset, "metadata offset overflow"))?;
        let payload_offset = metadata_offset
            .checked_add(descriptor.metadata_len)
            .ok_or_else(|| Error::corruption(path, offset, "payload offset overflow"))?;
        let index = u64::try_from(index)
            .map_err(|_| Error::corruption(path, offset, "record ordinal does not fit u64"))?;
        stored.push(StoredRecord {
            stream_id: epoch.stream_id,
            sequence: epoch
                .first_sequence
                .checked_add(index)
                .ok_or_else(|| Error::corruption(path, offset, "record sequence overflows u64"))?,
            descriptor_offset: offset,
            metadata_offset,
            metadata_len: descriptor.metadata_len,
            metadata_checksum: descriptor.metadata_checksum,
            payload_offset,
            payload_len: descriptor.payload_len,
            payload_checksum: descriptor.payload_checksum,
            released: false,
        });
        offset = payload_offset
            .checked_add(descriptor.payload_len)
            .ok_or_else(|| Error::corruption(path, offset, "record end overflow"))?;
    }
    let end_offset = offset
        .checked_add(EPOCH_COMMIT_LEN)
        .ok_or_else(|| Error::corruption(path, offset, "epoch end overflow"))?;
    debug_assert_eq!(end_offset.saturating_sub(start), epoch.encoded_bytes);
    Ok(PendingEpochWrite {
        epoch,
        records: stored,
        commit,
        end_offset,
    })
}

fn repair_tail(file: &mut File, path: &Path, length: u64) -> Result<()> {
    file.set_len(length)
        .and_then(|()| file.sync_data())
        .map_err(|error| {
            Error::io(
                "repair incomplete segment tail",
                path,
                DurabilityOutcome::Unknown,
                error,
            )
        })
}

pub(super) fn validate_removed_segment_header(
    path: &Path,
    expected_root: RootId,
    expected_segment_id: u64,
) -> Result<()> {
    let file = File::open(path).map_err(|error| {
        Error::io(
            "open removed segment",
            path,
            DurabilityOutcome::NotApplicable,
            error,
        )
    })?;
    let length = file
        .metadata()
        .map_err(|error| {
            Error::io(
                "read removed segment metadata",
                path,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })?
        .len();
    if length < SEGMENT_HEADER_LEN {
        return Err(Error::corruption(
            path,
            0,
            "removed segment remnant has incomplete header",
        ));
    }
    let mut bytes = [0_u8; SEGMENT_HEADER_LEN as usize];
    read_exact_at(&file, path, &mut bytes, 0)?;
    let header = SegmentHeader::decode(&bytes)
        .map_err(|error| Error::corruption(path, 0, error.to_string()))?;
    if header.root_id != expected_root || header.segment_id != expected_segment_id {
        return Err(Error::corruption(
            path,
            0,
            "removed segment remnant identity mismatch",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct CountingWriter {
        bytes: Vec<u8>,
        vectored_writes: usize,
    }

    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
            self.vectored_writes += 1;
            let mut written = 0;
            for slice in slices {
                self.bytes.extend_from_slice(slice);
                written += slice.len();
            }
            Ok(written)
        }
    }

    #[test]
    fn epoch_group_shares_one_vectored_write() {
        let epochs = vec![
            PreparedEpoch::new(
                StreamId::new(1),
                0,
                vec![Record::new(Bytes::from_static(b"first"))],
            )
            .unwrap(),
            PreparedEpoch::new(
                StreamId::new(2),
                0,
                vec![Record::new(Bytes::from_static(b"second"))],
            )
            .unwrap(),
        ];
        let expected_bytes = epochs.iter().map(|epoch| epoch.encoded_bytes).sum::<u64>();
        let mut writer = CountingWriter::default();

        let written = write_epoch_group(&mut writer, Path::new("segment.log"), 0, epochs).unwrap();

        assert_eq!(writer.vectored_writes, 1);
        assert_eq!(u64::try_from(writer.bytes.len()).unwrap(), expected_bytes);
        assert_eq!(written.len(), 2);
        assert_eq!(
            EpochCommit::decode(&written[1].commit).unwrap().epoch_start,
            written[0].end_offset
        );
        assert_eq!(written[1].end_offset, expected_bytes);
    }
}
