#[cfg(test)]
mod crash_tests;
mod files;
mod manifest;
mod segment;

use crate::config::{Capacity, Config};
use crate::error::{DurabilityOutcome, Error, Result};
use crate::format::{
    Checkpoint, CheckpointSegment, ManifestBody, ManifestLogHeader, ReleaseBody, ReleaseEncoding,
    RootSuperblock, SegmentLifecycle, SegmentRemovedBody, SegmentSealedBody, SequenceRange,
    StreamHighwater, CHECKPOINT_HEADER_LEN, MANIFEST_FRAME_HEADER_LEN, MANIFEST_LOG_HEADER_LEN,
    RECORD_DESCRIPTOR_LEN, ROOT_SUPERBLOCK_LEN, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN,
};
use crate::model::{
    CommitStats, MaintenanceStats, PendingSnapshot, ReadLimits, ReclaimReport, Record, RecordId,
    RecoveryStats, RootId, StorageStats, StreamId, StreamStats,
};
use files::{
    acquire_lock, atomic_replace, ensure_root_directory, ensure_segments_directory, file_len,
    parse_segment_name, parse_segment_temporary_name, read_complete_file, segment_path,
    sync_directory, RootLock, CHECKPOINT_FILE, MANIFEST_LOG_FILE, ROOT_FILE, ROOT_TEMP_FILE,
};
use manifest::{checkpoint_from_state, ControlRecovery, Manifest};
use segment::{validate_removed_segment_header, PreparedEpoch, Segment, StoredRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const AUTOMATIC_RECLAIM_SEGMENTS_PER_JOB: usize = 4;
const MANIFEST_COMPACTION_TRIGGER_BYTES: u64 = 8 * 1024 * 1024;

fn manifest_compaction_required(
    log_bytes: u64,
    actual_file_bytes: u64,
    maintenance_headroom_bytes: u64,
    capacity: Capacity,
) -> bool {
    log_bytes >= MANIFEST_COMPACTION_TRIGGER_BYTES
        || match capacity {
            Capacity::Unbounded => false,
            Capacity::Bounded { total_bytes, .. } => actual_file_bytes
                .checked_add(maintenance_headroom_bytes)
                .is_none_or(|required| required > total_bytes),
        }
}

#[derive(Debug)]
pub(crate) struct AppendUnit {
    pub(crate) stream_id: StreamId,
    pub(crate) records: Vec<Record>,
}

#[derive(Debug)]
pub(crate) struct ReleaseUnit {
    pub(crate) stream_id: StreamId,
    pub(crate) ids: Vec<RecordId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CapacityCheck {
    Admit,
    Wait {
        needed_bytes: u64,
        available_bytes: u64,
    },
    Exceeds {
        needed_bytes: u64,
        total_bytes: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclaimKind {
    Automatic,
    Explicit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SealReason {
    Size,
    Age,
    Reclaim,
}

#[derive(Clone, Copy, Debug)]
struct RecordPointer {
    segment_id: u64,
    ordinal: usize,
}

#[derive(Default)]
struct StreamState {
    highwater: Option<u64>,
    persisted_highwater: Option<u64>,
    // Used only for equality with the current active segment. Segment IDs are
    // never reused, so reclamation does not need to repair this marker.
    last_segment_id: Option<u64>,
    pending: BTreeMap<u64, RecordPointer>,
    pending_payload_bytes: u64,
}

pub(crate) struct Storage {
    config: Config,
    root_id: RootId,
    #[allow(dead_code)]
    lock: RootLock,
    segments_directory: PathBuf,
    manifest: Manifest,
    segments: BTreeMap<u64, Segment>,
    active_segment: Option<u64>,
    streams: BTreeMap<StreamId, StreamState>,
    next_segment_id: u64,
    actual_file_bytes: u64,
    durable_streams: u64,
    pending_records: u64,
    pending_payload_bytes: u64,
    reclaimable_segments: u64,
    reclaimable_bytes: u64,
    commits: CommitStats,
    maintenance: MaintenanceStats,
    recovery: RecoveryStats,
}

impl Storage {
    pub(crate) fn open(config: Config) -> Result<Self> {
        let started = Instant::now();
        config.validate()?;
        ensure_root_directory(&config.root)?;
        let lock = acquire_lock(&config.root)?;
        let initialized_temporaries = initialize_or_validate_root(&config.root)?;
        let root_path = config.root.join(ROOT_FILE);
        let root_bytes = read_complete_file(&root_path)?;
        let root = RootSuperblock::decode(&root_bytes)
            .map_err(|error| Error::corruption(&root_path, 0, error.to_string()))?;
        let segments_directory = ensure_segments_directory(&config.root)?;
        complete_empty_control_initialization(&config.root, root.root_id, &segments_directory)?;
        let root_temporaries = remove_root_temporaries(&config.root)?;

        let control = manifest::recover(&config.root, root.root_id)?;
        let mut storage = Self::recover_segments(
            config,
            root.root_id,
            lock,
            segments_directory,
            control,
            initialized_temporaries.saturating_add(root_temporaries),
        )?;
        storage.actual_file_bytes = storage.calculate_actual_file_bytes_from_disk()?;
        storage.recovery.elapsed = started.elapsed();
        storage.validate_capacity()?;
        Ok(storage)
    }

    fn recover_segments(
        config: Config,
        root_id: RootId,
        lock: RootLock,
        segments_directory: PathBuf,
        mut control: ControlRecovery,
        removed_root_temporaries: u64,
    ) -> Result<Self> {
        let (disk_segments, completed_deletions, removed_segment_temporaries) =
            enumerate_segments(&segments_directory, root_id, &control)?;
        let mut segments = BTreeMap::new();
        let mut missing_seals = Vec::new();
        let mut active_candidates = Vec::new();

        for (segment_id, path) in disk_segments {
            let expected = control.live_segments.get(&segment_id);
            let allow_active_tail_repair = expected
                .is_none_or(|segment| matches!(segment.lifecycle, SegmentLifecycle::Active));
            let checkpoint_record_count =
                expected.and_then(|segment| segment.checkpoint_record_count);
            let segment = Segment::scan(
                path,
                root_id,
                segment_id,
                allow_active_tail_repair,
                checkpoint_record_count,
            )?;
            match (expected, segment.footer) {
                (Some(expected), Some(footer)) => match expected.lifecycle {
                    SegmentLifecycle::Sealed(authoritative) if authoritative == footer => {}
                    SegmentLifecycle::Sealed(_) => {
                        return Err(Error::corruption(
                            &segment.path,
                            segment.file_len - SEGMENT_FOOTER_LEN,
                            "segment footer contradicts manifest state",
                        ));
                    }
                    SegmentLifecycle::Active => missing_seals.push(footer),
                },
                (Some(expected), None) => match expected.lifecycle {
                    SegmentLifecycle::Active => active_candidates.push(segment_id),
                    SegmentLifecycle::Sealed(_) => {
                        return Err(Error::corruption(
                            &segment.path,
                            segment.file_len,
                            "manifest-sealed segment has no complete footer",
                        ));
                    }
                },
                (None, Some(footer)) => missing_seals.push(footer),
                (None, None) => active_candidates.push(segment_id),
            }
            segments.insert(segment_id, segment);
        }

        for segment_id in control.live_segments.keys() {
            if !segments.contains_key(segment_id) {
                return Err(Error::corruption(
                    segment_path(&segments_directory, *segment_id),
                    0,
                    "manifest-live segment is missing",
                ));
            }
        }
        if active_candidates.len() > 1 {
            return Err(Error::corruption(
                &segments_directory,
                0,
                "more than one physical segment is active",
            ));
        }
        let active_segment = active_candidates.first().copied();
        if active_segment.is_some()
            && segments.last_key_value().map(|(segment_id, _)| *segment_id) != active_segment
        {
            return Err(Error::corruption(
                &segments_directory,
                0,
                "active segment is not the greatest extant segment",
            ));
        }

        missing_seals.sort_by_key(|footer| footer.segment_id);
        for footer in &missing_seals {
            let body = ManifestBody::SegmentSealed(SegmentSealedBody {
                segment_id: footer.segment_id,
                segment_bytes: footer.segment_bytes,
                epoch_count: footer.epoch_count,
                segment_digest: footer.segment_digest,
            });
            control
                .manifest
                .append_group(&[body], DurabilityOutcome::Unknown)?;
            control.live_segments.insert(
                footer.segment_id,
                manifest::ControlSegment {
                    lifecycle: SegmentLifecycle::Sealed(*footer),
                    checkpoint_record_count: None,
                    checkpoint_releases: None,
                },
            );
        }

        let recovery = RecoveryStats {
            manifest_frames_scanned: control.frames_scanned,
            segments_scanned: u64::try_from(segments.len()).unwrap_or(u64::MAX),
            epochs_scanned: segments.values().fold(0_u64, |total, segment| {
                total.saturating_add(u64::try_from(segment.epochs.len()).unwrap_or(u64::MAX))
            }),
            records_scanned: segments.values().fold(0_u64, |total, segment| {
                total.saturating_add(u64::try_from(segment.records.len()).unwrap_or(u64::MAX))
            }),
            repaired_active_tails: segments
                .values()
                .filter(|segment| segment.repaired_tail)
                .count()
                .try_into()
                .unwrap_or(u64::MAX),
            repaired_manifest_tails: u64::from(control.repaired_tail),
            completed_segment_seals: u64::try_from(missing_seals.len()).unwrap_or(u64::MAX),
            completed_segment_deletions: completed_deletions,
            removed_temporary_files: removed_root_temporaries
                .saturating_add(removed_segment_temporaries),
            elapsed: std::time::Duration::ZERO,
        };
        let mut storage = Self {
            config,
            root_id,
            lock,
            segments_directory,
            manifest: control.manifest,
            segments,
            active_segment,
            streams: BTreeMap::new(),
            next_segment_id: 0,
            actual_file_bytes: 0,
            durable_streams: 0,
            pending_records: 0,
            pending_payload_bytes: 0,
            reclaimable_segments: 0,
            reclaimable_bytes: 0,
            commits: CommitStats::default(),
            maintenance: MaintenanceStats::default(),
            recovery,
        };
        storage.rebuild_indexes(
            &control.checkpoint_stream_highwaters,
            &control.stream_highwaters,
            &control.live_segments,
            &control.removed_segments,
            &control.releases,
        )?;
        storage.next_segment_id = derive_next_segment_id(
            control.checkpoint_next_segment_id,
            storage.segments.keys().copied(),
            control.removed_segments.keys().copied(),
        )?;
        storage.refresh_recovered_aggregates()?;
        Ok(storage)
    }

    fn refresh_recovered_aggregates(&mut self) -> Result<()> {
        for segment in self.segments.values_mut() {
            segment.refresh_unreleased_records()?;
        }
        self.durable_streams = u64::try_from(
            self.streams
                .values()
                .filter(|stream| stream.highwater.is_some())
                .count(),
        )
        .map_err(|_| Error::invalid_config("stream count does not fit u64"))?;
        self.pending_records =
            self.streams.values().try_fold(0_u64, |total, stream| {
                total
                    .checked_add(u64::try_from(stream.pending.len()).map_err(|_| {
                        Error::invalid_config("pending record count does not fit u64")
                    })?)
                    .ok_or_else(|| Error::invalid_config("pending record count overflow"))
            })?;
        self.pending_payload_bytes = self.streams.values().try_fold(0_u64, |total, stream| {
            total
                .checked_add(stream.pending_payload_bytes)
                .ok_or_else(|| Error::invalid_config("pending payload byte count overflow"))
        })?;
        self.reclaimable_segments = 0;
        self.reclaimable_bytes = 0;
        for segment in self.segments.values() {
            if segment.footer.is_some() && segment.unreleased_records == 0 {
                self.reclaimable_segments = self.reclaimable_segments.saturating_add(1);
                self.reclaimable_bytes = self.reclaimable_bytes.saturating_add(segment.file_len);
            }
        }
        Ok(())
    }

    fn rebuild_indexes(
        &mut self,
        checkpoint_stream_highwaters: &BTreeMap<StreamId, u64>,
        durable_stream_highwaters: &BTreeMap<StreamId, u64>,
        control_segments: &BTreeMap<u64, manifest::ControlSegment>,
        removed_segments: &BTreeMap<u64, SegmentRemovedBody>,
        releases: &[ReleaseBody],
    ) -> Result<()> {
        for (stream_id, highwater) in checkpoint_stream_highwaters {
            self.streams.insert(
                *stream_id,
                StreamState {
                    highwater: Some(*highwater),
                    persisted_highwater: Some(*highwater),
                    ..StreamState::default()
                },
            );
        }

        let mut baseline_counts = BTreeMap::new();
        for (segment_id, segment) in &mut self.segments {
            let control_segment = control_segments.get(segment_id);
            let baseline = control_segment
                .and_then(|segment| segment.checkpoint_record_count)
                .unwrap_or(0);
            let record_count = u64::try_from(segment.records.len()).map_err(|_| {
                Error::corruption(&segment.path, 0, "segment record count does not fit u64")
            })?;
            if baseline > record_count
                || (baseline != 0 && !segment.has_checkpoint_boundary(baseline))
            {
                return Err(Error::corruption(
                    &segment.path,
                    0,
                    "checkpoint record boundary does not match an epoch boundary",
                ));
            }
            if control_segment.is_some_and(|entry| {
                matches!(entry.lifecycle, SegmentLifecycle::Sealed(_))
                    && entry.checkpoint_record_count.is_some()
                    && baseline != record_count
            }) {
                return Err(Error::corruption(
                    &segment.path,
                    0,
                    "checkpoint-sealed segment gained records after its checkpoint boundary",
                ));
            }
            if let Some(encoding) =
                control_segment.and_then(|segment| segment.checkpoint_releases.as_ref())
            {
                let flags = encoding.to_flags(baseline).map_err(|error| {
                    Error::corruption(
                        &segment.path,
                        0,
                        format!("invalid checkpoint release state: {error}"),
                    )
                })?;
                for (record, released) in segment.records.iter_mut().zip(flags) {
                    record.released = released;
                }
            }
            baseline_counts.insert(*segment_id, baseline);
        }

        let all_ids = self
            .segments
            .keys()
            .copied()
            .chain(removed_segments.keys().copied())
            .collect::<BTreeSet<_>>();
        let mut identities = BTreeSet::new();
        let mut locations = BTreeMap::new();
        let mut baseline_last = BTreeMap::new();
        for segment_id in all_ids {
            if let Some(segment) = self.segments.get(&segment_id) {
                let path = segment.path.clone();
                let records = segment.records.clone();
                let baseline = baseline_counts[&segment_id];
                for (ordinal, record) in records.iter().enumerate() {
                    if !identities.insert((record.stream_id, record.sequence)) {
                        return Err(Error::corruption(
                            &path,
                            record.descriptor_offset,
                            "duplicate stream sequence",
                        ));
                    }
                    locations.insert(
                        (record.stream_id, record.sequence),
                        RecordPointer {
                            segment_id,
                            ordinal,
                        },
                    );
                    if u64::try_from(ordinal).expect("usize fits u64") < baseline {
                        let highwater = self
                            .streams
                            .get(&record.stream_id)
                            .and_then(|stream| stream.highwater)
                            .ok_or_else(|| {
                                Error::corruption(
                                    &path,
                                    record.descriptor_offset,
                                    "checkpoint-baseline record belongs to an unknown stream",
                                )
                            })?;
                        if record.sequence > highwater {
                            return Err(Error::corruption(
                                &path,
                                record.descriptor_offset,
                                "checkpoint-baseline record exceeds stream high-water",
                            ));
                        }
                        if baseline_last
                            .insert(record.stream_id, record.sequence)
                            .is_some_and(|previous| previous >= record.sequence)
                        {
                            return Err(Error::corruption(
                                &path,
                                record.descriptor_offset,
                                "checkpoint-baseline stream sequences are not increasing",
                            ));
                        }
                    } else {
                        self.advance_recovered_sequence(
                            record.stream_id,
                            record.sequence,
                            &path,
                            record.descriptor_offset,
                        )?;
                    }
                    self.streams
                        .entry(record.stream_id)
                        .or_default()
                        .last_segment_id = Some(segment_id);
                }
            }
            if let Some(removed) = removed_segments.get(&segment_id) {
                for highwater in &removed.highwaters {
                    let stream = self.streams.entry(highwater.stream_id).or_default();
                    let expected = stream.highwater.map_or(0, |value| value.saturating_add(1));
                    if stream.highwater == Some(u64::MAX) || highwater.sequence < expected {
                        return Err(Error::corruption(
                            self.manifest.path(),
                            0,
                            "removed-segment high-water does not advance recovered sequence",
                        ));
                    }
                    stream.highwater = Some(highwater.sequence);
                    stream.persisted_highwater = Some(highwater.sequence);
                }
            }
        }

        for release in releases {
            let highwater = self
                .streams
                .get(&release.stream_id)
                .and_then(|stream| stream.highwater)
                .ok_or_else(|| {
                    Error::corruption(
                        self.manifest.path(),
                        0,
                        "Release references a stream with no durable sequence",
                    )
                })?;
            for range in &release.ranges {
                let end = range.end().map_err(|error| {
                    Error::corruption(self.manifest.path(), 0, error.to_string())
                })?;
                if end > highwater {
                    return Err(Error::corruption(
                        self.manifest.path(),
                        0,
                        "Release references an unknown future record",
                    ));
                }
                for sequence in range.start..=end {
                    if let Some(pointer) = locations.get(&(release.stream_id, sequence)) {
                        self.segments
                            .get_mut(&pointer.segment_id)
                            .expect("recovered record segment exists")
                            .records[pointer.ordinal]
                            .released = true;
                    }
                }
            }
        }

        for (segment_id, segment) in &self.segments {
            for (ordinal, record) in segment.records.iter().enumerate() {
                let stream = self.streams.entry(record.stream_id).or_default();
                if stream
                    .highwater
                    .is_none_or(|highwater| record.sequence > highwater)
                {
                    return Err(Error::corruption(
                        &segment.path,
                        record.descriptor_offset,
                        "record exceeds recovered stream high-water",
                    ));
                }
                if !record.released {
                    stream.pending.insert(
                        record.sequence,
                        RecordPointer {
                            segment_id: *segment_id,
                            ordinal,
                        },
                    );
                    stream.pending_payload_bytes = stream
                        .pending_payload_bytes
                        .checked_add(record.payload_len)
                        .ok_or_else(|| {
                            Error::corruption(
                                &segment.path,
                                record.payload_offset,
                                "pending payload byte count overflow",
                            )
                        })?;
                }
            }
        }

        for (stream_id, persisted) in durable_stream_highwaters {
            let stream = self.streams.entry(*stream_id).or_default();
            if stream.highwater.is_none_or(|current| current < *persisted) {
                return Err(Error::corruption(
                    self.manifest.path(),
                    0,
                    "manifest high-water exceeds recovered sequence evidence",
                ));
            }
            if stream
                .persisted_highwater
                .is_none_or(|current| current < *persisted)
            {
                stream.persisted_highwater = Some(*persisted);
            }
        }
        Ok(())
    }

    fn advance_recovered_sequence(
        &mut self,
        stream_id: StreamId,
        sequence: u64,
        path: &Path,
        offset: u64,
    ) -> Result<()> {
        let stream = self.streams.entry(stream_id).or_default();
        let expected = match stream.highwater {
            Some(value) => value.checked_add(1).ok_or_else(|| {
                Error::corruption(path, offset, "record follows exhausted stream high-water")
            })?,
            None => 0,
        };
        if sequence != expected {
            return Err(Error::corruption(
                path,
                offset,
                format!("stream sequence {sequence}, expected {expected}"),
            ));
        }
        stream.highwater = Some(sequence);
        Ok(())
    }

    pub(crate) fn append_group(&mut self, units: Vec<AppendUnit>) -> Result<Vec<Vec<RecordId>>> {
        if units.is_empty() {
            return Ok(Vec::new());
        }
        let unit_count = u64::try_from(units.len())
            .map_err(|_| Error::invalid_config("append unit count does not fit u64"))?;
        let appended_records =
            units.iter().try_fold(0_u64, |total, unit| {
                total
                    .checked_add(u64::try_from(unit.records.len()).map_err(|_| {
                        Error::invalid_config("append record count does not fit u64")
                    })?)
                    .ok_or_else(|| Error::invalid_config("append record count overflow"))
            })?;
        let mut next_sequences = self
            .streams
            .iter()
            .map(|(stream_id, stream)| (*stream_id, stream.highwater))
            .collect::<BTreeMap<_, _>>();
        let mut prepared = Vec::with_capacity(units.len());
        for unit in units {
            let first_sequence = match next_sequences.get(&unit.stream_id).copied().flatten() {
                Some(value) => value.checked_add(1).ok_or(Error::SequenceExhausted {
                    stream_id: unit.stream_id,
                })?,
                None => 0,
            };
            let epoch = PreparedEpoch::new(unit.stream_id, first_sequence, unit.records)?;
            if epoch.encoded_bytes > self.config.max_epoch_bytes {
                return Err(Error::EpochTooLarge {
                    encoded_bytes: epoch.encoded_bytes,
                    max_bytes: self.config.max_epoch_bytes,
                });
            }
            let count = u64::try_from(epoch.record_count())
                .map_err(|_| Error::invalid_config("append record count does not fit u64"))?;
            next_sequences.insert(
                unit.stream_id,
                Some(
                    first_sequence
                        .checked_add(count - 1)
                        .ok_or(Error::SequenceExhausted {
                            stream_id: unit.stream_id,
                        })?,
                ),
            );
            prepared.push(epoch);
        }

        let group_bytes = prepared.iter().try_fold(0_u64, |total, epoch| {
            total
                .checked_add(epoch.encoded_bytes)
                .ok_or_else(|| Error::invalid_config("append commit-group length overflow"))
        })?;
        let fresh_capacity = self
            .config
            .segment_bytes
            .checked_sub(SEGMENT_HEADER_LEN + SEGMENT_FOOTER_LEN)
            .expect("configuration validated segment bounds");
        if group_bytes > fresh_capacity {
            return Err(Error::Runtime {
                message: "reactor selected an append commit group larger than one segment"
                    .to_string(),
            });
        }
        if let Some(active) = self.active_segment {
            if self.active_expired()? {
                self.seal_active(SealReason::Age)?;
            } else if group_bytes > self.segment_epoch_capacity(active)? {
                self.seal_active(SealReason::Size)?;
            }
        }

        let (output, previous_segment_bytes, current_segment_bytes) =
            if let Some(active) = self.active_segment {
                let previous_segment_bytes = self.segments[&active].file_len;
                let start = self.segments[&active].records.len();
                let appended_streams = prepared
                    .iter()
                    .map(|epoch| epoch.stream_id)
                    .collect::<BTreeSet<_>>();
                let new_streams = appended_streams
                    .iter()
                    .filter(|stream_id| {
                        self.streams
                            .get(stream_id)
                            .and_then(|stream| stream.last_segment_id)
                            != Some(active)
                    })
                    .count();
                let ids = self
                    .segments
                    .get_mut(&active)
                    .expect("active segment exists")
                    .append(self.root_id, prepared, new_streams)?;
                self.index_durable_records(active, start)?;
                (ids, previous_segment_bytes, self.segments[&active].file_len)
            } else {
                let segment_id = self.allocate_segment_id()?;
                let created_at = unix_millis()?;
                let (segment, ids) = Segment::create(
                    &self.segments_directory,
                    self.root_id,
                    segment_id,
                    created_at,
                    prepared,
                )?;
                let current_segment_bytes = segment.file_len;
                self.segments.insert(segment_id, segment);
                self.active_segment = Some(segment_id);
                self.index_durable_records(segment_id, 0)?;
                (ids, 0, current_segment_bytes)
            };
        self.replace_actual_file_bytes(previous_segment_bytes, current_segment_bytes)?;
        self.commits.append_groups = self.commits.append_groups.saturating_add(1);
        self.commits.append_units = self.commits.append_units.saturating_add(unit_count);
        self.commits.append_records = self.commits.append_records.saturating_add(appended_records);
        self.commits.append_encoded_bytes = self
            .commits
            .append_encoded_bytes
            .saturating_add(group_bytes);
        self.commits.max_append_units = self.commits.max_append_units.max(unit_count);
        self.commits.max_append_encoded_bytes =
            self.commits.max_append_encoded_bytes.max(group_bytes);
        Ok(output)
    }

    fn allocate_segment_id(&mut self) -> Result<u64> {
        if self.next_segment_id == u64::MAX {
            return Err(Error::SegmentIdExhausted);
        }
        let id = self.next_segment_id;
        self.next_segment_id = id.saturating_add(1);
        Ok(id)
    }

    fn segment_epoch_capacity(&self, segment_id: u64) -> Result<u64> {
        let segment = &self.segments[&segment_id];
        Ok(self
            .config
            .segment_bytes
            .saturating_sub(segment.file_len)
            .saturating_sub(SEGMENT_FOOTER_LEN))
    }

    fn active_expired(&self) -> Result<bool> {
        let Some(max_age) = self.config.max_segment_age else {
            return Ok(false);
        };
        let Some(active) = self.active_segment else {
            return Ok(false);
        };
        let now = unix_millis()?;
        let age = u64::try_from(max_age.as_millis())
            .map_err(|_| Error::invalid_config("segment age does not fit u64"))?;
        Ok(now.saturating_sub(self.segments[&active].header.created_at_unix_millis) >= age)
    }

    fn index_durable_records(&mut self, segment_id: u64, start: usize) -> Result<()> {
        let records = self.segments[&segment_id].records[start..].to_vec();
        for (relative, record) in records.into_iter().enumerate() {
            let ordinal = start.checked_add(relative).ok_or_else(|| {
                Error::corruption(
                    &self.segments[&segment_id].path,
                    0,
                    "record ordinal overflow",
                )
            })?;
            let stream = self.streams.entry(record.stream_id).or_default();
            let became_durable = stream.highwater.is_none();
            let expected = stream.highwater.map_or(0, |value| value.saturating_add(1));
            if stream.highwater == Some(u64::MAX) || record.sequence != expected {
                return Err(Error::corruption(
                    &self.segments[&segment_id].path,
                    record.descriptor_offset,
                    "durable append produced a noncontiguous stream sequence",
                ));
            }
            stream.highwater = Some(record.sequence);
            stream.last_segment_id = Some(segment_id);
            stream.pending.insert(
                record.sequence,
                RecordPointer {
                    segment_id,
                    ordinal,
                },
            );
            stream.pending_payload_bytes = stream
                .pending_payload_bytes
                .checked_add(record.payload_len)
                .ok_or_else(|| {
                    Error::corruption(
                        &self.segments[&segment_id].path,
                        0,
                        "pending payload bytes overflow",
                    )
                })?;
            if became_durable {
                self.durable_streams = self.durable_streams.saturating_add(1);
            }
            self.pending_records = self.pending_records.saturating_add(1);
            self.pending_payload_bytes = self
                .pending_payload_bytes
                .checked_add(record.payload_len)
                .ok_or_else(|| {
                    Error::corruption(
                        &self.segments[&segment_id].path,
                        0,
                        "root pending payload bytes overflow",
                    )
                })?;
        }
        Ok(())
    }

    pub(crate) fn read(
        &self,
        stream_id: StreamId,
        limits: ReadLimits,
    ) -> Result<Option<PendingSnapshot>> {
        if limits.max_records == 0 {
            return Err(Error::InvalidReadLimits);
        }
        let Some(stream) = self.streams.get(&stream_id) else {
            return Ok(None);
        };
        let mut selected = Vec::<StoredRecord>::new();
        let mut segment_groups = Vec::<(u64, usize)>::new();
        let mut payload_bytes = 0_u64;
        for (sequence, pointer) in stream.pending.iter().take(limits.max_records) {
            let record = self
                .segments
                .get(&pointer.segment_id)
                .and_then(|segment| segment.records.get(pointer.ordinal))
                .ok_or_else(|| {
                    Error::corruption(
                        &self.segments_directory,
                        0,
                        "pending index points outside a segment",
                    )
                })?;
            let projected = payload_bytes
                .checked_add(record.payload_len)
                .ok_or_else(|| {
                    Error::corruption(
                        &self.segments[&pointer.segment_id].path,
                        record.payload_offset,
                        "read payload byte count overflow",
                    )
                })?;
            if projected > limits.max_bytes {
                if selected.is_empty() {
                    return Err(Error::ReadLimitTooSmall {
                        id: RecordId::from_parts(self.root_id, stream_id, *sequence),
                        required_bytes: record.payload_len,
                        max_bytes: limits.max_bytes,
                    });
                }
                break;
            }
            payload_bytes = projected;
            if segment_groups
                .last()
                .is_none_or(|(segment_id, _)| *segment_id != pointer.segment_id)
            {
                segment_groups.push((pointer.segment_id, selected.len()));
            }
            selected.push(record.clone());
        }
        if selected.is_empty() {
            return Ok(None);
        }
        let mut records = Vec::with_capacity(selected.len());
        for (index, (segment_id, start)) in segment_groups.iter().copied().enumerate() {
            let end = segment_groups
                .get(index + 1)
                .map_or(selected.len(), |(_, start)| *start);
            records.extend(
                self.segments[&segment_id].read_records(self.root_id, &selected[start..end])?,
            );
        }
        Ok(Some(PendingSnapshot::new(records)))
    }

    pub(crate) fn release_group(&mut self, units: Vec<ReleaseUnit>) -> Result<()> {
        let mut scheduled = BTreeSet::new();
        let mut bodies = Vec::new();
        let mut applications = Vec::new();
        let mut release_units = 0_u64;
        let mut release_records = 0_u64;
        let mut release_encoded_bytes = 0_u64;
        for unit in units {
            self.validate_release(&unit)?;
            let stream = self.streams.get(&unit.stream_id);
            let mut sequences = unit
                .ids
                .iter()
                .map(|id| id.sequence())
                .filter(|sequence| {
                    stream.is_some_and(|state| state.pending.contains_key(sequence))
                        && scheduled.insert((unit.stream_id, *sequence))
                })
                .collect::<Vec<_>>();
            sequences.sort_unstable();
            sequences.dedup();
            if sequences.is_empty() {
                continue;
            }
            let ranges = coalesce_sequences(&sequences);
            release_units = release_units.saturating_add(1);
            release_records =
                release_records.saturating_add(u64::try_from(sequences.len()).unwrap_or(u64::MAX));
            let range_count = u64::try_from(ranges.len())
                .map_err(|_| Error::invalid_config("release range count does not fit u64"))?;
            let frame_bytes = MANIFEST_FRAME_HEADER_LEN
                .checked_add(24)
                .and_then(|bytes| bytes.checked_add(range_count.checked_mul(16)?))
                .ok_or_else(|| Error::invalid_config("release frame byte count overflow"))?;
            release_encoded_bytes = release_encoded_bytes.saturating_add(frame_bytes);
            bodies.push(ManifestBody::Release(ReleaseBody {
                stream_id: unit.stream_id,
                ranges,
            }));
            applications.push((unit.stream_id, sequences));
        }
        if bodies.is_empty() {
            return Ok(());
        }
        self.append_manifest_group(&bodies, DurabilityOutcome::Unknown)?;
        #[cfg(test)]
        crate::test_crash::hit("release.after_manifest_sync");
        self.commits.release_groups = self.commits.release_groups.saturating_add(1);
        self.commits.release_units = self.commits.release_units.saturating_add(release_units);
        self.commits.release_records = self.commits.release_records.saturating_add(release_records);
        self.commits.release_encoded_bytes = self
            .commits
            .release_encoded_bytes
            .saturating_add(release_encoded_bytes);
        self.commits.max_release_units = self.commits.max_release_units.max(release_units);
        self.commits.max_release_encoded_bytes = self
            .commits
            .max_release_encoded_bytes
            .max(release_encoded_bytes);
        for (stream_id, sequences) in applications {
            for sequence in sequences {
                self.mark_record_released(stream_id, sequence);
            }
        }
        self.maybe_compact_manifest()?;
        Ok(())
    }

    fn validate_release(&self, unit: &ReleaseUnit) -> Result<()> {
        if unit.ids.len() > self.config.max_release_records {
            return Err(Error::ReleaseTooLarge {
                records: unit.ids.len(),
                max_records: self.config.max_release_records,
            });
        }
        let highwater = self
            .streams
            .get(&unit.stream_id)
            .and_then(|stream| stream.highwater);
        for id in &unit.ids {
            if id.root_id() != self.root_id || id.stream_id() != unit.stream_id {
                return Err(Error::RecordIdScopeMismatch {
                    id: *id,
                    expected_stream: unit.stream_id,
                });
            }
            if highwater.is_none_or(|value| id.sequence() > value) {
                return Err(Error::UnknownRecordId { id: *id });
            }
        }
        Ok(())
    }

    fn mark_record_released(&mut self, stream_id: StreamId, sequence: u64) {
        let pointer = self
            .streams
            .get_mut(&stream_id)
            .and_then(|stream| stream.pending.remove(&sequence));
        let Some(pointer) = pointer else {
            return;
        };
        let segment = self
            .segments
            .get_mut(&pointer.segment_id)
            .expect("pending segment exists");
        let record = &mut segment.records[pointer.ordinal];
        record.released = true;
        let payload_len = record.payload_len;
        let became_reclaimable = segment.footer.is_some() && segment.unreleased_records == 1;
        segment.unreleased_records = segment
            .unreleased_records
            .checked_sub(1)
            .expect("pending segment release accounting is exact");
        let reclaimable_bytes = became_reclaimable.then_some(segment.file_len);
        let stream = self.streams.get_mut(&stream_id).expect("stream exists");
        stream.pending_payload_bytes = stream
            .pending_payload_bytes
            .checked_sub(payload_len)
            .expect("pending payload accounting is exact");
        self.pending_records = self
            .pending_records
            .checked_sub(1)
            .expect("root pending record accounting is exact");
        self.pending_payload_bytes = self
            .pending_payload_bytes
            .checked_sub(payload_len)
            .expect("root pending payload accounting is exact");
        if let Some(bytes) = reclaimable_bytes {
            self.reclaimable_segments = self.reclaimable_segments.saturating_add(1);
            self.reclaimable_bytes = self.reclaimable_bytes.saturating_add(bytes);
        }
    }

    pub(crate) fn reclaim(&mut self, kind: ReclaimKind) -> Result<ReclaimReport> {
        if let Some(active) = self.active_segment {
            if self.segments[&active].unreleased_records == 0 {
                self.seal_active(SealReason::Reclaim)?;
            }
        }
        let limit = match kind {
            ReclaimKind::Automatic => AUTOMATIC_RECLAIM_SEGMENTS_PER_JOB,
            ReclaimKind::Explicit => usize::MAX,
        };
        let eligible = self
            .segments
            .iter()
            .filter(|(_, segment)| segment.footer.is_some() && segment.unreleased_records == 0)
            .map(|(segment_id, _)| *segment_id)
            .take(limit)
            .collect::<Vec<_>>();
        let mut report = ReclaimReport::default();
        for batch in eligible.chunks(AUTOMATIC_RECLAIM_SEGMENTS_PER_JOB) {
            let reclaimed = self.reclaim_batch(batch)?;
            report.segments = report.segments.saturating_add(reclaimed.segments);
            report.bytes = report.bytes.saturating_add(reclaimed.bytes);
        }
        if !report.is_empty() && self.segments.is_empty() {
            self.compact_manifest()?;
        } else {
            self.maybe_compact_manifest()?;
        }
        match kind {
            ReclaimKind::Automatic => {
                self.maintenance.automatic_reclaim_passes =
                    self.maintenance.automatic_reclaim_passes.saturating_add(1);
            }
            ReclaimKind::Explicit => {
                self.maintenance.explicit_reclaim_passes =
                    self.maintenance.explicit_reclaim_passes.saturating_add(1);
            }
        }
        Ok(report)
    }

    fn reclaim_batch(&mut self, eligible: &[u64]) -> Result<ReclaimReport> {
        debug_assert!(eligible.len() <= AUTOMATIC_RECLAIM_SEGMENTS_PER_JOB);
        if eligible.is_empty() {
            return Ok(ReclaimReport::default());
        }
        let mut persisted_highwaters = self
            .streams
            .iter()
            .filter_map(|(stream_id, stream)| {
                stream
                    .persisted_highwater
                    .map(|sequence| (*stream_id, sequence))
            })
            .collect::<BTreeMap<_, _>>();
        let removals = eligible
            .iter()
            .map(|segment_id| {
                let body = self.removal_body(*segment_id, &persisted_highwaters);
                for highwater in &body.highwaters {
                    persisted_highwaters.insert(highwater.stream_id, highwater.sequence);
                }
                body
            })
            .collect::<Vec<_>>();
        let frames = removals
            .iter()
            .cloned()
            .map(ManifestBody::SegmentRemoved)
            .collect::<Vec<_>>();
        self.append_manifest_group(&frames, DurabilityOutcome::Unknown)?;
        #[cfg(test)]
        crate::test_crash::hit("reclaim.after_manifest_sync");
        for body in &removals {
            for highwater in &body.highwaters {
                self.streams
                    .get_mut(&highwater.stream_id)
                    .expect("removed segment stream exists")
                    .persisted_highwater = Some(highwater.sequence);
            }
        }
        let mut removed = Vec::with_capacity(eligible.len());
        for segment_id in eligible {
            let segment = self
                .segments
                .remove(segment_id)
                .expect("eligible segment exists");
            let bytes = segment.file_len;
            self.reclaimable_segments = self
                .reclaimable_segments
                .checked_sub(1)
                .expect("eligible segment accounting is exact");
            self.reclaimable_bytes = self
                .reclaimable_bytes
                .checked_sub(bytes)
                .expect("eligible segment byte accounting is exact");
            let path = segment.path.clone();
            drop(segment);
            #[cfg(test)]
            crate::test_crash::inject_io("reclaim.delete").map_err(|error| {
                Error::io(
                    "delete reclaimed segment",
                    &path,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
            fs::remove_file(&path).map_err(|error| {
                Error::io(
                    "delete reclaimed segment",
                    &path,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
            #[cfg(test)]
            crate::test_crash::hit("reclaim.after_delete");
            removed.push(bytes);
        }
        #[cfg(test)]
        crate::test_crash::inject_io("reclaim.directory_sync").map_err(|error| {
            Error::io(
                "sync segment directory",
                &self.segments_directory,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        sync_directory(&self.segments_directory, DurabilityOutcome::Unknown)?;
        #[cfg(test)]
        crate::test_crash::hit("reclaim.after_directory_sync");
        let report = ReclaimReport {
            segments: u64::try_from(removed.len()).unwrap_or(u64::MAX),
            bytes: removed
                .iter()
                .fold(0_u64, |total, bytes| total.saturating_add(*bytes)),
        };
        self.subtract_actual_file_bytes(report.bytes)?;
        self.maintenance.reclaimed_segments = self
            .maintenance
            .reclaimed_segments
            .saturating_add(report.segments);
        self.maintenance.reclaimed_bytes = self
            .maintenance
            .reclaimed_bytes
            .saturating_add(report.bytes);
        Ok(report)
    }

    fn removal_body(
        &self,
        segment_id: u64,
        persisted_highwaters: &BTreeMap<StreamId, u64>,
    ) -> SegmentRemovedBody {
        let mut maxima = BTreeMap::new();
        for record in &self.segments[&segment_id].records {
            maxima
                .entry(record.stream_id)
                .and_modify(|sequence: &mut u64| *sequence = (*sequence).max(record.sequence))
                .or_insert(record.sequence);
        }
        let highwaters = maxima
            .into_iter()
            .filter_map(|(stream_id, sequence)| {
                persisted_highwaters
                    .get(&stream_id)
                    .copied()
                    .is_none_or(|current| sequence > current)
                    .then_some(StreamHighwater {
                        stream_id,
                        sequence,
                    })
            })
            .collect();
        SegmentRemovedBody {
            segment_id,
            highwaters,
        }
    }

    pub(crate) fn seal_expired(&mut self) -> Result<bool> {
        if self.active_segment.is_some() && self.active_expired()? {
            self.seal_active(SealReason::Age)?;
            self.maybe_compact_manifest()?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn next_age_delay(&self) -> Result<Option<std::time::Duration>> {
        let Some(max_age) = self.config.max_segment_age else {
            return Ok(None);
        };
        let Some(active) = self.active_segment else {
            return Ok(Some(max_age));
        };
        let now = unix_millis()?;
        let max_millis = u64::try_from(max_age.as_millis())
            .map_err(|_| Error::invalid_config("segment age does not fit u64"))?;
        let elapsed = now.saturating_sub(self.segments[&active].header.created_at_unix_millis);
        Ok(Some(std::time::Duration::from_millis(
            max_millis.saturating_sub(elapsed),
        )))
    }

    fn seal_active(&mut self, reason: SealReason) -> Result<()> {
        let Some(segment_id) = self.active_segment else {
            return Ok(());
        };
        let previous_segment_bytes = self.segments[&segment_id].file_len;
        let footer = self
            .segments
            .get_mut(&segment_id)
            .expect("active segment exists")
            .seal_data()?;
        self.replace_actual_file_bytes(previous_segment_bytes, footer.segment_bytes)?;
        self.append_manifest_group(
            &[ManifestBody::SegmentSealed(SegmentSealedBody {
                segment_id,
                segment_bytes: footer.segment_bytes,
                epoch_count: footer.epoch_count,
                segment_digest: footer.segment_digest,
            })],
            DurabilityOutcome::Unknown,
        )?;
        #[cfg(test)]
        crate::test_crash::hit("seal.after_manifest_sync");
        if self.segments[&segment_id].unreleased_records == 0 {
            self.reclaimable_segments = self.reclaimable_segments.saturating_add(1);
            self.reclaimable_bytes = self
                .reclaimable_bytes
                .saturating_add(self.segments[&segment_id].file_len);
        }
        match reason {
            SealReason::Size => {
                self.maintenance.size_rollovers = self.maintenance.size_rollovers.saturating_add(1);
            }
            SealReason::Age => {
                self.maintenance.age_rollovers = self.maintenance.age_rollovers.saturating_add(1);
            }
            SealReason::Reclaim => {
                self.maintenance.reclaim_rollovers =
                    self.maintenance.reclaim_rollovers.saturating_add(1);
            }
        }
        self.active_segment = None;
        Ok(())
    }

    fn compact_manifest(&mut self) -> Result<()> {
        let segments = self
            .segments
            .iter()
            .map(|(segment_id, segment)| {
                let flags = segment
                    .records
                    .iter()
                    .map(|record| record.released)
                    .collect::<Vec<_>>();
                (
                    *segment_id,
                    CheckpointSegment {
                        lifecycle: segment
                            .footer
                            .map_or(SegmentLifecycle::Active, SegmentLifecycle::Sealed),
                        record_count: u64::try_from(segment.records.len())
                            .expect("segment record count fits u64"),
                        releases: ReleaseEncoding::from_flags(&flags),
                    },
                )
            })
            .collect();
        let stream_highwaters = self
            .streams
            .iter()
            .filter_map(|(stream_id, stream)| stream.highwater.map(|value| (*stream_id, value)))
            .collect();
        let checkpoint = checkpoint_from_state(
            self.root_id,
            self.manifest.last_seq,
            self.next_segment_id,
            stream_highwaters,
            segments,
        );
        let previous_control_bytes = self.manifest.control_file_len()?;
        self.manifest.compact(&checkpoint)?;
        let current_control_bytes = self.manifest.control_file_len()?;
        self.replace_actual_file_bytes(previous_control_bytes, current_control_bytes)?;
        self.maintenance.manifest_compactions =
            self.maintenance.manifest_compactions.saturating_add(1);
        for stream in self.streams.values_mut() {
            stream.persisted_highwater = stream.highwater;
        }
        Ok(())
    }

    fn maybe_compact_manifest(&mut self) -> Result<bool> {
        let log_bytes = self.manifest.file_len();
        if !manifest_compaction_required(
            log_bytes,
            self.actual_file_bytes,
            self.maintenance_headroom_bytes()?,
            self.config.capacity,
        ) {
            return Ok(false);
        }
        self.compact_manifest()?;
        Ok(true)
    }

    pub(crate) fn has_automatic_reclaim_work(&self) -> bool {
        self.reclaimable_segments != 0
            || self
                .active_segment
                .is_some_and(|segment_id| self.segments[&segment_id].unreleased_records == 0)
    }

    pub(crate) fn known_streams(&self) -> Vec<StreamId> {
        self.streams
            .iter()
            .filter_map(|(stream_id, state)| state.highwater.map(|_| *stream_id))
            .collect()
    }

    pub(crate) fn stream_stats(&self, stream_id: StreamId) -> StreamStats {
        self.streams
            .get(&stream_id)
            .map_or_else(StreamStats::default, |stream| StreamStats {
                durable_known: stream.highwater.is_some(),
                pending_records: u64::try_from(stream.pending.len()).unwrap_or(u64::MAX),
                pending_payload_bytes: stream.pending_payload_bytes,
            })
    }

    pub(crate) fn storage_stats(&self) -> Result<StorageStats> {
        let headroom = self.maintenance_headroom_bytes()?;
        let data_admissible_bytes = match self.config.capacity {
            Capacity::Unbounded => None,
            Capacity::Bounded { total_bytes, .. } => {
                Some(total_bytes.saturating_sub(self.actual_file_bytes.saturating_add(headroom)))
            }
        };
        let live_segments = u64::try_from(self.segments.len())
            .map_err(|_| Error::invalid_config("segment count does not fit u64"))?;
        Ok(StorageStats {
            configured_capacity_bytes: match self.config.capacity {
                Capacity::Unbounded => None,
                Capacity::Bounded { total_bytes, .. } => Some(total_bytes),
            },
            full_policy: match self.config.capacity {
                Capacity::Unbounded => None,
                Capacity::Bounded { when_full, .. } => Some(when_full),
            },
            durable_streams: self.durable_streams,
            pending_records: self.pending_records,
            pending_payload_bytes: self.pending_payload_bytes,
            actual_file_bytes: self.actual_file_bytes,
            maintenance_headroom_bytes: headroom,
            data_admissible_bytes,
            live_segments,
            sealed_segments: live_segments.saturating_sub(u64::from(self.active_segment.is_some())),
            reclaimable_segments: self.reclaimable_segments,
            reclaimable_bytes: self.reclaimable_bytes,
        })
    }

    pub(crate) const fn commit_stats(&self) -> CommitStats {
        self.commits
    }

    pub(crate) const fn maintenance_stats(&self) -> MaintenanceStats {
        self.maintenance
    }

    pub(crate) const fn recovery_stats(&self) -> RecoveryStats {
        self.recovery
    }

    pub(crate) fn stream_highwater(&self, stream_id: StreamId) -> Option<u64> {
        self.streams
            .get(&stream_id)
            .and_then(|stream| stream.highwater)
    }

    pub(crate) fn root_id(&self) -> RootId {
        self.root_id
    }

    pub(crate) fn check_append_capacity(&self, units: &[AppendUnit]) -> Result<CapacityCheck> {
        let Capacity::Bounded { total_bytes, .. } = self.config.capacity else {
            return Ok(CapacityCheck::Admit);
        };
        let mut record_counts = BTreeMap::<StreamId, u64>::new();
        let mut total_records = 0_u64;
        let mut group_bytes = 0_u64;
        for unit in units {
            let record_count = u64::try_from(unit.records.len())
                .map_err(|_| Error::invalid_config("append record count does not fit u64"))?;
            total_records = total_records
                .checked_add(record_count)
                .ok_or_else(|| Error::invalid_config("append record count overflow"))?;
            let stream_records = record_counts.entry(unit.stream_id).or_default();
            *stream_records = stream_records
                .checked_add(record_count)
                .ok_or_else(|| Error::invalid_config("append stream record count overflow"))?;
            group_bytes = group_bytes
                .checked_add(encoded_epoch_bytes(&unit.records)?)
                .ok_or_else(|| Error::invalid_config("append group byte count overflow"))?;
        }
        let active_fits = if let Some(active) = self.active_segment {
            !self.active_expired()? && group_bytes <= self.segment_epoch_capacity(active)?
        } else {
            false
        };
        let target_existing = if active_fits {
            self.active_segment
        } else {
            None
        };
        let creates_segment = target_existing.is_none();
        let seals_segment = self.active_segment.is_some() && creates_segment;

        let mut projected_actual = self
            .actual_file_bytes
            .checked_add(group_bytes)
            .ok_or_else(|| Error::invalid_config("append capacity projection overflow"))?;
        if creates_segment {
            projected_actual = projected_actual
                .checked_add(SEGMENT_HEADER_LEN)
                .ok_or_else(|| Error::invalid_config("append capacity projection overflow"))?;
        }
        if seals_segment {
            projected_actual = projected_actual
                .checked_add(SEGMENT_FOOTER_LEN)
                .and_then(|bytes| bytes.checked_add(80))
                .ok_or_else(|| Error::invalid_config("append capacity projection overflow"))?;
        }
        let projected_headroom = self.projected_headroom(
            &record_counts,
            total_records,
            target_existing,
            creates_segment,
        )?;
        let projected_weight = projected_actual
            .checked_add(projected_headroom)
            .ok_or_else(|| Error::invalid_config("append capacity projection overflow"))?;
        if projected_weight <= total_bytes {
            return Ok(CapacityCheck::Admit);
        }

        let current_weight = self
            .actual_file_bytes
            .checked_add(self.maintenance_headroom_bytes()?)
            .ok_or_else(|| Error::invalid_config("capacity accounting overflow"))?;
        let needed_bytes = projected_weight.saturating_sub(current_weight);
        let available_bytes = total_bytes.saturating_sub(current_weight);
        let minimum =
            self.minimum_root_weight_after_append(&record_counts, total_records, group_bytes)?;
        if minimum > total_bytes {
            Ok(CapacityCheck::Exceeds {
                needed_bytes: minimum,
                total_bytes,
            })
        } else {
            Ok(CapacityCheck::Wait {
                needed_bytes,
                available_bytes,
            })
        }
    }

    pub(crate) fn append_prefix_for_active_segment(&self, units: &[AppendUnit]) -> Result<usize> {
        let Some(active) = self.active_segment else {
            return Ok(units.len());
        };
        if self.active_expired()? {
            return Ok(units.len());
        }
        let available = self.segment_epoch_capacity(active)?;
        let mut bytes = 0_u64;
        let mut selected = 0;
        for unit in units {
            let epoch_bytes = encoded_epoch_bytes(&unit.records)?;
            let Some(projected) = bytes.checked_add(epoch_bytes) else {
                break;
            };
            if projected > available {
                break;
            }
            bytes = projected;
            selected += 1;
        }
        if selected == 0 {
            Ok(units.len())
        } else {
            Ok(selected)
        }
    }

    fn projected_headroom(
        &self,
        record_counts: &BTreeMap<StreamId, u64>,
        total_records: u64,
        target_existing: Option<u64>,
        creates_segment: bool,
    ) -> Result<u64> {
        let new_streams = record_counts
            .keys()
            .filter(|stream_id| !self.streams.contains_key(stream_id))
            .count();
        let projected_streams = self
            .streams
            .len()
            .checked_add(new_streams)
            .ok_or_else(|| Error::invalid_config("stream count overflow"))?;
        let stream_count = u64::try_from(projected_streams)
            .map_err(|_| Error::invalid_config("stream count does not fit u64"))?;
        let mut checkpoint = CHECKPOINT_HEADER_LEN
            .checked_add(24)
            .and_then(|bytes| bytes.checked_add(stream_count.checked_mul(16)?))
            .ok_or_else(|| Error::invalid_config("checkpoint reserve overflow"))?;
        let mut largest_segment_streams = 0_u64;
        for (segment_id, segment) in &self.segments {
            let added = u64::from(target_existing == Some(*segment_id))
                .checked_mul(total_records)
                .ok_or_else(|| Error::invalid_config("record count overflow"))?;
            let records = u64::try_from(segment.records.len())
                .map_err(|_| Error::invalid_config("record count does not fit u64"))?
                .checked_add(added)
                .ok_or_else(|| Error::invalid_config("record count overflow"))?;
            checkpoint = checkpoint
                .checked_add(72)
                .and_then(|bytes| bytes.checked_add(records.div_ceil(64).checked_mul(8)?))
                .ok_or_else(|| Error::invalid_config("checkpoint reserve overflow"))?;
            let mut streams = segment.unique_stream_count();
            if target_existing == Some(*segment_id) {
                let added_streams = record_counts
                    .keys()
                    .filter(|stream_id| {
                        self.streams
                            .get(stream_id)
                            .and_then(|stream| stream.last_segment_id)
                            != Some(*segment_id)
                    })
                    .count();
                streams = streams
                    .checked_add(added_streams)
                    .ok_or_else(|| Error::invalid_config("segment stream count overflow"))?;
            }
            largest_segment_streams = largest_segment_streams.max(
                u64::try_from(streams)
                    .map_err(|_| Error::invalid_config("segment stream count does not fit u64"))?,
            );
        }
        if creates_segment {
            checkpoint = checkpoint
                .checked_add(72)
                .and_then(|bytes| bytes.checked_add(total_records.div_ceil(64).checked_mul(8)?))
                .ok_or_else(|| Error::invalid_config("checkpoint reserve overflow"))?;
            largest_segment_streams = largest_segment_streams.max(
                u64::try_from(record_counts.len())
                    .map_err(|_| Error::invalid_config("stream count does not fit u64"))?,
            );
        }
        let manifest = self.largest_manifest_group(largest_segment_streams)?;
        checkpoint
            .checked_add(manifest)
            .and_then(|bytes| bytes.checked_add(SEGMENT_FOOTER_LEN))
            .ok_or_else(|| Error::invalid_config("maintenance headroom overflow"))
    }

    fn minimum_root_weight_after_append(
        &self,
        record_counts: &BTreeMap<StreamId, u64>,
        total_records: u64,
        group_bytes: u64,
    ) -> Result<u64> {
        let new_streams = record_counts
            .keys()
            .filter(|stream_id| !self.streams.contains_key(stream_id))
            .count();
        let stream_count = self
            .streams
            .len()
            .checked_add(new_streams)
            .ok_or_else(|| Error::invalid_config("stream count overflow"))?;
        let stream_count = u64::try_from(stream_count)
            .map_err(|_| Error::invalid_config("stream count does not fit u64"))?;
        let checkpoint_without_segment = CHECKPOINT_HEADER_LEN
            .checked_add(24)
            .and_then(|bytes| bytes.checked_add(stream_count.checked_mul(16)?))
            .ok_or_else(|| Error::invalid_config("minimum checkpoint size overflow"))?;
        let actual = ROOT_SUPERBLOCK_LEN
            .checked_add(checkpoint_without_segment)
            .and_then(|bytes| bytes.checked_add(MANIFEST_LOG_HEADER_LEN))
            .and_then(|bytes| bytes.checked_add(SEGMENT_HEADER_LEN))
            .and_then(|bytes| bytes.checked_add(group_bytes))
            .ok_or_else(|| Error::invalid_config("minimum root size overflow"))?;
        let checkpoint_reserve = checkpoint_without_segment
            .checked_add(72)
            .and_then(|bytes| bytes.checked_add(total_records.div_ceil(64).checked_mul(8)?))
            .ok_or_else(|| Error::invalid_config("minimum checkpoint reserve overflow"))?;
        let segment_streams = u64::try_from(record_counts.len())
            .map_err(|_| Error::invalid_config("stream count does not fit u64"))?;
        actual
            .checked_add(checkpoint_reserve)
            .and_then(|bytes| bytes.checked_add(self.largest_manifest_group(segment_streams).ok()?))
            .and_then(|bytes| bytes.checked_add(SEGMENT_FOOTER_LEN))
            .ok_or_else(|| Error::invalid_config("minimum root capacity overflow"))
    }

    fn validate_capacity(&self) -> Result<()> {
        if let Capacity::Bounded { total_bytes, .. } = self.config.capacity {
            let required = self
                .actual_file_bytes
                .checked_add(self.maintenance_headroom_bytes()?)
                .ok_or_else(|| Error::invalid_config("capacity accounting overflow"))?;
            if required > total_bytes {
                return Err(Error::invalid_config(format!(
                    "recovered root requires {required} bytes including maintenance headroom, exceeding bounded capacity {total_bytes}"
                )));
            }
        }
        Ok(())
    }

    fn maintenance_headroom_bytes(&self) -> Result<u64> {
        let stream_count = u64::try_from(self.streams.len())
            .map_err(|_| Error::invalid_config("stream count does not fit u64"))?;
        let mut checkpoint = CHECKPOINT_HEADER_LEN
            .checked_add(24)
            .and_then(|bytes| bytes.checked_add(stream_count.checked_mul(16)?))
            .ok_or_else(|| Error::invalid_config("checkpoint reserve overflow"))?;
        let mut largest_segment_streams = 0_u64;
        for segment in self.segments.values() {
            let records = u64::try_from(segment.records.len())
                .map_err(|_| Error::invalid_config("record count does not fit u64"))?;
            let bitmap_bytes = records
                .div_ceil(64)
                .checked_mul(8)
                .ok_or_else(|| Error::invalid_config("checkpoint bitmap reserve overflow"))?;
            checkpoint = checkpoint
                .checked_add(72)
                .and_then(|bytes| bytes.checked_add(bitmap_bytes))
                .ok_or_else(|| Error::invalid_config("checkpoint reserve overflow"))?;
            largest_segment_streams = largest_segment_streams.max(
                u64::try_from(segment.unique_stream_count())
                    .map_err(|_| Error::invalid_config("segment stream count does not fit u64"))?,
            );
        }
        let manifest = self.largest_manifest_group(largest_segment_streams)?;
        checkpoint
            .checked_add(manifest)
            .and_then(|bytes| {
                bytes.checked_add(if self.active_segment.is_some() {
                    SEGMENT_FOOTER_LEN
                } else {
                    0
                })
            })
            .ok_or_else(|| Error::invalid_config("maintenance headroom overflow"))
    }

    fn largest_manifest_group(&self, largest_segment_streams: u64) -> Result<u64> {
        let release_records = u64::try_from(self.config.max_release_records)
            .map_err(|_| Error::invalid_config("release bound does not fit u64"))?;
        let release_frame = MANIFEST_FRAME_HEADER_LEN
            .checked_add(24)
            .and_then(|bytes| bytes.checked_add(release_records.checked_mul(16)?))
            .ok_or_else(|| Error::invalid_config("release frame reserve overflow"))?;
        let removal_frame = 64_u64
            .checked_add(
                largest_segment_streams
                    .checked_mul(16)
                    .ok_or_else(|| Error::invalid_config("removal frame reserve overflow"))?,
            )
            .ok_or_else(|| Error::invalid_config("removal frame reserve overflow"))?;
        let removal_frames = u64::try_from(AUTOMATIC_RECLAIM_SEGMENTS_PER_JOB)
            .map_err(|_| Error::invalid_config("reclaim batch size does not fit u64"))?;
        let removal_group = removal_frame
            .checked_mul(removal_frames)
            .ok_or_else(|| Error::invalid_config("removal group reserve overflow"))?;
        Ok(release_frame.max(80).max(removal_group))
    }

    fn append_manifest_group(
        &mut self,
        bodies: &[ManifestBody],
        outcome: DurabilityOutcome,
    ) -> Result<()> {
        let previous_control_bytes = self.manifest.control_file_len()?;
        self.manifest.append_group(bodies, outcome)?;
        let current_control_bytes = self.manifest.control_file_len()?;
        self.replace_actual_file_bytes(previous_control_bytes, current_control_bytes)
    }

    fn replace_actual_file_bytes(&mut self, previous: u64, current: u64) -> Result<()> {
        self.actual_file_bytes = self
            .actual_file_bytes
            .checked_sub(previous)
            .and_then(|bytes| bytes.checked_add(current))
            .ok_or_else(|| {
                Error::corruption(
                    &self.config.root,
                    0,
                    "root byte accounting replacement overflow",
                )
            })?;
        Ok(())
    }

    fn subtract_actual_file_bytes(&mut self, bytes: u64) -> Result<()> {
        self.actual_file_bytes = self.actual_file_bytes.checked_sub(bytes).ok_or_else(|| {
            Error::corruption(&self.config.root, 0, "root byte accounting underflow")
        })?;
        Ok(())
    }

    fn calculate_actual_file_bytes_from_disk(&self) -> Result<u64> {
        let mut bytes = ROOT_SUPERBLOCK_LEN;
        for path in [
            self.config.root.join(CHECKPOINT_FILE),
            self.config.root.join(MANIFEST_LOG_FILE),
        ] {
            bytes = bytes
                .checked_add(file_len(&path)?)
                .ok_or_else(|| Error::corruption(&path, 0, "root byte accounting overflow"))?;
        }
        for segment in self.segments.values() {
            bytes = bytes.checked_add(segment.file_len).ok_or_else(|| {
                Error::corruption(&segment.path, 0, "root byte accounting overflow")
            })?;
        }
        Ok(bytes)
    }
}

fn initialize_or_validate_root(root: &Path) -> Result<u64> {
    let path = root.join(ROOT_FILE);
    if path.exists() {
        return Ok(0);
    }
    let segments = root.join(files::SEGMENTS_DIRECTORY);
    if segments.exists()
        && fs::read_dir(&segments)
            .map_err(|error| {
                Error::io(
                    "enumerate partial segment directory",
                    &segments,
                    DurabilityOutcome::NotApplicable,
                    error,
                )
            })?
            .next()
            .is_some()
    {
        return Err(Error::corruption(
            root,
            0,
            "ROOT is missing while canonical segment state exists",
        ));
    }
    if root.join(CHECKPOINT_FILE).exists() || root.join(MANIFEST_LOG_FILE).exists() {
        return Err(Error::corruption(
            root,
            0,
            "ROOT is missing while canonical manifest state exists",
        ));
    }
    let removed_temporaries = remove_root_temporaries(root)?;
    let root_id = RootId::random().map_err(|error| {
        Error::io(
            "generate root identity",
            root,
            DurabilityOutcome::Unknown,
            error,
        )
    })?;
    atomic_replace(
        &root.join(ROOT_TEMP_FILE),
        &path,
        &RootSuperblock { root_id }.encode(),
        DurabilityOutcome::Unknown,
    )?;
    Ok(removed_temporaries)
}

fn complete_empty_control_initialization(
    root: &Path,
    root_id: RootId,
    segments_directory: &Path,
) -> Result<()> {
    let checkpoint = root.join(CHECKPOINT_FILE);
    let manifest_log = root.join(MANIFEST_LOG_FILE);
    if checkpoint.exists() && manifest_log.exists() {
        return Ok(());
    }
    if fs::read_dir(segments_directory)
        .map_err(|error| {
            Error::io(
                "enumerate segment directory",
                segments_directory,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })?
        .next()
        .is_some()
    {
        return Err(Error::corruption(
            root,
            0,
            "manifest artifacts are missing while segment state exists",
        ));
    }
    if checkpoint.exists() {
        let bytes = read_complete_file(&checkpoint)?;
        let state = Checkpoint::decode(&bytes)
            .map_err(|error| Error::corruption(&checkpoint, 0, error.to_string()))?;
        if state.root_id != root_id
            || state.last_applied_seq != 0
            || state.next_segment_id != 0
            || !state.stream_highwaters.is_empty()
            || !state.segments.is_empty()
        {
            return Err(Error::corruption(
                &checkpoint,
                0,
                "missing manifest peer is not a partial empty-root initialization",
            ));
        }
    }
    if manifest_log.exists() {
        let bytes = read_complete_file(&manifest_log)?;
        let header = ManifestLogHeader::decode(&bytes)
            .map_err(|error| Error::corruption(&manifest_log, 0, error.to_string()))?;
        if header.root_id != root_id || header.base_seq != 1 {
            return Err(Error::corruption(
                &manifest_log,
                0,
                "missing checkpoint peer is not a partial empty-root initialization",
            ));
        }
    }
    for path in [&checkpoint, &manifest_log] {
        if path.exists() {
            fs::remove_file(path).map_err(|error| {
                Error::io(
                    "remove partial empty-root control file",
                    path,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
        }
    }
    sync_directory(root, DurabilityOutcome::Unknown)?;
    manifest::create_initial(root, root_id)
}

fn remove_root_temporaries(root: &Path) -> Result<u64> {
    let mut removed = 0_u64;
    for name in [
        ROOT_TEMP_FILE,
        files::CHECKPOINT_TEMP_FILE,
        files::MANIFEST_LOG_TEMP_FILE,
    ] {
        let path = root.join(name);
        if path.exists() {
            fs::remove_file(&path).map_err(|error| {
                Error::io(
                    "remove stale root temporary",
                    &path,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
            removed = removed.saturating_add(1);
        }
    }
    if removed != 0 {
        sync_directory(root, DurabilityOutcome::Unknown)?;
    }
    Ok(removed)
}

fn enumerate_segments(
    directory: &Path,
    root_id: RootId,
    control: &ControlRecovery,
) -> Result<(BTreeMap<u64, PathBuf>, u64, u64)> {
    let mut segments = BTreeMap::new();
    let mut completed_deletions = 0_u64;
    let mut removed_temporaries = 0_u64;
    for entry in fs::read_dir(directory).map_err(|error| {
        Error::io(
            "enumerate segment directory",
            directory,
            DurabilityOutcome::NotApplicable,
            error,
        )
    })? {
        let entry = entry.map_err(|error| {
            Error::io(
                "read segment directory entry",
                directory,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })?;
        let name = entry.file_name();
        let path = entry.path();
        if parse_segment_temporary_name(&name).is_some() {
            fs::remove_file(&path).map_err(|error| {
                Error::io(
                    "remove stale temporary segment",
                    &path,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
            removed_temporaries = removed_temporaries.saturating_add(1);
            continue;
        }
        let Some(segment_id) = parse_segment_name(&name) else {
            return Err(Error::corruption(
                &path,
                0,
                "unknown entry in canonical segment directory",
            ));
        };
        if control.removed_segments.contains_key(&segment_id) {
            validate_removed_segment_header(&path, root_id, segment_id)?;
            fs::remove_file(&path).map_err(|error| {
                Error::io(
                    "complete durable segment deletion",
                    &path,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
            completed_deletions = completed_deletions.saturating_add(1);
            continue;
        }
        if segments.insert(segment_id, path.clone()).is_some() {
            return Err(Error::corruption(
                &path,
                0,
                "duplicate canonical segment ID",
            ));
        }
    }
    if completed_deletions != 0 || removed_temporaries != 0 {
        sync_directory(directory, DurabilityOutcome::Unknown)?;
    }

    for segment_id in segments.keys() {
        if !control.live_segments.contains_key(segment_id)
            && *segment_id < control.checkpoint_next_segment_id
        {
            return Err(Error::corruption(
                &segments[segment_id],
                0,
                "unexplained canonical segment ID",
            ));
        }
    }
    let suffix_ids = control
        .live_segments
        .iter()
        .filter_map(|(segment_id, segment)| {
            (segment.checkpoint_record_count.is_none()
                && *segment_id >= control.checkpoint_next_segment_id)
                .then_some(*segment_id)
        })
        .chain(
            control
                .removed_segments
                .keys()
                .copied()
                .filter(|segment_id| *segment_id >= control.checkpoint_next_segment_id),
        )
        .chain(
            segments
                .keys()
                .copied()
                .filter(|segment_id| !control.live_segments.contains_key(segment_id)),
        )
        .collect::<BTreeSet<_>>();
    let mut expected = Some(control.checkpoint_next_segment_id);
    for segment_id in suffix_ids {
        if expected != Some(segment_id) {
            return Err(Error::corruption(
                directory,
                0,
                format!(
                    "post-checkpoint segment allocation has ID {segment_id}, expected {}",
                    expected.map_or_else(|| "exhausted".to_string(), |id| id.to_string())
                ),
            ));
        }
        expected = segment_id.checked_add(1);
    }
    Ok((segments, completed_deletions, removed_temporaries))
}

fn derive_next_segment_id(
    checkpoint_next: u64,
    live: impl Iterator<Item = u64>,
    removed: impl Iterator<Item = u64>,
) -> Result<u64> {
    let greatest = live.chain(removed).max();
    let derived = match greatest {
        Some(u64::MAX) => return Err(Error::SegmentIdExhausted),
        Some(id) => id.checked_add(1).ok_or(Error::SegmentIdExhausted)?,
        None => 0,
    };
    Ok(checkpoint_next.max(derived))
}

fn coalesce_sequences(sequences: &[u64]) -> Vec<SequenceRange> {
    let mut ranges = Vec::new();
    for sequence in sequences {
        match ranges.last_mut() {
            Some(SequenceRange { start, len })
                if start
                    .checked_add(*len)
                    .is_some_and(|next| next == *sequence) =>
            {
                *len += 1;
            }
            _ => ranges.push(SequenceRange {
                start: *sequence,
                len: 1,
            }),
        }
    }
    ranges
}

fn unix_millis() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Runtime {
            message: format!("system clock is before Unix epoch: {error}"),
        })?;
    u64::try_from(duration.as_millis()).map_err(|_| Error::Runtime {
        message: "Unix millisecond timestamp does not fit u64".to_string(),
    })
}

pub(crate) fn encoded_epoch_bytes(records: &[Record]) -> Result<u64> {
    if records.is_empty() {
        return Err(Error::EmptyAppend);
    }
    records.iter().try_fold(
        crate::format::EPOCH_HEADER_LEN + crate::format::EPOCH_COMMIT_LEN,
        |total, record| {
            let metadata = u64::try_from(record.metadata.len())
                .map_err(|_| Error::invalid_config("metadata length does not fit u64"))?;
            let payload = u64::try_from(record.payload.len())
                .map_err(|_| Error::invalid_config("payload length does not fit u64"))?;
            total
                .checked_add(RECORD_DESCRIPTOR_LEN)
                .and_then(|bytes| bytes.checked_add(metadata))
                .and_then(|bytes| bytes.checked_add(payload))
                .ok_or_else(|| Error::invalid_config("encoded epoch length overflow"))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::TempDir;

    fn assert_file_byte_accounting(storage: &Storage) {
        let control_bytes = file_len(&storage.config.root.join(CHECKPOINT_FILE))
            .unwrap()
            .checked_add(file_len(&storage.config.root.join(MANIFEST_LOG_FILE)).unwrap())
            .unwrap();
        assert_eq!(storage.manifest.control_file_len().unwrap(), control_bytes);
        assert_eq!(
            storage.actual_file_bytes,
            storage.calculate_actual_file_bytes_from_disk().unwrap()
        );
    }

    fn config(directory: &TempDir) -> Config {
        Config::new(directory.path(), Capacity::Unbounded)
            .with_max_epoch_bytes(1024 * 1024)
            .with_segment_bytes(2 * 1024 * 1024)
            .with_max_commit_bytes(2 * 1024 * 1024)
    }

    #[test]
    fn manifest_compaction_policy_uses_log_and_capacity_thresholds() {
        assert!(!manifest_compaction_required(
            MANIFEST_COMPACTION_TRIGGER_BYTES - 1,
            100,
            100,
            Capacity::Unbounded,
        ));
        assert!(manifest_compaction_required(
            MANIFEST_COMPACTION_TRIGGER_BYTES,
            100,
            100,
            Capacity::Unbounded,
        ));
        let bounded = Capacity::Bounded {
            total_bytes: 200,
            when_full: crate::config::FullPolicy::RejectNew,
        };
        assert!(!manifest_compaction_required(1, 100, 100, bounded));
        assert!(manifest_compaction_required(1, 101, 100, bounded));
    }

    #[test]
    fn file_byte_accounting_tracks_every_durable_transition() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(91);
        {
            let mut storage = Storage::open(config(&directory)).unwrap();
            assert_file_byte_accounting(&storage);

            let first = storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"first"))],
                }])
                .unwrap()
                .remove(0)
                .remove(0);
            assert_file_byte_accounting(&storage);

            let second = storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"second"))],
                }])
                .unwrap()
                .remove(0)
                .remove(0);
            assert_file_byte_accounting(&storage);

            storage.seal_active(SealReason::Size).unwrap();
            assert_file_byte_accounting(&storage);

            storage.compact_manifest().unwrap();
            assert_file_byte_accounting(&storage);

            storage
                .release_group(vec![ReleaseUnit {
                    stream_id,
                    ids: vec![first, second],
                }])
                .unwrap();
            assert_file_byte_accounting(&storage);

            let report = storage.reclaim(ReclaimKind::Explicit).unwrap();
            assert_eq!(report.segments, 1);
            assert_file_byte_accounting(&storage);
        }

        let storage = Storage::open(config(&directory)).unwrap();
        assert_file_byte_accounting(&storage);
        assert_eq!(storage.stream_stats(stream_id).pending_records, 0);
    }

    #[test]
    fn segment_stream_count_tracks_appends_and_recovery() {
        let directory = TempDir::new().unwrap();
        let first = StreamId::new(1);
        let second = StreamId::new(2);
        let third = StreamId::new(3);
        {
            let mut storage = Storage::open(config(&directory)).unwrap();
            storage
                .append_group(vec![
                    AppendUnit {
                        stream_id: first,
                        records: vec![Record::new(Bytes::from_static(b"first"))],
                    },
                    AppendUnit {
                        stream_id: second,
                        records: vec![Record::new(Bytes::from_static(b"second"))],
                    },
                ])
                .unwrap();
            storage
                .append_group(vec![AppendUnit {
                    stream_id: first,
                    records: vec![Record::new(Bytes::from_static(b"again"))],
                }])
                .unwrap();
            let active = storage.active_segment.unwrap();
            assert_eq!(storage.segments[&active].unique_stream_count(), 2);
            assert_eq!(storage.streams[&first].last_segment_id, Some(active));
            assert_eq!(storage.streams[&second].last_segment_id, Some(active));
            storage.seal_active(SealReason::Size).unwrap();
            storage
                .append_group(vec![
                    AppendUnit {
                        stream_id: first,
                        records: vec![Record::new(Bytes::from_static(b"new segment"))],
                    },
                    AppendUnit {
                        stream_id: third,
                        records: vec![Record::new(Bytes::from_static(b"third"))],
                    },
                ])
                .unwrap();
            let next = storage.active_segment.unwrap();
            assert_ne!(next, active);
            assert_eq!(storage.segments[&active].unique_stream_count(), 2);
            assert_eq!(storage.segments[&next].unique_stream_count(), 2);
            assert_eq!(storage.streams[&first].last_segment_id, Some(next));
            assert_eq!(storage.streams[&second].last_segment_id, Some(active));
            assert_eq!(storage.streams[&third].last_segment_id, Some(next));
        }

        let mut storage = Storage::open(config(&directory)).unwrap();
        let active = storage.active_segment.unwrap();
        let sealed = *storage.segments.keys().next().unwrap();
        assert_ne!(active, sealed);
        assert_eq!(storage.segments[&sealed].unique_stream_count(), 2);
        assert_eq!(storage.segments[&active].unique_stream_count(), 2);
        assert_eq!(storage.streams[&first].last_segment_id, Some(active));
        assert_eq!(storage.streams[&second].last_segment_id, Some(sealed));
        assert_eq!(storage.streams[&third].last_segment_id, Some(active));

        storage
            .append_group(vec![AppendUnit {
                stream_id: second,
                records: vec![Record::new(Bytes::from_static(b"second again"))],
            }])
            .unwrap();
        assert_eq!(storage.segments[&active].unique_stream_count(), 3);
        assert_eq!(storage.streams[&second].last_segment_id, Some(active));
    }

    #[test]
    fn append_release_reclaim_and_recover() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(7);
        let ids = {
            let mut storage = Storage::open(config(&directory)).unwrap();
            let ids = storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"one"))],
                }])
                .unwrap()
                .remove(0);
            assert_eq!(storage.commit_stats().append_groups, 1);
            assert_eq!(storage.commit_stats().append_records, 1);
            let snapshot = storage
                .read(stream_id, ReadLimits::new(8, 1024))
                .unwrap()
                .unwrap();
            assert_eq!(snapshot[0].payload, Bytes::from_static(b"one"));
            storage
                .release_group(vec![ReleaseUnit {
                    stream_id,
                    ids: ids.clone(),
                }])
                .unwrap();
            assert_eq!(storage.commit_stats().release_groups, 1);
            assert_eq!(storage.commit_stats().release_records, 1);
            let report = storage.reclaim(ReclaimKind::Explicit).unwrap();
            assert_eq!(report.segments, 1);
            assert_eq!(storage.maintenance_stats().explicit_reclaim_passes, 1);
            assert_eq!(storage.maintenance_stats().reclaimed_segments, 1);
            ids
        };

        let storage = Storage::open(config(&directory)).unwrap();
        assert_eq!(storage.known_streams(), vec![stream_id]);
        assert!(storage
            .read(stream_id, ReadLimits::new(8, 1024))
            .unwrap()
            .is_none());
        assert_eq!(ids[0].stream_id(), stream_id);
    }

    #[test]
    fn coalesced_read_preserves_order_across_released_gaps() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(70);
        let mut storage = Storage::open(config(&directory)).unwrap();
        let ids = storage
            .append_group(vec![AppendUnit {
                stream_id,
                records: (0_u8..4)
                    .map(|value| Record {
                        metadata: Bytes::from(vec![value; 8]),
                        payload: Bytes::from(vec![value; 64]),
                    })
                    .collect(),
            }])
            .unwrap()
            .remove(0);
        storage
            .release_group(vec![ReleaseUnit {
                stream_id,
                ids: vec![ids[1], ids[2]],
            }])
            .unwrap();

        let snapshot = storage
            .read(stream_id, ReadLimits::new(4, 256))
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].id, ids[0]);
        assert_eq!(snapshot[0].metadata, Bytes::from(vec![0; 8]));
        assert_eq!(snapshot[0].payload, Bytes::from(vec![0; 64]));
        assert_eq!(snapshot[1].id, ids[3]);
        assert_eq!(snapshot[1].metadata, Bytes::from(vec![3; 8]));
        assert_eq!(snapshot[1].payload, Bytes::from(vec![3; 64]));
    }

    #[test]
    fn coalesced_read_fails_the_snapshot_on_body_corruption() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(71);
        let mut storage = Storage::open(config(&directory)).unwrap();
        storage
            .append_group(vec![AppendUnit {
                stream_id,
                records: (0_u8..4)
                    .map(|value| Record {
                        metadata: Bytes::from(vec![value; 8]),
                        payload: Bytes::from(vec![value; 64]),
                    })
                    .collect(),
            }])
            .unwrap();
        let segment_id = storage.active_segment.unwrap();
        let segment = &storage.segments[&segment_id];
        let payload_offset = segment.records[2].payload_offset;
        let path = segment.path.clone();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(payload_offset)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xff;
        file.seek(SeekFrom::Start(payload_offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_data().unwrap();

        assert!(matches!(
            storage.read(stream_id, ReadLimits::new(4, 256)),
            Err(Error::Corruption { offset, .. }) if offset == payload_offset
        ));
    }

    #[test]
    fn bounded_coalesced_read_preserves_records_across_span_splits() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(72);
        let mut storage = Storage::open(
            config(&directory)
                .with_max_epoch_bytes(2 * 1024 * 1024)
                .with_segment_bytes(3 * 1024 * 1024)
                .with_max_commit_bytes(2 * 1024 * 1024),
        )
        .unwrap();
        let expected = (0_u8..20)
            .map(|value| Record {
                metadata: Bytes::from(vec![value; 8]),
                payload: Bytes::from(vec![value; 64 * 1024]),
            })
            .collect::<Vec<_>>();
        let ids = storage
            .append_group(vec![AppendUnit {
                stream_id,
                records: expected.clone(),
            }])
            .unwrap()
            .remove(0);

        let snapshot = storage
            .read(stream_id, ReadLimits::new(expected.len(), 2 * 1024 * 1024))
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.len(), expected.len());
        for ((pending, expected), id) in snapshot.iter().zip(&expected).zip(ids) {
            assert_eq!(pending.id, id);
            assert_eq!(pending.metadata, expected.metadata);
            assert_eq!(pending.payload, expected.payload);
        }
    }

    #[test]
    fn vectored_epoch_write_preserves_records_across_platform_iovec_bounds() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(73);
        let expected = (0_u16..500)
            .map(|index| {
                let value = (index % 251) as u8;
                Record {
                    metadata: if index % 3 == 0 {
                        Bytes::new()
                    } else {
                        Bytes::from(vec![value; 8])
                    },
                    payload: if index % 4 == 0 {
                        Bytes::new()
                    } else {
                        Bytes::from(vec![value; 64])
                    },
                }
            })
            .collect::<Vec<_>>();
        let ids = {
            let mut storage = Storage::open(config(&directory)).unwrap();
            storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: expected.clone(),
                }])
                .unwrap()
                .remove(0)
        };

        let storage = Storage::open(config(&directory)).unwrap();
        let snapshot = storage
            .read(stream_id, ReadLimits::new(expected.len(), 64 * 500))
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.len(), expected.len());
        for ((pending, expected), id) in snapshot.iter().zip(&expected).zip(ids) {
            assert_eq!(pending.id, id);
            assert_eq!(pending.metadata, expected.metadata);
            assert_eq!(pending.payload, expected.payload);
        }
    }

    #[test]
    fn bounded_release_keeps_a_small_durable_manifest_suffix() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(8);
        let bounded = Config::new(
            directory.path(),
            Capacity::Bounded {
                total_bytes: 16 * 1024 * 1024,
                when_full: crate::config::FullPolicy::RejectNew,
            },
        )
        .with_max_epoch_bytes(128 * 1024)
        .with_segment_bytes(256 * 1024)
        .with_max_release_records(1024)
        .with_max_commit_bytes(256 * 1024);
        {
            let mut storage = Storage::open(bounded.clone()).unwrap();
            let id = storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"pending"))],
                }])
                .unwrap()
                .remove(0)
                .remove(0);
            storage
                .release_group(vec![ReleaseUnit {
                    stream_id,
                    ids: vec![id],
                }])
                .unwrap();
            assert_eq!(storage.maintenance_stats().manifest_compactions, 0);
            assert!(storage.manifest.file_len() > MANIFEST_LOG_HEADER_LEN);
        }

        let storage = Storage::open(bounded).unwrap();
        assert_eq!(storage.stream_stats(stream_id).pending_records, 0);
    }

    #[test]
    fn automatic_reclaim_limits_and_batches_each_storage_job() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(9);
        let compact_segments = Config::new(directory.path(), Capacity::Unbounded)
            .with_max_epoch_bytes(128 * 1024)
            .with_segment_bytes(160 * 1024)
            .with_max_release_records(1024)
            .with_max_commit_bytes(256 * 1024);
        {
            let mut storage = Storage::open(compact_segments.clone()).unwrap();
            let mut ids = Vec::new();
            for value in 0_u8..10 {
                ids.extend(
                    storage
                        .append_group(vec![AppendUnit {
                            stream_id,
                            records: vec![Record::new(Bytes::from(vec![value; 120 * 1024]))],
                        }])
                        .unwrap()
                        .remove(0),
                );
            }
            storage
                .release_group(vec![ReleaseUnit { stream_id, ids }])
                .unwrap();

            let first = storage.reclaim(ReclaimKind::Automatic).unwrap();
            assert_eq!(first.segments, 4);
            assert!(storage.has_automatic_reclaim_work());
            let second = storage.reclaim(ReclaimKind::Automatic).unwrap();
            assert_eq!(second.segments, 4);
            assert!(storage.has_automatic_reclaim_work());
            let third = storage.reclaim(ReclaimKind::Automatic).unwrap();
            assert_eq!(third.segments, 2);
            assert!(!storage.has_automatic_reclaim_work());
            assert_eq!(storage.maintenance_stats().automatic_reclaim_passes, 3);
            assert_eq!(storage.maintenance_stats().manifest_compactions, 1);
        }

        let storage = Storage::open(compact_segments).unwrap();
        assert_eq!(storage.stream_stats(stream_id).pending_records, 0);
        assert_eq!(storage.storage_stats().unwrap().live_segments, 0);
    }

    #[test]
    fn explicit_reclaim_finishes_all_bounded_batches_in_one_pass() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(10);
        let config = Config::new(directory.path(), Capacity::Unbounded)
            .with_max_epoch_bytes(128 * 1024)
            .with_segment_bytes(160 * 1024)
            .with_max_release_records(1024)
            .with_max_commit_bytes(256 * 1024);
        let mut storage = Storage::open(config).unwrap();
        let mut ids = Vec::new();
        for value in 0_u8..10 {
            ids.extend(
                storage
                    .append_group(vec![AppendUnit {
                        stream_id,
                        records: vec![Record::new(Bytes::from(vec![value; 120 * 1024]))],
                    }])
                    .unwrap()
                    .remove(0),
            );
        }
        storage
            .release_group(vec![ReleaseUnit { stream_id, ids }])
            .unwrap();

        let report = storage.reclaim(ReclaimKind::Explicit).unwrap();
        assert_eq!(report.segments, 10);
        assert_eq!(storage.maintenance_stats().explicit_reclaim_passes, 1);
        assert_eq!(storage.maintenance_stats().reclaimed_segments, 10);
        assert_eq!(storage.storage_stats().unwrap().live_segments, 0);
    }

    #[test]
    fn release_frame_alone_excludes_record_after_recovery() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(11);
        {
            let mut storage = Storage::open(config(&directory)).unwrap();
            let id = storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"pending"))],
                }])
                .unwrap()
                .remove(0)
                .remove(0);
            storage
                .release_group(vec![ReleaseUnit {
                    stream_id,
                    ids: vec![id],
                }])
                .unwrap();
        }

        let storage = Storage::open(config(&directory)).unwrap();
        assert!(storage
            .read(stream_id, ReadLimits::new(1, 1024))
            .unwrap()
            .is_none());
    }

    #[test]
    fn incomplete_active_epoch_is_repaired_but_complete_corruption_fails() {
        let repair_root = TempDir::new().unwrap();
        let stream_id = StreamId::new(13);
        {
            let mut storage = Storage::open(config(&repair_root)).unwrap();
            for payload in [b"first".as_slice(), b"second".as_slice()] {
                storage
                    .append_group(vec![AppendUnit {
                        stream_id,
                        records: vec![Record::new(Bytes::copy_from_slice(payload))],
                    }])
                    .unwrap();
            }
        }
        let path = segment_path(&repair_root.path().join(files::SEGMENTS_DIRECTORY), 0);
        let length = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(length - 1)
            .unwrap();
        let repaired = Storage::open(config(&repair_root)).unwrap();
        assert_eq!(repaired.recovery_stats().repaired_active_tails, 1);
        let snapshot = repaired
            .read(stream_id, ReadLimits::new(8, 1024))
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].payload, Bytes::from_static(b"first"));

        let corrupt_root = TempDir::new().unwrap();
        {
            let mut storage = Storage::open(config(&corrupt_root)).unwrap();
            storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"complete"))],
                }])
                .unwrap();
        }
        let path = segment_path(&corrupt_root.path().join(files::SEGMENTS_DIRECTORY), 0);
        flip_last_byte(&path);
        assert!(matches!(
            Storage::open(config(&corrupt_root)),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn recovery_reports_completion_of_an_unpublished_segment_seal() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(14);
        {
            let mut storage = Storage::open(config(&directory)).unwrap();
            storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"sealed"))],
                }])
                .unwrap();
            let active = storage.active_segment.unwrap();
            storage
                .segments
                .get_mut(&active)
                .unwrap()
                .seal_data()
                .unwrap();
        }

        {
            let recovered = Storage::open(config(&directory)).unwrap();
            assert_eq!(recovered.recovery_stats().completed_segment_seals, 1);
            assert_eq!(recovered.stream_stats(stream_id).pending_records, 1);
        }

        let stable = Storage::open(config(&directory)).unwrap();
        assert_eq!(stable.recovery_stats().completed_segment_seals, 0);
        assert_eq!(stable.recovery_stats().manifest_frames_scanned, 1);
    }

    #[test]
    fn checkpoint_baseline_damage_fails_without_truncating_the_segment() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(15);
        {
            let mut storage = Storage::open(config(&directory)).unwrap();
            for payload in [b"first".as_slice(), b"checkpointed".as_slice()] {
                storage
                    .append_group(vec![AppendUnit {
                        stream_id,
                        records: vec![Record::new(Bytes::copy_from_slice(payload))],
                    }])
                    .unwrap();
            }
            storage.compact_manifest().unwrap();
        }

        let path = segment_path(&directory.path().join(files::SEGMENTS_DIRECTORY), 0);
        let damaged_length = fs::metadata(&path).unwrap().len() - 1;
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(damaged_length).unwrap();
        file.sync_data().unwrap();
        drop(file);

        assert!(matches!(
            Storage::open(config(&directory)),
            Err(Error::Corruption { .. })
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), damaged_length);
    }

    #[test]
    fn incomplete_tail_after_checkpoint_baseline_is_repaired() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(16);
        {
            let mut storage = Storage::open(config(&directory)).unwrap();
            storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"checkpointed"))],
                }])
                .unwrap();
            storage.compact_manifest().unwrap();
            storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"incomplete"))],
                }])
                .unwrap();
        }

        let path = segment_path(&directory.path().join(files::SEGMENTS_DIRECTORY), 0);
        let damaged_length = fs::metadata(&path).unwrap().len() - 1;
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(damaged_length).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let recovered = Storage::open(config(&directory)).unwrap();
        assert_eq!(recovered.recovery_stats().repaired_active_tails, 1);
        let snapshot = recovered
            .read(stream_id, ReadLimits::new(8, 1024))
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].payload, Bytes::from_static(b"checkpointed"));
    }

    #[test]
    fn incomplete_manifest_frame_is_repaired_but_complete_corruption_fails() {
        let repair_root = TempDir::new().unwrap();
        let stream_id = StreamId::new(17);
        {
            let mut storage = Storage::open(config(&repair_root)).unwrap();
            let ids = storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![
                        Record::new(Bytes::from_static(b"first")),
                        Record::new(Bytes::from_static(b"second")),
                    ],
                }])
                .unwrap()
                .remove(0);
            storage
                .release_group(vec![ReleaseUnit {
                    stream_id,
                    ids: vec![ids[0]],
                }])
                .unwrap();
        }
        let manifest = repair_root.path().join(MANIFEST_LOG_FILE);
        let length = fs::metadata(&manifest).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&manifest)
            .unwrap()
            .set_len(length - 1)
            .unwrap();
        let repaired = Storage::open(config(&repair_root)).unwrap();
        assert_eq!(repaired.recovery_stats().repaired_manifest_tails, 1);
        assert_eq!(repaired.stream_stats(stream_id).pending_records, 2);

        let corrupt_root = TempDir::new().unwrap();
        {
            let mut storage = Storage::open(config(&corrupt_root)).unwrap();
            let ids = storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![
                        Record::new(Bytes::from_static(b"first")),
                        Record::new(Bytes::from_static(b"second")),
                    ],
                }])
                .unwrap()
                .remove(0);
            storage
                .release_group(vec![ReleaseUnit {
                    stream_id,
                    ids: vec![ids[0]],
                }])
                .unwrap();
        }
        let manifest = corrupt_root.path().join(MANIFEST_LOG_FILE);
        flip_last_byte(&manifest);
        assert!(matches!(
            Storage::open(config(&corrupt_root)),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn missing_control_peer_cannot_reset_nonempty_durable_state() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(19);
        {
            let mut storage = Storage::open(config(&directory)).unwrap();
            let id = storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from_static(b"record"))],
                }])
                .unwrap()
                .remove(0)
                .remove(0);
            storage
                .release_group(vec![ReleaseUnit {
                    stream_id,
                    ids: vec![id],
                }])
                .unwrap();
            storage.reclaim(ReclaimKind::Explicit).unwrap();
        }
        fs::remove_file(directory.path().join(MANIFEST_LOG_FILE)).unwrap();
        assert!(matches!(
            Storage::open(config(&directory)),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn expired_active_segment_seals_at_the_reactor_deadline_boundary() {
        let directory = TempDir::new().unwrap();
        let mut storage = Storage::open(
            config(&directory).with_max_segment_age(std::time::Duration::from_millis(100)),
        )
        .unwrap();
        storage
            .append_group(vec![AppendUnit {
                stream_id: StreamId::new(23),
                records: vec![Record::new(Bytes::from_static(b"age"))],
            }])
            .unwrap();
        let active = storage.active_segment.unwrap();
        storage
            .segments
            .get_mut(&active)
            .unwrap()
            .header
            .created_at_unix_millis = unix_millis().unwrap().saturating_sub(100);
        assert_eq!(
            storage.next_age_delay().unwrap(),
            Some(std::time::Duration::ZERO)
        );
        assert!(storage.seal_expired().unwrap());
        assert!(storage.segments[&active].footer.is_some());
        assert!(storage.active_segment.is_none());
    }

    #[test]
    fn append_commit_group_rolls_over_as_one_physical_unit() {
        let directory = TempDir::new().unwrap();
        let config = Config::new(directory.path(), Capacity::Unbounded)
            .with_max_epoch_bytes(256)
            .with_segment_bytes(500)
            .with_max_release_records(8)
            .with_max_commit_bytes(404);
        let mut storage = Storage::open(config).unwrap();
        storage
            .append_group(vec![AppendUnit {
                stream_id: StreamId::new(29),
                records: vec![Record::new(Bytes::from(vec![0_u8; 72]))],
            }])
            .unwrap();

        let ids = storage
            .append_group(vec![
                AppendUnit {
                    stream_id: StreamId::new(29),
                    records: vec![Record::new(Bytes::from(vec![1_u8; 22]))],
                },
                AppendUnit {
                    stream_id: StreamId::new(30),
                    records: vec![Record::new(Bytes::from(vec![2_u8; 22]))],
                },
            ])
            .unwrap();

        assert_eq!(storage.segments.len(), 2);
        assert!(storage.segments[&0].footer.is_some());
        assert_eq!(storage.segments[&1].epochs.len(), 2);
        assert_eq!(storage.segments[&1].records.len(), 2);
        assert_eq!(storage.active_segment, Some(1));
        assert_eq!(ids[0][0].sequence(), 1);
        assert_eq!(ids[1][0].sequence(), 0);
    }

    #[test]
    fn post_checkpoint_segment_id_gap_fails_closed() {
        let directory = TempDir::new().unwrap();
        {
            let _storage = Storage::open(config(&directory)).unwrap();
        }
        let root_path = directory.path().join(ROOT_FILE);
        let root = RootSuperblock::decode(&read_complete_file(&root_path).unwrap()).unwrap();
        let segments = directory.path().join(files::SEGMENTS_DIRECTORY);
        for (segment_id, sequence) in [(0, 0), (2, 1)] {
            let epoch = PreparedEpoch::new(
                StreamId::new(31),
                sequence,
                vec![Record::new(Bytes::from_static(b"record"))],
            )
            .unwrap();
            let (segment, _) = Segment::create(
                &segments,
                root.root_id,
                segment_id,
                unix_millis().unwrap(),
                vec![epoch],
            )
            .unwrap();
            drop(segment);
        }

        assert!(matches!(
            Storage::open(config(&directory)),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn lower_reopen_writer_bound_seals_an_oversized_active_segment() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(33);
        {
            let mut storage = Storage::open(
                Config::new(directory.path(), Capacity::Unbounded)
                    .with_max_epoch_bytes(512)
                    .with_segment_bytes(1_000)
                    .with_max_release_records(8)
                    .with_max_commit_bytes(512),
            )
            .unwrap();
            storage
                .append_group(vec![AppendUnit {
                    stream_id,
                    records: vec![Record::new(Bytes::from(vec![0_u8; 300]))],
                }])
                .unwrap();
        }

        let mut storage = Storage::open(
            Config::new(directory.path(), Capacity::Unbounded)
                .with_max_epoch_bytes(256)
                .with_segment_bytes(400)
                .with_max_release_records(8)
                .with_max_commit_bytes(256),
        )
        .unwrap();
        storage
            .append_group(vec![AppendUnit {
                stream_id,
                records: vec![Record::new(Bytes::from_static(b"new bound"))],
            }])
            .unwrap();

        assert_eq!(storage.segments.len(), 2);
        assert!(storage.segments[&0].footer.is_some());
        assert_eq!(storage.active_segment, Some(1));
        assert_eq!(storage.stream_stats(stream_id).pending_records, 2);
    }

    #[test]
    fn sequence_gap_inside_one_segment_fails_closed() {
        let directory = TempDir::new().unwrap();
        {
            let _storage = Storage::open(config(&directory)).unwrap();
        }
        let root_path = directory.path().join(ROOT_FILE);
        let root = RootSuperblock::decode(&read_complete_file(&root_path).unwrap()).unwrap();
        let stream_id = StreamId::new(35);
        let epochs = vec![
            PreparedEpoch::new(stream_id, 0, vec![Record::new(Bytes::from_static(b"zero"))])
                .unwrap(),
            PreparedEpoch::new(stream_id, 2, vec![Record::new(Bytes::from_static(b"gap"))])
                .unwrap(),
        ];
        let (segment, _) = Segment::create(
            &directory.path().join(files::SEGMENTS_DIRECTORY),
            root.root_id,
            0,
            unix_millis().unwrap(),
            epochs,
        )
        .unwrap();
        drop(segment);

        assert!(matches!(
            Storage::open(config(&directory)),
            Err(Error::Corruption { .. })
        ));
    }

    fn flip_last_byte(path: &Path) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let length = file.metadata().unwrap().len();
        file.seek(SeekFrom::Start(length - 1)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.seek(SeekFrom::Start(length - 1)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_data().unwrap();
    }
}
