use super::files::{
    atomic_replace, read_complete_file, read_exact_at, CHECKPOINT_FILE, CHECKPOINT_TEMP_FILE,
    MANIFEST_LOG_FILE, MANIFEST_LOG_TEMP_FILE,
};
use crate::error::{DurabilityOutcome, Error, Result};
use crate::format::{
    Checkpoint, CheckpointSegment, ManifestBody, ManifestFrame, ManifestFrameHeader,
    ManifestLogHeader, ReleaseBody, ReleaseEncoding, SegmentLifecycle, SegmentRemovedBody,
    MANIFEST_FRAME_HEADER_LEN, MANIFEST_LOG_HEADER_LEN,
};
use crate::model::{RootId, StreamId};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub(super) struct ControlSegment {
    pub(super) lifecycle: SegmentLifecycle,
    pub(super) checkpoint_record_count: Option<u64>,
    pub(super) checkpoint_releases: Option<ReleaseEncoding>,
}

pub(super) struct ControlRecovery {
    pub(super) frames_scanned: u64,
    pub(super) repaired_tail: bool,
    pub(super) checkpoint_next_segment_id: u64,
    pub(super) checkpoint_stream_highwaters: BTreeMap<StreamId, u64>,
    pub(super) stream_highwaters: BTreeMap<StreamId, u64>,
    pub(super) live_segments: BTreeMap<u64, ControlSegment>,
    pub(super) removed_segments: BTreeMap<u64, SegmentRemovedBody>,
    pub(super) releases: Vec<ReleaseBody>,
    pub(super) manifest: Manifest,
}

pub(super) struct Manifest {
    root: PathBuf,
    path: PathBuf,
    file: File,
    pub(super) last_seq: u64,
}

pub(super) fn create_initial(root: &Path, root_id: RootId) -> Result<()> {
    let checkpoint = Checkpoint {
        root_id,
        last_applied_seq: 0,
        next_segment_id: 0,
        stream_highwaters: BTreeMap::new(),
        segments: BTreeMap::new(),
    };
    let checkpoint_bytes = checkpoint.encode().map_err(|error| {
        Error::corruption(
            root.join(CHECKPOINT_FILE),
            0,
            format!("cannot encode initial checkpoint: {error}"),
        )
    })?;
    atomic_replace(
        &root.join(CHECKPOINT_TEMP_FILE),
        &root.join(CHECKPOINT_FILE),
        &checkpoint_bytes,
        DurabilityOutcome::Unknown,
    )?;

    let log = ManifestLogHeader {
        root_id,
        base_seq: 1,
    }
    .encode();
    atomic_replace(
        &root.join(MANIFEST_LOG_TEMP_FILE),
        &root.join(MANIFEST_LOG_FILE),
        &log,
        DurabilityOutcome::Unknown,
    )
}

pub(super) fn recover(root: &Path, root_id: RootId) -> Result<ControlRecovery> {
    let checkpoint_path = root.join(CHECKPOINT_FILE);
    let checkpoint_bytes = read_complete_file(&checkpoint_path)?;
    let checkpoint = Checkpoint::decode(&checkpoint_bytes)
        .map_err(|error| Error::corruption(&checkpoint_path, 0, error.to_string()))?;
    if checkpoint.root_id != root_id {
        return Err(Error::corruption(
            &checkpoint_path,
            8,
            "checkpoint root ID does not match ROOT",
        ));
    }

    let mut live_segments = checkpoint
        .segments
        .iter()
        .map(|(segment_id, segment)| {
            (
                *segment_id,
                ControlSegment {
                    lifecycle: segment.lifecycle,
                    checkpoint_record_count: Some(segment.record_count),
                    checkpoint_releases: Some(segment.releases.clone()),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let checkpoint_stream_highwaters = checkpoint.stream_highwaters.clone();
    let mut stream_highwaters = checkpoint.stream_highwaters;
    let mut removed_segments = BTreeMap::new();
    let mut releases = Vec::new();

    let log_path = root.join(MANIFEST_LOG_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)
        .map_err(|error| {
            Error::io(
                "open manifest log",
                &log_path,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })?;
    let mut file_len = file
        .metadata()
        .map_err(|error| {
            Error::io(
                "read manifest log metadata",
                &log_path,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })?
        .len();
    if file_len < MANIFEST_LOG_HEADER_LEN {
        return Err(Error::corruption(
            &log_path,
            0,
            "manifest log is shorter than its authoritative header",
        ));
    }

    let mut header_bytes = [0_u8; MANIFEST_LOG_HEADER_LEN as usize];
    read_exact_at(&file, &log_path, &mut header_bytes, 0)?;
    let header = ManifestLogHeader::decode(&header_bytes)
        .map_err(|error| Error::corruption(&log_path, 0, error.to_string()))?;
    if header.root_id != root_id {
        return Err(Error::corruption(
            &log_path,
            8,
            "manifest log root ID does not match ROOT",
        ));
    }
    if header.base_seq == 0 {
        return Err(Error::corruption(
            &log_path,
            24,
            "manifest log base sequence must be nonzero",
        ));
    }
    if checkpoint.last_applied_seq < u64::MAX && header.base_seq > checkpoint.last_applied_seq + 1 {
        return Err(Error::corruption(
            &log_path,
            24,
            "manifest log base creates a sequence gap after checkpoint",
        ));
    }

    let mut physical_seq = header.base_seq;
    let mut last_seq = checkpoint.last_applied_seq;
    let mut frames_scanned = 0_u64;
    let mut repaired_tail = false;
    let mut offset = MANIFEST_LOG_HEADER_LEN;
    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < MANIFEST_FRAME_HEADER_LEN {
            truncate_tail(&mut file, &log_path, offset)?;
            file_len = offset;
            repaired_tail = true;
            break;
        }

        let mut frame_header_bytes = [0_u8; MANIFEST_FRAME_HEADER_LEN as usize];
        read_exact_at(&file, &log_path, &mut frame_header_bytes, offset)?;
        let frame_header = ManifestFrameHeader::decode(&frame_header_bytes)
            .map_err(|error| Error::corruption(&log_path, offset, error.to_string()))?;
        if frame_header.manifest_seq != physical_seq {
            return Err(Error::corruption(
                &log_path,
                offset + 8,
                format!(
                    "manifest sequence {}, expected {physical_seq}",
                    frame_header.manifest_seq
                ),
            ));
        }
        let frame_len = MANIFEST_FRAME_HEADER_LEN
            .checked_add(frame_header.body_len)
            .ok_or_else(|| {
                Error::corruption(&log_path, offset, "manifest frame length overflow")
            })?;
        let frame_end = offset
            .checked_add(frame_len)
            .ok_or_else(|| Error::corruption(&log_path, offset, "manifest frame end overflow"))?;
        if frame_end > file_len {
            truncate_tail(&mut file, &log_path, offset)?;
            file_len = offset;
            repaired_tail = true;
            break;
        }
        let body_len = usize::try_from(frame_header.body_len).map_err(|_| {
            Error::corruption(
                &log_path,
                offset + 24,
                "manifest body length does not fit usize",
            )
        })?;
        let mut body = Vec::new();
        body.try_reserve_exact(body_len)
            .map_err(|error| Error::Runtime {
                message: format!("cannot reserve manifest frame body: {error}"),
            })?;
        body.resize(body_len, 0);
        read_exact_at(
            &file,
            &log_path,
            &mut body,
            offset + MANIFEST_FRAME_HEADER_LEN,
        )?;
        let frame = ManifestFrame::decode(frame_header, &body)
            .map_err(|error| Error::corruption(&log_path, offset, error.to_string()))?;
        frames_scanned = frames_scanned.saturating_add(1);

        if frame.manifest_seq > checkpoint.last_applied_seq {
            let expected = last_seq.checked_add(1).ok_or_else(|| {
                Error::corruption(&log_path, offset, "manifest sequence overflow")
            })?;
            if frame.manifest_seq != expected {
                return Err(Error::corruption(
                    &log_path,
                    offset + 8,
                    format!(
                        "manifest suffix sequence {}, expected {expected}",
                        frame.manifest_seq
                    ),
                ));
            }
            apply_frame(
                &log_path,
                offset,
                frame.body,
                &mut live_segments,
                &mut removed_segments,
                &mut releases,
                &mut stream_highwaters,
            )?;
            last_seq = frame.manifest_seq;
        }

        offset = frame_end;
        if offset < file_len {
            physical_seq = physical_seq.checked_add(1).ok_or_else(|| {
                Error::corruption(&log_path, offset, "manifest physical sequence overflow")
            })?;
        }
    }

    file.seek(SeekFrom::Start(file_len)).map_err(|error| {
        Error::io(
            "seek manifest log",
            &log_path,
            DurabilityOutcome::NotApplicable,
            error,
        )
    })?;

    Ok(ControlRecovery {
        frames_scanned,
        repaired_tail,
        checkpoint_next_segment_id: checkpoint.next_segment_id,
        checkpoint_stream_highwaters,
        stream_highwaters,
        live_segments,
        removed_segments,
        releases,
        manifest: Manifest {
            root: root.to_path_buf(),
            path: log_path,
            file,
            last_seq,
        },
    })
}

fn apply_frame(
    path: &Path,
    offset: u64,
    body: ManifestBody,
    live_segments: &mut BTreeMap<u64, ControlSegment>,
    removed_segments: &mut BTreeMap<u64, SegmentRemovedBody>,
    releases: &mut Vec<ReleaseBody>,
    stream_highwaters: &mut BTreeMap<StreamId, u64>,
) -> Result<()> {
    match body {
        ManifestBody::Release(release) => releases.push(release),
        ManifestBody::SegmentSealed(sealed) => {
            if removed_segments.contains_key(&sealed.segment_id) {
                return Err(Error::corruption(
                    path,
                    offset,
                    "SegmentSealed follows SegmentRemoved for the same segment",
                ));
            }
            let footer = crate::format::SegmentFooter {
                segment_id: sealed.segment_id,
                segment_bytes: sealed.segment_bytes,
                epoch_count: sealed.epoch_count,
                segment_digest: sealed.segment_digest,
            };
            match live_segments.get_mut(&sealed.segment_id) {
                Some(segment) if matches!(segment.lifecycle, SegmentLifecycle::Active) => {
                    segment.lifecycle = SegmentLifecycle::Sealed(footer);
                }
                Some(_) => {
                    return Err(Error::corruption(
                        path,
                        offset,
                        "duplicate or contradictory SegmentSealed transition",
                    ));
                }
                None => {
                    live_segments.insert(
                        sealed.segment_id,
                        ControlSegment {
                            lifecycle: SegmentLifecycle::Sealed(footer),
                            checkpoint_record_count: None,
                            checkpoint_releases: None,
                        },
                    );
                }
            }
        }
        ManifestBody::SegmentRemoved(removed) => {
            let Some(segment) = live_segments.remove(&removed.segment_id) else {
                return Err(Error::corruption(
                    path,
                    offset,
                    "SegmentRemoved references a segment that is not manifest-live",
                ));
            };
            if !matches!(segment.lifecycle, SegmentLifecycle::Sealed(_)) {
                return Err(Error::corruption(
                    path,
                    offset,
                    "SegmentRemoved references an active segment",
                ));
            }
            for highwater in &removed.highwaters {
                if stream_highwaters
                    .get(&highwater.stream_id)
                    .is_some_and(|current| *current >= highwater.sequence)
                {
                    return Err(Error::corruption(
                        path,
                        offset,
                        "SegmentRemoved high-water does not advance durable state",
                    ));
                }
                stream_highwaters.insert(highwater.stream_id, highwater.sequence);
            }
            if removed_segments
                .insert(removed.segment_id, removed)
                .is_some()
            {
                return Err(Error::corruption(
                    path,
                    offset,
                    "duplicate SegmentRemoved transition",
                ));
            }
        }
    }
    Ok(())
}

fn truncate_tail(file: &mut File, path: &Path, length: u64) -> Result<()> {
    file.set_len(length)
        .and_then(|()| file.sync_data())
        .map_err(|error| {
            Error::io(
                "repair incomplete manifest tail",
                path,
                DurabilityOutcome::Unknown,
                error,
            )
        })
}

impl Manifest {
    pub(super) fn file_len(&self) -> Result<u64> {
        self.file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|error| {
                Error::io(
                    "read manifest log metadata",
                    &self.path,
                    DurabilityOutcome::NotApplicable,
                    error,
                )
            })
    }

    pub(super) fn append_group(
        &mut self,
        bodies: &[ManifestBody],
        outcome: DurabilityOutcome,
    ) -> Result<Vec<u64>> {
        if bodies.is_empty() {
            return Ok(Vec::new());
        }
        let mut next = self
            .last_seq
            .checked_add(1)
            .ok_or(Error::ManifestSequenceExhausted)?;
        let mut frames = Vec::with_capacity(bodies.len());
        let mut sequences = Vec::with_capacity(bodies.len());
        for body in bodies {
            let frame = ManifestFrame {
                manifest_seq: next,
                body: body.clone(),
            }
            .encode()
            .map_err(|error| Error::corruption(&self.path, 0, error.to_string()))?;
            frames.push(frame);
            sequences.push(next);
            if frames.len() < bodies.len() {
                next = next
                    .checked_add(1)
                    .ok_or(Error::ManifestSequenceExhausted)?;
            }
        }

        self.file
            .seek(SeekFrom::End(0))
            .map_err(|error| Error::io("seek manifest log", &self.path, outcome, error))?;
        for frame in &frames {
            #[cfg(test)]
            if let Some(error) = crate::test_crash::injected_io_error("manifest.frame.short_write")
            {
                self.file
                    .write_all(&frame[..frame.len().div_ceil(2)])
                    .map_err(|error| {
                        Error::io("write manifest frame", &self.path, outcome, error)
                    })?;
                return Err(Error::io(
                    "write manifest frame",
                    &self.path,
                    outcome,
                    error,
                ));
            }
            self.file
                .write_all(frame)
                .map_err(|error| Error::io("write manifest frame", &self.path, outcome, error))?;
        }
        #[cfg(test)]
        crate::test_crash::inject_io("manifest.append.sync_data")
            .map_err(|error| Error::io("sync manifest log", &self.path, outcome, error))?;
        self.file
            .sync_data()
            .map_err(|error| Error::io("sync manifest log", &self.path, outcome, error))?;
        self.last_seq = *sequences.last().expect("nonempty sequence list");
        Ok(sequences)
    }

    pub(super) fn compact(&mut self, checkpoint: &Checkpoint) -> Result<()> {
        if checkpoint.last_applied_seq != self.last_seq {
            return Err(Error::corruption(
                self.root.join(CHECKPOINT_FILE),
                24,
                "checkpoint sequence does not match manifest state",
            ));
        }
        let checkpoint_bytes = checkpoint.encode().map_err(|error| {
            Error::corruption(self.root.join(CHECKPOINT_FILE), 0, error.to_string())
        })?;
        atomic_replace(
            &self.root.join(CHECKPOINT_TEMP_FILE),
            &self.root.join(CHECKPOINT_FILE),
            &checkpoint_bytes,
            DurabilityOutcome::Unknown,
        )?;

        let base_seq = self.last_seq.saturating_add(1);
        let header = ManifestLogHeader {
            root_id: checkpoint.root_id,
            base_seq,
        }
        .encode();
        atomic_replace(
            &self.root.join(MANIFEST_LOG_TEMP_FILE),
            &self.path,
            &header,
            DurabilityOutcome::Unknown,
        )?;
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|error| {
                Error::io(
                    "reopen compacted manifest log",
                    &self.path,
                    DurabilityOutcome::Unknown,
                    error,
                )
            })?;
        self.file.seek(SeekFrom::End(0)).map_err(|error| {
            Error::io(
                "seek compacted manifest log",
                &self.path,
                DurabilityOutcome::Unknown,
                error,
            )
        })?;
        Ok(())
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

pub(super) fn checkpoint_from_state(
    root_id: RootId,
    last_applied_seq: u64,
    next_segment_id: u64,
    stream_highwaters: BTreeMap<StreamId, u64>,
    segments: BTreeMap<u64, CheckpointSegment>,
) -> Checkpoint {
    Checkpoint {
        root_id,
        last_applied_seq,
        next_segment_id,
        stream_highwaters,
        segments,
    }
}
