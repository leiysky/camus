#![no_main]

use arbitrary::Arbitrary;
use camus::{Config, Log, Record, StreamId};
use libfuzzer_sys::fuzz_target;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAX_OPERATIONS: usize = 16;
const MAX_APPENDS: usize = 8;
const MAX_METADATA_BYTES: usize = 128;
const MAX_PAYLOAD_BYTES: usize = 512;
const MAX_TOTAL_PAYLOAD_BYTES: usize = 4 * 1024;
const EPOCH_MARKER_BYTES: u64 = 72;

#[derive(Arbitrary, Debug)]
struct RecoveryCase {
    stream_id: u8,
    segment_size: u16,
    operations: Vec<Operation>,
    mutation: TailMutation,
}

#[derive(Arbitrary, Debug)]
enum Operation {
    Append { metadata: Vec<u8>, payload: Vec<u8> },
    EndEpoch,
}

#[derive(Arbitrary, Debug)]
enum TailMutation {
    None,
    Truncate { bytes_from_end: u16 },
    Flip { offset_from_end: u16, mask: u8 },
}

fuzz_target!(|case: RecoveryCase| {
    run_case(case);
});

fn run_case(case: RecoveryCase) {
    let directory = tempfile::tempdir().expect("the fuzz fixture needs a temporary directory");
    let config = Config::new(directory.path())
        .with_segment_bytes(256 + u64::from(case.segment_size % 1_793));
    let mut log = Log::open(config.clone()).expect("bounded valid Camus config must open");
    let stream_id = StreamId::new(u32::from(case.stream_id % 4));

    let mut pending = Vec::new();
    let mut expected_payloads = HashMap::<String, Vec<u8>>::new();
    let mut expected_ids = Vec::new();
    let mut append_count = 0usize;
    let mut total_payload_bytes = 0usize;
    let mut last_epoch = None;

    for operation in case.operations.into_iter().take(MAX_OPERATIONS) {
        match operation {
            Operation::Append { metadata, payload } if append_count < MAX_APPENDS => {
                let remaining = MAX_TOTAL_PAYLOAD_BYTES.saturating_sub(total_payload_bytes);
                let payload_len = payload.len().min(MAX_PAYLOAD_BYTES).min(remaining);
                let payload = payload[..payload_len].to_vec();
                let metadata_len = metadata.len().min(MAX_METADATA_BYTES);
                let record_id = format!("record-{append_count:02}");
                pending.push(
                    Record::new(record_id.clone(), payload.clone())
                        .with_metadata(metadata[..metadata_len].to_vec()),
                );
                expected_payloads.insert(record_id.clone(), payload);
                expected_ids.push(record_id);
                append_count += 1;
                total_payload_bytes += payload_len;
            }
            Operation::EndEpoch => {
                if let Some(epoch) = append_epoch(&mut log, stream_id, &mut pending) {
                    last_epoch = Some(epoch);
                }
            }
            Operation::Append { .. } => {}
        }
    }

    if append_count == 0 {
        let record_id = "record-00".to_string();
        let payload = vec![case.segment_size as u8];
        pending.push(Record::new(record_id.clone(), payload.clone()));
        expected_payloads.insert(record_id.clone(), payload);
        expected_ids.push(record_id);
        append_count = 1;
    }
    if let Some(epoch) = append_epoch(&mut log, stream_id, &mut pending) {
        last_epoch = Some(epoch);
    }
    let last_epoch = last_epoch.expect("every case writes at least one valid epoch");
    drop(log);

    let tail =
        tail_segment(directory.path(), stream_id).expect("a valid append creates a tail segment");
    let clean_tail = matches!(&case.mutation, TailMutation::None);
    mutate_tail(&tail, case.mutation).expect("bounded tail mutation must succeed");

    let mut recovered_log = Log::open(config)
        .expect("a clean log or a mutation confined to its final frame must recover");
    let first_recovery = recovered_log.recovery().clone();
    let recovered_ids = first_recovery
        .records
        .iter()
        .map(|record| record.meta.record_id.as_str())
        .collect::<Vec<_>>();
    let expected_len = if clean_tail {
        append_count
    } else {
        append_count - last_epoch.record_count
    };
    assert_eq!(recovered_ids.len(), expected_len);
    let expected_prefix = expected_ids
        .iter()
        .take(expected_len)
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(recovered_ids, expected_prefix);
    assert_eq!(recovered_log.stats().repaired_tails, u64::from(!clean_tail));

    let mut seen = HashSet::new();
    for record in &first_recovery.records {
        assert_eq!(record.stream_id, stream_id);
        assert!(seen.insert(record.meta.record_id.clone()));
        let expected = expected_payloads
            .get(&record.meta.record_id)
            .expect("recovery must not invent record ids");
        let actual = recovered_log
            .read(&record.location)
            .expect("every recovered location must pass its checksum");
        assert_eq!(actual.as_ref(), expected.as_slice());
    }

    let second_recovery = recovered_log
        .recover()
        .expect("a repaired log must remain recoverable");
    let second_ids = second_recovery
        .records
        .iter()
        .map(|record| record.meta.record_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(recovered_ids, second_ids);
}

struct Epoch {
    record_count: usize,
}

fn append_epoch(log: &mut Log, stream_id: StreamId, pending: &mut Vec<Record>) -> Option<Epoch> {
    if pending.is_empty() {
        return None;
    }
    let expected = pending.len();
    let locations = log
        .append_batch_to(stream_id, pending)
        .expect("bounded valid records must append");
    assert_eq!(locations.len(), expected);
    pending.clear();
    Some(Epoch {
        record_count: expected,
    })
}

fn tail_segment(root: &Path, stream_id: StreamId) -> Option<PathBuf> {
    let directory = if stream_id.get() == 0 {
        root.join("segments")
    } else {
        root.join("streams")
            .join(format!("stream-{:010}", stream_id.get()))
    };
    let mut segments = fs::read_dir(directory)
        .ok()?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            entry.file_type().is_ok_and(|kind| kind.is_file())
                && name.starts_with("segment-")
                && name.ends_with(".log")
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    segments.sort_unstable();
    segments.pop()
}

fn mutate_tail(path: &Path, mutation: TailMutation) -> std::io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let length = file.metadata()?.len();
    assert!(length >= EPOCH_MARKER_BYTES);

    match mutation {
        TailMutation::None => Ok(()),
        TailMutation::Truncate { bytes_from_end } => {
            let window = EPOCH_MARKER_BYTES - 1;
            let remove = 1 + u64::from(bytes_from_end) % window;
            file.set_len(length.saturating_sub(remove))
                .and_then(|()| file.sync_data())
        }
        TailMutation::Flip {
            offset_from_end,
            mask,
        } => {
            // Mutation stays inside the final epoch marker, so recovery must
            // discard the whole final epoch rather than fail on older data.
            let offset = length - 1 - u64::from(offset_from_end) % EPOCH_MARKER_BYTES;
            let mut byte = [0_u8; 1];
            file.seek(SeekFrom::Start(offset))
                .and_then(|_| file.read_exact(&mut byte))
                .and_then(|()| {
                    byte[0] ^= mask | 1;
                    file.seek(SeekFrom::Start(offset))?;
                    file.write_all(&byte)?;
                    file.sync_data()
                })
        }
    }
}
