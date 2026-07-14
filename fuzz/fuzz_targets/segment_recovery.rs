#![no_main]

use arbitrary::Arbitrary;
use bytes::Bytes;
use camus::{Capacity, Config, Error, Log, ReadLimits, Record, StreamId};
use libfuzzer_sys::fuzz_target;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAX_OPERATIONS: usize = 16;
const MAX_APPENDS: usize = 8;
const MAX_METADATA_BYTES: usize = 128;
const MAX_PAYLOAD_BYTES: usize = 512;
const MAX_TOTAL_PAYLOAD_BYTES: usize = 4 * 1024;
const EPOCH_COMMIT_BYTES: u64 = 40;

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

fuzz_target!(|case: RecoveryCase| run_case(case));

fn run_case(case: RecoveryCase) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("fuzz runtime must build");
    runtime.block_on(run_case_async(case));
}

async fn run_case_async(case: RecoveryCase) {
    let directory = tempfile::tempdir().expect("fuzz fixture needs a temporary directory");
    let config = || {
        Config::new(directory.path(), Capacity::Unbounded)
            .with_max_epoch_bytes(16 * 1024)
            .with_segment_bytes(20 * 1024 + u64::from(case.segment_size % 16_384))
    };
    let log = Log::open(config()).await.expect("valid root must open");
    let stream_id = StreamId::new(u64::from(case.stream_id % 4));
    let stream = log.stream(stream_id);

    let mut pending = Vec::new();
    let mut expected = Vec::<(camus::RecordId, Bytes)>::new();
    let mut appended_epochs = 0_usize;
    let mut total_payload = 0_usize;
    for operation in case.operations.into_iter().take(MAX_OPERATIONS) {
        match operation {
            Operation::Append { metadata, payload } if expected.len() < MAX_APPENDS => {
                let remaining = MAX_TOTAL_PAYLOAD_BYTES.saturating_sub(total_payload);
                let payload_len = payload.len().min(MAX_PAYLOAD_BYTES).min(remaining);
                let metadata_len = metadata.len().min(MAX_METADATA_BYTES);
                pending.push(
                    Record::new(Bytes::copy_from_slice(&payload[..payload_len]))
                        .with_metadata(Bytes::copy_from_slice(&metadata[..metadata_len])),
                );
                total_payload += payload_len;
            }
            Operation::EndEpoch if !pending.is_empty() => {
                let payloads = pending
                    .iter()
                    .map(|record| record.payload.clone())
                    .collect::<Vec<_>>();
                let ids = stream
                    .append_batch(std::mem::take(&mut pending))
                    .await
                    .unwrap();
                expected.extend(ids.into_iter().zip(payloads));
                appended_epochs += 1;
            }
            Operation::Append { .. } | Operation::EndEpoch => {}
        }
    }
    if expected.is_empty() && pending.is_empty() {
        pending.push(Record::new(Bytes::from_static(b"seed")));
    }
    if !pending.is_empty() {
        let payloads = pending
            .iter()
            .map(|record| record.payload.clone())
            .collect::<Vec<_>>();
        let ids = stream.append_batch(pending).await.unwrap();
        expected.extend(ids.into_iter().zip(payloads));
        appended_epochs += 1;
    }
    log.shutdown().await.unwrap();

    let tail = tail_segment(directory.path()).expect("append creates a segment");
    let clean = matches!(case.mutation, TailMutation::None);
    let truncated = matches!(case.mutation, TailMutation::Truncate { .. });
    mutate_tail(&tail, case.mutation).expect("bounded tail mutation must succeed");

    let reopened = Log::open(config()).await;
    if !clean && (!truncated || appended_epochs == 1) {
        assert!(matches!(reopened, Err(Error::Corruption { .. })));
        return;
    }
    let log = reopened.expect("clean data or incomplete repairable tail must open");
    let stream = log.stream(stream_id);
    let recovered_count = usize::try_from(stream.stats().pending_records).unwrap();
    assert!(recovered_count <= expected.len());
    if clean {
        assert_eq!(recovered_count, expected.len());
    }
    if recovered_count != 0 {
        let snapshot = stream
            .read(ReadLimits::new(
                recovered_count,
                MAX_TOTAL_PAYLOAD_BYTES as u64 + 1,
            ))
            .await
            .unwrap();
        for (record, (expected_id, expected_payload)) in snapshot.iter().zip(&expected) {
            assert_eq!(record.id, *expected_id);
            assert_eq!(&record.payload, expected_payload);
        }
    }
    log.shutdown().await.unwrap();
}

fn tail_segment(root: &Path) -> Option<PathBuf> {
    let mut segments = fs::read_dir(root.join("segments"))
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
    assert!(length >= EPOCH_COMMIT_BYTES);
    match mutation {
        TailMutation::None => Ok(()),
        TailMutation::Truncate { bytes_from_end } => {
            let remove = 1 + u64::from(bytes_from_end) % EPOCH_COMMIT_BYTES;
            file.set_len(length - remove)
                .and_then(|()| file.sync_data())
        }
        TailMutation::Flip {
            offset_from_end,
            mask,
        } => {
            let offset = length - 1 - u64::from(offset_from_end) % EPOCH_COMMIT_BYTES;
            let mut byte = [0_u8; 1];
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut byte)?;
            byte[0] ^= mask | 1;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&byte)?;
            file.sync_data()
        }
    }
}
