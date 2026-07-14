#![no_main]

use arbitrary::Arbitrary;
use bytes::Bytes;
use camus::{Capacity, Config, Error, Log, ReadLimits, Record, StreamId};
use libfuzzer_sys::fuzz_target;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

const MAX_RECORDS: usize = 8;
const MAX_METADATA_BYTES: usize = 128;
const MAX_PAYLOAD_BYTES: usize = 512;

#[derive(Arbitrary, Debug)]
struct RecoveryCase {
    stream_id: u8,
    records: Vec<FuzzRecord>,
    release_mask: Vec<bool>,
    mutation: ManifestMutation,
}

#[derive(Arbitrary, Debug)]
struct FuzzRecord {
    metadata: Vec<u8>,
    payload: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
enum ManifestMutation {
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

async fn run_case_async(mut case: RecoveryCase) {
    while case.records.len() < 2 {
        case.records.push(FuzzRecord {
            metadata: Vec::new(),
            payload: vec![case.records.len() as u8],
        });
    }
    case.records.truncate(MAX_RECORDS);
    let directory = tempfile::tempdir().expect("fuzz fixture needs a temporary directory");
    let config = || Config::new(directory.path(), Capacity::Unbounded);
    let log = Log::open(config()).await.expect("valid root must open");
    let stream_id = StreamId::new(u64::from(case.stream_id % 4));
    let stream = log.stream(stream_id);
    let input = case
        .records
        .iter()
        .map(|record| {
            Record::new(Bytes::copy_from_slice(
                &record.payload[..record.payload.len().min(MAX_PAYLOAD_BYTES)],
            ))
            .with_metadata(Bytes::copy_from_slice(
                &record.metadata[..record.metadata.len().min(MAX_METADATA_BYTES)],
            ))
        })
        .collect::<Vec<_>>();
    let expected_payloads = input
        .iter()
        .map(|record| record.payload.clone())
        .collect::<Vec<_>>();
    let ids = stream.append_batch(input).await.unwrap();

    let mut released = ids
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            case.release_mask
                .get(index % case.release_mask.len().max(1))
                .copied()
                .unwrap_or(false)
        })
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();
    if released.is_empty() {
        released.push(ids[0]);
    }
    released.retain(|id| *id != *ids.last().unwrap());
    if released.is_empty() {
        released.push(ids[0]);
    }

    let manifest = directory.path().join("MANIFEST.log");
    let release_start = fs::metadata(&manifest).unwrap().len();
    stream.release(released.clone()).await.unwrap();
    let release_end = fs::metadata(&manifest).unwrap().len();
    log.shutdown().await.unwrap();

    let clean = matches!(case.mutation, ManifestMutation::None);
    let truncated = matches!(case.mutation, ManifestMutation::Truncate { .. });
    mutate_manifest(&manifest, release_start, release_end, case.mutation).unwrap();
    let reopened = Log::open(config()).await;
    if !clean && !truncated {
        assert!(matches!(reopened, Err(Error::Corruption { .. })));
        return;
    }
    let log = reopened.expect("clean or incomplete final manifest frame must open");
    let stream = log.stream(stream_id);
    let expected_pending = if clean {
        ids.len() - released.len()
    } else {
        ids.len()
    };
    assert_eq!(stream.stats().pending_records, expected_pending as u64);
    let snapshot = stream
        .read(ReadLimits::new(
            ids.len(),
            (MAX_RECORDS * MAX_PAYLOAD_BYTES) as u64 + 1,
        ))
        .await
        .unwrap();
    for record in &snapshot {
        let index = ids.iter().position(|id| *id == record.id).unwrap();
        assert_eq!(record.payload, expected_payloads[index]);
        assert!(!clean || !released.contains(&record.id));
    }
    log.shutdown().await.unwrap();
}

fn mutate_manifest(
    path: &std::path::Path,
    release_start: u64,
    release_end: u64,
    mutation: ManifestMutation,
) -> std::io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let release_len = release_end - release_start;
    assert!(release_len > 0);
    match mutation {
        ManifestMutation::None => Ok(()),
        ManifestMutation::Truncate { bytes_from_end } => {
            let remove = 1 + u64::from(bytes_from_end) % release_len;
            file.set_len(release_end - remove)
                .and_then(|()| file.sync_data())
        }
        ManifestMutation::Flip {
            offset_from_end,
            mask,
        } => {
            let offset = release_end - 1 - u64::from(offset_from_end) % release_len;
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
