#![no_main]

use arbitrary::Arbitrary;
use camus::{Config, Error, Log, Record, StreamId};
use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;
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

fuzz_target!(|case: RecoveryCase| {
    run_case(case);
});

fn run_case(mut case: RecoveryCase) {
    if case.records.is_empty() {
        case.records.push(FuzzRecord {
            metadata: Vec::new(),
            payload: vec![0],
        });
    }
    case.records.truncate(MAX_RECORDS);

    let directory = tempfile::tempdir().expect("the fuzz fixture needs a temporary directory");
    let config = Config::new(directory.path());
    let mut log = Log::open(config.clone()).expect("a valid Camus root must open");
    let stream_id = StreamId::new(u32::from(case.stream_id % 4));
    let mut expected_payloads = HashMap::new();
    let records = case
        .records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let record_id = format!("record-{index:02}");
            let metadata_len = record.metadata.len().min(MAX_METADATA_BYTES);
            let payload_len = record.payload.len().min(MAX_PAYLOAD_BYTES);
            let payload = record.payload[..payload_len].to_vec();
            expected_payloads.insert(record_id.clone(), payload.clone());
            Record::new(record_id, payload).with_metadata(record.metadata[..metadata_len].to_vec())
        })
        .collect::<Vec<_>>();
    log.append_batch_to(stream_id, &records)
        .expect("bounded valid records must append");

    let mut released = records
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            case.release_mask
                .get(index % case.release_mask.len().max(1))
                .copied()
                .unwrap_or(false)
        })
        .map(|(_, record)| record.record_id.clone())
        .collect::<Vec<_>>();
    if released.is_empty() {
        released.push(records[0].record_id.clone());
    }

    let manifest = directory.path().join("MANIFEST");
    let release_start = fs::metadata(&manifest)
        .expect("the manifest must exist")
        .len();
    log.release_from(stream_id, released.iter().map(String::as_str))
        .expect("known unique IDs must release");
    let release_end = fs::metadata(&manifest)
        .expect("the release must extend the manifest")
        .len();
    drop(log);

    let clean = matches!(&case.mutation, ManifestMutation::None);
    mutate_manifest(&manifest, release_start, release_end, case.mutation)
        .expect("bounded manifest mutation must succeed");

    let mut log = match Log::open(config) {
        Ok(log) => log,
        Err(Error::Corruption { .. }) if !clean => return,
        Err(error) => panic!("unexpected manifest recovery error: {error}"),
    };

    let release_membership = released
        .iter()
        .map(|record_id| log.state().is_released(stream_id, record_id))
        .collect::<Vec<_>>();
    assert!(
        release_membership.iter().all(|released| *released)
            || release_membership.iter().all(|released| !*released),
        "one manifest record must apply atomically"
    );
    if clean {
        assert!(release_membership.iter().all(|released| *released));
    }

    for record in &log.recovery().records {
        let expected = expected_payloads
            .get(&record.meta.record_id)
            .expect("recovery must not invent record IDs");
        let actual = log
            .read(&record.location)
            .expect("recovered locations must validate");
        assert_eq!(actual.as_ref(), expected.as_slice());
    }
    log.recover()
        .expect("a successfully repaired manifest must remain recoverable");
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
