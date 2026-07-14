use camus::{Config, Log, Record};

const RECORD_COUNT: usize = 4_096;
const BATCH_SIZE: usize = 128;
const PAYLOAD_BYTES: usize = 256;

#[test]
fn multi_segment_public_lifecycle_survives_reopen_read_release_and_reclaim() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config::new(directory.path()).with_segment_bytes(64 * 1024);
    let mut ids = Vec::with_capacity(RECORD_COUNT);
    let mut expected_payloads = Vec::with_capacity(RECORD_COUNT);
    let mut locations = Vec::with_capacity(RECORD_COUNT);

    let mut log = Log::open(config.clone()).unwrap();
    for batch_start in (0..RECORD_COUNT).step_by(BATCH_SIZE) {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        for index in batch_start..batch_start + BATCH_SIZE {
            let id = format!("record-{index:05}");
            let payload = vec![(index % 251) as u8; PAYLOAD_BYTES];
            batch.push(
                Record::new(id.clone(), payload.clone())
                    .with_metadata((index as u64).to_le_bytes().to_vec()),
            );
            ids.push(id);
            expected_payloads.push(payload);
        }
        locations.extend(log.append_batch(&batch).unwrap());
    }
    assert_eq!(log.stats().epoch_syncs, (RECORD_COUNT / BATCH_SIZE) as u64);
    drop(log);

    let mut log = Log::open(config.clone()).unwrap();
    assert_eq!(log.recovery().pending_records_iter().count(), RECORD_COUNT);
    let payloads = log.read_many(&locations).unwrap();
    for (actual, expected) in payloads.iter().zip(&expected_payloads) {
        assert_eq!(actual.as_ref(), expected.as_slice());
    }

    log.release(ids.iter().map(String::as_str)).unwrap();
    assert_eq!(log.recovery().pending_records_iter().count(), 0);
    let reclaimed = log.reclaim_active_for_storage_pressure().unwrap();
    assert!(reclaimed.segments > 1);
    drop(log);

    let log = Log::open(config).unwrap();
    assert_eq!(log.recovery().pending_records_iter().count(), 0);
}
