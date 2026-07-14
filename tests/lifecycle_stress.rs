use bytes::Bytes;
use camus::{Capacity, Config, Log, ReadLimits, Record, StreamId};

const RECORD_COUNT: usize = 4_096;
const BATCH_SIZE: usize = 128;
const PAYLOAD_BYTES: usize = 256;

fn config(root: &std::path::Path) -> Config {
    Config::new(root, Capacity::Unbounded)
        .with_max_epoch_bytes(48 * 1024)
        .with_segment_bytes(64 * 1024)
        .with_max_release_records(RECORD_COUNT)
        .with_max_commit_bytes(256 * 1024)
}

#[tokio::test]
async fn multi_segment_public_lifecycle_survives_reopen_read_release_and_reclaim() {
    let directory = tempfile::tempdir().unwrap();
    let stream_id = StreamId::new(1);
    let mut expected_payloads = Vec::with_capacity(RECORD_COUNT);
    {
        let log = Log::open(config(directory.path())).await.unwrap();
        let stream = log.stream(stream_id);
        for batch_start in (0..RECORD_COUNT).step_by(BATCH_SIZE) {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            for index in batch_start..batch_start + BATCH_SIZE {
                let payload = Bytes::from(vec![(index % 251) as u8; PAYLOAD_BYTES]);
                batch.push(
                    Record::new(payload.clone())
                        .with_metadata(Bytes::copy_from_slice(&(index as u64).to_le_bytes())),
                );
                expected_payloads.push(payload);
            }
            stream.append_batch(batch).await.unwrap();
        }
        assert_eq!(stream.stats().pending_records, RECORD_COUNT as u64);
        log.shutdown().await.unwrap();
    }

    let log = Log::open(config(directory.path())).await.unwrap();
    let stream = log.stream(stream_id);
    let snapshot = stream
        .read(ReadLimits::new(RECORD_COUNT, 2 * 1024 * 1024))
        .await
        .unwrap();
    assert_eq!(snapshot.len(), RECORD_COUNT);
    for (actual, expected) in snapshot.iter().zip(&expected_payloads) {
        assert_eq!(&actual.payload, expected);
    }
    let ids = snapshot.iter().map(|record| record.id).collect();
    stream.release(ids).await.unwrap();
    assert_eq!(stream.stats().pending_records, 0);
    let _ = log.reclaim().await.unwrap();
    log.shutdown().await.unwrap();

    let log = Log::open(config(directory.path())).await.unwrap();
    assert_eq!(log.stream(stream_id).stats().pending_records, 0);
    log.shutdown().await.unwrap();
}
