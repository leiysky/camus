use bytes::Bytes;
use camus::{Capacity, Config, FullPolicy, Log, ReadLimits, Record, StreamId};
use std::path::Path;

const CAPACITY_BYTES: u64 = 256 * 1024;
const MAX_EPOCH_BYTES: u64 = 12 * 1024;
const SEGMENT_BYTES: u64 = 13 * 1024;
const SEGMENT_NAME: &str = "segment-00000000000000000001.log";

const ROOT_BYTES: &[u8] = include_bytes!("fixtures/format-v1/v1-basic/root/ROOT");
const LOCK_BYTES: &[u8] = include_bytes!("fixtures/format-v1/v1-basic/root/camus.lock");
const CHECKPOINT_BYTES: &[u8] = include_bytes!("fixtures/format-v1/v1-basic/root/MANIFEST.chk");
const MANIFEST_BYTES: &[u8] = include_bytes!("fixtures/format-v1/v1-basic/root/MANIFEST.log");
const SEGMENT_BYTES_V1: &[u8] =
    include_bytes!("fixtures/format-v1/v1-basic/root/segments/segment-00000000000000000001.log");

#[tokio::test]
async fn historical_format_v1_root_opens_and_accepts_new_writes() {
    let directory = tempfile::tempdir().expect("create compatibility root parent");
    let root = directory.path().join("root");
    materialize_fixture(&root);

    let log = Log::open(config(&root))
        .await
        .expect("open historical format-v1 root");
    assert_eq!(
        log.known_streams(),
        vec![StreamId::new(7), StreamId::new(9)]
    );
    assert_eq!(log.stats().storage.pending_records, 3);

    let seven = log.stream(StreamId::new(7));
    let nine = log.stream(StreamId::new(9));
    assert_eq!(seven.stats().pending_records, 2);
    assert_eq!(nine.stats().pending_records, 1);

    let seven_pending = seven
        .read(ReadLimits::new(8, 16 * 1024))
        .await
        .expect("read stream 7 fixture records");
    assert_eq!(seven_pending.len(), 2);
    assert_eq!(
        seven_pending[0].metadata,
        Bytes::from_static(b"seven-large")
    );
    assert_eq!(seven_pending[0].payload, Bytes::from(vec![0xb2; 8 * 1024]));
    assert_eq!(seven_pending[1].metadata, Bytes::from_static(b"seven-two"));
    assert_eq!(seven_pending[1].payload, Bytes::from_static(b"seven-tail"));

    let nine_pending = nine
        .read(ReadLimits::new(8, 1024))
        .await
        .expect("read stream 9 fixture records");
    assert_eq!(nine_pending.len(), 1);
    assert_eq!(nine_pending[0].metadata, Bytes::from_static(b"nine-one"));
    assert_eq!(nine_pending[0].payload, Bytes::from_static(b"pending-nine"));

    let appended = nine
        .append(
            Record::new(Bytes::from_static(b"candidate-write"))
                .with_metadata(Bytes::from_static(b"post-fixture")),
        )
        .await
        .expect("append through the candidate writer");
    assert_ne!(appended, nine_pending[0].id);

    seven
        .release(seven_pending.iter().map(|record| record.id).collect())
        .await
        .expect("release historical stream 7 records");
    nine.release(vec![nine_pending[0].id, appended])
        .await
        .expect("release historical and candidate stream 9 records");
    log.shutdown()
        .await
        .expect("shut down updated fixture copy");

    let reopened = Log::open(config(&root))
        .await
        .expect("reopen updated historical root");
    assert_eq!(reopened.stats().storage.pending_records, 0);
    assert_eq!(
        reopened.known_streams(),
        vec![StreamId::new(7), StreamId::new(9)]
    );
    reopened.shutdown().await.expect("shut down reopened root");
}

fn config(root: &Path) -> Config {
    Config::new(
        root,
        Capacity::Bounded {
            total_bytes: CAPACITY_BYTES,
            when_full: FullPolicy::Block,
        },
    )
    .with_max_epoch_bytes(MAX_EPOCH_BYTES)
    .with_segment_bytes(SEGMENT_BYTES)
    .with_max_commit_bytes(MAX_EPOCH_BYTES)
    .with_max_release_records(16)
}

fn materialize_fixture(root: &Path) {
    let segments = root.join("segments");
    std::fs::create_dir_all(&segments).expect("create fixture segment directory");
    for (path, bytes) in [
        (root.join("ROOT"), ROOT_BYTES),
        (root.join("camus.lock"), LOCK_BYTES),
        (root.join("MANIFEST.chk"), CHECKPOINT_BYTES),
        (root.join("MANIFEST.log"), MANIFEST_BYTES),
        (segments.join(SEGMENT_NAME), SEGMENT_BYTES_V1),
    ] {
        std::fs::write(&path, bytes)
            .unwrap_or_else(|error| panic!("write fixture file {}: {error}", path.display()));
    }
}
