use bytes::Bytes;
use camus::{Capacity, Config, FullPolicy, Log, ReadLimits, Record, StreamId};
use std::path::Path;

const CAPACITY_BYTES: u64 = 256 * 1024;
const MAX_EPOCH_BYTES: u64 = 12 * 1024;
const SEGMENT_BYTES: u64 = 13 * 1024;
const SEGMENT_NAME: &str = "segment-00000000000000000001.log";

struct Fixture {
    root: &'static [u8],
    lock: &'static [u8],
    checkpoint: &'static [u8],
    manifest: &'static [u8],
    segment: &'static [u8],
}

const PRE_PUBLIC_FIXTURE: Fixture = Fixture {
    root: include_bytes!("fixtures/format-v1/v1-basic/root/ROOT"),
    lock: include_bytes!("fixtures/format-v1/v1-basic/root/camus.lock"),
    checkpoint: include_bytes!("fixtures/format-v1/v1-basic/root/MANIFEST.chk"),
    manifest: include_bytes!("fixtures/format-v1/v1-basic/root/MANIFEST.log"),
    segment: include_bytes!(
        "fixtures/format-v1/v1-basic/root/segments/segment-00000000000000000001.log"
    ),
};

const PUBLISHED_RC1_FIXTURE: Fixture = Fixture {
    root: include_bytes!("fixtures/format-v1/v1.0.0-rc.1/root/ROOT"),
    lock: include_bytes!("fixtures/format-v1/v1.0.0-rc.1/root/camus.lock"),
    checkpoint: include_bytes!("fixtures/format-v1/v1.0.0-rc.1/root/MANIFEST.chk"),
    manifest: include_bytes!("fixtures/format-v1/v1.0.0-rc.1/root/MANIFEST.log"),
    segment: include_bytes!(
        "fixtures/format-v1/v1.0.0-rc.1/root/segments/segment-00000000000000000001.log"
    ),
};

#[tokio::test]
async fn historical_format_v1_root_opens_and_accepts_new_writes() {
    let directory = tempfile::tempdir().expect("create compatibility root parent");
    let root = directory.path().join("root");
    materialize_fixture(&root, &PRE_PUBLIC_FIXTURE);

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

#[tokio::test]
async fn published_rc1_root_opens_and_accepts_new_writes() {
    let directory = tempfile::tempdir().expect("create compatibility root parent");
    let root = directory.path().join("root");
    materialize_fixture(&root, &PUBLISHED_RC1_FIXTURE);

    let log = Log::open(config(&root))
        .await
        .expect("open published 1.0.0-rc.1 root");
    assert_eq!(
        log.known_streams(),
        vec![StreamId::new(21), StreamId::new(34)]
    );
    assert_eq!(log.stats().storage.pending_records, 2);
    assert_eq!(log.stats().storage.live_segments, 1);

    let twenty_one = log.stream(StreamId::new(21));
    let thirty_four = log.stream(StreamId::new(34));
    let twenty_one_pending = twenty_one
        .read(ReadLimits::new(8, 16 * 1024))
        .await
        .expect("read stream 21 fixture record");
    assert_eq!(twenty_one_pending.len(), 1);
    assert_eq!(
        twenty_one_pending[0].metadata,
        Bytes::from_static(b"rc1-pending-large")
    );
    assert_eq!(
        twenty_one_pending[0].payload,
        Bytes::from(vec![0x22; 8 * 1024])
    );

    let thirty_four_pending = thirty_four
        .read(ReadLimits::new(8, 1024))
        .await
        .expect("read stream 34 fixture record");
    assert_eq!(thirty_four_pending.len(), 1);
    assert_eq!(
        thirty_four_pending[0].metadata,
        Bytes::from_static(b"rc1-pending-small")
    );
    assert_eq!(
        thirty_four_pending[0].payload,
        Bytes::from_static(b"pending-stream-34")
    );

    let appended = thirty_four
        .append(
            Record::new(Bytes::from_static(b"candidate-after-rc1"))
                .with_metadata(Bytes::from_static(b"post-published-fixture")),
        )
        .await
        .expect("append through the candidate writer");
    assert_ne!(appended, thirty_four_pending[0].id);

    twenty_one
        .release(vec![twenty_one_pending[0].id])
        .await
        .expect("release published stream 21 record");
    thirty_four
        .release(vec![thirty_four_pending[0].id, appended])
        .await
        .expect("release published and candidate stream 34 records");
    log.shutdown()
        .await
        .expect("shut down updated fixture copy");

    let reopened = Log::open(config(&root))
        .await
        .expect("reopen updated published root");
    assert_eq!(reopened.stats().storage.pending_records, 0);
    assert_eq!(
        reopened.known_streams(),
        vec![StreamId::new(21), StreamId::new(34)]
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

fn materialize_fixture(root: &Path, fixture: &Fixture) {
    let segments = root.join("segments");
    std::fs::create_dir_all(&segments).expect("create fixture segment directory");
    for (path, bytes) in [
        (root.join("ROOT"), fixture.root),
        (root.join("camus.lock"), fixture.lock),
        (root.join("MANIFEST.chk"), fixture.checkpoint),
        (root.join("MANIFEST.log"), fixture.manifest),
        (segments.join(SEGMENT_NAME), fixture.segment),
    ] {
        std::fs::write(&path, bytes)
            .unwrap_or_else(|error| panic!("write fixture file {}: {error}", path.display()));
    }
}
