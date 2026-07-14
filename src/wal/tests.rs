use super::*;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::{FileExt, PermissionsExt};
use tempfile::TempDir;

fn config(temp: &TempDir, segment_bytes: u64) -> FileWalConfig {
    let mut config = FileWalConfig::new(temp.path());
    config.segment_bytes = segment_bytes;
    config
}

fn open(temp: &TempDir, segment_bytes: u64) -> FileWal {
    FileWal::open(config(temp, segment_bytes)).unwrap()
}

fn record(id: &str, payload: &[u8]) -> AppendRecord {
    AppendRecord::new(id, Bytes::copy_from_slice(payload))
        .with_metadata(Bytes::from(format!("metadata:{id}")))
}

fn segment(temp: &TempDir, id: u64) -> PathBuf {
    stream_segment(temp, DEFAULT_STREAM, id)
}

fn stream_segment(temp: &TempDir, stream_id: StreamId, id: u64) -> PathBuf {
    shard_directory(temp.path(), stream_id.get()).join(format!("segment-{id:020}.log"))
}

fn manifest(temp: &TempDir) -> PathBuf {
    temp.path().join("MANIFEST")
}

fn pending_ids(wal: &FileWal) -> Vec<String> {
    wal.recovery()
        .pending_records()
        .into_iter()
        .map(|record| record.meta.record_id)
        .collect()
}

struct SignalWake(std::sync::mpsc::Sender<()>);

impl std::task::Wake for SignalWake {
    fn wake(self: std::sync::Arc<Self>) {
        let _ = self.0.send(());
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        let _ = self.0.send(());
    }
}

fn signal_waker() -> (std::task::Waker, std::sync::mpsc::Receiver<()>) {
    let (sender, receiver) = std::sync::mpsc::channel();
    (
        std::task::Waker::from(std::sync::Arc::new(SignalWake(sender))),
        receiver,
    )
}

fn poll_wait(
    future: &mut std::pin::Pin<Box<WaitForStream>>,
    waker: &std::task::Waker,
) -> std::task::Poll<WalResult<()>> {
    let mut context = std::task::Context::from_waker(waker);
    std::future::Future::poll(future.as_mut(), &mut context)
}

fn assert_injected_failure<T>(result: WalResult<T>) {
    let error = result.err().expect("the armed crash point must fail");
    assert!(
        matches!(&error, WalError::Io(error) if error.to_string().contains("injected crash")),
        "unexpected failure: {error}"
    );
}

#[test]
fn version_one_wire_codecs_have_stable_bytes() {
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    let metadata = encode_record_meta(&RecordMeta {
        record_id: "id".into(),
        metadata: Bytes::from_static(&[0, 255]),
    })
    .unwrap();
    let payload_checksum = xxhash_rust::xxh3::xxh3_64(b"abc");
    let record_prefix = segment_record_prefix(RECORD_KIND, &metadata, 3, payload_checksum).unwrap();
    let epoch_commit = EpochCommit {
        epoch_start: FILE_HEADER_LEN,
        frame_count: 1,
        descriptors_checksum: 0x0102_0304_0506_0708,
    };
    let commit = epoch_commit.encode();
    let release_metadata = br#"{"record_ids":["id"]}"#;
    let manifest_prefix = manifest_record_prefix(RELEASE_KIND, release_metadata).unwrap();

    assert_eq!(
        hex(&file_header(SEGMENT_MAGIC, SEGMENT_FORMAT_VERSION, 7, 9)),
        "43414d534547303101002000070000000900000000000000c91e29e7a55c38be"
    );
    assert_eq!(hex(&metadata), "02000000696400ff");
    assert_eq!(
        decode_record_meta(&metadata).unwrap(),
        RecordMeta {
            record_id: "id".into(),
            metadata: Bytes::from_static(&[0, 255]),
        }
    );
    assert_eq!(
        hex(&record_prefix),
        "43414d5245433031010001000800000003000000000000003b0000000000000050392f89945faf789e9d7293d6a7e623"
    );
    assert_eq!(
        hex(&commit),
        "200000000000000001000000000000000807060504030201"
    );
    assert_eq!(EpochCommit::decode(&commit), Ok(epoch_commit));
    assert_eq!(
        serde_json::to_vec(&ReleaseV1 {
            record_ids: vec!["id".into()]
        })
        .unwrap(),
        release_metadata
    );
    assert_eq!(
        serde_json::to_vec(&SegmentRotationV1 {
            shard_id: 7,
            previous_segment_id: Some(9),
            new_segment_id: 10,
            created_at_unix_millis: Some(123),
        })
        .unwrap(),
        br#"{"shard_id":7,"previous_segment_id":9,"new_segment_id":10,"created_at_unix_millis":123}"#
    );
    assert_eq!(
        serde_json::to_vec(&SegmentRemovalV1 {
            shard_id: 7,
            segment_ids: vec![1, 2],
        })
        .unwrap(),
        br#"{"shard_id":7,"segment_ids":[1,2]}"#
    );
    assert_eq!(
        serde_json::to_vec(&SegmentSnapshotV1 {
            shard_id: 0,
            segment_id: 9,
            lifecycle: SegmentLifecycle::Sealed,
            created_at_unix_millis: None,
        })
        .unwrap(),
        br#"{"shard_id":0,"segment_id":9,"lifecycle":"Sealed"}"#
    );
    assert_eq!(
        serde_json::to_vec(&StreamReleaseV1 {
            stream_id: 7,
            record_ids: vec!["id".into()],
        })
        .unwrap(),
        br#"{"stream_id":7,"record_ids":["id"]}"#
    );
    assert_eq!(
        serde_json::to_vec(&SegmentTimestampV1 {
            shard_id: 7,
            segment_id: 9,
            created_at_unix_millis: 123,
        })
        .unwrap(),
        br#"{"shard_id":7,"segment_id":9,"created_at_unix_millis":123}"#
    );
    let legacy_location: WalLocation = serde_json::from_str(
        r#"{"segment_id":1,"frame_offset":32,"frame_len":48,"payload_offset":80,"payload_len":0,"payload_checksum":0}"#,
    )
    .unwrap();
    assert_eq!(legacy_location.stream_id, DEFAULT_STREAM);
    assert_eq!(
        hex(&manifest_prefix),
        "43414d4d52433031010001001500000035000000000000000b9fe05cff160f8e"
    );
    assert!(
        serde_json::from_slice::<ReleaseV1>(br#"{"record_ids":["id"],"future":true}"#).is_err()
    );
}

#[test]
fn large_segment_removals_are_split_into_recoverable_manifest_frames() {
    let removal = SegmentRemovalV1 {
        shard_id: 7,
        segment_ids: (0..=MAX_SEGMENT_IDS_PER_REMOVAL_RECORD as u64).collect(),
    };
    let (encoded, record_count) = encode_segment_removal_records(&removal).unwrap();
    assert_eq!(record_count, 2);

    let mut decoded_ids = Vec::new();
    let mut offset = 0_usize;
    while offset < encoded.len() {
        let prefix_end = offset + MANIFEST_RECORD_PREFIX_LEN as usize;
        let prefix: &[u8; MANIFEST_RECORD_PREFIX_LEN as usize] =
            encoded[offset..prefix_end].try_into().unwrap();
        let parsed = parse_manifest_record_prefix(prefix).unwrap();
        assert_eq!(parsed.kind, SEGMENT_REMOVAL_KIND);
        assert!(parsed.metadata_len <= MAX_METADATA_LEN);

        let end = offset + parsed.frame_len as usize;
        let metadata = &encoded[prefix_end..end];
        let mut checksum = Xxh3::new();
        checksum.update(&prefix[..24]);
        checksum.update(metadata);
        assert_eq!(checksum.digest(), parsed.metadata_checksum);

        let decoded: SegmentRemovalV1 = serde_json::from_slice(metadata).unwrap();
        assert_eq!(decoded.shard_id, removal.shard_id);
        decoded_ids.extend(decoded.segment_ids);
        offset = end;
    }
    assert_eq!(decoded_ids, removal.segment_ids);
}

#[test]
fn batch_append_uses_one_sync_and_recovers_opaque_bytes() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let records = [
        AppendRecord::new("a", Bytes::from_static(b"first"))
            .with_metadata(Bytes::from_static(&[0, 255, 1])),
        record("b", b"second"),
    ];

    let before = wal.stats();
    let locations = wal.append_batch(&records).unwrap();
    assert_eq!(locations.len(), 2);
    assert_eq!(wal.stats().epoch_syncs - before.epoch_syncs, 1);
    assert_eq!(wal.stats().record_frames - before.record_frames, 2);
    assert_eq!(
        wal.read_many(&locations).unwrap(),
        [Bytes::from_static(b"first"), Bytes::from_static(b"second")]
    );
    drop(wal);

    let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let recovered = wal.recovery().pending_records();
    assert_eq!(
        recovered
            .iter()
            .map(|record| record.meta.record_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(recovered[0].meta.metadata.as_ref(), [0, 255, 1]);
    let recovered_locations = recovered
        .iter()
        .map(|record| record.location.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        wal.read_many(&recovered_locations).unwrap(),
        [Bytes::from_static(b"first"), Bytes::from_static(b"second")]
    );
}

#[test]
fn logical_streams_isolate_ids_reads_releases_and_reclamation() {
    let temp = TempDir::new().unwrap();
    let stream_one = StreamId::new(1);
    let stream_two = StreamId::new(2);
    let config = FileWalConfig::new(temp.path())
        .with_stream_rollover(stream_one, RolloverPolicy::new(FILE_HEADER_LEN + 1));
    let mut wal = FileWal::open(config.clone()).unwrap();

    let one_old = wal
        .append_to(stream_one, record("shared-id", b"stream one old"))
        .unwrap();
    let two = wal
        .append_to(stream_two, record("shared-id", b"stream two"))
        .unwrap();
    let one_active = wal
        .append_to(stream_one, record("one-active", b"stream one active"))
        .unwrap();

    assert_eq!(one_old.stream_id, stream_one);
    assert_eq!(two.stream_id, stream_two);
    assert_eq!(one_old.segment_id, 0);
    assert_eq!(two.segment_id, 0);
    assert_eq!(one_active.segment_id, 1);
    assert!(stream_segment(&temp, stream_one, 0).exists());
    assert!(stream_segment(&temp, stream_one, 1).exists());
    assert!(stream_segment(&temp, stream_two, 0).exists());
    assert_eq!(
        wal.streams().collect::<Vec<_>>(),
        [DEFAULT_STREAM, stream_one, stream_two]
    );
    assert_eq!(
        wal.read_many(&[two.clone(), one_old.clone(), one_active.clone()])
            .unwrap(),
        [
            Bytes::from_static(b"stream two"),
            Bytes::from_static(b"stream one old"),
            Bytes::from_static(b"stream one active"),
        ]
    );
    let mut wrong_stream = one_active.clone();
    wrong_stream.stream_id = stream_two;
    assert!(matches!(
        wal.read(&wrong_stream),
        Err(WalError::InvalidLocation(_))
    ));
    assert!(!wal.is_poisoned());

    wal.release_from(stream_one, ["shared-id"]).unwrap();
    assert_eq!(
        wal.recovery()
            .pending_records_for(stream_one)
            .iter()
            .map(|record| record.meta.record_id.as_str())
            .collect::<Vec<_>>(),
        ["one-active"]
    );
    assert_eq!(
        wal.recovery().pending_records_for(stream_two)[0]
            .meta
            .record_id,
        "shared-id"
    );
    assert!(wal.state().is_released(stream_one, "shared-id"));
    assert!(!wal.state().is_released(stream_two, "shared-id"));

    let report = wal.reclaim().unwrap();
    assert_eq!(report.segments, 1);
    assert!(!stream_segment(&temp, stream_one, 0).exists());
    assert!(stream_segment(&temp, stream_two, 0).exists());
    drop(wal);

    let wal = FileWal::open(config).unwrap();
    assert_eq!(
        wal.recovery()
            .pending_records_iter()
            .map(|record| (record.stream_id, record.meta.record_id.as_str()))
            .collect::<Vec<_>>(),
        [(stream_one, "one-active"), (stream_two, "shared-id")]
    );
    assert_eq!(wal.read(&two).unwrap(), b"stream two".as_slice());
}

#[test]
fn wait_for_stream_is_level_triggered_and_tracks_release_state() {
    let temp = TempDir::new().unwrap();
    let stream = StreamId::new(7);
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let readiness = wal.readiness();
    let (waker, awakened) = signal_waker();
    let mut waiting = Box::pin(readiness.wait_for(stream));

    assert!(!readiness.is_ready(stream));
    assert!(poll_wait(&mut waiting, &waker).is_pending());
    assert!(matches!(
        wal.append_to(stream, record("", b"invalid")),
        Err(WalError::InvalidRecord(_))
    ));
    assert!(poll_wait(&mut waiting, &waker).is_pending());
    assert!(!readiness.is_ready(stream));
    wal.append_batch_to(stream, &[record("one", b"one"), record("two", b"two")])
        .unwrap();
    awakened.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        poll_wait(&mut waiting, &waker),
        std::task::Poll::Ready(Ok(()))
    ));
    assert!(readiness.is_ready(stream));

    let mut immediate = Box::pin(wal.wait_for(stream));
    assert!(matches!(
        poll_wait(&mut immediate, &waker),
        std::task::Poll::Ready(Ok(()))
    ));

    wal.release_from(stream, ["one"]).unwrap();
    assert!(readiness.is_ready(stream));
    wal.release_from(stream, ["two"]).unwrap();
    assert!(!readiness.is_ready(stream));

    let mut after_release = Box::pin(readiness.wait_for(stream));
    assert!(poll_wait(&mut after_release, &waker).is_pending());
    drop(after_release);
    assert!(readiness.shared.state.lock().unwrap().waiters.is_empty());
}

#[test]
fn wait_for_stream_wakes_all_waiters_and_recovers_existing_pending_work() {
    let temp = TempDir::new().unwrap();
    let stream = StreamId::new(9);
    let config = FileWalConfig::new(temp.path());
    let mut wal = FileWal::open(config.clone()).unwrap();
    let mut first = Box::pin(wal.wait_for(stream));
    let mut second = Box::pin(wal.readiness().wait_for(stream));
    let (first_waker, first_awakened) = signal_waker();
    let (second_waker, second_awakened) = signal_waker();
    assert!(poll_wait(&mut first, &first_waker).is_pending());
    assert!(poll_wait(&mut second, &second_waker).is_pending());

    wal.append_to(stream, record("pending", b"payload"))
        .unwrap();
    first_awakened.recv_timeout(Duration::from_secs(5)).unwrap();
    second_awakened
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert!(poll_wait(&mut first, &first_waker).is_ready());
    assert!(poll_wait(&mut second, &second_waker).is_ready());
    drop(wal);

    let wal = FileWal::open(config).unwrap();
    let mut recovered = Box::pin(wal.wait_for(stream));
    assert!(matches!(
        poll_wait(&mut recovered, &first_waker),
        std::task::Poll::Ready(Ok(()))
    ));
}

#[test]
fn wait_for_stream_closes_on_poison_and_reopen_restores_readiness() {
    let temp = TempDir::new().unwrap();
    let stream = StreamId::new(3);
    let config = FileWalConfig::new(temp.path());
    let mut wal = FileWal::open(config.clone()).unwrap();
    let mut waiting = Box::pin(wal.wait_for(stream));
    let (waker, awakened) = signal_waker();
    assert!(poll_wait(&mut waiting, &waker).is_pending());
    wal.root.arm_failpoint(TestFailPoint::EpochSynced);

    assert_injected_failure(wal.append_to(stream, record("uncertain", b"payload")));
    awakened.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        poll_wait(&mut waiting, &waker),
        std::task::Poll::Ready(Err(WalError::ReadinessClosed))
    ));
    assert!(wal.is_poisoned());
    drop(wal);

    let wal = FileWal::open(config).unwrap();
    let mut recovered = Box::pin(wal.wait_for(stream));
    assert!(matches!(
        poll_wait(&mut recovered, &waker),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(pending_ids(&wal), ["uncertain"]);
}

#[test]
fn wait_for_stream_closes_when_the_owning_log_is_dropped() {
    let temp = TempDir::new().unwrap();
    let stream = StreamId::new(4);
    let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let mut waiting = Box::pin(wal.wait_for(stream));
    let (waker, awakened) = signal_waker();
    assert!(poll_wait(&mut waiting, &waker).is_pending());

    drop(wal);
    awakened.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        poll_wait(&mut waiting, &waker),
        std::task::Poll::Ready(Err(WalError::ReadinessClosed))
    ));
}

#[test]
fn age_rollover_is_persisted_per_stream_and_checks_idle_streams_explicitly() {
    let temp = TempDir::new().unwrap();
    let stream_one = StreamId::new(1);
    let stream_two = StreamId::new(2);
    let config = FileWalConfig::new(temp.path())
        .with_max_segment_age(std::time::Duration::from_secs(1))
        .with_stream_rollover(
            stream_two,
            RolloverPolicy::new(DEFAULT_SEGMENT_BYTES)
                .with_max_segment_age(std::time::Duration::from_secs(2)),
        );
    let mut wal = FileWal::open(config.clone()).unwrap();
    let first = wal
        .append_batch_to_at(stream_one, &[record("one", b"one")], 10_000)
        .unwrap()
        .pop()
        .unwrap();
    wal.append_batch_to_at(stream_two, &[record("two", b"two")], 10_000)
        .unwrap();
    assert_eq!(first.segment_id, 0);
    assert_eq!(
        wal.state()
            .active_segment_created_at(stream_one.get())
            .unwrap(),
        Some(10_000)
    );
    drop(wal);

    let mut wal = FileWal::open(config).unwrap();
    assert!(wal.rollover_expired_at(10_999).unwrap().is_empty());
    assert_eq!(wal.rollover_expired_at(11_000).unwrap(), [stream_one]);
    assert!(stream_segment(&temp, stream_one, 1).exists());
    assert!(!wal.rollover(stream_one).unwrap());

    assert_eq!(wal.rollover_expired_at(12_000).unwrap(), [stream_two]);
    assert!(stream_segment(&temp, stream_two, 1).exists());
    assert!(wal.rollover_expired_at(13_000).unwrap().is_empty());

    let in_empty_active = wal
        .append_batch_to_at(stream_one, &[record("one-next", b"next")], 12_000)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(in_empty_active.segment_id, 1);
    let after_age = wal
        .append_batch_to_at(stream_one, &[record("one-aged", b"aged")], 13_000)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(after_age.segment_id, 2);
}

#[test]
fn multi_stream_age_rotation_crash_windows_follow_manifest_authority() {
    let cases = [
        (TestFailPoint::SegmentCreated, false),
        (TestFailPoint::RotationManifestWritten, true),
        (TestFailPoint::RotationManifestSynced, true),
    ];

    for (failpoint, rotations_visible) in cases {
        let temp = TempDir::new().unwrap();
        let streams = [StreamId::new(1), StreamId::new(2)];
        let config =
            FileWalConfig::new(temp.path()).with_max_segment_age(std::time::Duration::from_secs(1));
        let mut wal = FileWal::open(config.clone()).unwrap();
        wal.append_batch_to_at(streams[0], &[record("one", b"one")], 10_000)
            .unwrap();
        wal.append_batch_to_at(streams[1], &[record("two", b"two")], 10_000)
            .unwrap();
        wal.root.arm_failpoint(failpoint);

        assert_injected_failure(wal.rollover_expired_at(11_000));
        assert!(wal.is_poisoned());
        drop(wal);

        let wal = FileWal::open(config).unwrap();
        assert_eq!(
            wal.recovery()
                .pending_records_iter()
                .map(|record| record.meta.record_id.as_str())
                .collect::<Vec<_>>(),
            ["one", "two"]
        );
        for stream_id in streams {
            assert_eq!(
                stream_segment(&temp, stream_id, 1).exists(),
                rotations_visible
            );
        }
    }
}

#[test]
fn expired_streams_share_one_manifest_sync() {
    let temp = TempDir::new().unwrap();
    let streams = [StreamId::new(1), StreamId::new(2)];
    let config =
        FileWalConfig::new(temp.path()).with_max_segment_age(std::time::Duration::from_secs(1));
    let mut wal = FileWal::open(config).unwrap();
    for (index, stream_id) in streams.into_iter().enumerate() {
        wal.append_batch_to_at(
            stream_id,
            &[record(&format!("record-{index}"), b"payload")],
            10_000,
        )
        .unwrap();
    }
    let before = wal.stats().manifest_syncs;

    assert_eq!(wal.rollover_expired_at(11_000).unwrap(), streams);
    assert_eq!(wal.stats().manifest_syncs - before, 1);
}

#[test]
fn torn_multi_stream_age_manifest_applies_only_complete_rotations() {
    let temp = TempDir::new().unwrap();
    let streams = [StreamId::new(1), StreamId::new(2)];
    let config =
        FileWalConfig::new(temp.path()).with_max_segment_age(std::time::Duration::from_secs(1));
    let mut wal = FileWal::open(config.clone()).unwrap();
    for (index, stream_id) in streams.into_iter().enumerate() {
        wal.append_batch_to_at(
            stream_id,
            &[record(&format!("record-{index}"), b"payload")],
            10_000,
        )
        .unwrap();
    }
    let path = manifest(&temp);
    let rotation_start = fs::metadata(&path).unwrap().len();
    assert_eq!(wal.rollover_expired_at(11_000).unwrap(), streams);
    drop(wal);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut prefix = [0_u8; MANIFEST_RECORD_PREFIX_LEN as usize];
    file.read_exact_at(&mut prefix, rotation_start).unwrap();
    let first_rotation_len = parse_manifest_record_prefix(&prefix).unwrap().frame_len;
    file.set_len(rotation_start + first_rotation_len + 10)
        .unwrap();
    file.sync_data().unwrap();
    drop(file);

    let wal = FileWal::open(config).unwrap();
    assert_eq!(wal.stats().repaired_tails, 1);
    assert!(stream_segment(&temp, streams[0], 1).exists());
    assert!(!stream_segment(&temp, streams[1], 1).exists());
    assert_eq!(
        wal.recovery()
            .pending_records_iter()
            .map(|record| record.meta.record_id.as_str())
            .collect::<Vec<_>>(),
        ["record-0", "record-1"]
    );
}

#[test]
fn legacy_active_segment_gets_one_durable_age_baseline() {
    let temp = TempDir::new().unwrap();
    let directory = temp.path().join("segments");
    fs::create_dir(&directory).unwrap();
    let mut stats = WalStats::default();
    drop(create_segment(&directory, 0, 0, &mut stats).unwrap());

    let mut manifest_bytes = file_header(MANIFEST_MAGIC, MANIFEST_FORMAT_VERSION, 0, 0).to_vec();
    let rotation = SegmentRotationV1 {
        shard_id: 0,
        previous_segment_id: None,
        new_segment_id: 0,
        created_at_unix_millis: None,
    };
    encode_manifest_record(
        &mut manifest_bytes,
        SEGMENT_ROTATION_KIND,
        &serde_json::to_vec(&rotation).unwrap(),
    )
    .unwrap();
    fs::write(manifest(&temp), &manifest_bytes).unwrap();

    let config =
        FileWalConfig::new(temp.path()).with_max_segment_age(std::time::Duration::from_secs(1));
    let wal = FileWal::open(config.clone()).unwrap();
    let baseline = wal
        .state()
        .active_segment_created_at(DEFAULT_STREAM.get())
        .unwrap()
        .unwrap();
    assert!(baseline > 0);
    let upgraded_manifest_len = fs::metadata(manifest(&temp)).unwrap().len();
    assert!(upgraded_manifest_len > manifest_bytes.len() as u64);
    drop(wal);

    let wal = FileWal::open(config).unwrap();
    assert_eq!(
        wal.state()
            .active_segment_created_at(DEFAULT_STREAM.get())
            .unwrap(),
        Some(baseline)
    );
    assert_eq!(
        fs::metadata(manifest(&temp)).unwrap().len(),
        upgraded_manifest_len
    );
}

#[test]
fn interrupted_new_stream_creation_is_reconciled_from_the_manifest() {
    let temp = TempDir::new().unwrap();
    let stream_id = StreamId::new(77);
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    wal.root.arm_failpoint(TestFailPoint::SegmentCreated);

    assert_injected_failure(wal.append_to(stream_id, record("record", b"payload")));
    assert!(wal.is_poisoned());
    drop(wal);

    let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    assert_eq!(wal.streams().collect::<Vec<_>>(), [DEFAULT_STREAM]);
    assert!(!stream_segment(&temp, stream_id, 0).exists());
}

#[test]
fn single_append_returns_its_durable_location() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);

    let location = wal.append(record("one", b"payload")).unwrap();

    assert_eq!(wal.read(&location).unwrap(), b"payload".as_slice());
    assert_eq!(wal.recovery().pending_records().len(), 1);
}

#[test]
fn duplicate_record_ids_reject_the_whole_batch_before_writing() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let before = wal.stats();

    let error = wal
        .append_batch(&[record("same", b"a"), record("same", b"b")])
        .unwrap_err();

    assert!(matches!(error, WalError::DuplicateRecord(id) if id == "same"));
    assert!(wal.recovery().records.is_empty());
    assert_eq!(wal.stats().epoch_syncs, before.epoch_syncs);

    wal.append(record("existing", b"durable")).unwrap();
    let error = wal
        .append_batch(&[record("new", b"new"), record("existing", b"again")])
        .unwrap_err();
    assert!(matches!(error, WalError::DuplicateRecord(id) if id == "existing"));
    assert_eq!(wal.recovery().records.len(), 1);
}

#[test]
fn invalid_ids_and_oversized_metadata_are_rejected_before_writing() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);

    assert!(matches!(
        wal.append(AppendRecord::new("", Bytes::new())),
        Err(WalError::InvalidRecord(_))
    ));
    assert!(matches!(
        wal.append(AppendRecord::new(
            "x".repeat(MAX_RECORD_ID_BYTES + 1),
            Bytes::new(),
        )),
        Err(WalError::InvalidRecord(_))
    ));
    assert!(matches!(
        wal.append(AppendRecord::new("large", Bytes::new()).with_metadata(vec![
            0;
            MAX_METADATA_LEN
                as usize
        ]),),
        Err(WalError::InvalidRecord(_))
    ));
    assert!(wal.recovery().records.is_empty());
    assert_eq!(wal.stats().epoch_syncs, 0);
    assert!(!wal.is_poisoned());

    let absent_stream = StreamId::new(99);
    assert!(matches!(
        wal.append_to(absent_stream, AppendRecord::new("", Bytes::new())),
        Err(WalError::InvalidRecord(_))
    ));
    assert!(!wal.streams().any(|stream_id| stream_id == absent_stream));
    assert!(!shard_directory(temp.path(), absent_stream.get()).exists());
    assert!(!wal.is_poisoned());

    let oversized_release = validate_release_metadata_len(MAX_METADATA_LEN as usize + 1);
    assert!(matches!(
        wal.finish_operation(oversized_release),
        Err(WalError::InvalidRecord(_))
    ));
    assert!(!wal.is_poisoned());
}

#[test]
fn invalid_stream_rollover_policies_are_rejected_before_open_mutates_storage() {
    let too_small = TempDir::new().unwrap();
    let error = FileWal::open(
        FileWalConfig::new(too_small.path())
            .with_stream_rollover(StreamId::new(3), RolloverPolicy::new(FILE_HEADER_LEN)),
    )
    .err()
    .expect("a per-stream size below the header must fail");
    assert!(matches!(error, WalError::InvalidConfig(_)));
    assert!(!manifest(&too_small).exists());

    let too_young = TempDir::new().unwrap();
    let error = FileWal::open(
        FileWalConfig::new(too_young.path())
            .with_max_segment_age(std::time::Duration::from_nanos(1)),
    )
    .err()
    .expect("sub-millisecond age cannot be represented durably");
    assert!(matches!(error, WalError::InvalidConfig(_)));
    assert!(!manifest(&too_young).exists());
}

#[test]
fn storage_root_has_one_process_owner() {
    let temp = TempDir::new().unwrap();
    let config = config(&temp, DEFAULT_SEGMENT_BYTES);
    let wal = FileWal::open(config.clone()).unwrap();

    let error = FileWal::open(config.clone())
        .err()
        .expect("a second owner must be rejected");
    assert!(matches!(error, WalError::RootInUse(_)));

    drop(wal);
    FileWal::open(config).unwrap();
}

#[test]
fn newly_created_storage_is_private_by_default() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("private-root");
    let stream_id = StreamId::new(5);
    let mut wal = FileWal::open(FileWalConfig::new(&root)).unwrap();
    wal.append_to(stream_id, record("stream-record", b"payload"))
        .unwrap();

    for directory in [
        &root,
        &root.join("segments"),
        &root.join("streams"),
        &shard_directory(&root, stream_id.get()),
    ] {
        let mode = fs::metadata(directory).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{} is not private", directory.display());
    }
    for file in [
        root.join("camus.lock"),
        root.join("MANIFEST"),
        root.join("segments/segment-00000000000000000000.log"),
        shard_directory(&root, stream_id.get()).join("segment-00000000000000000000.log"),
    ] {
        let mode = fs::metadata(&file).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{} is not private", file.display());
    }

    drop(wal);
}

#[test]
fn an_incomplete_epoch_is_removed_as_one_atomic_tail() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    wal.append(record("durable", b"first")).unwrap();
    let torn = wal
        .append_batch(&[record("torn-a", b"second"), record("torn-b", b"third")])
        .unwrap();
    drop(wal);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&temp, 0))
        .unwrap();
    file.set_len(torn[1].frame_offset + 10).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    assert_eq!(wal.stats().repaired_tails, 1);
    assert_eq!(
        wal.recovery()
            .pending_records()
            .iter()
            .map(|record| record.meta.record_id.as_str())
            .collect::<Vec<_>>(),
        ["durable"]
    );
}

#[test]
fn every_active_epoch_frame_boundary_recovers_to_a_complete_epoch() {
    for cut_case in 0..8 {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        wal.append(record("durable", b"first")).unwrap();
        let uncertain = wal.append(record("uncertain", b"second payload")).unwrap();
        let path = segment(&temp, uncertain.segment_id);
        let file_len = fs::metadata(&path).unwrap().len();
        let marker_start = uncertain.frame_offset + uncertain.frame_len;
        let cut = match cut_case {
            0 => uncertain.frame_offset,
            1 => uncertain.frame_offset + 1,
            2 => uncertain.frame_offset + SEGMENT_RECORD_PREFIX_LEN - 1,
            3 => uncertain.frame_offset + SEGMENT_RECORD_PREFIX_LEN,
            4 => marker_start - 1,
            5 => marker_start,
            6 => marker_start + SEGMENT_RECORD_PREFIX_LEN,
            7 => file_len - 1,
            _ => unreachable!(),
        };
        drop(wal);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(cut).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        assert_eq!(pending_ids(&wal), ["durable"]);
        assert_eq!(fs::metadata(path).unwrap().len(), uncertain.frame_offset);
        assert_eq!(wal.stats().repaired_tails, u64::from(cut_case != 0));
    }
}

#[test]
fn metadata_corruption_before_a_valid_suffix_fails_closed() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let first = wal.append(record("first", b"one")).unwrap();
    wal.append(record("second", b"two")).unwrap();
    drop(wal);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&temp, 0))
        .unwrap();
    file.write_at(b"X", first.frame_offset + SEGMENT_RECORD_PREFIX_LEN + 4)
        .unwrap();
    file.sync_data().unwrap();
    drop(file);

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("non-tail corruption must fail closed");
    assert!(matches!(error, WalError::Corruption { .. }));
}

#[test]
fn payload_checksums_are_rechecked_on_targeted_reads() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let location = wal.append(record("record", b"payload")).unwrap();
    drop(wal);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&temp, 0))
        .unwrap();
    file.write_at(b"X", location.payload_offset).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    assert_eq!(wal.recovery().pending_records().len(), 1);
    assert!(matches!(
        wal.read(&location),
        Err(WalError::Corruption { .. })
    ));
    assert!(wal.is_poisoned());
    assert!(matches!(wal.read(&location), Err(WalError::Poisoned)));
}

#[test]
fn reads_revalidate_record_descriptors_and_do_not_poison_on_bad_locations() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let location = wal.append(record("record", b"payload")).unwrap();

    let mut mismatched = location.clone();
    mismatched.payload_checksum ^= 1;
    assert!(matches!(
        wal.read(&mismatched),
        Err(WalError::InvalidLocation(_))
    ));
    assert!(!wal.is_poisoned());

    let mut foreign = location;
    foreign.segment_id += 100;
    assert!(matches!(
        wal.read(&foreign),
        Err(WalError::InvalidLocation(_))
    ));
    assert!(!wal.is_poisoned());
}

#[test]
fn reads_detect_metadata_damage_that_happens_after_recovery() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let location = wal.append(record("record", b"payload")).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&temp, location.segment_id))
        .unwrap();
    file.write_at(b"X", location.frame_offset + SEGMENT_RECORD_PREFIX_LEN + 4)
        .unwrap();
    file.sync_data().unwrap();
    drop(file);

    assert!(matches!(
        wal.read(&location),
        Err(WalError::Corruption { .. })
    ));
    assert!(wal.is_poisoned());
}

#[test]
fn batch_reads_preserve_input_order_and_coalesce_adjacent_frames() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let locations = wal
        .append_batch(&[record("a", b"first"), record("b", b"second")])
        .unwrap();
    let before = wal.stats();

    let payloads = wal
        .read_many(&[locations[1].clone(), locations[0].clone()])
        .unwrap();

    assert_eq!(
        payloads,
        [Bytes::from_static(b"second"), Bytes::from_static(b"first")]
    );
    let delta = wal.stats();
    assert_eq!(delta.read_calls - before.read_calls, 1);
    assert_eq!(delta.read_segment_opens - before.read_segment_opens, 1);
    assert_eq!(delta.read_ranges - before.read_ranges, 1);
    assert_eq!(
        delta.read_frame_bytes - before.read_frame_bytes,
        locations
            .iter()
            .map(|location| location.frame_len)
            .sum::<u64>()
    );
}

#[test]
fn release_is_durable_and_exact_replay_is_idempotent() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    wal.append_batch(&[record("a", b"one"), record("b", b"two")])
        .unwrap();
    let before = wal.stats().manifest_syncs;

    wal.release(["a"]).unwrap();
    assert_eq!(wal.stats().manifest_syncs - before, 1);
    assert_eq!(wal.recovery().pending_records()[0].meta.record_id, "b");
    let replay_before = wal.stats().manifest_syncs;
    wal.release(["a"]).unwrap();
    assert_eq!(wal.stats().manifest_syncs, replay_before);
    drop(wal);

    let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    assert_eq!(wal.recovery().pending_records()[0].meta.record_id, "b");
    assert!(wal.state().released_record_ids().contains("a"));
}

#[test]
fn release_rejects_unknown_or_repeated_ids_atomically() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    wal.append(record("a", b"one")).unwrap();
    let before = wal.stats().manifest_syncs;

    assert!(matches!(
        wal.release(["missing"]),
        Err(WalError::UnknownRecord(id)) if id == "missing"
    ));
    assert!(matches!(
        wal.release(["a", "a"]),
        Err(WalError::InvalidRecord(_))
    ));
    assert_eq!(wal.stats().manifest_syncs, before);
    assert_eq!(wal.recovery().pending_records().len(), 1);
    assert!(!wal.is_poisoned());
}

#[test]
fn recovery_snapshot_tracks_rotation_and_deferred_compaction() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, 64);
    wal.append(record("old", b"old payload")).unwrap();
    wal.append(record("active", b"active payload")).unwrap();

    assert_eq!(
        wal.recovery().state.segments(0),
        wal.state().segments(0),
        "append-triggered rotation must update the recovery snapshot"
    );

    wal.release(["old"]).unwrap();
    let report = wal.reclaim_with_limits(0, 0).unwrap();
    assert_eq!(report.segments, 1);
    assert_eq!(
        wal.recovery().state.segments(0),
        wal.state().segments(0),
        "deferred manifest compaction must not leave recovery state stale"
    );
    assert_eq!(
        wal.recovery().state.released_record_ids(),
        wal.state().released_record_ids()
    );
}

#[test]
fn reclaim_removes_only_fully_released_sealed_segments() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, 64);
    let old = wal.append(record("old", b"old payload")).unwrap();
    let active = wal.append(record("active", b"active payload")).unwrap();
    assert_ne!(old.segment_id, active.segment_id);

    wal.release(["old"]).unwrap();
    assert_eq!(wal.recovery().pending_records()[0].meta.record_id, "active");
    let before = wal.storage_bytes().unwrap();
    let report = wal.reclaim().unwrap();

    assert_eq!(report.segments, 1);
    assert!(report.bytes > 0);
    assert!(wal.storage_bytes().unwrap() < before);
    assert!(!segment(&temp, old.segment_id).exists());
    assert!(segment(&temp, active.segment_id).exists());
    assert_eq!(wal.recovery().records.len(), 1);
    assert!(wal.state().released_record_ids().is_empty());

    drop(wal);
    let wal = open(&temp, 64);
    assert_eq!(wal.recovery().pending_records()[0].meta.record_id, "active");
}

#[test]
fn storage_pressure_can_rotate_and_reclaim_a_released_active_segment() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    wal.append(record("done", b"payload")).unwrap();
    wal.release(["done"]).unwrap();

    assert_eq!(wal.reclaim().unwrap().segments, 0);
    let report = wal.reclaim_active_for_storage_pressure().unwrap();

    assert_eq!(report.segments, 1);
    assert!(wal.recovery().records.is_empty());
    assert!(wal.state().released_record_ids().is_empty());
    assert!(segment(&temp, 1).exists());
}

#[test]
fn a_torn_release_record_is_repaired_without_hiding_the_record() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    wal.append(record("a", b"payload")).unwrap();
    let manifest = temp.path().join("MANIFEST");
    let before_release = fs::metadata(&manifest).unwrap().len();
    wal.release(["a"]).unwrap();
    let after_release = fs::metadata(&manifest).unwrap().len();
    assert!(after_release > before_release);
    drop(wal);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&manifest)
        .unwrap();
    file.set_len(after_release - 3).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    assert_eq!(wal.stats().repaired_tails, 1);
    assert_eq!(fs::metadata(manifest).unwrap().len(), before_release);
    assert_eq!(wal.recovery().pending_records().len(), 1);
    assert!(wal.state().released_record_ids().is_empty());
}

#[test]
fn every_manifest_tail_boundary_preserves_an_unreleased_record() {
    for cut_case in 0..5 {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        wal.append(record("record", b"payload")).unwrap();
        let path = manifest(&temp);
        let release_start = fs::metadata(&path).unwrap().len();
        wal.release(["record"]).unwrap();
        let release_end = fs::metadata(&path).unwrap().len();
        let cut = match cut_case {
            0 => release_start,
            1 => release_start + 1,
            2 => release_start + MANIFEST_RECORD_PREFIX_LEN - 1,
            3 => release_start + MANIFEST_RECORD_PREFIX_LEN,
            4 => release_end - 1,
            _ => unreachable!(),
        };
        drop(wal);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(cut).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        assert_eq!(pending_ids(&wal), ["record"]);
        assert_eq!(fs::metadata(path).unwrap().len(), release_start);
        assert_eq!(wal.stats().repaired_tails, u64::from(cut_case != 0));
    }
}

#[test]
fn sealed_segment_damage_or_truncation_fails_closed() {
    for truncate in [false, true] {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, 64);
        let old = wal.append(record("old", b"old payload")).unwrap();
        wal.append(record("active", b"active payload")).unwrap();
        let path = segment(&temp, old.segment_id);
        drop(wal);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        if truncate {
            file.set_len(file.metadata().unwrap().len() - 1).unwrap();
        } else {
            file.write_at(b"X", old.frame_offset + SEGMENT_RECORD_PREFIX_LEN + 4)
                .unwrap();
        }
        file.sync_data().unwrap();
        drop(file);

        let error = FileWal::open(config(&temp, 64))
            .err()
            .expect("sealed segment damage must fail closed");
        assert!(matches!(error, WalError::Corruption { .. }));
    }
}

#[test]
fn manifest_corruption_before_a_valid_suffix_fails_closed() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    wal.append_batch(&[record("a", b"one"), record("b", b"two")])
        .unwrap();
    let path = manifest(&temp);
    let first_release = fs::metadata(&path).unwrap().len();
    wal.release(["a"]).unwrap();
    wal.release(["b"]).unwrap();
    drop(wal);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.write_at(b"X", first_release + MANIFEST_RECORD_PREFIX_LEN)
        .unwrap();
    file.sync_data().unwrap();
    drop(file);

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("manifest corruption before a valid suffix must fail closed");
    assert!(matches!(error, WalError::Corruption { .. }));
}

#[test]
fn interrupted_segment_creation_is_cleaned_but_data_is_not_guessed() {
    let empty_temp = TempDir::new().unwrap();
    drop(open(&empty_temp, DEFAULT_SEGMENT_BYTES));
    let empty_extra = segment(&empty_temp, 1);
    fs::write(
        &empty_extra,
        file_header(SEGMENT_MAGIC, SEGMENT_FORMAT_VERSION, 0, 1),
    )
    .unwrap();
    let stale_temporary = segment_temporary_path(&empty_temp.path().join("segments"), 2);
    fs::write(&stale_temporary, b"interrupted creation").unwrap();

    drop(open(&empty_temp, DEFAULT_SEGMENT_BYTES));
    assert!(!empty_extra.exists());
    assert!(!stale_temporary.exists());

    let data_temp = TempDir::new().unwrap();
    let mut wal = open(&data_temp, DEFAULT_SEGMENT_BYTES);
    wal.append(record("record", b"payload")).unwrap();
    drop(wal);
    let unmanifested = segment(&data_temp, 1);
    fs::copy(segment(&data_temp, 0), &unmanifested).unwrap();

    let error = FileWal::open(config(&data_temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("an unmanifested segment containing data must fail closed");
    assert!(matches!(error, WalError::Corruption { .. }));
}

#[test]
fn unmanifested_logical_stream_data_fails_closed() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    wal.append(record("record", b"payload")).unwrap();
    drop(wal);

    let stream_id = StreamId::new(42);
    let directory = shard_directory(temp.path(), stream_id.get());
    fs::create_dir_all(&directory).unwrap();
    fs::copy(segment(&temp, 0), stream_segment(&temp, stream_id, 0)).unwrap();

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("unmanifested stream payload bytes must not be guessed");
    assert!(matches!(error, WalError::Corruption { .. }));
}

#[test]
fn interrupted_manifest_creation_is_restarted_or_cleaned() {
    let missing_manifest = TempDir::new().unwrap();
    let creating = missing_manifest.path().join("MANIFEST.create");
    fs::write(&creating, b"partial manifest header").unwrap();

    drop(open(&missing_manifest, DEFAULT_SEGMENT_BYTES));
    assert!(manifest(&missing_manifest).exists());
    assert!(!creating.exists());

    let existing_manifest = TempDir::new().unwrap();
    drop(open(&existing_manifest, DEFAULT_SEGMENT_BYTES));
    let stale = existing_manifest.path().join("MANIFEST.create");
    fs::write(&stale, b"stale manifest creation").unwrap();

    drop(open(&existing_manifest, DEFAULT_SEGMENT_BYTES));
    assert!(!stale.exists());
}

#[test]
fn incomplete_or_corrupt_manifest_checkpoint_fails_closed() {
    for truncate in [false, true] {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, 64);
        wal.append(record("old", b"old payload")).unwrap();
        wal.append(record("active", b"active payload")).unwrap();
        wal.release(["old"]).unwrap();
        assert_eq!(wal.reclaim().unwrap().segments, 1);
        assert!(wal.state().released_record_ids().is_empty());
        let path = manifest(&temp);
        drop(wal);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        if truncate {
            file.set_len(file.metadata().unwrap().len() - 1).unwrap();
        } else {
            file.write_at(b"X", FILE_HEADER_LEN + MANIFEST_RECORD_PREFIX_LEN)
                .unwrap();
        }
        file.sync_data().unwrap();
        drop(file);

        let error = FileWal::open(config(&temp, 64))
            .err()
            .expect("a damaged authoritative checkpoint must fail closed");
        assert!(matches!(error, WalError::Corruption { .. }));
    }
}

#[test]
fn append_crash_windows_are_atomic_and_poison_the_open_handle() {
    let cases = [
        (TestFailPoint::EpochFramesWritten, false, 1),
        (TestFailPoint::EpochMarkerWritten, true, 0),
        (TestFailPoint::EpochSynced, true, 0),
    ];

    for (failpoint, epoch_visible, repaired_tails) in cases {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        wal.append(record("durable", b"first")).unwrap();
        wal.root.arm_failpoint(failpoint);

        let result = wal.append_batch(&[
            record("uncertain-a", b"second"),
            record("uncertain-b", b"third"),
        ]);

        assert_injected_failure(result);
        assert!(wal.is_poisoned());
        assert!(matches!(
            wal.append(record("must-reopen", b"blocked")),
            Err(WalError::Poisoned)
        ));
        drop(wal);

        let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        let expected = if epoch_visible {
            vec!["durable", "uncertain-a", "uncertain-b"]
        } else {
            vec!["durable"]
        };
        assert_eq!(pending_ids(&wal), expected);
        assert_eq!(wal.stats().repaired_tails, repaired_tails);
    }
}

#[test]
fn rotation_crash_windows_reconcile_only_manifested_segments() {
    let cases = [
        (TestFailPoint::SegmentCreated, false),
        (TestFailPoint::RotationManifestWritten, true),
        (TestFailPoint::RotationManifestSynced, true),
    ];

    for (failpoint, rotation_visible) in cases {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, 64);
        wal.append(record("old", b"old payload")).unwrap();
        wal.root.arm_failpoint(failpoint);

        let result = wal.append(record("uncertain", b"new payload"));

        assert_injected_failure(result);
        assert!(wal.is_poisoned());
        drop(wal);

        let wal = open(&temp, 64);
        assert_eq!(pending_ids(&wal), ["old"]);
        assert_eq!(segment(&temp, 1).exists(), rotation_visible);
    }
}

#[test]
fn release_crash_windows_never_hide_a_record_without_a_complete_manifest_record() {
    for failpoint in [
        TestFailPoint::ReleaseManifestWritten,
        TestFailPoint::ReleaseManifestSynced,
    ] {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        wal.append(record("record", b"payload")).unwrap();
        wal.root.arm_failpoint(failpoint);

        let result = wal.release(["record"]);

        assert_injected_failure(result);
        assert!(wal.is_poisoned());
        drop(wal);

        let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        assert!(pending_ids(&wal).is_empty());
        assert!(wal.state().released_record_ids().contains("record"));
    }
}

#[test]
fn stream_scoped_release_crash_windows_recover_the_extended_record() {
    for failpoint in [
        TestFailPoint::ReleaseManifestWritten,
        TestFailPoint::ReleaseManifestSynced,
    ] {
        let temp = TempDir::new().unwrap();
        let stream_id = StreamId::new(9);
        let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        wal.append_to(stream_id, record("record", b"payload"))
            .unwrap();
        wal.root.arm_failpoint(failpoint);

        assert_injected_failure(wal.release_from(stream_id, ["record"]));
        assert!(wal.is_poisoned());
        drop(wal);

        let wal = open(&temp, DEFAULT_SEGMENT_BYTES);
        assert!(wal.recovery().pending_records_for(stream_id).is_empty());
        assert!(wal.state().is_released(stream_id, "record"));
    }
}

#[test]
fn removal_crash_windows_converge_to_the_synced_manifest_state() {
    for failpoint in [
        TestFailPoint::RemovalManifestWritten,
        TestFailPoint::RemovalManifestSynced,
        TestFailPoint::SegmentsDeleted,
    ] {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, 64);
        let old = wal.append(record("old", b"old payload")).unwrap();
        wal.append(record("active", b"active payload")).unwrap();
        wal.release(["old"]).unwrap();
        wal.root.arm_failpoint(failpoint);

        let result = wal.reclaim();

        assert_injected_failure(result);
        assert!(wal.is_poisoned());
        drop(wal);

        let wal = open(&temp, 64);
        assert_eq!(pending_ids(&wal), ["active"]);
        assert!(!segment(&temp, old.segment_id).exists());
    }
}

#[test]
fn multi_stream_removal_is_published_before_any_stream_file_is_deleted() {
    for failpoint in [
        TestFailPoint::RemovalManifestWritten,
        TestFailPoint::RemovalManifestSynced,
        TestFailPoint::SegmentsDeleted,
    ] {
        let temp = TempDir::new().unwrap();
        let streams = [StreamId::new(1), StreamId::new(2)];
        let config = FileWalConfig::new(temp.path()).with_segment_bytes(FILE_HEADER_LEN + 1);
        let mut wal = FileWal::open(config.clone()).unwrap();
        for (index, stream_id) in streams.into_iter().enumerate() {
            wal.append_to(stream_id, record(&format!("old-{index}"), b"old"))
                .unwrap();
            wal.append_to(stream_id, record(&format!("active-{index}"), b"active"))
                .unwrap();
            wal.release_from(stream_id, [format!("old-{index}")])
                .unwrap();
        }
        wal.root.arm_failpoint(failpoint);

        assert_injected_failure(wal.reclaim());
        assert!(wal.is_poisoned());
        drop(wal);

        let wal = FileWal::open(config).unwrap();
        assert_eq!(
            wal.recovery()
                .pending_records_iter()
                .map(|record| record.meta.record_id.as_str())
                .collect::<Vec<_>>(),
            ["active-0", "active-1"]
        );
        for stream_id in streams {
            assert!(!stream_segment(&temp, stream_id, 0).exists());
            assert!(stream_segment(&temp, stream_id, 1).exists());
        }
    }
}

#[test]
fn manifest_compaction_crash_windows_keep_an_authoritative_manifest() {
    for failpoint in [
        TestFailPoint::CompactionSynced,
        TestFailPoint::CompactionRenamed,
    ] {
        let temp = TempDir::new().unwrap();
        let mut wal = open(&temp, 64);
        let old = wal.append(record("old", b"old payload")).unwrap();
        wal.append(record("active", b"active payload")).unwrap();
        wal.release(["old"]).unwrap();
        wal.root.arm_failpoint(failpoint);

        let result = wal.reclaim();

        assert_injected_failure(result);
        assert!(wal.is_poisoned());
        drop(wal);

        let temporary = temp.path().join("MANIFEST.compact");
        assert_eq!(
            temporary.exists(),
            failpoint == TestFailPoint::CompactionSynced
        );
        let wal = open(&temp, 64);
        assert_eq!(pending_ids(&wal), ["active"]);
        assert!(!segment(&temp, old.segment_id).exists());
        assert!(!temporary.exists());
    }
}

#[test]
fn unreclaimed_release_tombstones_reject_id_reuse_without_poisoning() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, 64);
    wal.append(record("old", b"old payload")).unwrap();
    wal.append(record("active", b"active payload")).unwrap();
    wal.release(["old"]).unwrap();
    assert_eq!(wal.reclaim_with_limits(0, u64::MAX).unwrap().segments, 1);
    assert!(wal.state().released_record_ids().contains("old"));

    let error = wal
        .append(record("old", b"must not be hidden"))
        .unwrap_err();

    assert!(matches!(error, WalError::DuplicateRecord(id) if id == "old"));
    assert!(!wal.is_poisoned());
    assert_eq!(pending_ids(&wal), ["active"]);
}

#[test]
fn a_missing_manifest_segment_fails_closed() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let location = wal.append(record("a", b"payload")).unwrap();
    drop(wal);
    fs::remove_file(segment(&temp, location.segment_id)).unwrap();

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("a manifest segment cannot disappear");
    assert!(matches!(error, WalError::Corruption { .. }));
}

#[test]
fn unsupported_storage_wire_version_is_rejected() {
    let temp = TempDir::new().unwrap();
    drop(open(&temp, DEFAULT_SEGMENT_BYTES));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&temp, 0))
        .unwrap();
    file.write_at(&u16::MAX.to_le_bytes(), 8).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("unsupported formats must not be guessed");
    assert!(matches!(error, WalError::Corruption { .. }));
}

#[test]
fn a_manifest_stream_with_a_missing_segment_fails_closed() {
    let temp = TempDir::new().unwrap();
    drop(open(&temp, DEFAULT_SEGMENT_BYTES));

    let rotation = SegmentRotationV1 {
        shard_id: 1,
        previous_segment_id: None,
        new_segment_id: 0,
        created_at_unix_millis: None,
    };
    let metadata = serde_json::to_vec(&rotation).unwrap();
    let mut encoded = Vec::new();
    encode_manifest_record(&mut encoded, SEGMENT_ROTATION_KIND, &metadata).unwrap();
    let mut file = OpenOptions::new()
        .append(true)
        .open(manifest(&temp))
        .unwrap();
    file.write_all(&encoded).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("a declared stream segment must exist");
    assert!(matches!(error, WalError::Corruption { .. }));
}

#[test]
fn checksummed_invalid_manifest_tail_fails_closed() {
    let temp = TempDir::new().unwrap();
    drop(open(&temp, DEFAULT_SEGMENT_BYTES));

    let mut encoded = Vec::new();
    encode_manifest_record(&mut encoded, RELEASE_KIND, br#"{"record_ids":[]}"#).unwrap();
    let path = manifest(&temp);
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&encoded).unwrap();
    file.sync_data().unwrap();
    drop(file);
    let invalid_len = fs::metadata(&path).unwrap().len();

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("a complete checksummed invalid manifest record must fail closed");
    assert!(matches!(error, WalError::Corruption { .. }));
    assert_eq!(fs::metadata(path).unwrap().len(), invalid_len);
}

#[test]
fn checksummed_invalid_segment_timestamp_fails_closed() {
    let temp = TempDir::new().unwrap();
    drop(open(&temp, DEFAULT_SEGMENT_BYTES));

    let timestamp = SegmentTimestampV1 {
        shard_id: DEFAULT_STREAM.get(),
        segment_id: 0,
        created_at_unix_millis: 1,
    };
    let mut encoded = Vec::new();
    encode_manifest_record(
        &mut encoded,
        SEGMENT_TIMESTAMP_KIND,
        &serde_json::to_vec(&timestamp).unwrap(),
    )
    .unwrap();
    let path = manifest(&temp);
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&encoded).unwrap();
    file.sync_data().unwrap();
    drop(file);
    let invalid_len = fs::metadata(&path).unwrap().len();

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("a duplicate authoritative timestamp must fail closed");
    assert!(matches!(error, WalError::Corruption { .. }));
    assert_eq!(fs::metadata(path).unwrap().len(), invalid_len);
}

#[test]
fn segment_snapshots_are_checkpoint_only() {
    let temp = TempDir::new().unwrap();
    drop(open(&temp, DEFAULT_SEGMENT_BYTES));

    let metadata = serde_json::to_vec(&SegmentSnapshotV1 {
        shard_id: 0,
        segment_id: 1,
        lifecycle: SegmentLifecycle::Active,
        created_at_unix_millis: None,
    })
    .unwrap();
    let mut encoded = Vec::new();
    encode_manifest_record(&mut encoded, SEGMENT_SNAPSHOT_KIND, &metadata).unwrap();
    let mut file = OpenOptions::new()
        .append(true)
        .open(manifest(&temp))
        .unwrap();
    file.write_all(&encoded).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("a snapshot event outside a checkpoint must fail closed");
    assert!(matches!(
        error,
        WalError::Corruption { message, .. } if message.contains("outside a manifest checkpoint")
    ));
}

#[test]
fn checksummed_unknown_segment_frame_fails_closed() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(&temp, DEFAULT_SEGMENT_BYTES);
    let location = wal.append(record("durable", b"payload")).unwrap();
    drop(wal);

    let path = segment(&temp, location.segment_id);
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&segment_record_prefix(255, &[], 0, 0).unwrap())
        .unwrap();
    file.sync_data().unwrap();
    drop(file);
    let invalid_len = fs::metadata(&path).unwrap().len();

    let error = FileWal::open(config(&temp, DEFAULT_SEGMENT_BYTES))
        .err()
        .expect("a complete checksummed unknown segment frame must fail closed");
    assert!(matches!(error, WalError::Corruption { .. }));
    assert_eq!(fs::metadata(path).unwrap().len(), invalid_len);
}
