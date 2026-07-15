use bytes::Bytes;
use camus::{
    Capacity, Config, Error, ErrorKind, FullPolicy, Log, ReadLimits, Record, RecordId, RootState,
    StreamId,
};
use std::time::Duration;

fn config(root: &std::path::Path) -> Config {
    Config::new(root, Capacity::Unbounded)
}

#[tokio::test]
async fn unreleased_record_is_replayed_until_release() {
    let directory = tempfile::tempdir().unwrap();
    let stream_id = StreamId::new(7);
    let id = {
        let log = Log::open(config(directory.path())).await.unwrap();
        let stream = log.stream(stream_id);
        let id = stream
            .append(
                Record::new(Bytes::from_static(b"payload"))
                    .with_metadata(Bytes::from_static(b"metadata")),
            )
            .await
            .unwrap();
        log.shutdown().await.unwrap();
        id
    };

    let log = Log::open(config(directory.path())).await.unwrap();
    let stream = log.stream(stream_id);
    let pending = stream.read(ReadLimits::new(8, 1024)).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id);
    assert_eq!(pending[0].metadata, Bytes::from_static(b"metadata"));
    assert_eq!(pending[0].payload, Bytes::from_static(b"payload"));
    stream.release(vec![id]).await.unwrap();
    log.shutdown().await.unwrap();

    let log = Log::open(config(directory.path())).await.unwrap();
    assert_eq!(log.stream(stream_id).stats().pending_records, 0);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn waiting_read_is_woken_by_a_durable_append() {
    let directory = tempfile::tempdir().unwrap();
    let log = Log::open(config(directory.path())).await.unwrap();
    let stream = log.stream(StreamId::new(11));
    let waiter_stream = stream.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        started_tx.send(()).unwrap();
        waiter_stream.read(ReadLimits::new(1, 1024)).await
    });
    started_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while log.stats().pressure.readiness_wait.current == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("read did not enter the readiness wait");

    let id = stream
        .append(Record::new(Bytes::from_static(b"ready")))
        .await
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("waiting read did not wake")
        .unwrap()
        .unwrap();
    assert_eq!(snapshot[0].id, id);
    let waits = log.stats().pressure.readiness_wait;
    assert_eq!(waits.current, 0);
    assert_eq!(waits.waits, 1);
    assert_eq!(waits.elapsed.observations, 1);
    stream.release(vec![id]).await.unwrap();
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn logical_stream_handles_share_pending_state_without_cross_stream_release() {
    let directory = tempfile::tempdir().unwrap();
    let first_id = StreamId::new(21);
    let second_id = StreamId::new(22);
    let log = Log::open(config(directory.path())).await.unwrap();
    let first = log.stream(first_id);
    let first_again = log.stream(first_id);
    let second = log.stream(second_id);

    let first_record = first
        .append(Record::new(Bytes::from_static(b"first")))
        .await
        .unwrap();
    let second_record = second
        .append(Record::new(Bytes::from_static(b"second")))
        .await
        .unwrap();
    assert_eq!(log.known_streams(), vec![first_id, second_id]);

    let observed = first_again.read(ReadLimits::new(8, 1024)).await.unwrap();
    assert_eq!(observed[0].id, first_record);
    first.release(vec![first_record]).await.unwrap();
    assert_eq!(first_again.stats().pending_records, 0);
    assert_eq!(second.stats().pending_records, 1);

    second.release(vec![second_record]).await.unwrap();
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_returns_the_longest_in_order_prefix_within_hard_limits() {
    let directory = tempfile::tempdir().unwrap();
    let log = Log::open(config(directory.path())).await.unwrap();
    let stream = log.stream(StreamId::new(25));
    let ids = stream
        .append_batch(vec![
            Record::new(Bytes::from_static(b"one")),
            Record::new(Bytes::from_static(b"three")),
            Record::new(Bytes::from_static(b"two")),
        ])
        .await
        .unwrap();

    let error = stream.read(ReadLimits::new(8, 2)).await.unwrap_err();
    assert!(matches!(
        error,
        Error::ReadLimitTooSmall {
            id,
            required_bytes: 3,
            max_bytes: 2,
        } if id == ids[0]
    ));

    let prefix = stream.read(ReadLimits::new(8, 7)).await.unwrap();
    assert_eq!(prefix.len(), 1);
    assert_eq!(prefix[0].id, ids[0]);

    let count_limited = stream.read(ReadLimits::new(2, 8)).await.unwrap();
    assert_eq!(
        count_limited
            .iter()
            .map(|record| record.id)
            .collect::<Vec<_>>(),
        ids[..2]
    );

    stream.release(ids).await.unwrap();
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn release_validates_scope_and_future_ids_but_is_idempotent_after_reclaim() {
    let directory = tempfile::tempdir().unwrap();
    let log = Log::open(config(directory.path())).await.unwrap();
    let stream_id = StreamId::new(27);
    let stream = log.stream(stream_id);
    let id = stream
        .append(Record::new(Bytes::from_static(b"release")))
        .await
        .unwrap();

    let other_stream = log.stream(StreamId::new(28));
    let other_stream_id = other_stream
        .append(Record::new(Bytes::from_static(b"other stream")))
        .await
        .unwrap();
    assert!(matches!(
        stream.release(vec![other_stream_id]).await,
        Err(Error::RecordIdScopeMismatch {
            id: rejected,
            expected_stream,
        }) if rejected == other_stream_id && expected_stream == stream_id
    ));

    let other_directory = tempfile::tempdir().unwrap();
    let other_log = Log::open(config(other_directory.path())).await.unwrap();
    let other_root_id = other_log
        .stream(stream_id)
        .append(Record::new(Bytes::from_static(b"other root")))
        .await
        .unwrap();
    assert!(matches!(
        stream.release(vec![other_root_id]).await,
        Err(Error::RecordIdScopeMismatch { id: rejected, .. }) if rejected == other_root_id
    ));

    let mut future_bytes = id.to_bytes();
    future_bytes[24..].copy_from_slice(&1_u64.to_le_bytes());
    let future_id = RecordId::from_bytes(future_bytes);
    assert!(matches!(
        stream.release(vec![future_id]).await,
        Err(Error::UnknownRecordId { id: rejected }) if rejected == future_id
    ));

    stream.release(vec![id, id]).await.unwrap();
    log.reclaim().await.unwrap();
    stream.release(vec![id]).await.unwrap();
    assert_eq!(stream.stats().pending_records, 0);

    other_stream.release(vec![other_stream_id]).await.unwrap();
    other_log
        .stream(stream_id)
        .release(vec![other_root_id])
        .await
        .unwrap();
    other_log.shutdown().await.unwrap();
    log.shutdown().await.unwrap();
}

fn bounded_config(root: &std::path::Path, when_full: FullPolicy) -> Config {
    Config::new(
        root,
        Capacity::Bounded {
            total_bytes: 12 * 1024,
            when_full,
        },
    )
    .with_max_epoch_bytes(2 * 1024)
    .with_segment_bytes(4 * 1024)
    .with_max_release_records(64)
    .with_max_commit_bytes(4 * 1024)
}

#[tokio::test]
async fn bounded_reject_and_block_preserve_existing_pending_data() {
    let directory = tempfile::tempdir().unwrap();
    let stream_id = StreamId::new(31);
    let mut ids = Vec::new();
    {
        let log = Log::open(bounded_config(directory.path(), FullPolicy::RejectNew))
            .await
            .unwrap();
        let stream = log.stream(stream_id);
        loop {
            match stream
                .append(Record::new(Bytes::from(vec![0x5a; 512])))
                .await
            {
                Ok(id) => ids.push(id),
                Err(Error::RejectedCapacity { .. }) => break,
                Err(error) => panic!("unexpected bounded append error: {error}"),
            }
        }
        assert!(!ids.is_empty());
        assert_eq!(stream.stats().pending_records, ids.len() as u64);
        assert!(log.stats().storage.actual_file_bytes <= 12 * 1024);
        log.shutdown().await.unwrap();
    }

    let log = Log::open(bounded_config(directory.path(), FullPolicy::Block))
        .await
        .unwrap();
    let stream = log.stream(stream_id);
    let blocked_stream = stream.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let admitted_before = log.stats().pressure.admitted_commands;
    let blocked = tokio::spawn(async move {
        started_tx.send(()).unwrap();
        blocked_stream
            .append(Record::new(Bytes::from(vec![0x33; 512])))
            .await
    });
    started_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = log.stats();
            if stats.pressure.admitted_commands > admitted_before
                && stats.pressure.capacity_wait.current > 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("append did not reach the capacity wait");
    let blocked_admissions = log.stats().pressure.admitted_commands;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(log.stats().pressure.admitted_commands, blocked_admissions);

    stream.release(ids).await.unwrap();
    let admitted = tokio::time::timeout(Duration::from_secs(5), blocked)
        .await
        .expect("blocked append did not resume after capacity was reclaimed")
        .unwrap()
        .unwrap();
    assert_eq!(stream.stats().pending_records, 1);
    stream.release(vec![admitted]).await.unwrap();
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_block_rejects_an_append_that_can_never_fit() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config::new(
        directory.path(),
        Capacity::Bounded {
            total_bytes: 600,
            when_full: FullPolicy::Block,
        },
    )
    .with_max_epoch_bytes(1_024)
    .with_segment_bytes(2_048)
    .with_max_release_records(8)
    .with_max_commit_bytes(1_024);
    let log = Log::open(config).await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        log.stream(StreamId::new(37))
            .append(Record::new(Bytes::from_static(b"cannot fit"))),
    )
    .await
    .expect("an impossible append waited under Block");
    assert!(matches!(result, Err(Error::ExceedsCapacity { .. })));
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn root_observability_separates_calls_commits_and_maintenance() {
    let directory = tempfile::tempdir().unwrap();
    let log = Log::open(config(directory.path()).with_detailed_observability())
        .await
        .unwrap();
    let stream = log.stream(StreamId::new(41));

    assert_eq!(log.health().state, RootState::Running);
    assert!(log.stats().detailed_timings);

    let error = stream.read(ReadLimits::new(0, 1024)).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidReadLimits);
    assert_eq!(error.kind().as_str(), "invalid_read_limits");

    let ids = stream
        .append_batch(vec![
            Record::new(Bytes::from_static(b"first")),
            Record::new(Bytes::from_static(b"second")),
        ])
        .await
        .unwrap();
    let snapshot = stream.read(ReadLimits::new(2, 1024)).await.unwrap();
    assert_eq!(snapshot.len(), 2);
    stream.release(vec![ids[0], ids[0], ids[1]]).await.unwrap();
    log.reclaim().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while log.stats().pressure.storage_job_elapsed.observations < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed storage-job timings were not published");

    let stats = log.stats();
    assert_eq!(stats.storage.pending_records, 0);
    assert_eq!(stats.operations.append.started, 1);
    assert_eq!(stats.operations.append.succeeded, 1);
    assert_eq!(stats.operations.append.records, 2);
    assert_eq!(stats.operations.append.payload_bytes, 11);
    assert_eq!(stats.operations.append.elapsed.observations, 1);
    assert_eq!(stats.operations.read.started, 2);
    assert_eq!(stats.operations.read.succeeded, 1);
    assert_eq!(stats.operations.read.failed, 1);
    assert_eq!(stats.operations.read.records, 2);
    assert_eq!(stats.operations.read.elapsed.observations, 2);
    assert_eq!(stats.operations.release.succeeded, 1);
    assert_eq!(stats.operations.release.records, 3);
    assert_eq!(stats.operations.release.elapsed.observations, 1);
    assert_eq!(stats.operations.reclaim.succeeded, 1);
    assert_eq!(stats.operations.reclaim.elapsed.observations, 1);

    assert_eq!(stats.commits.append_groups, 1);
    assert_eq!(stats.commits.append_units, 1);
    assert_eq!(stats.commits.append_records, 2);
    assert_eq!(stats.commits.release_groups, 1);
    assert_eq!(stats.commits.release_units, 1);
    assert_eq!(stats.commits.release_records, 2);
    assert!(stats.pressure.storage_job_elapsed.observations >= 4);
    assert!(stats.pressure.reactor_dispatch_wait.observations >= 4);
    assert_eq!(stats.pressure.storage_jobs.append.observations, 1);
    assert_eq!(stats.pressure.storage_jobs.read.observations, 1);
    assert_eq!(stats.pressure.storage_jobs.release.observations, 1);
    assert!(stats.pressure.storage_jobs.reclaim.observations >= 1);
    assert_eq!(stats.pressure.storage_jobs.segment_rollover.observations, 0);
    assert_eq!(stats.maintenance.explicit_reclaim_passes, 1);
    assert_eq!(stats.maintenance.reclaimed_segments, 1);
    assert_eq!(stats.pressure.queue_wait.waits, 0);
    assert_eq!(log.health().state, RootState::Running);

    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn default_observability_counts_without_detailed_call_timing() {
    let directory = tempfile::tempdir().unwrap();
    let log = Log::open(config(directory.path())).await.unwrap();
    log.stream(StreamId::new(43))
        .append(Record::new(Bytes::from_static(b"payload")))
        .await
        .unwrap();

    let stats = log.stats();
    assert!(!stats.detailed_timings);
    assert_eq!(stats.operations.append.succeeded, 1);
    assert_eq!(stats.operations.append.elapsed.observations, 0);
    assert_eq!(stats.commits.append_groups, 1);
    assert_eq!(stats.pressure.storage_job_elapsed.observations, 0);
    assert_eq!(stats.pressure.reactor_dispatch_wait.observations, 0);
    assert_eq!(stats.pressure.storage_jobs.append.observations, 0);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_waiting_read_is_reported_as_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let log = Log::open(config(directory.path()).with_detailed_observability())
        .await
        .unwrap();
    let stream = log.stream(StreamId::new(47));
    let mut waiting = Box::pin(stream.read(ReadLimits::new(1, 1024)));

    tokio::select! {
        biased;
        result = &mut waiting => panic!("empty stream read completed unexpectedly: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert_eq!(log.stats().pressure.readiness_wait.current, 1);
    drop(waiting);

    let stats = log.stats();
    assert_eq!(stats.pressure.readiness_wait.current, 0);
    assert_eq!(stats.pressure.readiness_wait.waits, 1);
    assert_eq!(stats.operations.read.started, 1);
    assert_eq!(stats.operations.read.cancelled, 1);
    assert_eq!(stats.operations.read.elapsed.observations, 1);
    log.shutdown().await.unwrap();
}
