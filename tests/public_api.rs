use camus::{Config, Log, Record, RolloverPolicy, StreamId, WaitForStream, DEFAULT_STREAM};
use std::time::Duration;

struct SignalWake(std::sync::mpsc::Sender<()>);

impl std::task::Wake for SignalWake {
    fn wake(self: std::sync::Arc<Self>) {
        let _ = self.0.send(());
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        let _ = self.0.send(());
    }
}

fn poll_wait(
    future: &mut std::pin::Pin<Box<WaitForStream>>,
    waker: &std::task::Waker,
) -> std::task::Poll<camus::Result<()>> {
    let mut context = std::task::Context::from_waker(waker);
    std::future::Future::poll(future.as_mut(), &mut context)
}

#[test]
fn unreleased_record_is_replayed_until_release() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config::new(directory.path());

    let mut log = Log::open(config.clone()).unwrap();
    let location = log
        .append(
            Record::new("record-1", b"payload".as_slice()).with_metadata(b"metadata".as_slice()),
        )
        .unwrap();
    assert_eq!(log.read(&location).unwrap(), b"payload".as_slice());
    drop(log);

    let mut log = Log::open(config.clone()).unwrap();
    let pending = log.recovery().pending_records();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].meta.record_id, "record-1");
    assert_eq!(pending[0].meta.metadata, b"metadata".as_slice());
    log.release(["record-1"]).unwrap();
    drop(log);

    let log = Log::open(config).unwrap();
    assert!(log.recovery().pending_records().is_empty());
}

#[test]
fn async_wait_for_wakes_a_stream_consumer() {
    let directory = tempfile::tempdir().unwrap();
    let stream = StreamId::new(7);
    let mut log = Log::open(Config::new(directory.path())).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let waker = std::task::Waker::from(std::sync::Arc::new(SignalWake(sender)));
    let mut ready = Box::pin(log.wait_for(stream));
    assert!(poll_wait(&mut ready, &waker).is_pending());

    let location = log
        .append_to(
            stream,
            Record::new("event-1", b"payload".as_slice()).with_metadata(b"metadata".as_slice()),
        )
        .unwrap();
    receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        poll_wait(&mut ready, &waker),
        std::task::Poll::Ready(Ok(()))
    ));
    let pending = log.recovery().pending_records_for(stream);
    assert_eq!(pending[0].meta.record_id, "event-1");
    assert_eq!(pending[0].meta.metadata, b"metadata".as_slice());
    assert_eq!(pending[0].location, location);
    assert_eq!(
        log.read(&pending[0].location).unwrap(),
        b"payload".as_slice()
    );
    log.release_from(stream, ["event-1"]).unwrap();
    assert!(!log.readiness().is_ready(stream));
}

#[test]
fn logical_streams_have_independent_identity_and_rollover() {
    let directory = tempfile::tempdir().unwrap();
    let fast = StreamId::new(11);
    let slow = StreamId::new(22);
    let config = Config::new(directory.path())
        .with_stream_rollover(
            fast,
            RolloverPolicy::new(33).with_max_segment_age(Duration::from_secs(1)),
        )
        .with_stream_rollover(slow, RolloverPolicy::new(1024 * 1024));

    let mut log = Log::open(config.clone()).unwrap();
    let fast_old = log
        .append_to(fast, Record::new("same-id", b"fast-old".as_slice()))
        .unwrap();
    let slow_location = log
        .append_to(slow, Record::new("same-id", b"slow".as_slice()))
        .unwrap();
    let fast_active = log
        .append_to(fast, Record::new("fast-active", b"fast-new".as_slice()))
        .unwrap();
    assert_eq!(fast_old.segment_id, 0);
    assert_eq!(fast_active.segment_id, 1);
    assert_eq!(slow_location.segment_id, 0);
    assert_eq!(
        log.streams().collect::<Vec<_>>(),
        [DEFAULT_STREAM, fast, slow]
    );
    log.release_from(fast, ["same-id"]).unwrap();
    drop(log);

    let log = Log::open(config).unwrap();
    assert_eq!(
        log.recovery()
            .pending_records_for(fast)
            .iter()
            .map(|record| record.meta.record_id.as_str())
            .collect::<Vec<_>>(),
        ["fast-active"]
    );
    assert_eq!(
        log.recovery().pending_records_for(slow)[0].meta.record_id,
        "same-id"
    );
    assert_eq!(log.read(&slow_location).unwrap(), b"slow".as_slice());
}
