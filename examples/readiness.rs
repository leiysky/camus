//! Use `Stream::read` itself as the readiness Future—no callback or separate
//! subscription API is required.

use bytes::Bytes;
use camus::{Capacity, Config, Log, ReadLimits, Record, Result, StreamId};

const UPLOADS: StreamId = StreamId::new(7);

#[tokio::main]
async fn main() -> Result<()> {
    let directory = tempfile::tempdir().expect("create temporary root");
    let log = Log::open(Config::new(directory.path(), Capacity::Unbounded)).await?;
    let uploads = log.stream(UPLOADS);
    let waiter = uploads.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();

    let consumer = tokio::spawn(async move {
        started_tx.send(()).expect("producer dropped");
        waiter.read(ReadLimits::new(32, 1024 * 1024)).await
    });
    started_rx.await.expect("consumer dropped");

    let id = uploads
        .append(Record::new(Bytes::from_static(b"opaque upload")))
        .await?;
    let snapshot = consumer.await.expect("consumer task panicked")?;
    assert_eq!(snapshot[0].id, id);

    // Reading observes shared pending state; it does not claim the record.
    // Release only after the downstream effect is durable.
    uploads.release(vec![id]).await?;
    log.shutdown().await
}
