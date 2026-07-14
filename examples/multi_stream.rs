//! Drain two logical streams that share one physical root-wide data log.

use bytes::Bytes;
use camus::{Capacity, Config, Log, ReadLimits, Record, Result, Stream, StreamId};

const UPLOADS: StreamId = StreamId::new(7);
const AUDIT: StreamId = StreamId::new(9);

async fn drain_once(stream: &Stream) -> Result<usize> {
    let snapshot = stream.read(ReadLimits::new(32, 8 * 1024 * 1024)).await?;
    let mut completed = Vec::with_capacity(snapshot.len());
    for record in &snapshot {
        println!(
            "stream {} delivered {} bytes",
            stream.id(),
            record.payload.len()
        );
        completed.push(record.id);
    }
    let count = completed.len();
    stream.release(completed).await?;
    Ok(count)
}

#[tokio::main]
async fn main() -> Result<()> {
    let directory = tempfile::tempdir().expect("create temporary root");
    let log = Log::open(Config::new(directory.path(), Capacity::Unbounded)).await?;
    let uploads = log.stream(UPLOADS);
    let audit = log.stream(AUDIT);

    uploads
        .append_batch(vec![
            Record::new(Bytes::from_static(b"image bytes"))
                .with_metadata(Bytes::from_static(b"content-type=image/png")),
            Record::new(Bytes::from_static(b"video bytes"))
                .with_metadata(Bytes::from_static(b"content-type=video/mp4")),
        ])
        .await?;
    audit
        .append(Record::new(Bytes::from_static(b"upload accepted")))
        .await?;

    assert_eq!(log.known_streams(), vec![UPLOADS, AUDIT]);
    assert_eq!(drain_once(&uploads).await?, 2);
    assert_eq!(audit.stats().pending_records, 1);
    assert_eq!(drain_once(&audit).await?, 1);
    log.shutdown().await
}
