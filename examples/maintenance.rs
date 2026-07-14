//! Configure a bounded root and request an explicit maintenance pass.

use bytes::Bytes;
use camus::{Capacity, Config, FullPolicy, Log, ReadLimits, Record, Result, StreamId};

const BUFFER: StreamId = StreamId::new(3);

#[tokio::main]
async fn main() -> Result<()> {
    let directory = tempfile::tempdir().expect("create temporary root");
    let config = Config::new(
        directory.path(),
        Capacity::Bounded {
            total_bytes: 8 * 1024 * 1024,
            when_full: FullPolicy::Block,
        },
    )
    .with_max_epoch_bytes(256 * 1024)
    .with_segment_bytes(512 * 1024)
    .with_max_release_records(1024)
    .with_max_commit_bytes(512 * 1024);
    let log = Log::open(config).await?;
    let stream = log.stream(BUFFER);

    stream
        .append(Record::new(Bytes::from(vec![b'x'; 128 * 1024])))
        .await?;
    let pending = stream.read(ReadLimits::new(16, 512 * 1024)).await?;
    stream
        .release(pending.iter().map(|record| record.id).collect())
        .await?;

    // Reclamation is automatic; this call is an optional barrier for one
    // maintenance pass and may therefore report that automatic work won.
    let report = log.reclaim().await?;
    let stats = log.stats();
    println!(
        "explicit reclaim removed {} segments / {} bytes; root now uses {} bytes",
        report.segments, report.bytes, stats.storage.actual_file_bytes
    );
    log.shutdown().await
}
