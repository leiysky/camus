//! Pull root statistics and asynchronously watch low-frequency health changes.

use bytes::Bytes;
use camus::{Capacity, Config, Log, ReadLimits, Record, RootState, StreamId};

#[tokio::main]
async fn main() -> camus::Result<()> {
    let directory = tempfile::tempdir().expect("create temporary root");
    let log =
        Log::open(Config::new(directory.path(), Capacity::Unbounded).with_detailed_observability())
            .await?;
    let mut health = log.watch_health();
    assert_eq!(health.current().state, RootState::Running);

    let observer = tokio::spawn(async move {
        while let Some(update) = health.changed().await {
            println!(
                "health generation={} state={} failure={:?}",
                update.generation, update.state, update.failure
            );
            if update.state == RootState::Closed {
                break;
            }
        }
    });

    let stream = log.stream(StreamId::new(7));
    let ids = stream
        .append_batch(vec![
            Record::new(Bytes::from_static(b"one")),
            Record::new(Bytes::from_static(b"two")),
        ])
        .await?;
    let pending = stream.read(ReadLimits::new(16, 1024)).await?;
    assert_eq!(pending.len(), 2);
    stream.release(ids).await?;

    let stats = log.stats();
    println!(
        "pending={} append_calls={} append_groups={} release_records={} storage_job_observations={}",
        stats.storage.pending_records,
        stats.operations.append.succeeded,
        stats.commits.append_groups,
        stats.commits.release_records,
        stats.pressure.storage_job_elapsed.observations,
    );
    assert!(stats.detailed_timings);
    assert_eq!(stats.storage.pending_records, 0);

    log.shutdown().await?;
    observer.await.expect("health observer task");
    Ok(())
}
