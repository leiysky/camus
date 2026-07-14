//! Run application-scheduled rollover and reclamation, and distinguish
//! ordinary maintenance from explicit storage-pressure reclamation.

use camus::{Config, Log, ReclaimReport, Record, Result, StreamId, DEFAULT_STREAM};
use std::time::Duration;

fn maintenance_tick(log: &mut Log) -> Result<(Vec<StreamId>, ReclaimReport)> {
    // Call this from the application's timer. Camus has no timer thread.
    let age_rotations = log.rollover_expired()?;
    let reclaimed = log.reclaim()?;
    Ok((age_rotations, reclaimed))
}

fn main() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = Config::new(directory.path())
        .with_segment_bytes(256)
        .with_max_segment_age(Duration::from_secs(60 * 60));
    let mut log = Log::open(config)?;

    // segment_bytes is a target. The first epoch is larger than the target but
    // is never split. The next append rotates the non-empty active segment.
    let first = log.append(Record::new("blob-1", vec![b'a'; 512]))?;
    let second = log.append(Record::new("blob-2", vec![b'b'; 64]))?;
    assert_eq!(first.segment_id, 0);
    assert_eq!(second.segment_id, 1);

    log.release(["blob-1", "blob-2"])?;
    assert!(log.recovery().pending_records_iter().next().is_none());

    let (age_rotations, ordinary) = maintenance_tick(&mut log)?;
    assert!(age_rotations.is_empty());
    assert_eq!(ordinary.segments, 1); // Sealed segment 0 was removed.

    // Ordinary reclaim never rotates an active segment just to delete it.
    // Under an explicit storage-pressure policy, Camus may rotate a fully
    // released active segment and then reclaim it in manifest order.
    let pressure = log.reclaim_active_for_storage_pressure()?;
    assert_eq!(pressure.segments, 1);
    assert!(!log.rollover(DEFAULT_STREAM)?); // New active is empty.

    let stats = log.stats();
    println!(
        "epochs={}, segment_header_syncs={}, manifest_syncs={}, storage_bytes={}",
        stats.epoch_syncs,
        stats.segment_header_syncs,
        stats.manifest_syncs,
        log.storage_bytes()?
    );
    Ok(())
}
