//! Drain bounded batches from independent logical streams and release only
//! records whose external effect succeeded.

use camus::{Config, Log, Record, Result, RolloverPolicy, StreamId};
use std::io;
use std::time::Duration;

const UPLOADS: StreamId = StreamId::new(7);
const AUDIT: StreamId = StreamId::new(9);

fn drain_once(
    log: &mut Log,
    stream: StreamId,
    limit: usize,
    mut deliver: impl FnMut(&str, &[u8], &[u8]) -> io::Result<()>,
) -> Result<usize> {
    let records = log
        .recovery()
        .pending_records_for_iter(stream)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Ok(0);
    }

    let locations = records
        .iter()
        .map(|record| record.location.clone())
        .collect::<Vec<_>>();
    let payloads = log.read_many(&locations)?;

    let mut completed = Vec::new();
    for (record, payload) in records.iter().zip(&payloads) {
        match deliver(
            &record.meta.record_id,
            record.meta.metadata.as_ref(),
            payload.as_ref(),
        ) {
            Ok(()) => completed.push(record.meta.record_id.clone()),
            Err(error) => eprintln!("{} remains pending: {error}", record.meta.record_id),
        }
    }

    if !completed.is_empty() {
        log.release_from(stream, completed.iter().map(String::as_str))?;
    }
    Ok(completed.len())
}

fn main() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = Config::new(directory.path())
        .with_stream_rollover(
            UPLOADS,
            RolloverPolicy::new(64 * 1024 * 1024)
                .with_max_segment_age(Duration::from_secs(15 * 60)),
        )
        .with_stream_rollover(AUDIT, RolloverPolicy::new(8 * 1024 * 1024));
    let mut log = Log::open(config)?;

    // This call is one uploads-stream durability epoch.
    log.append_batch_to(
        UPLOADS,
        &[
            Record::new("request-42", b"image bytes".as_slice())
                .with_metadata(b"content-type=image/png".as_slice()),
            Record::new("request-43", b"video bytes".as_slice())
                .with_metadata(b"content-type=video/mp4".as_slice()),
        ],
    )?;

    // The same ID is legal in another stream. This append is a separate
    // durability epoch; Camus has no cross-stream transaction.
    log.append_to(
        AUDIT,
        Record::new("request-42", b"upload accepted".as_slice()),
    )?;

    assert_eq!(
        log.streams().map(StreamId::get).collect::<Vec<_>>(),
        [0, 7, 9]
    );

    // One effect fails transiently. The helper releases only request-42 and
    // leaves request-43 recoverable for the next attempt.
    let completed = drain_once(&mut log, UPLOADS, 32, |id, metadata, payload| {
        if id == "request-43" {
            return Err(io::Error::other("destination temporarily unavailable"));
        }
        println!(
            "delivered {id}: metadata={}, payload={}",
            String::from_utf8_lossy(metadata),
            String::from_utf8_lossy(payload)
        );
        Ok(())
    })?;
    assert_eq!(completed, 1);
    assert_eq!(
        log.recovery().pending_records_for(UPLOADS)[0]
            .meta
            .record_id,
        "request-43"
    );

    let completed = drain_once(&mut log, UPLOADS, 32, |id, _, payload| {
        println!("retry delivered {id}: {}", String::from_utf8_lossy(payload));
        Ok(())
    })?;
    assert_eq!(completed, 1);
    assert!(log
        .recovery()
        .pending_records_for_iter(UPLOADS)
        .next()
        .is_none());

    // Upload releases never affect the same ID in the audit stream.
    let audit = log.recovery().pending_records_for(AUDIT);
    assert_eq!(audit.len(), 1);
    assert_eq!(log.read(&audit[0].location)?.as_ref(), b"upload accepted");
    log.release_from(AUDIT, ["request-42"])?;
    Ok(())
}
