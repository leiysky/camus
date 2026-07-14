//! Recover an unreleased record after restart and demonstrate at-least-once
//! replay when an external effect succeeds before the release is durable.

use camus::{Config, Log, Record, Result};

const RECORD_ID: &str = "order-42";

fn main() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = Config::new(directory.path());

    // A successful append is durable before it returns.
    {
        let mut log = Log::open(config.clone())?;
        log.append(
            Record::new(RECORD_ID, b"ship parcel 42".as_slice())
                .with_metadata(b"tenant=acme".as_slice()),
        )?;
    }

    // Opening the root performs recovery. No explicit recover() call is
    // needed. Pretend the println! below is a durable downstream effect.
    {
        let log = Log::open(config.clone())?;
        let pending = log.recovery().pending_records();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].meta.record_id, RECORD_ID);
        assert_eq!(pending[0].meta.metadata.as_ref(), b"tenant=acme");

        let payload = log.read(&pending[0].location)?;
        println!("first delivery: {}", String::from_utf8_lossy(&payload));

        // Dropping without release models a crash after the external effect.
        // Camus must return the record again rather than risk losing it.
    }

    {
        let mut log = Log::open(config.clone())?;
        let pending = log.recovery().pending_records();
        assert_eq!(pending.len(), 1);
        let payload = log.read(&pending[0].location)?;
        println!("replayed delivery: {}", String::from_utf8_lossy(&payload));

        // Release only after the application can durably prove the external
        // effect. The destination should use RECORD_ID as an idempotency key
        // when duplicate effects matter.
        log.release([RECORD_ID])?;
        assert!(log.recovery().pending_records_iter().next().is_none());
    }

    let log = Log::open(config)?;
    assert!(log.recovery().pending_records_iter().next().is_none());
    println!("release survived restart; no record is pending");
    Ok(())
}
