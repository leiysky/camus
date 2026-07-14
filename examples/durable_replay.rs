//! Reopen an unreleased record and demonstrate at-least-once replay.

use bytes::Bytes;
use camus::{Capacity, Config, Log, ReadLimits, Record, Result, StreamId};

const OUTBOX: StreamId = StreamId::new(1);

#[tokio::main]
async fn main() -> Result<()> {
    let directory = tempfile::tempdir().expect("create temporary root");
    let config = || Config::new(directory.path(), Capacity::Unbounded);

    {
        let log = Log::open(config()).await?;
        log.stream(OUTBOX)
            .append(
                Record::new(Bytes::from_static(b"ship parcel 42"))
                    .with_metadata(Bytes::from_static(b"idempotency-key=order-42")),
            )
            .await?;
        log.shutdown().await?;
    }

    // Pretend this print is a durable downstream effect, then stop without
    // releasing. The next open must return the same storage record again.
    let id = {
        let log = Log::open(config()).await?;
        let pending = log.stream(OUTBOX).read(ReadLimits::new(1, 1024)).await?;
        println!(
            "first delivery: {}",
            String::from_utf8_lossy(&pending[0].payload)
        );
        let id = pending[0].id;
        log.shutdown().await?;
        id
    };

    {
        let log = Log::open(config()).await?;
        let outbox = log.stream(OUTBOX);
        let replay = outbox.read(ReadLimits::new(1, 1024)).await?;
        assert_eq!(replay[0].id, id);
        println!(
            "replayed delivery: {}",
            String::from_utf8_lossy(&replay[0].payload)
        );

        // Release only after the application can durably prove the external
        // effect. Retrying that effect must be safe if the process stopped
        // before this release completed.
        outbox.release(vec![id]).await?;
        log.shutdown().await?;
    }

    let log = Log::open(config()).await?;
    assert_eq!(log.stream(OUTBOX).stats().pending_records, 0);
    log.shutdown().await
}
