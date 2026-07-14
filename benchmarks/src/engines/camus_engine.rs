use super::Engine;
use crate::model::{InputRecord, PendingRecord, Token};
use anyhow::{bail, Result};
use async_trait::async_trait;
use camus::{Capacity, Config, Log, ReadLimits, Record, RecordId, StreamId};
use std::path::Path;

pub(crate) struct CamusEngine {
    log: Log,
}

impl CamusEngine {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        let log = Log::open(Config::new(path, Capacity::Unbounded)).await?;
        Ok(Self { log })
    }
}

#[async_trait]
impl Engine for CamusEngine {
    async fn append_batch(&self, stream: u64, records: Vec<InputRecord>) -> Result<Vec<Token>> {
        let records = records
            .into_iter()
            .map(|record| Record {
                metadata: record.metadata,
                payload: record.payload,
            })
            .collect();
        Ok(self
            .log
            .stream(StreamId::new(stream))
            .append_batch(records)
            .await?
            .into_iter()
            .map(Token::Camus)
            .collect())
    }

    async fn read(
        &self,
        stream: u64,
        max_records: usize,
        max_payload_bytes: u64,
    ) -> Result<Vec<PendingRecord>> {
        Ok(self
            .log
            .stream(StreamId::new(stream))
            .read(ReadLimits::new(max_records, max_payload_bytes))
            .await?
            .into_iter()
            .map(|record| PendingRecord {
                token: Token::Camus(record.id),
                metadata: record.metadata,
                payload: record.payload,
            })
            .collect())
    }

    async fn release(&self, stream: u64, tokens: Vec<Token>) -> Result<()> {
        let ids = tokens
            .into_iter()
            .map(|token| match token {
                Token::Camus(id) => Ok(id),
                Token::Kv(_) => bail!("received a KV token in the Camus adapter"),
            })
            .collect::<Result<Vec<RecordId>>>()?;
        self.log.stream(StreamId::new(stream)).release(ids).await?;
        Ok(())
    }

    async fn pending_count(&self) -> Result<u64> {
        Ok(self.log.stats().pending_records)
    }

    async fn shutdown(&self) -> Result<()> {
        self.log.shutdown().await?;
        Ok(())
    }
}
