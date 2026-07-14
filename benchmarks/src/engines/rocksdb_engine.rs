use super::Engine;
use crate::model::{
    decode_value, encode_value, key_stream, kv_key, InputRecord, PendingRecord, Token,
};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct RocksDbEngine {
    db: Arc<DB>,
}

impl RocksDbEngine {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        let path = PathBuf::from(path);
        let db = tokio::task::spawn_blocking(move || {
            let mut options = Options::default();
            options.create_if_missing(true);
            options.set_compression_type(DBCompressionType::None);
            DB::open(&options, path).context("open RocksDB")
        })
        .await
        .context("join RocksDB open task")??;
        Ok(Self { db: Arc::new(db) })
    }
}

#[async_trait]
impl Engine for RocksDbEngine {
    async fn append_batch(&self, stream: u64, records: Vec<InputRecord>) -> Result<Vec<Token>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            let mut tokens = Vec::with_capacity(records.len());
            for record in records {
                let key = kv_key(stream, record.sequence);
                batch.put(key, encode_value(&record)?);
                tokens.push(Token::Kv(key));
            }
            let mut options = WriteOptions::default();
            options.disable_wal(false);
            options.set_sync(true);
            db.write_opt(batch, &options)
                .context("durable RocksDB write")?;
            Ok(tokens)
        })
        .await
        .context("join RocksDB write task")?
    }

    async fn read(
        &self,
        stream: u64,
        max_records: usize,
        max_payload_bytes: u64,
    ) -> Result<Vec<PendingRecord>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let start = kv_key(stream, 0);
            let mut payload_bytes = 0_u64;
            let mut records = Vec::with_capacity(max_records);
            for entry in db.iterator(IteratorMode::From(&start, Direction::Forward)) {
                let (key, value) = entry.context("iterate RocksDB records")?;
                if key_stream(&key)? != stream {
                    break;
                }
                let (metadata, payload) = decode_value(&value)?;
                let next_bytes = payload_bytes
                    .checked_add(u64::try_from(payload.len()).context("payload length overflow")?)
                    .context("read payload byte total overflow")?;
                if next_bytes > max_payload_bytes {
                    if records.is_empty() {
                        bail!("first RocksDB record exceeds the read byte limit");
                    }
                    break;
                }
                let mut token = [0_u8; 16];
                token.copy_from_slice(&key);
                records.push(PendingRecord {
                    token: Token::Kv(token),
                    metadata,
                    payload,
                });
                payload_bytes = next_bytes;
                if records.len() == max_records {
                    break;
                }
            }
            Ok(records)
        })
        .await
        .context("join RocksDB read task")?
    }

    async fn release(&self, stream: u64, tokens: Vec<Token>) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            for token in tokens {
                let Token::Kv(key) = token else {
                    bail!("received a Camus token in the RocksDB adapter");
                };
                if key_stream(&key)? != stream {
                    bail!("RocksDB release token belongs to another stream");
                }
                batch.delete(key);
            }
            let mut options = WriteOptions::default();
            options.disable_wal(false);
            options.set_sync(true);
            db.write_opt(batch, &options)
                .context("durable RocksDB delete")?;
            Ok(())
        })
        .await
        .context("join RocksDB delete task")?
    }

    async fn pending_count(&self) -> Result<u64> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut count = 0_u64;
            for entry in db.iterator(IteratorMode::Start) {
                entry.context("count RocksDB records")?;
                count = count.checked_add(1).context("pending count overflow")?;
            }
            Ok(count)
        })
        .await
        .context("join RocksDB count task")?
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
