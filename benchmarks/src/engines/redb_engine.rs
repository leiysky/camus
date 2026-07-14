use super::Engine;
use crate::model::{
    decode_value, encode_value, key_stream, kv_key, InputRecord, PendingRecord, Token,
};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const RECORDS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("records");

pub(crate) struct RedbEngine {
    db: Arc<Database>,
}

impl RedbEngine {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        let file = PathBuf::from(path).join("redb.db");
        let db = tokio::task::spawn_blocking(move || -> Result<Database> {
            let create = !file.exists();
            let db = if create {
                Database::create(&file).context("create redb")?
            } else {
                Database::open(&file).context("open redb")?
            };
            if create {
                let mut transaction = db.begin_write().context("begin redb schema transaction")?;
                transaction
                    .set_durability(Durability::Immediate)
                    .context("set redb schema durability")?;
                transaction
                    .open_table(RECORDS)
                    .context("create redb records table")?;
                transaction.commit().context("commit redb schema")?;
            }
            Ok(db)
        })
        .await
        .context("join redb open task")??;
        Ok(Self { db: Arc::new(db) })
    }
}

#[async_trait]
impl Engine for RedbEngine {
    async fn append_batch(&self, stream: u64, records: Vec<InputRecord>) -> Result<Vec<Token>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut transaction = db.begin_write().context("begin redb write")?;
            transaction
                .set_durability(Durability::Immediate)
                .context("set redb write durability")?;
            let mut tokens = Vec::with_capacity(records.len());
            {
                let mut table = transaction.open_table(RECORDS).context("open redb table")?;
                for record in records {
                    let key = kv_key(stream, record.sequence);
                    let value = encode_value(&record)?;
                    table
                        .insert(key.as_slice(), value.as_slice())
                        .context("insert redb record")?;
                    tokens.push(Token::Kv(key));
                }
            }
            transaction.commit().context("durable redb write")?;
            Ok(tokens)
        })
        .await
        .context("join redb write task")?
    }

    async fn read(
        &self,
        stream: u64,
        max_records: usize,
        max_payload_bytes: u64,
    ) -> Result<Vec<PendingRecord>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let transaction = db.begin_read().context("begin redb read")?;
            let table = transaction.open_table(RECORDS).context("open redb table")?;
            let mut payload_bytes = 0_u64;
            let mut records = Vec::with_capacity(max_records);
            for entry in table.iter().context("iterate redb records")? {
                let (key, value) = entry.context("read redb record")?;
                if key_stream(key.value())? != stream {
                    if records.is_empty() {
                        continue;
                    }
                    break;
                }
                let (metadata, payload) = decode_value(value.value())?;
                let next_bytes = payload_bytes
                    .checked_add(u64::try_from(payload.len()).context("payload length overflow")?)
                    .context("read payload byte total overflow")?;
                if next_bytes > max_payload_bytes {
                    if records.is_empty() {
                        bail!("first redb record exceeds the read byte limit");
                    }
                    break;
                }
                let mut token = [0_u8; 16];
                token.copy_from_slice(key.value());
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
        .context("join redb read task")?
    }

    async fn release(&self, stream: u64, tokens: Vec<Token>) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut transaction = db.begin_write().context("begin redb delete")?;
            transaction
                .set_durability(Durability::Immediate)
                .context("set redb delete durability")?;
            {
                let mut table = transaction.open_table(RECORDS).context("open redb table")?;
                for token in tokens {
                    let Token::Kv(key) = token else {
                        bail!("received a Camus token in the redb adapter");
                    };
                    if key_stream(&key)? != stream {
                        bail!("redb release token belongs to another stream");
                    }
                    table.remove(key.as_slice()).context("delete redb record")?;
                }
            }
            transaction.commit().context("durable redb delete")?;
            Ok(())
        })
        .await
        .context("join redb delete task")?
    }

    async fn pending_count(&self) -> Result<u64> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let transaction = db.begin_read().context("begin redb count")?;
            let table = transaction.open_table(RECORDS).context("open redb table")?;
            let mut count = 0_u64;
            for entry in table.iter().context("iterate redb records")? {
                entry.context("count redb record")?;
                count = count.checked_add(1).context("pending count overflow")?;
            }
            Ok(count)
        })
        .await
        .context("join redb count task")?
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
