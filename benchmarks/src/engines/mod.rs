mod camus_engine;
mod simple_file_engine;

#[cfg(feature = "redb-engine")]
mod redb_engine;
#[cfg(feature = "rocksdb-engine")]
mod rocksdb_engine;

use crate::model::{InputRecord, PendingRecord, Token};
use anyhow::Result;
use async_trait::async_trait;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EngineKind {
    Camus,
    SimpleAppendFile,
    Rocksdb,
    Redb,
}

impl EngineKind {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Camus => "camus",
            Self::SimpleAppendFile => "simple_append_file",
            Self::Rocksdb => "rocksdb",
            Self::Redb => "redb",
        }
    }
}

#[async_trait]
pub(crate) trait Engine: Send + Sync {
    async fn append_batch(&self, stream: u64, records: Vec<InputRecord>) -> Result<Vec<Token>>;

    async fn read(
        &self,
        stream: u64,
        max_records: usize,
        max_payload_bytes: u64,
    ) -> Result<Vec<PendingRecord>>;

    async fn release(&self, stream: u64, tokens: Vec<Token>) -> Result<()>;

    async fn pending_count(&self) -> Result<u64>;

    async fn shutdown(&self) -> Result<()>;
}

pub(crate) async fn open(kind: EngineKind, path: &Path) -> Result<Arc<dyn Engine>> {
    match kind {
        EngineKind::Camus => Ok(Arc::new(camus_engine::CamusEngine::open(path).await?)),
        EngineKind::SimpleAppendFile => Ok(Arc::new(
            simple_file_engine::SimpleAppendFileEngine::open(path).await?,
        )),
        EngineKind::Rocksdb => {
            #[cfg(feature = "rocksdb-engine")]
            {
                return Ok(Arc::new(rocksdb_engine::RocksDbEngine::open(path).await?));
            }
            #[cfg(not(feature = "rocksdb-engine"))]
            anyhow::bail!("the rocksdb engine was not compiled; enable feature rocksdb-engine");
        }
        EngineKind::Redb => {
            #[cfg(feature = "redb-engine")]
            {
                return Ok(Arc::new(redb_engine::RedbEngine::open(path).await?));
            }
            #[cfg(not(feature = "redb-engine"))]
            anyhow::bail!("the redb engine was not compiled; enable feature redb-engine");
        }
    }
}
