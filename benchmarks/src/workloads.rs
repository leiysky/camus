use crate::engines::{open, Engine, EngineKind};
use crate::metrics::{duration_ns, Measurement};
use crate::model::{records, InputRecord, Token};
use anyhow::{bail, ensure, Context, Result};
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::{Builder, TempDir};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkloadConfig {
    pub(crate) metadata_bytes: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) concurrency: usize,
    pub(crate) batch_records: usize,
    pub(crate) read_batch_records: usize,
    pub(crate) release_batch_records: usize,
    pub(crate) setup_batch_records: usize,
}

pub(crate) async fn sequential_append(
    kind: EngineKind,
    data_directory: &Path,
    count: usize,
    config: WorkloadConfig,
) -> Result<Measurement> {
    let directory = case_directory(data_directory, kind, "append-sequential")?;
    let engine = open(kind, directory.path()).await?;
    let prepared = records(count, 0, 1, config.metadata_bytes, config.payload_bytes);
    let mut latencies = Vec::with_capacity(count);
    let wall = Instant::now();
    for record in prepared {
        let started = Instant::now();
        let tokens = engine.append_batch(0, vec![record]).await?;
        ensure!(
            tokens.len() == 1,
            "append returned an unexpected token count"
        );
        latencies.push(duration_ns(started.elapsed()));
    }
    let elapsed = wall.elapsed();
    validate_pending(&engine, count).await?;
    finish(directory, engine, count, count, elapsed, latencies).await
}

pub(crate) async fn concurrent_append(
    kind: EngineKind,
    data_directory: &Path,
    count: usize,
    logical_streams: usize,
    config: WorkloadConfig,
) -> Result<Measurement> {
    let directory = case_directory(data_directory, kind, "append-concurrent")?;
    let engine = open(kind, directory.path()).await?;
    let workers = config.concurrency.min(count);
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut per_worker = vec![Vec::<(u64, InputRecord)>::new(); workers];
    for index in 0..count {
        let worker = index % workers;
        let stream = u64::try_from(worker % logical_streams).context("stream ID overflow")?;
        let mut record = records(
            1,
            stream,
            u64::try_from(index).context("record sequence overflow")? + 1,
            config.metadata_bytes,
            config.payload_bytes,
        );
        per_worker[worker].push((stream, record.remove(0)));
    }

    let mut tasks = JoinSet::new();
    for prepared in per_worker {
        let engine = engine.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let mut latencies = Vec::with_capacity(prepared.len());
            for (stream, record) in prepared {
                let started = Instant::now();
                let tokens = engine.append_batch(stream, vec![record]).await?;
                ensure!(
                    tokens.len() == 1,
                    "append returned an unexpected token count"
                );
                latencies.push(duration_ns(started.elapsed()));
            }
            Result::<Vec<u64>>::Ok(latencies)
        });
    }

    let wall = Instant::now();
    barrier.wait().await;
    let mut latencies = Vec::with_capacity(count);
    while let Some(result) = tasks.join_next().await {
        latencies.extend(result.context("join concurrent append worker")??);
    }
    let elapsed = wall.elapsed();
    ensure!(
        latencies.len() == count,
        "concurrent append lost measurements"
    );
    validate_pending(&engine, count).await?;
    finish(directory, engine, count, count, elapsed, latencies).await
}

pub(crate) async fn batch_append(
    kind: EngineKind,
    data_directory: &Path,
    count: usize,
    config: WorkloadConfig,
) -> Result<Measurement> {
    let directory = case_directory(data_directory, kind, "append-batch")?;
    let engine = open(kind, directory.path()).await?;
    let prepared = records(count, 0, 1, config.metadata_bytes, config.payload_bytes);
    let operations = count.div_ceil(config.batch_records);
    let mut latencies = Vec::with_capacity(operations);
    let wall = Instant::now();
    for chunk in prepared.chunks(config.batch_records) {
        let batch = chunk.to_vec();
        let expected = batch.len();
        let started = Instant::now();
        let tokens = engine.append_batch(0, batch).await?;
        ensure!(
            tokens.len() == expected,
            "batch append returned an unexpected token count"
        );
        latencies.push(duration_ns(started.elapsed()));
    }
    let elapsed = wall.elapsed();
    validate_pending(&engine, count).await?;
    finish(directory, engine, count, operations, elapsed, latencies).await
}

pub(crate) async fn read_snapshot(
    kind: EngineKind,
    data_directory: &Path,
    count: usize,
    config: WorkloadConfig,
) -> Result<Measurement> {
    let directory = case_directory(data_directory, kind, "read-snapshot")?;
    let engine = open(kind, directory.path()).await?;
    drop(prepare(&engine, count, config).await?);
    validate_pending(&engine, count).await?;

    let max_payload_bytes = u64::try_from(config.payload_bytes)
        .context("payload byte size overflow")?
        .checked_mul(u64::try_from(count).context("read snapshot count overflow")?)
        .context("read snapshot payload bound overflow")?;
    let started = Instant::now();
    let pending = engine.read(0, count, max_payload_bytes).await?;
    let elapsed = started.elapsed();
    ensure!(
        pending.len() == count,
        "verified read returned an incomplete snapshot"
    );
    let observed_bytes = pending.iter().try_fold(0_usize, |total, record| {
        total
            .checked_add(record.metadata.len())
            .and_then(|bytes| bytes.checked_add(record.payload.len()))
            .context("observed byte count overflow")
    })?;
    black_box(observed_bytes);
    finish(
        directory,
        engine,
        count,
        1,
        elapsed,
        vec![duration_ns(elapsed)],
    )
    .await
}

pub(crate) async fn release_batch(
    kind: EngineKind,
    data_directory: &Path,
    count: usize,
    config: WorkloadConfig,
) -> Result<Measurement> {
    let directory = case_directory(data_directory, kind, "release-batch")?;
    let engine = open(kind, directory.path()).await?;
    let tokens = prepare(&engine, count, config).await?;
    validate_pending(&engine, count).await?;

    let operations = count.div_ceil(config.release_batch_records);
    let mut latencies = Vec::with_capacity(operations);
    let wall = Instant::now();
    for chunk in tokens.chunks(config.release_batch_records) {
        let started = Instant::now();
        engine.release(0, chunk.to_vec()).await?;
        latencies.push(duration_ns(started.elapsed()));
    }
    let elapsed = wall.elapsed();
    ensure!(
        engine.pending_count().await? == 0,
        "release benchmark left pending records"
    );
    finish(directory, engine, count, operations, elapsed, latencies).await
}

pub(crate) async fn drain(
    kind: EngineKind,
    data_directory: &Path,
    count: usize,
    config: WorkloadConfig,
) -> Result<Measurement> {
    let directory = case_directory(data_directory, kind, "drain")?;
    let engine = open(kind, directory.path()).await?;
    drop(prepare(&engine, count, config).await?);
    validate_pending(&engine, count).await?;

    let max_payload_bytes = u64::try_from(config.payload_bytes)
        .context("payload byte size overflow")?
        .checked_mul(u64::try_from(config.read_batch_records).context("read batch overflow")?)
        .context("read payload bound overflow")?;
    let mut processed = 0_usize;
    let mut observed_bytes = 0_usize;
    let mut latencies = Vec::with_capacity(count.div_ceil(config.read_batch_records));
    let wall = Instant::now();
    while processed < count {
        let started = Instant::now();
        let pending = engine
            .read(0, config.read_batch_records, max_payload_bytes)
            .await?;
        if pending.is_empty() {
            bail!("engine returned an empty pending batch before drain completed");
        }
        observed_bytes = pending.iter().try_fold(observed_bytes, |total, record| {
            total
                .checked_add(record.metadata.len())
                .and_then(|bytes| bytes.checked_add(record.payload.len()))
                .context("observed byte count overflow")
        })?;
        let batch_len = pending.len();
        let tokens = pending.into_iter().map(|record| record.token).collect();
        engine.release(0, tokens).await?;
        processed = processed
            .checked_add(batch_len)
            .context("drain record count overflow")?;
        latencies.push(duration_ns(started.elapsed()));
    }
    let elapsed = wall.elapsed();
    black_box(observed_bytes);
    ensure!(
        engine.pending_count().await? == 0,
        "drain left pending records"
    );
    finish(
        directory,
        engine,
        count,
        latencies.len(),
        elapsed,
        latencies,
    )
    .await
}

pub(crate) async fn warm_restart(
    kind: EngineKind,
    data_directory: &Path,
    count: usize,
    config: WorkloadConfig,
) -> Result<Measurement> {
    let directory = case_directory(data_directory, kind, "warm-restart")?;
    let engine = open(kind, directory.path()).await?;
    drop(prepare(&engine, count, config).await?);
    validate_pending(&engine, count).await?;
    engine.shutdown().await?;
    drop(engine);

    let max_payload_bytes = u64::try_from(config.payload_bytes)
        .context("payload byte size overflow")?
        .checked_mul(u64::try_from(config.read_batch_records).context("read batch overflow")?)
        .context("read payload bound overflow")?;
    let started = Instant::now();
    let reopened = open(kind, directory.path()).await?;
    let first_batch = reopened
        .read(0, config.read_batch_records, max_payload_bytes)
        .await?;
    let elapsed = started.elapsed();
    ensure!(!first_batch.is_empty(), "warm restart found no first batch");
    validate_pending(&reopened, count).await?;
    finish(
        directory,
        reopened,
        count,
        1,
        elapsed,
        vec![duration_ns(elapsed)],
    )
    .await
}

async fn prepare(
    engine: &Arc<dyn Engine>,
    count: usize,
    config: WorkloadConfig,
) -> Result<Vec<Token>> {
    let prepared = records(count, 0, 1, config.metadata_bytes, config.payload_bytes);
    let mut output = Vec::with_capacity(count);
    for chunk in prepared.chunks(config.setup_batch_records) {
        let expected = chunk.len();
        let tokens = engine.append_batch(0, chunk.to_vec()).await?;
        ensure!(
            tokens.len() == expected,
            "setup append returned an unexpected token count"
        );
        output.extend(tokens);
    }
    Ok(output)
}

async fn validate_pending(engine: &Arc<dyn Engine>, expected: usize) -> Result<()> {
    let expected = u64::try_from(expected).context("expected pending count overflow")?;
    ensure!(
        engine.pending_count().await? == expected,
        "engine pending count does not match the workload"
    );
    Ok(())
}

async fn finish(
    directory: TempDir,
    engine: Arc<dyn Engine>,
    records: usize,
    operations: usize,
    elapsed: std::time::Duration,
    latencies_ns: Vec<u64>,
) -> Result<Measurement> {
    engine.shutdown().await?;
    drop(engine);
    let storage_bytes = filesystem_bytes(directory.path())?;
    Ok(Measurement {
        records: u64::try_from(records).context("measurement record count overflow")?,
        operations: u64::try_from(operations).context("measurement operation count overflow")?,
        elapsed,
        latencies_ns,
        storage_bytes,
    })
}

fn case_directory(data_directory: &Path, kind: EngineKind, workload: &str) -> Result<TempDir> {
    Builder::new()
        .prefix(&format!("{}-{workload}-", kind.name()))
        .tempdir_in(data_directory)
        .with_context(|| {
            format!(
                "create benchmark directory under {}",
                data_directory.display()
            )
        })
}

fn filesystem_bytes(path: &Path) -> Result<u64> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path)
        .with_context(|| format!("read benchmark directory {}", path.display()))?
    {
        let entry = entry.context("read benchmark directory entry")?;
        let metadata = entry.metadata().context("read benchmark file metadata")?;
        if metadata.is_dir() {
            total = total
                .checked_add(filesystem_bytes(&entry.path())?)
                .context("benchmark storage byte total overflow")?;
        } else if metadata.is_file() {
            total = total
                .checked_add(metadata.len())
                .context("benchmark storage byte total overflow")?;
        }
    }
    Ok(total)
}
