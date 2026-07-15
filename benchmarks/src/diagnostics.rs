use crate::metrics::{duration_ns, environment, Environment, LatencySummary};
use anyhow::{ensure, Context, Result};
use camus::{Capacity, Config, DurationStats, Log, ReadLimits, Record, RecordId, StreamId};
use clap::Args;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tempfile::Builder;
use tokio::task::JoinSet;

const REPORT_SCHEMA_VERSION: u32 = 1;
const MANIFEST_LOG_HEADER_BYTES: u64 = 40;
const MANIFEST_COMPACTION_TRIGGER_BYTES: u64 = 8 * 1024 * 1024;
const RELEASE_FRAME_FIXED_BYTES: u64 = 72;
const RELEASE_RANGE_BYTES: u64 = 16;
const EMPTY_EPOCH_FIXED_BYTES: u64 = 88;
const RECORD_DESCRIPTOR_BYTES: u64 = 40;
const ANCHOR_STREAM: StreamId = StreamId::new(1);
const SPARSE_STREAM: StreamId = StreamId::new(2);
const FOREGROUND_STREAM_BASE: u64 = 10_000;

/// Arguments for the Camus-only manifest compaction diagnostic.
#[derive(Debug, Args)]
pub(crate) struct ManifestCompactionArgs {
    /// Records released in alternating sparse sets to grow the manifest.
    #[arg(long, default_value_t = 524_288)]
    sparse_records: usize,

    /// Records in each setup append epoch.
    #[arg(long, default_value_t = 4_096)]
    append_batch_records: usize,

    /// IDs in each sparse release frame.
    #[arg(long, default_value_t = 65_536)]
    release_batch_records: usize,

    /// Concurrent foreground stream workers.
    #[arg(long, default_value_t = 8)]
    foreground_workers: usize,

    /// Append/read/release cycles per foreground worker.
    #[arg(long, default_value_t = 32)]
    foreground_operations: usize,

    /// Payload bytes in each foreground record.
    #[arg(long, default_value_t = 64)]
    foreground_payload_bytes: usize,

    /// Parent directory on the device under test.
    #[arg(long, default_value = "target/benchmark-data")]
    data_directory: PathBuf,

    /// JSON report path. Defaults under target/benchmark-results.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Preserve the completed temporary root for offline inspection.
    #[arg(long)]
    keep_data: bool,

    /// Free-form environment note stored in the report.
    #[arg(long)]
    note: Option<String>,
}

#[derive(Debug, Serialize)]
struct ManifestCompactionReport {
    schema_version: u32,
    environment: Environment,
    configuration: ManifestCompactionConfiguration,
    setup: SetupReport,
    compaction: CompactionReport,
    foreground: ForegroundReport,
    final_state: FinalStateReport,
}

#[derive(Debug, Serialize)]
struct ManifestCompactionConfiguration {
    sparse_records: usize,
    append_batch_records: usize,
    release_batch_records: usize,
    foreground_workers: usize,
    foreground_operations_per_worker: usize,
    foreground_payload_bytes: usize,
    manifest_compaction_trigger_bytes: u64,
}

#[derive(Debug, Serialize)]
struct SetupReport {
    append_epochs: usize,
    sparse_release_frames: usize,
    trigger_release_frame: usize,
    manifest_bytes_before_trigger: u64,
    trigger_frame_encoded_bytes: u64,
}

#[derive(Debug, Serialize)]
struct CompactionReport {
    count_before: u64,
    count_after_trigger: u64,
    trigger_release_latency_ns: u64,
    trigger_completed_before_foreground: bool,
    storage_job_observed_in_flight: bool,
    release_storage_jobs_before_trigger_reply: DurationDelta,
}

#[derive(Debug, Serialize)]
struct ForegroundReport {
    operations: usize,
    append_latency_ns: LatencySummary,
    read_latency_ns: LatencySummary,
    release_latency_ns: LatencySummary,
}

#[derive(Debug, Serialize)]
struct FinalStateReport {
    pending_records: u64,
    live_segments: u64,
    manifest_compactions: u64,
    reclaimed_segments: u64,
}

#[derive(Debug, Serialize)]
struct DurationDelta {
    observations: u64,
    total_ns: u64,
    new_session_max_ns: Option<u64>,
}

#[derive(Debug)]
struct ReleasePlan {
    groups: Vec<Vec<RecordId>>,
    trigger_group: usize,
    manifest_bytes_before_trigger: u64,
    trigger_frame_bytes: u64,
}

#[derive(Debug)]
struct ForegroundMeasurements {
    append_ns: Vec<u64>,
    read_ns: Vec<u64>,
    release_ns: Vec<u64>,
}

pub(crate) async fn manifest_compaction(arguments: ManifestCompactionArgs) -> Result<()> {
    validate_arguments(&arguments)?;
    std::fs::create_dir_all(&arguments.data_directory).with_context(|| {
        format!(
            "create benchmark data directory {}",
            arguments.data_directory.display()
        )
    })?;
    let data_directory = arguments
        .data_directory
        .canonicalize()
        .context("canonicalize benchmark data directory")?;
    let directory = Builder::new()
        .prefix("camus-manifest-compaction-")
        .tempdir_in(&data_directory)
        .with_context(|| {
            format!(
                "create diagnostic directory under {}",
                data_directory.display()
            )
        })?;
    let root = directory.path().to_path_buf();

    let max_release_frame_bytes = release_frame_bytes(arguments.release_batch_records)?;
    let log = Log::open(
        Config::new(&root, Capacity::Unbounded)
            .with_max_release_records(arguments.release_batch_records)
            .with_max_commit_bytes(max_release_frame_bytes.max(camus::DEFAULT_MAX_COMMIT_BYTES))
            .with_detailed_observability(),
    )
    .await
    .context("open compaction diagnostic root")?;

    let anchor = log
        .stream(ANCHOR_STREAM)
        .append(Record::new(vec![0_u8]))
        .await
        .context("append reclamation anchor")?;
    let sparse = log.stream(SPARSE_STREAM);
    let mut sparse_ids = Vec::with_capacity(arguments.sparse_records);
    for batch_start in (0..arguments.sparse_records).step_by(arguments.append_batch_records) {
        let batch_len = arguments
            .append_batch_records
            .min(arguments.sparse_records - batch_start);
        let records = (0..batch_len)
            .map(|_| Record::new(bytes::Bytes::new()))
            .collect::<Vec<_>>();
        sparse_ids.extend(
            sparse
                .append_batch(records)
                .await
                .context("append sparse setup records")?,
        );
    }
    ensure!(
        sparse_ids.len() == arguments.sparse_records,
        "setup append returned the wrong number of IDs"
    );

    let plan = release_plan(sparse_ids, arguments.release_batch_records)?;
    eprintln!(
        "prepared {} records; compaction is expected in sparse release frame {}/{}",
        arguments.sparse_records,
        plan.trigger_group + 1,
        plan.groups.len()
    );
    let total_release_frames = plan.groups.len();
    let mut groups = plan.groups.into_iter();
    for group in groups.by_ref().take(plan.trigger_group) {
        sparse
            .release(group)
            .await
            .context("write sparse setup release")?;
    }

    let trigger_ids = groups
        .next()
        .context("release plan did not contain its trigger group")?;
    let before_trigger = log.stats();
    let trigger_started = Instant::now();
    let mut trigger = Box::pin(sparse.release(trigger_ids));
    let mut trigger_completed_before_foreground = false;
    let mut immediate_result = None;
    let mut storage_job_observed_in_flight = false;
    for _ in 0..1_024 {
        if log.stats().pressure.active_storage_jobs != 0 {
            storage_job_observed_in_flight = true;
            break;
        }
        tokio::select! {
            biased;
            result = &mut trigger => {
                trigger_completed_before_foreground = true;
                immediate_result = Some(result);
                break;
            }
            () = tokio::task::yield_now() => {}
        }
    }

    let (trigger_latency, after_trigger, foreground) = if let Some(result) = immediate_result {
        result.context("write compaction-triggering release")?;
        let trigger_latency = trigger_started.elapsed();
        let after_trigger = log.stats();
        let foreground = run_foreground(&log, &arguments).await?;
        (trigger_latency, after_trigger, foreground)
    } else {
        let trigger_observation = async {
            trigger
                .await
                .context("write compaction-triggering release")?;
            Result::<_>::Ok((trigger_started.elapsed(), log.stats()))
        };
        let (trigger_result, foreground_result) =
            tokio::join!(trigger_observation, run_foreground(&log, &arguments));
        let (trigger_latency, observed_stats) = trigger_result?;
        ensure!(
            observed_stats.maintenance.manifest_compactions
                > before_trigger.maintenance.manifest_compactions,
            "the expected release completed without manifest compaction"
        );
        let foreground = foreground_result?;
        (trigger_latency, observed_stats, foreground)
    };
    ensure!(
        after_trigger.maintenance.manifest_compactions
            > before_trigger.maintenance.manifest_compactions,
        "the expected release completed without manifest compaction"
    );

    for group in groups {
        sparse
            .release(group)
            .await
            .context("write trailing sparse release")?;
    }
    log.stream(ANCHOR_STREAM)
        .release(vec![anchor])
        .await
        .context("release reclamation anchor")?;
    log.reclaim().await.context("reclaim diagnostic root")?;
    let final_stats = log.stats();
    ensure!(
        final_stats.storage.pending_records == 0,
        "diagnostic cleanup left pending records"
    );

    let report = ManifestCompactionReport {
        schema_version: REPORT_SCHEMA_VERSION,
        environment: environment(&data_directory, arguments.note),
        configuration: ManifestCompactionConfiguration {
            sparse_records: arguments.sparse_records,
            append_batch_records: arguments.append_batch_records,
            release_batch_records: arguments.release_batch_records,
            foreground_workers: arguments.foreground_workers,
            foreground_operations_per_worker: arguments.foreground_operations,
            foreground_payload_bytes: arguments.foreground_payload_bytes,
            manifest_compaction_trigger_bytes: MANIFEST_COMPACTION_TRIGGER_BYTES,
        },
        setup: SetupReport {
            append_epochs: arguments
                .sparse_records
                .div_ceil(arguments.append_batch_records)
                + 1,
            sparse_release_frames: total_release_frames,
            trigger_release_frame: plan.trigger_group + 1,
            manifest_bytes_before_trigger: plan.manifest_bytes_before_trigger,
            trigger_frame_encoded_bytes: plan.trigger_frame_bytes,
        },
        compaction: CompactionReport {
            count_before: before_trigger.maintenance.manifest_compactions,
            count_after_trigger: after_trigger.maintenance.manifest_compactions,
            trigger_release_latency_ns: duration_ns(trigger_latency),
            trigger_completed_before_foreground,
            storage_job_observed_in_flight,
            release_storage_jobs_before_trigger_reply: duration_delta(
                before_trigger.pressure.storage_jobs.release,
                after_trigger.pressure.storage_jobs.release,
            ),
        },
        foreground: ForegroundReport {
            operations: foreground.append_ns.len(),
            append_latency_ns: summarize(&foreground.append_ns)?,
            read_latency_ns: summarize(&foreground.read_ns)?,
            release_latency_ns: summarize(&foreground.release_ns)?,
        },
        final_state: FinalStateReport {
            pending_records: final_stats.storage.pending_records,
            live_segments: final_stats.storage.live_segments,
            manifest_compactions: final_stats.maintenance.manifest_compactions,
            reclaimed_segments: final_stats.maintenance.reclaimed_segments,
        },
    };

    log.shutdown().await.context("shutdown diagnostic root")?;
    let output = arguments.output.unwrap_or_else(default_output_path);
    write_report(&output, &report)?;
    println!(
        "compaction release: {:.3} ms; foreground append p99: {:.3} ms",
        report.compaction.trigger_release_latency_ns as f64 / 1_000_000.0,
        report.foreground.append_latency_ns.p99 as f64 / 1_000_000.0
    );
    println!("report: {}", output.display());
    if arguments.keep_data {
        let kept = directory.keep();
        println!("data: {}", kept.display());
    }
    Ok(())
}

fn validate_arguments(arguments: &ManifestCompactionArgs) -> Result<()> {
    ensure!(
        arguments.sparse_records > 0,
        "--sparse-records must be positive"
    );
    ensure!(
        arguments.append_batch_records > 0,
        "--append-batch-records must be positive"
    );
    ensure!(
        arguments.release_batch_records > 0,
        "--release-batch-records must be positive"
    );
    ensure!(
        arguments.release_batch_records <= camus::DEFAULT_MAX_RELEASE_RECORDS,
        "--release-batch-records cannot exceed Camus's current public maximum"
    );
    ensure!(
        arguments.foreground_workers > 0,
        "--foreground-workers must be positive"
    );
    ensure!(
        arguments.foreground_operations > 0,
        "--foreground-operations must be positive"
    );
    ensure!(
        arguments.foreground_payload_bytes > 0,
        "--foreground-payload-bytes must be positive"
    );
    ensure!(
        arguments.sparse_records <= 1_000_000,
        "--sparse-records is capped at 1,000,000 to keep setup in one default segment"
    );
    let setup_epoch_bytes = u64::try_from(arguments.append_batch_records)
        .context("append batch size overflow")?
        .checked_mul(RECORD_DESCRIPTOR_BYTES)
        .and_then(|bytes| bytes.checked_add(EMPTY_EPOCH_FIXED_BYTES))
        .context("append epoch size overflow")?;
    ensure!(
        setup_epoch_bytes <= camus::DEFAULT_MAX_EPOCH_BYTES,
        "--append-batch-records exceeds Camus's default 8 MiB epoch bound"
    );
    let foreground_epoch_bytes = u64::try_from(arguments.foreground_payload_bytes)
        .context("foreground payload size overflow")?
        .checked_add(EMPTY_EPOCH_FIXED_BYTES + RECORD_DESCRIPTOR_BYTES)
        .context("foreground epoch size overflow")?;
    ensure!(
        foreground_epoch_bytes <= camus::DEFAULT_MAX_EPOCH_BYTES,
        "--foreground-payload-bytes exceeds Camus's default 8 MiB epoch bound"
    );
    Ok(())
}

fn release_plan(ids: Vec<RecordId>, batch_records: usize) -> Result<ReleasePlan> {
    ensure!(batch_records > 0, "release batch size must be positive");
    let mut even = Vec::with_capacity(ids.len().div_ceil(2));
    let mut odd = Vec::with_capacity(ids.len() / 2);
    for (index, id) in ids.into_iter().enumerate() {
        if index % 2 == 0 {
            even.push(id);
        } else {
            odd.push(id);
        }
    }
    let ordered = [even, odd];
    let mut groups = Vec::new();
    for parity in ordered {
        groups.extend(parity.chunks(batch_records).map(<[_]>::to_vec));
    }

    let mut manifest_bytes = MANIFEST_LOG_HEADER_BYTES;
    for (index, group) in groups.iter().enumerate() {
        let frame_bytes = release_frame_bytes(group.len())?;
        let next_bytes = manifest_bytes
            .checked_add(frame_bytes)
            .context("manifest size calculation overflow")?;
        if next_bytes >= MANIFEST_COMPACTION_TRIGGER_BYTES {
            return Ok(ReleasePlan {
                groups,
                trigger_group: index,
                manifest_bytes_before_trigger: manifest_bytes,
                trigger_frame_bytes: frame_bytes,
            });
        }
        manifest_bytes = next_bytes;
    }
    anyhow::bail!(
        "{} sparse records in batches of {} do not reach the current {}-byte manifest compaction threshold",
        groups.iter().map(Vec::len).sum::<usize>(),
        batch_records,
        MANIFEST_COMPACTION_TRIGGER_BYTES
    )
}

fn release_frame_bytes(ranges: usize) -> Result<u64> {
    u64::try_from(ranges)
        .context("release range count overflow")?
        .checked_mul(RELEASE_RANGE_BYTES)
        .and_then(|bytes| bytes.checked_add(RELEASE_FRAME_FIXED_BYTES))
        .context("release frame size overflow")
}

async fn run_foreground(
    log: &Log,
    arguments: &ManifestCompactionArgs,
) -> Result<ForegroundMeasurements> {
    let mut tasks = JoinSet::new();
    for worker in 0..arguments.foreground_workers {
        let worker = u64::try_from(worker).context("foreground worker ID overflow")?;
        let stream = log.stream(StreamId::new(
            FOREGROUND_STREAM_BASE
                .checked_add(worker)
                .context("foreground stream ID overflow")?,
        ));
        let operations = arguments.foreground_operations;
        let payload_bytes = arguments.foreground_payload_bytes;
        tasks.spawn(async move {
            let mut measurements = ForegroundMeasurements {
                append_ns: Vec::with_capacity(operations),
                read_ns: Vec::with_capacity(operations),
                release_ns: Vec::with_capacity(operations),
            };
            for operation in 0..operations {
                let marker = u8::try_from((worker as usize + operation) % 251)
                    .expect("value is constrained below 251");
                let payload = vec![marker; payload_bytes];

                let started = Instant::now();
                let id = stream
                    .append(Record::new(payload.clone()))
                    .await
                    .context("append foreground record")?;
                measurements.append_ns.push(duration_ns(started.elapsed()));

                let started = Instant::now();
                let snapshot = stream
                    .read(ReadLimits::new(1, u64::try_from(payload_bytes)?))
                    .await
                    .context("read foreground record")?;
                measurements.read_ns.push(duration_ns(started.elapsed()));
                ensure!(
                    snapshot.len() == 1,
                    "foreground read returned multiple records"
                );
                ensure!(
                    snapshot[0].id == id,
                    "foreground read returned the wrong ID"
                );
                ensure!(
                    snapshot[0].payload.as_ref() == payload,
                    "foreground read returned the wrong payload"
                );

                let started = Instant::now();
                stream
                    .release(vec![id])
                    .await
                    .context("release foreground record")?;
                measurements.release_ns.push(duration_ns(started.elapsed()));
            }
            Result::<_>::Ok(measurements)
        });
    }

    let expected = arguments
        .foreground_workers
        .checked_mul(arguments.foreground_operations)
        .context("foreground operation count overflow")?;
    let mut combined = ForegroundMeasurements {
        append_ns: Vec::with_capacity(expected),
        read_ns: Vec::with_capacity(expected),
        release_ns: Vec::with_capacity(expected),
    };
    while let Some(result) = tasks.join_next().await {
        let measurement = result.context("join foreground worker")??;
        combined.append_ns.extend(measurement.append_ns);
        combined.read_ns.extend(measurement.read_ns);
        combined.release_ns.extend(measurement.release_ns);
    }
    ensure!(
        combined.append_ns.len() == expected
            && combined.read_ns.len() == expected
            && combined.release_ns.len() == expected,
        "foreground diagnostic lost measurements"
    );
    Ok(combined)
}

fn summarize(values: &[u64]) -> Result<LatencySummary> {
    ensure!(!values.is_empty(), "cannot summarize an empty measurement");
    let mut histogram = Histogram::<u64>::new(3).context("create latency histogram")?;
    for value in values {
        histogram
            .record((*value).max(1))
            .context("record latency")?;
    }
    Ok(LatencySummary {
        p50: histogram.value_at_quantile(0.50),
        p95: histogram.value_at_quantile(0.95),
        p99: histogram.value_at_quantile(0.99),
        max: histogram.max(),
    })
}

fn duration_delta(before: DurationStats, after: DurationStats) -> DurationDelta {
    DurationDelta {
        observations: after.observations.saturating_sub(before.observations),
        total_ns: duration_ns(after.total.saturating_sub(before.total)),
        new_session_max_ns: (after.max > before.max).then(|| duration_ns(after.max)),
    }
}

fn write_report(path: &Path, report: &ManifestCompactionReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create report directory {}", parent.display()))?;
    }
    let writer = BufWriter::new(
        File::create(path).with_context(|| format!("create report {}", path.display()))?,
    );
    serde_json::to_writer_pretty(writer, report).context("serialize compaction report")
}

fn default_output_path() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    PathBuf::from(format!(
        "target/benchmark-results/manifest-compaction-{timestamp}.json"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(count: usize) -> Vec<RecordId> {
        (0..count)
            .map(|sequence| {
                let mut bytes = [0_u8; RecordId::BYTE_LEN];
                bytes[24..].copy_from_slice(&(sequence as u64).to_le_bytes());
                RecordId::from_bytes(bytes)
            })
            .collect()
    }

    #[test]
    fn default_sparse_plan_crosses_the_threshold_in_frame_eight() {
        let plan = release_plan(ids(524_288), 65_536).unwrap();
        assert_eq!(plan.groups.len(), 8);
        assert_eq!(plan.trigger_group, 7);
        assert_eq!(plan.manifest_bytes_before_trigger, 7_340_576);
        assert_eq!(plan.trigger_frame_bytes, 1_048_648);
        assert!(
            plan.manifest_bytes_before_trigger + plan.trigger_frame_bytes
                >= MANIFEST_COMPACTION_TRIGGER_BYTES
        );
    }

    #[test]
    fn sparse_plan_rejects_a_workload_below_the_threshold() {
        let error = release_plan(ids(1_024), 1_024).unwrap_err();
        assert!(error.to_string().contains("do not reach"));
    }

    #[test]
    fn duration_delta_only_reports_a_new_session_max() {
        let mut before = DurationStats::default();
        before.observations = 2;
        before.total = std::time::Duration::from_nanos(20);
        before.max = std::time::Duration::from_nanos(15);
        let mut after = DurationStats::default();
        after.observations = 3;
        after.total = std::time::Duration::from_nanos(35);
        after.max = std::time::Duration::from_nanos(15);
        let delta = duration_delta(before, after);
        assert_eq!(delta.observations, 1);
        assert_eq!(delta.total_ns, 15);
        assert_eq!(delta.new_session_max_ns, None);
    }
}
