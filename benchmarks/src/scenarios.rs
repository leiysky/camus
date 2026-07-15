use crate::metrics::{duration_ns, environment, Environment, LatencySummary};
use crate::model::records;
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use camus::{
    Capacity, CommitStats, Config, Log, PendingRecord, ReadLimits, Record, RecordId, StreamId,
};
use clap::{Args, ValueEnum};
use hdrhistogram::Histogram;
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::{Builder, TempDir};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const REPORT_SCHEMA_VERSION: u32 = 1;
const RECORD_HEADER_BYTES: usize = 24;
const RECORD_MAGIC: &[u8; 8] = b"CAMUSAPP";

/// Arguments for finite Camus application-scenario workloads.
#[derive(Debug, Args)]
pub(crate) struct ScenarioArgs {
    /// Scenarios to run. Defaults to all scenarios.
    #[arg(long, value_enum, value_delimiter = ',')]
    scenarios: Option<Vec<ScenarioKind>>,

    /// Workload-size and sample-count preset.
    #[arg(long, value_enum, default_value = "reference")]
    profile: ScenarioProfile,

    /// Replace the profile's independent sample count.
    #[arg(long)]
    samples: Option<usize>,

    /// Parent directory on the device under test.
    #[arg(long, default_value = "target/benchmark-data")]
    data_directory: PathBuf,

    /// JSON report path. Defaults under target/benchmark-results.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Free-form environment note stored in the raw report.
    #[arg(long)]
    note: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
enum ScenarioKind {
    Outbox,
    TelemetrySpool,
    UploadStaging,
    MultiStreamWriteBehind,
}

impl ScenarioKind {
    const ALL: [Self; 4] = [
        Self::Outbox,
        Self::TelemetrySpool,
        Self::UploadStaging,
        Self::MultiStreamWriteBehind,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Outbox => "outbox_live_handoff",
            Self::TelemetrySpool => "telemetry_batch_spool",
            Self::UploadStaging => "upload_staging_recovery",
            Self::MultiStreamWriteBehind => "multi_stream_write_behind",
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::Outbox => {
                "Eight concurrent transaction executors append 1 KiB events to one destination stream while one worker validates and durably releases bounded batches."
            }
            Self::TelemetrySpool => {
                "Four collectors append 64-record batches of 256-byte samples to source streams while one worker per stream drains 512-record batches."
            }
            Self::UploadStaging => {
                "Four upload lanes stage 256 KiB chunks, restart with the complete backlog pending, then validate, release, and reclaim every chunk."
            }
            Self::MultiStreamWriteBehind => {
                "Thirty-two independent 4 KiB write-behind streams append and drain concurrently through one physical root."
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ScenarioProfile {
    Smoke,
    Reference,
}

impl ScenarioProfile {
    const fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Reference => "reference",
        }
    }

    const fn samples(self) -> usize {
        match self {
            Self::Smoke => 1,
            Self::Reference => 3,
        }
    }

    const fn workload(self, scenario: ScenarioKind) -> ScenarioWorkload {
        match (self, scenario) {
            (Self::Smoke, ScenarioKind::Outbox) => ScenarioWorkload {
                streams: 1,
                producers: 8,
                records: 1_024,
                metadata_bytes: RECORD_HEADER_BYTES,
                payload_bytes: 1_024,
                append_batch_records: 1,
                read_batch_records: 128,
                restart_before_drain: false,
            },
            (Self::Reference, ScenarioKind::Outbox) => ScenarioWorkload {
                streams: 1,
                producers: 8,
                records: 16_384,
                metadata_bytes: RECORD_HEADER_BYTES,
                payload_bytes: 1_024,
                append_batch_records: 1,
                read_batch_records: 128,
                restart_before_drain: false,
            },
            (Self::Smoke, ScenarioKind::TelemetrySpool) => ScenarioWorkload {
                streams: 4,
                producers: 4,
                records: 4_096,
                metadata_bytes: RECORD_HEADER_BYTES,
                payload_bytes: 256,
                append_batch_records: 64,
                read_batch_records: 512,
                restart_before_drain: false,
            },
            (Self::Reference, ScenarioKind::TelemetrySpool) => ScenarioWorkload {
                streams: 4,
                producers: 4,
                records: 131_072,
                metadata_bytes: RECORD_HEADER_BYTES,
                payload_bytes: 256,
                append_batch_records: 64,
                read_batch_records: 512,
                restart_before_drain: false,
            },
            (Self::Smoke, ScenarioKind::UploadStaging) => ScenarioWorkload {
                streams: 4,
                producers: 4,
                records: 32,
                metadata_bytes: RECORD_HEADER_BYTES,
                payload_bytes: 256 * 1024,
                append_batch_records: 4,
                read_batch_records: 16,
                restart_before_drain: true,
            },
            (Self::Reference, ScenarioKind::UploadStaging) => ScenarioWorkload {
                streams: 4,
                producers: 4,
                records: 512,
                metadata_bytes: RECORD_HEADER_BYTES,
                payload_bytes: 256 * 1024,
                append_batch_records: 4,
                read_batch_records: 16,
                restart_before_drain: true,
            },
            (Self::Smoke, ScenarioKind::MultiStreamWriteBehind) => ScenarioWorkload {
                streams: 32,
                producers: 32,
                records: 2_048,
                metadata_bytes: RECORD_HEADER_BYTES,
                payload_bytes: 4 * 1024,
                append_batch_records: 1,
                read_batch_records: 64,
                restart_before_drain: false,
            },
            (Self::Reference, ScenarioKind::MultiStreamWriteBehind) => ScenarioWorkload {
                streams: 32,
                producers: 32,
                records: 32_768,
                metadata_bytes: RECORD_HEADER_BYTES,
                payload_bytes: 4 * 1024,
                append_batch_records: 1,
                read_batch_records: 64,
                restart_before_drain: false,
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ScenarioWorkload {
    streams: usize,
    producers: usize,
    records: usize,
    metadata_bytes: usize,
    payload_bytes: usize,
    append_batch_records: usize,
    read_batch_records: usize,
    restart_before_drain: bool,
}

#[derive(Debug, Serialize)]
struct ScenarioReport {
    schema_version: u32,
    environment: Environment,
    profile: String,
    samples: usize,
    results: Vec<ScenarioResult>,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioConfiguration {
    streams: usize,
    producers: usize,
    records: usize,
    metadata_bytes: usize,
    payload_bytes: usize,
    append_batch_records: usize,
    read_batch_records: usize,
    restart_before_drain: bool,
}

impl From<ScenarioWorkload> for ScenarioConfiguration {
    fn from(workload: ScenarioWorkload) -> Self {
        Self {
            streams: workload.streams,
            producers: workload.producers,
            records: workload.records,
            metadata_bytes: workload.metadata_bytes,
            payload_bytes: workload.payload_bytes,
            append_batch_records: workload.append_batch_records,
            read_batch_records: workload.read_batch_records,
            restart_before_drain: workload.restart_before_drain,
        }
    }
}

#[derive(Debug, Serialize)]
struct ScenarioResult {
    scenario: String,
    description: String,
    configuration: ScenarioConfiguration,
    samples: usize,
    total_elapsed_ns: u64,
    records_per_second: f64,
    logical_mib_per_second: f64,
    latency_ns: BTreeMap<String, LatencySummary>,
    max_observed_pending_records: u64,
    max_observed_actual_file_bytes: u64,
    median_recovered_pending_records: u64,
    final_pending_records: u64,
    median_final_file_bytes: u64,
    integrity_validated: bool,
    append_records_per_commit_group: f64,
    release_records_per_commit_group: f64,
    reclaimed_segments: u64,
}

#[derive(Debug)]
struct ScenarioMeasurement {
    elapsed: Duration,
    latencies: BTreeMap<&'static str, Vec<u64>>,
    max_pending_records: u64,
    max_actual_file_bytes: u64,
    recovered_pending_records: u64,
    final_file_bytes: u64,
    append_records: u64,
    append_groups: u64,
    release_records: u64,
    release_groups: u64,
    reclaimed_segments: u64,
}

#[derive(Default)]
struct Watermarks {
    pending_records: AtomicU64,
    actual_file_bytes: AtomicU64,
}

impl Watermarks {
    fn observe(&self, log: &Log) {
        let storage = log.stats().storage;
        self.pending_records
            .fetch_max(storage.pending_records, Ordering::Relaxed);
        self.actual_file_bytes
            .fetch_max(storage.actual_file_bytes, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ProducerOutput {
    append_ns: Vec<u64>,
}

#[derive(Debug)]
struct ConsumerOutput {
    read_ns: Vec<u64>,
    release_ns: Vec<u64>,
    drain_batch_ns: Vec<u64>,
    handoff_ns: Vec<u64>,
    first_read_ns: u64,
    completion_ns: u64,
}

pub(crate) async fn run(arguments: ScenarioArgs) -> Result<()> {
    let scenarios = arguments
        .scenarios
        .clone()
        .unwrap_or_else(|| ScenarioKind::ALL.to_vec());
    ensure!(!scenarios.is_empty(), "at least one scenario is required");
    let samples = arguments
        .samples
        .unwrap_or_else(|| arguments.profile.samples());
    ensure!(samples > 0, "--samples must be greater than zero");
    ensure!(
        scenarios.iter().collect::<HashSet<_>>().len() == scenarios.len(),
        "each scenario may be selected only once"
    );

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

    let mut results = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let workload = arguments.profile.workload(scenario);
        validate_workload(workload)?;
        let mut measurements = Vec::with_capacity(samples);
        for sample in 1..=samples {
            eprintln!(
                "running scenario={} sample={sample}/{samples}",
                scenario.name()
            );
            let measurement = if workload.restart_before_drain {
                run_recovery_scenario(&data_directory, scenario, workload).await?
            } else {
                run_live_scenario(&data_directory, scenario, workload).await?
            };
            measurements.push(measurement);
        }
        results.push(aggregate_scenario(scenario, workload, measurements)?);
    }

    let mut measured_environment = environment(&data_directory, arguments.note);
    measured_environment
        .engine_versions
        .retain(|engine, _| engine == "camus");
    let report = ScenarioReport {
        schema_version: REPORT_SCHEMA_VERSION,
        environment: measured_environment,
        profile: arguments.profile.name().to_string(),
        samples,
        results,
    };
    print_results(&report.results);
    let output = arguments.output.unwrap_or_else(default_output_path);
    write_report(&output, &report)?;
    println!("report: {}", output.display());
    Ok(())
}

fn validate_workload(workload: ScenarioWorkload) -> Result<()> {
    ensure!(workload.streams > 0, "scenario needs at least one stream");
    ensure!(
        workload.producers >= workload.streams,
        "every stream needs at least one producer"
    );
    ensure!(workload.records > 0, "scenario needs at least one record");
    ensure!(
        workload.records >= workload.producers,
        "every producer needs at least one record"
    );
    ensure!(
        workload.metadata_bytes >= RECORD_HEADER_BYTES,
        "scenario metadata must fit its validation header"
    );
    ensure!(
        workload.append_batch_records > 0,
        "append batch size must be positive"
    );
    ensure!(
        workload.read_batch_records > 0,
        "read batch size must be positive"
    );
    let epoch_bytes = u64::try_from(workload.append_batch_records)
        .context("append batch size overflow")?
        .checked_mul(
            u64::try_from(workload.metadata_bytes)
                .context("metadata size overflow")?
                .checked_add(u64::try_from(workload.payload_bytes).context("payload overflow")?)
                .and_then(|bytes| bytes.checked_add(40))
                .context("record size overflow")?,
        )
        .and_then(|bytes| bytes.checked_add(88))
        .context("epoch size overflow")?;
    ensure!(
        epoch_bytes <= camus::DEFAULT_MAX_EPOCH_BYTES,
        "scenario append batch exceeds Camus's default epoch bound"
    );
    Ok(())
}

async fn run_live_scenario(
    data_directory: &Path,
    scenario: ScenarioKind,
    workload: ScenarioWorkload,
) -> Result<ScenarioMeasurement> {
    let directory = scenario_directory(data_directory, scenario)?;
    let log = Log::open(Config::new(directory.path(), Capacity::Unbounded))
        .await
        .context("open live scenario root")?;
    let assignments = producer_assignments(workload);
    let stream_targets = stream_targets(&assignments, workload.streams);
    let starts = Arc::new(
        (0..workload.records)
            .map(|_| OnceLock::new())
            .collect::<Vec<_>>(),
    );
    let watermarks = Arc::new(Watermarks::default());
    watermarks.observe(&log);
    let barrier = Arc::new(Barrier::new(
        workload
            .producers
            .checked_add(workload.streams)
            .and_then(|tasks| tasks.checked_add(1))
            .context("scenario task count overflow")?,
    ));
    let wall = Instant::now();
    let mut producers = spawn_producers(
        &log,
        workload,
        assignments,
        Some(starts.clone()),
        barrier.clone(),
        watermarks.clone(),
    );
    let mut consumers = spawn_consumers(
        &log,
        workload,
        stream_targets,
        Some(starts),
        barrier.clone(),
        watermarks.clone(),
        wall,
    );
    barrier.wait().await;

    let append_ns = join_producers(&mut producers).await?;
    let consumer_outputs = join_consumers(&mut consumers).await?;
    let elapsed = wall.elapsed();
    ensure!(
        log.stats().storage.pending_records == 0,
        "live scenario left pending records"
    );
    let commits = log.stats().commits;
    validate_commit_totals(workload, commits, commits)?;
    let mut latencies = consumer_latencies(consumer_outputs);
    latencies.insert("append", append_ns);
    log.reclaim().await.context("reclaim live scenario root")?;
    let final_stats = log.stats();
    log.shutdown()
        .await
        .context("shutdown live scenario root")?;

    Ok(ScenarioMeasurement {
        elapsed,
        latencies,
        max_pending_records: watermarks.pending_records.load(Ordering::Relaxed),
        max_actual_file_bytes: watermarks.actual_file_bytes.load(Ordering::Relaxed),
        recovered_pending_records: 0,
        final_file_bytes: final_stats.storage.actual_file_bytes,
        append_records: commits.append_records,
        append_groups: commits.append_groups,
        release_records: commits.release_records,
        release_groups: commits.release_groups,
        reclaimed_segments: final_stats.maintenance.reclaimed_segments,
    })
}

async fn run_recovery_scenario(
    data_directory: &Path,
    scenario: ScenarioKind,
    workload: ScenarioWorkload,
) -> Result<ScenarioMeasurement> {
    let directory = scenario_directory(data_directory, scenario)?;
    let log = Log::open(Config::new(directory.path(), Capacity::Unbounded))
        .await
        .context("open staging scenario root")?;
    let assignments = producer_assignments(workload);
    let stream_targets = stream_targets(&assignments, workload.streams);
    let watermarks = Arc::new(Watermarks::default());
    let producer_barrier = Arc::new(Barrier::new(
        workload
            .producers
            .checked_add(1)
            .context("producer task count overflow")?,
    ));
    let wall = Instant::now();
    let mut producers = spawn_producers(
        &log,
        workload,
        assignments,
        None,
        producer_barrier.clone(),
        watermarks.clone(),
    );
    producer_barrier.wait().await;
    let append_ns = join_producers(&mut producers).await?;
    watermarks.observe(&log);
    let before_restart = log.stats();
    ensure!(
        before_restart.storage.pending_records == u64::try_from(workload.records)?,
        "staging setup did not retain the complete backlog"
    );
    log.shutdown().await.context("shutdown staged backlog")?;
    drop(log);

    let reopen_started = Instant::now();
    let reopened = Log::open(Config::new(directory.path(), Capacity::Unbounded))
        .await
        .context("reopen staged backlog")?;
    let reopen_ns = duration_ns(reopen_started.elapsed());
    let recovered = reopened.stats().storage.pending_records;
    ensure!(
        recovered == u64::try_from(workload.records)?,
        "restart did not recover the complete staged backlog"
    );
    watermarks.observe(&reopened);

    let consumer_barrier = Arc::new(Barrier::new(
        workload
            .streams
            .checked_add(1)
            .context("consumer task count overflow")?,
    ));
    let drain_started = Instant::now();
    let mut consumers = spawn_consumers(
        &reopened,
        workload,
        stream_targets,
        None,
        consumer_barrier.clone(),
        watermarks.clone(),
        wall,
    );
    consumer_barrier.wait().await;
    let consumer_outputs = join_consumers(&mut consumers).await?;
    let drain_ns = duration_ns(drain_started.elapsed());
    let elapsed = wall.elapsed();
    ensure!(
        reopened.stats().storage.pending_records == 0,
        "recovered staging drain left pending records"
    );
    let release_commits = reopened.stats().commits;
    validate_commit_totals(workload, before_restart.commits, release_commits)?;
    let mut latencies = consumer_latencies(consumer_outputs);
    latencies.insert("append", append_ns);
    latencies.insert("reopen", vec![reopen_ns]);
    latencies.insert("drain_total", vec![drain_ns]);
    reopened
        .reclaim()
        .await
        .context("reclaim recovered staging root")?;
    let final_stats = reopened.stats();
    reopened
        .shutdown()
        .await
        .context("shutdown recovered staging root")?;

    Ok(ScenarioMeasurement {
        elapsed,
        latencies,
        max_pending_records: watermarks.pending_records.load(Ordering::Relaxed),
        max_actual_file_bytes: watermarks.actual_file_bytes.load(Ordering::Relaxed),
        recovered_pending_records: recovered,
        final_file_bytes: final_stats.storage.actual_file_bytes,
        append_records: before_restart.commits.append_records,
        append_groups: before_restart.commits.append_groups,
        release_records: release_commits.release_records,
        release_groups: release_commits.release_groups,
        reclaimed_segments: final_stats.maintenance.reclaimed_segments,
    })
}

fn producer_assignments(workload: ScenarioWorkload) -> Vec<(usize, Vec<usize>)> {
    let mut assignments = (0..workload.producers)
        .map(|producer| (producer % workload.streams, Vec::new()))
        .collect::<Vec<_>>();
    for record in 0..workload.records {
        assignments[record % workload.producers].1.push(record);
    }
    assignments
}

fn stream_targets(assignments: &[(usize, Vec<usize>)], streams: usize) -> Vec<usize> {
    let mut targets = vec![0_usize; streams];
    for (stream, records) in assignments {
        targets[*stream] += records.len();
    }
    targets
}

fn spawn_producers(
    log: &Log,
    workload: ScenarioWorkload,
    assignments: Vec<(usize, Vec<usize>)>,
    starts: Option<Arc<Vec<OnceLock<Instant>>>>,
    barrier: Arc<Barrier>,
    watermarks: Arc<Watermarks>,
) -> JoinSet<Result<ProducerOutput>> {
    let mut tasks = JoinSet::new();
    for (stream, assigned) in assignments {
        let stream_handle = log.stream(StreamId::new(stream as u64));
        let log = log.clone();
        let starts = starts.clone();
        let barrier = barrier.clone();
        let watermarks = watermarks.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let mut append_ns =
                Vec::with_capacity(assigned.len().div_ceil(workload.append_batch_records));
            for batch in assigned.chunks(workload.append_batch_records) {
                let records = batch
                    .iter()
                    .map(|record| scenario_record(workload, stream, *record))
                    .collect::<Vec<_>>();
                if let Some(starts) = &starts {
                    let started = Instant::now();
                    for record in batch {
                        starts[*record]
                            .set(started)
                            .expect("each scenario record is appended once");
                    }
                }
                let started = Instant::now();
                let ids = stream_handle
                    .append_batch(records)
                    .await
                    .context("append scenario record batch")?;
                append_ns.push(duration_ns(started.elapsed()));
                ensure!(
                    ids.len() == batch.len(),
                    "scenario append returned the wrong ID count"
                );
                watermarks.observe(&log);
            }
            Ok(ProducerOutput { append_ns })
        });
    }
    tasks
}

fn spawn_consumers(
    log: &Log,
    workload: ScenarioWorkload,
    stream_targets: Vec<usize>,
    starts: Option<Arc<Vec<OnceLock<Instant>>>>,
    barrier: Arc<Barrier>,
    watermarks: Arc<Watermarks>,
    wall: Instant,
) -> JoinSet<Result<ConsumerOutput>> {
    let mut tasks = JoinSet::new();
    for (stream, target) in stream_targets.into_iter().enumerate() {
        let stream_handle = log.stream(StreamId::new(stream as u64));
        let log = log.clone();
        let starts = starts.clone();
        let barrier = barrier.clone();
        let watermarks = watermarks.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let mut processed = 0_usize;
            let mut seen = HashSet::with_capacity(target);
            let mut read_ns = Vec::new();
            let mut release_ns = Vec::new();
            let mut drain_batch_ns = Vec::new();
            let mut handoff_ns = Vec::with_capacity(target);
            let mut first_read_ns = None;
            let mut previous_sequence = None;
            while processed < target {
                let remaining = target - processed;
                let max_records = workload.read_batch_records.min(remaining);
                let max_bytes = u64::try_from(workload.payload_bytes)
                    .context("read payload size overflow")?
                    .checked_mul(u64::try_from(max_records).context("read count overflow")?)
                    .context("read byte bound overflow")?;
                let cycle_started = Instant::now();
                let read_started = Instant::now();
                let snapshot = stream_handle
                    .read(ReadLimits::new(max_records, max_bytes))
                    .await
                    .context("read scenario records")?;
                let read_elapsed = duration_ns(read_started.elapsed());
                first_read_ns.get_or_insert(read_elapsed);
                read_ns.push(read_elapsed);
                ensure!(
                    !snapshot.is_empty(),
                    "scenario read returned an empty batch"
                );

                let mut ids = Vec::with_capacity(snapshot.len());
                let mut logical_records = Vec::with_capacity(snapshot.len());
                for record in &snapshot {
                    let sequence = record_sequence(record.id);
                    ensure!(
                        previous_sequence.is_none_or(|previous| sequence > previous),
                        "scenario stream records are not in RecordId sequence order"
                    );
                    previous_sequence = Some(sequence);
                    let logical_record = validate_record(workload, stream, record)?;
                    ensure!(
                        seen.insert(logical_record),
                        "scenario observed a record again after successful release"
                    );
                    ids.push(record.id);
                    logical_records.push(logical_record);
                }
                let release_started = Instant::now();
                stream_handle
                    .release(ids)
                    .await
                    .context("release scenario records")?;
                release_ns.push(duration_ns(release_started.elapsed()));
                drain_batch_ns.push(duration_ns(cycle_started.elapsed()));
                if let Some(starts) = &starts {
                    for record in logical_records {
                        let started = starts[record]
                            .get()
                            .context("consumer observed a record before producer timing began")?;
                        handoff_ns.push(duration_ns(started.elapsed()));
                    }
                }
                processed = processed
                    .checked_add(snapshot.len())
                    .context("processed record count overflow")?;
                watermarks.observe(&log);
            }
            ensure!(seen.len() == target, "scenario consumer lost records");
            Ok(ConsumerOutput {
                read_ns,
                release_ns,
                drain_batch_ns,
                handoff_ns,
                first_read_ns: first_read_ns.context("scenario consumer performed no read")?,
                completion_ns: duration_ns(wall.elapsed()),
            })
        });
    }
    tasks
}

async fn join_producers(tasks: &mut JoinSet<Result<ProducerOutput>>) -> Result<Vec<u64>> {
    let mut append_ns = Vec::new();
    while let Some(result) = tasks.join_next().await {
        append_ns.extend(result.context("join scenario producer")??.append_ns);
    }
    Ok(append_ns)
}

async fn join_consumers(
    tasks: &mut JoinSet<Result<ConsumerOutput>>,
) -> Result<Vec<ConsumerOutput>> {
    let mut output = Vec::new();
    while let Some(result) = tasks.join_next().await {
        output.push(result.context("join scenario consumer")??);
    }
    Ok(output)
}

fn consumer_latencies(outputs: Vec<ConsumerOutput>) -> BTreeMap<&'static str, Vec<u64>> {
    let mut read = Vec::new();
    let mut release = Vec::new();
    let mut drain_batch = Vec::new();
    let mut handoff = Vec::new();
    let mut first_read = Vec::new();
    let mut completion = Vec::new();
    for output in outputs {
        read.extend(output.read_ns);
        release.extend(output.release_ns);
        drain_batch.extend(output.drain_batch_ns);
        handoff.extend(output.handoff_ns);
        first_read.push(output.first_read_ns);
        completion.push(output.completion_ns);
    }
    let mut latencies = BTreeMap::new();
    latencies.insert("read", read);
    latencies.insert("release", release);
    latencies.insert("drain_batch", drain_batch);
    if !handoff.is_empty() {
        latencies.insert("handoff", handoff);
    }
    latencies.insert("first_read", first_read);
    latencies.insert("stream_completion", completion);
    latencies
}

fn scenario_record(workload: ScenarioWorkload, stream: usize, logical_record: usize) -> Record {
    let mut source = records(
        1,
        stream as u64,
        logical_record as u64,
        workload.metadata_bytes,
        workload.payload_bytes,
    )
    .remove(0);
    let mut metadata = source.metadata.to_vec();
    metadata[..8].copy_from_slice(RECORD_MAGIC);
    metadata[8..16].copy_from_slice(&(stream as u64).to_le_bytes());
    metadata[16..24].copy_from_slice(&(logical_record as u64).to_le_bytes());
    source.metadata = Bytes::from(metadata);
    Record {
        metadata: source.metadata,
        payload: source.payload,
    }
}

fn validate_record(
    workload: ScenarioWorkload,
    expected_stream: usize,
    record: &PendingRecord,
) -> Result<usize> {
    validate_record_bytes(workload, expected_stream, &record.metadata, &record.payload)
}

fn validate_record_bytes(
    workload: ScenarioWorkload,
    expected_stream: usize,
    metadata: &Bytes,
    payload: &Bytes,
) -> Result<usize> {
    ensure!(
        metadata.len() >= RECORD_HEADER_BYTES,
        "scenario metadata is truncated"
    );
    ensure!(
        &metadata[..8] == RECORD_MAGIC,
        "scenario metadata magic mismatch"
    );
    let mut stream_bytes = [0_u8; 8];
    stream_bytes.copy_from_slice(&metadata[8..16]);
    let stream = usize::try_from(u64::from_le_bytes(stream_bytes))
        .context("scenario stream ID does not fit usize")?;
    ensure!(stream == expected_stream, "scenario record crossed streams");
    let mut record_bytes = [0_u8; 8];
    record_bytes.copy_from_slice(&metadata[16..24]);
    let logical_record = usize::try_from(u64::from_le_bytes(record_bytes))
        .context("scenario record ID does not fit usize")?;
    ensure!(
        logical_record < workload.records,
        "scenario record ID is out of bounds"
    );
    let expected = scenario_record(workload, expected_stream, logical_record);
    ensure!(
        *metadata == expected.metadata,
        "scenario metadata verification failed"
    );
    ensure!(
        *payload == expected.payload,
        "scenario payload verification failed"
    );
    Ok(logical_record)
}

fn record_sequence(id: RecordId) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&id.as_bytes()[24..]);
    u64::from_le_bytes(bytes)
}

fn validate_commit_totals(
    workload: ScenarioWorkload,
    append: CommitStats,
    release: CommitStats,
) -> Result<()> {
    let expected = u64::try_from(workload.records).context("scenario record count overflow")?;
    ensure!(
        append.append_records == expected,
        "append commit statistics do not cover every scenario record"
    );
    ensure!(
        release.release_records == expected,
        "release commit statistics do not cover every scenario record"
    );
    Ok(())
}

fn aggregate_scenario(
    scenario: ScenarioKind,
    workload: ScenarioWorkload,
    measurements: Vec<ScenarioMeasurement>,
) -> Result<ScenarioResult> {
    ensure!(!measurements.is_empty(), "scenario has no measurements");
    let samples = measurements.len();
    let mut elapsed_ns = 0_u64;
    let mut latency_values = BTreeMap::<String, Vec<u64>>::new();
    let mut recovered = Vec::with_capacity(samples);
    let mut final_file_bytes = Vec::with_capacity(samples);
    let mut max_pending_records = 0_u64;
    let mut max_actual_file_bytes = 0_u64;
    let mut append_records = 0_u64;
    let mut append_groups = 0_u64;
    let mut release_records = 0_u64;
    let mut release_groups = 0_u64;
    let mut reclaimed_segments = 0_u64;
    for measurement in measurements {
        elapsed_ns = elapsed_ns
            .checked_add(duration_ns(measurement.elapsed))
            .context("scenario elapsed time overflow")?;
        for (operation, values) in measurement.latencies {
            latency_values
                .entry(operation.to_string())
                .or_default()
                .extend(values);
        }
        max_pending_records = max_pending_records.max(measurement.max_pending_records);
        max_actual_file_bytes = max_actual_file_bytes.max(measurement.max_actual_file_bytes);
        recovered.push(measurement.recovered_pending_records);
        final_file_bytes.push(measurement.final_file_bytes);
        append_records = append_records.saturating_add(measurement.append_records);
        append_groups = append_groups.saturating_add(measurement.append_groups);
        release_records = release_records.saturating_add(measurement.release_records);
        release_groups = release_groups.saturating_add(measurement.release_groups);
        reclaimed_segments = reclaimed_segments.saturating_add(measurement.reclaimed_segments);
    }
    let latency_ns = latency_values
        .into_iter()
        .map(|(operation, values)| Ok((operation, summarize(&values)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    recovered.sort_unstable();
    final_file_bytes.sort_unstable();
    let total_records = workload
        .records
        .checked_mul(samples)
        .context("scenario record total overflow")?;
    let elapsed_seconds = elapsed_ns as f64 / 1_000_000_000.0;
    let records_per_second = total_records as f64 / elapsed_seconds;
    let logical_bytes = workload
        .metadata_bytes
        .checked_add(workload.payload_bytes)
        .context("scenario logical record size overflow")?;

    Ok(ScenarioResult {
        scenario: scenario.name().to_string(),
        description: scenario.description().to_string(),
        configuration: workload.into(),
        samples,
        total_elapsed_ns: elapsed_ns,
        records_per_second,
        logical_mib_per_second: records_per_second * logical_bytes as f64 / (1024.0 * 1024.0),
        latency_ns,
        max_observed_pending_records: max_pending_records,
        max_observed_actual_file_bytes: max_actual_file_bytes,
        median_recovered_pending_records: recovered[samples / 2],
        final_pending_records: 0,
        median_final_file_bytes: final_file_bytes[samples / 2],
        integrity_validated: true,
        append_records_per_commit_group: ratio(append_records, append_groups),
        release_records_per_commit_group: ratio(release_records, release_groups),
        reclaimed_segments,
    })
}

fn summarize(values: &[u64]) -> Result<LatencySummary> {
    ensure!(!values.is_empty(), "cannot summarize an empty latency set");
    let mut histogram = Histogram::<u64>::new(3).context("create scenario latency histogram")?;
    for value in values {
        histogram
            .record((*value).max(1))
            .context("record scenario latency")?;
    }
    Ok(LatencySummary {
        p50: histogram.value_at_quantile(0.50),
        p95: histogram.value_at_quantile(0.95),
        p99: histogram.value_at_quantile(0.99),
        max: histogram.max(),
    })
}

fn ratio(records: u64, groups: u64) -> f64 {
    if groups == 0 {
        0.0
    } else {
        records as f64 / groups as f64
    }
}

fn scenario_directory(data_directory: &Path, scenario: ScenarioKind) -> Result<TempDir> {
    Builder::new()
        .prefix(&format!("camus-{}-", scenario.name()))
        .tempdir_in(data_directory)
        .with_context(|| {
            format!(
                "create scenario directory under {}",
                data_directory.display()
            )
        })
}

fn print_results(results: &[ScenarioResult]) {
    println!(
        "{:<31} {:>12} {:>11} {:>11} {:>11} {:>12}",
        "scenario", "records/s", "MiB/s", "append p99", "release p99", "handoff/reopen"
    );
    for result in results {
        let append = result
            .latency_ns
            .get("append")
            .map_or(0.0, |latency| latency.p99 as f64 / 1_000_000.0);
        let release = result
            .latency_ns
            .get("release")
            .map_or(0.0, |latency| latency.p99 as f64 / 1_000_000.0);
        let terminal = result
            .latency_ns
            .get("handoff")
            .or_else(|| result.latency_ns.get("reopen"))
            .map_or(0.0, |latency| latency.p99 as f64 / 1_000_000.0);
        println!(
            "{:<31} {:>12.0} {:>11.2} {:>10.3}ms {:>10.3}ms {:>11.3}ms",
            result.scenario,
            result.records_per_second,
            result.logical_mib_per_second,
            append,
            release,
            terminal,
        );
    }
}

fn write_report(path: &Path, report: &ScenarioReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create report directory {}", parent.display()))?;
    }
    let writer = BufWriter::new(
        File::create(path).with_context(|| format!("create report {}", path.display()))?,
    );
    serde_json::to_writer_pretty(writer, report).context("serialize scenario report")
}

fn default_output_path() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    PathBuf::from(format!(
        "target/benchmark-results/scenarios-{timestamp}.json"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_assignment_covers_each_record_once_and_balances_streams() {
        let workload = ScenarioProfile::Smoke.workload(ScenarioKind::MultiStreamWriteBehind);
        let assignments = producer_assignments(workload);
        let mut observed = assignments
            .iter()
            .flat_map(|(_, records)| records.iter().copied())
            .collect::<Vec<_>>();
        observed.sort_unstable();
        assert_eq!(observed, (0..workload.records).collect::<Vec<_>>());
        assert!(stream_targets(&assignments, workload.streams)
            .into_iter()
            .all(|records| records == workload.records / workload.streams));
    }

    #[test]
    fn record_validation_detects_content_and_stream_damage() {
        let workload = ScenarioProfile::Smoke.workload(ScenarioKind::Outbox);
        let record = scenario_record(workload, 0, 17);
        assert_eq!(
            validate_record_bytes(workload, 0, &record.metadata, &record.payload).unwrap(),
            17
        );
        let damaged = Bytes::from(vec![0_u8; workload.payload_bytes]);
        assert!(validate_record_bytes(workload, 0, &record.metadata, &damaged).is_err());
        assert!(validate_record_bytes(workload, 1, &record.metadata, &record.payload).is_err());
    }

    #[test]
    fn every_profile_fits_the_public_epoch_bound() {
        for profile in [ScenarioProfile::Smoke, ScenarioProfile::Reference] {
            for scenario in ScenarioKind::ALL {
                validate_workload(profile.workload(scenario)).unwrap();
            }
        }
    }
}
