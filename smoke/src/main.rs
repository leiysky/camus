mod telemetry;
mod workload;

use anyhow::{bail, ensure, Context, Result};
use camus::{Capacity, Config, FullPolicy, Log, RootStats};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use telemetry::{ExporterSummary, Sample, ValidationSummary, VmExporter};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};
use workload::{consumer, producer, ConsumerConfig, Control, Measurements, Phase, ProducerConfig};

const MIB: u64 = 1024 * 1024;
const DEFAULT_CAPACITY_BYTES: u64 = 1024 * MIB;
const DEFAULT_SEGMENT_BYTES: u64 = 64 * MIB;

#[derive(Debug, Parser)]
#[command(
    name = "camus-long-smoke",
    about = "Long-running capacity, throughput, and latency smoke test for Camus"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a bounded-capacity workload and produce a VictoriaMetrics-backed report.
    Run(RunArgs),
    /// Rebuild a report from saved run metadata and VictoriaMetrics samples.
    Report(ReportArgs),
}

#[derive(Clone, Debug, Args, Deserialize, Serialize)]
struct RunArgs {
    /// Measured run time before the final drain, in seconds.
    #[arg(long, default_value_t = 6 * 60 * 60)]
    duration_seconds: u64,

    /// Interval between metrics samples, in seconds.
    #[arg(long, default_value_t = 5)]
    sample_interval_seconds: u64,

    /// Time spent regulating around the steady waterline before pressure fill.
    #[arg(long, default_value_t = 10 * 60)]
    steady_seconds: u64,

    /// Time producers remain blocked at the high waterline.
    #[arg(long, default_value_t = 60)]
    pressure_hold_seconds: u64,

    /// Maximum time allowed to reach the steady or full waterline.
    #[arg(long, default_value_t = 10 * 60)]
    fill_timeout_seconds: u64,

    /// Maximum time allowed for one recovery phase.
    #[arg(long, default_value_t = 10 * 60)]
    recovery_timeout_seconds: u64,

    /// Maximum time allowed for the final complete drain.
    #[arg(long, default_value_t = 15 * 60)]
    drain_timeout_seconds: u64,

    /// Root-wide Camus capacity.
    #[arg(long, default_value_t = DEFAULT_CAPACITY_BYTES)]
    capacity_bytes: u64,

    /// Physical segment size used by the smoke root.
    #[arg(long, default_value_t = DEFAULT_SEGMENT_BYTES)]
    segment_bytes: u64,

    /// Number of logical streams. Exactly one consumer is created per stream.
    #[arg(long, default_value_t = 16)]
    streams: usize,

    /// Number of concurrent append producers.
    #[arg(long, default_value_t = 32)]
    producers: usize,

    /// Records in one atomic append batch.
    #[arg(long, default_value_t = 16)]
    append_batch_records: usize,

    /// Records in one read and exact release cycle.
    #[arg(long, default_value_t = 512)]
    read_batch_records: usize,

    /// Opaque metadata bytes per record. Must be at least 24.
    #[arg(long, default_value_t = 32)]
    metadata_bytes: usize,

    /// Opaque payload bytes per record.
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,

    /// Lower edge of the regulated steady-state capacity band.
    #[arg(long, default_value_t = 0.45)]
    steady_low_ratio: f64,

    /// Upper edge of the regulated steady-state capacity band.
    #[arg(long, default_value_t = 0.65)]
    steady_high_ratio: f64,

    /// Minimum safe-capacity utilization required before pressure hold begins.
    #[arg(long, default_value_t = 0.95)]
    pressure_high_ratio: f64,

    /// Capacity utilization required to finish recovery.
    #[arg(long, default_value_t = 0.20)]
    recovery_low_ratio: f64,

    /// Reactor command queue bound.
    #[arg(long, default_value_t = 1024)]
    command_queue_capacity: usize,

    /// Parent directory for disposable smoke storage.
    #[arg(long, default_value = "target/long-smoke-data")]
    data_directory: PathBuf,

    /// Parent directory for local metrics, metadata, and the generated report.
    #[arg(long, default_value = "target/long-smoke-results")]
    output_directory: PathBuf,

    /// Stable run label. Defaults to a timestamp and process ID.
    #[arg(long)]
    run_id: Option<String>,

    /// Base URL of a single-node VictoriaMetrics service.
    #[arg(long, default_value = "http://127.0.0.1:8428")]
    victoria_metrics_url: String,

    /// Record metrics locally without pushing to VictoriaMetrics.
    #[arg(long)]
    no_victoria_metrics: bool,

    /// Permit a short diagnostic run that completes no full pressure/recovery cycle.
    #[arg(long)]
    allow_no_capacity_cycle: bool,

    /// Preserve the drained Camus root after a successful run.
    #[arg(long)]
    keep_data: bool,
}

#[derive(Clone, Debug, Args)]
struct ReportArgs {
    /// `run.json` written by a completed smoke run.
    #[arg(long)]
    run_metadata: PathBuf,

    /// Base URL of the single-node VictoriaMetrics service used by the run.
    #[arg(long, default_value = "http://127.0.0.1:8428")]
    victoria_metrics_url: String,

    /// Directory for `victoria-export.jsonl` and `report.md`.
    #[arg(long)]
    output_directory: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RunMetadata {
    schema_version: u32,
    run_id: String,
    git_commit: Option<String>,
    git_dirty: Option<bool>,
    started_unix_ms: u64,
    ended_unix_ms: u64,
    elapsed_seconds: f64,
    configuration: RunArgs,
    samples: u64,
    exporter: ExporterSummary,
    validation: ValidationSummary,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Run(arguments) => run(arguments).await,
        Command::Report(arguments) => report(arguments),
    }
}

fn report(arguments: ReportArgs) -> Result<()> {
    let reader = std::io::BufReader::new(
        File::open(&arguments.run_metadata)
            .with_context(|| format!("open run metadata {}", arguments.run_metadata.display()))?,
    );
    let metadata: RunMetadata = serde_json::from_reader(reader)
        .with_context(|| format!("read run metadata {}", arguments.run_metadata.display()))?;
    ensure!(
        metadata.schema_version == 1,
        "unsupported long-smoke metadata schema {}",
        metadata.schema_version
    );
    ensure!(
        metadata.exporter.enabled,
        "run metadata was created without VictoriaMetrics"
    );
    let output_directory = arguments.output_directory.unwrap_or_else(|| {
        arguments
            .run_metadata
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    });
    std::fs::create_dir_all(&output_directory)
        .with_context(|| format!("create report directory {}", output_directory.display()))?;
    telemetry::write_victoria_report(
        &arguments.victoria_metrics_url,
        &output_directory,
        &metadata,
    )?;
    println!("report: {}", output_directory.join("report.md").display());
    Ok(())
}

async fn run(arguments: RunArgs) -> Result<()> {
    validate(&arguments)?;
    let run_id = arguments.run_id.clone().unwrap_or_else(default_run_id);
    validate_run_id(&run_id)?;

    let output_directory = arguments.output_directory.join(&run_id);
    let data_directory = arguments.data_directory.join(&run_id);
    ensure!(
        !output_directory.exists(),
        "output directory already exists: {}",
        output_directory.display()
    );
    ensure!(
        !data_directory.exists(),
        "data directory already exists: {}",
        data_directory.display()
    );
    std::fs::create_dir_all(&output_directory)
        .with_context(|| format!("create output directory {}", output_directory.display()))?;
    std::fs::create_dir_all(&data_directory)
        .with_context(|| format!("create data directory {}", data_directory.display()))?;

    let started_unix_ms = unix_ms();
    let started = Instant::now();
    let config = storage_config(&arguments, &data_directory)?;
    let log = Log::open(config.clone()).await.context("open smoke root")?;
    let measurements = Measurements::new()?;
    let (control_tx, control_rx) = watch::channel(Control::new(true, false));
    let (stop_tx, stop_rx) = watch::channel(false);
    let (failure_tx, mut failure_rx) = mpsc::unbounded_channel::<String>();
    let mut workers = JoinSet::new();

    for producer_id in 0..arguments.producers {
        let stream_id = producer_id % arguments.streams;
        let stream = log.stream(camus::StreamId::new(
            u64::try_from(stream_id).context("stream ID overflow")?,
        ));
        let control = control_rx.clone();
        let stop = stop_rx.clone();
        let measurements = measurements.clone();
        let failure_tx = failure_tx.clone();
        let metadata_bytes = arguments.metadata_bytes;
        let payload_bytes = arguments.payload_bytes;
        let batch_records = arguments.append_batch_records;
        workers.spawn(async move {
            let result = producer(
                ProducerConfig {
                    producer_id,
                    metadata_bytes,
                    payload_bytes,
                    batch_records,
                },
                stream,
                control,
                stop,
                measurements,
            )
            .await;
            if let Err(error) = &result {
                let _ = failure_tx.send(format!("producer {producer_id}: {error:#}"));
            }
            result
        });
    }
    for stream_index in 0..arguments.streams {
        let stream = log.stream(camus::StreamId::new(
            u64::try_from(stream_index).context("stream ID overflow")?,
        ));
        let control = control_rx.clone();
        let stop = stop_rx.clone();
        let measurements = measurements.clone();
        let failure_tx = failure_tx.clone();
        let max_records = arguments.read_batch_records;
        let max_bytes = u64::try_from(arguments.payload_bytes)
            .context("payload size overflow")?
            .checked_mul(u64::try_from(max_records).context("read batch size overflow")?)
            .context("read payload bound overflow")?;
        workers.spawn(async move {
            let result = consumer(
                ConsumerConfig {
                    stream_index,
                    stream_count: arguments.streams,
                    max_records,
                    max_bytes,
                },
                stream,
                control,
                stop,
                measurements,
            )
            .await;
            if let Err(error) = &result {
                let _ = failure_tx.send(format!("consumer {stream_index}: {error:#}"));
            }
            result
        });
    }
    drop(failure_tx);

    let metrics_path = output_directory.join("metrics.prom");
    let mut local_metrics = BufWriter::new(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&metrics_path)
            .with_context(|| format!("create local metrics file {}", metrics_path.display()))?,
    );
    let exporter = if arguments.no_victoria_metrics {
        None
    } else {
        Some(VmExporter::start(&arguments.victoria_metrics_url)?)
    };

    let mut phase = Phase::Warmup;
    let mut phase_started = Instant::now();
    let mut completed_cycles = 0_u64;
    let mut samples = 0_u64;
    let mut last_sample = Instant::now();
    let mut last_metric_timestamp_ms = started_unix_ms;
    let mut last_pressure_waits = 0_u64;
    let run_deadline = started + Duration::from_secs(arguments.duration_seconds);
    let mut control_tick = interval(Duration::from_millis(100));
    control_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut sample_tick = interval(Duration::from_secs(arguments.sample_interval_seconds));
    sample_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    sample_tick.tick().await;

    loop {
        tokio::select! {
            _ = control_tick.tick() => {
                let now = Instant::now();
                let stats = log.stats();
                let utilization = capacity_utilization(&stats);
                if now >= run_deadline && phase != Phase::FinalDrain {
                    transition(
                        &mut phase,
                        Phase::FinalDrain,
                        &mut phase_started,
                        &control_tx,
                        false,
                        true,
                    );
                }
                match phase {
                    Phase::Warmup => {
                        if utilization >= arguments.steady_high_ratio {
                            transition(
                                &mut phase,
                                Phase::Steady,
                                &mut phase_started,
                                &control_tx,
                                true,
                                true,
                            );
                        } else if phase_started.elapsed() > Duration::from_secs(arguments.fill_timeout_seconds) {
                            bail!("warmup did not reach the steady high waterline before timeout");
                        }
                    }
                    Phase::Steady => {
                        let current = *control_tx.borrow();
                        let consumers_enabled = if utilization >= arguments.steady_high_ratio {
                            true
                        } else if utilization <= arguments.steady_low_ratio {
                            false
                        } else {
                            current.consumers_enabled
                        };
                        if current.consumers_enabled != consumers_enabled {
                            control_tx.send_replace(Control::new(true, consumers_enabled));
                        }
                        if phase_started.elapsed() >= Duration::from_secs(arguments.steady_seconds) {
                            last_pressure_waits = stats.pressure.capacity_wait.waits;
                            transition(
                                &mut phase,
                                Phase::PressureFill,
                                &mut phase_started,
                                &control_tx,
                                true,
                                false,
                            );
                        }
                    }
                    Phase::PressureFill => {
                        let pressure_wait_observed = stats.pressure.capacity_wait.current > 0
                            || stats.pressure.capacity_wait.waits > last_pressure_waits;
                        if pressure_wait_observed && utilization >= arguments.pressure_high_ratio {
                            transition(
                                &mut phase,
                                Phase::PressureHold,
                                &mut phase_started,
                                &control_tx,
                                true,
                                false,
                            );
                        } else if phase_started.elapsed() > Duration::from_secs(arguments.fill_timeout_seconds) {
                            bail!("pressure fill did not produce a capacity-blocked append before timeout");
                        }
                    }
                    Phase::PressureHold => {
                        if phase_started.elapsed() >= Duration::from_secs(arguments.pressure_hold_seconds) {
                            transition(
                                &mut phase,
                                Phase::Recovery,
                                &mut phase_started,
                                &control_tx,
                                false,
                                true,
                            );
                        }
                    }
                    Phase::Recovery => {
                        let appends_idle = measurements.active_append_calls.load(Ordering::Acquire) == 0;
                        if appends_idle && utilization <= arguments.recovery_low_ratio {
                            completed_cycles = completed_cycles.saturating_add(1);
                            transition(
                                &mut phase,
                                Phase::Steady,
                                &mut phase_started,
                                &control_tx,
                                true,
                                false,
                            );
                        } else if phase_started.elapsed() > Duration::from_secs(arguments.recovery_timeout_seconds) {
                            bail!("recovery did not reach the low waterline before timeout");
                        }
                    }
                    Phase::FinalDrain => {
                        let appends_idle = measurements.active_append_calls.load(Ordering::Acquire) == 0;
                        if appends_idle && stats.storage.pending_records == 0 {
                            break;
                        }
                        if phase_started.elapsed() > Duration::from_secs(arguments.drain_timeout_seconds) {
                            bail!("final drain did not empty the root before timeout");
                        }
                    }
                }
            }
            _ = sample_tick.tick() => {
                let now = Instant::now();
                let interval_elapsed = now.saturating_duration_since(last_sample);
                last_sample = now;
                let timestamp_ms = unix_ms().max(last_metric_timestamp_ms.saturating_add(1));
                last_metric_timestamp_ms = timestamp_ms;
                let sample = Sample::capture(
                    timestamp_ms,
                    started.elapsed(),
                    interval_elapsed,
                    phase,
                    completed_cycles,
                    &log,
                    &measurements,
                    &data_directory,
                    exporter.as_ref(),
                );
                write_sample(&mut local_metrics, &run_id, &sample)?;
                if let Some(exporter) = &exporter {
                    exporter.submit(sample.prometheus(&run_id))?;
                }
                samples = samples.saturating_add(1);
                eprintln!(
                    "run={} phase={} elapsed={:.0}s waterline={:.1}% pending={} append={:.0}/s release={:.0}/s capacity_waiters={}",
                    run_id,
                    phase.as_str(),
                    started.elapsed().as_secs_f64(),
                    sample.capacity_utilization_ratio * 100.0,
                    sample.stats.storage.pending_records,
                    sample.append.records_per_second(interval_elapsed),
                    sample.release.records_per_second(interval_elapsed),
                    sample.stats.pressure.capacity_wait.current,
                );
            }
            failure = failure_rx.recv() => {
                match failure {
                    Some(failure) => bail!("smoke worker failed: {failure}"),
                    None => bail!("all smoke workers stopped before final drain"),
                }
            }
        }
    }

    stop_tx.send_replace(true);
    while let Some(result) = workers.join_next().await {
        result.context("join smoke worker")??;
    }

    log.reclaim().await.context("final explicit reclaim")?;
    let final_stats = log.stats();
    ensure!(
        final_stats.storage.pending_records == 0,
        "final stats still contain pending records"
    );
    ensure!(
        final_stats.commits.append_records == final_stats.commits.release_records,
        "durable append and release record totals differ"
    );
    ensure!(
        final_stats.operations.read.records == final_stats.commits.append_records
            && final_stats.operations.release.records == final_stats.commits.release_records,
        "read or release delivered a record more than once after durable release"
    );
    ensure!(
        final_stats.operations.append.failed == 0
            && final_stats.operations.release.failed == 0
            && final_stats.operations.read.failed == 0,
        "one or more logical operations failed"
    );
    ensure!(
        final_stats.operations.append.cancelled == 0
            && final_stats.operations.release.cancelled == 0,
        "a mutation operation was cancelled"
    );
    ensure!(
        measurements.integrity_errors.load(Ordering::Acquire) == 0,
        "record integrity validation failed"
    );
    if !arguments.allow_no_capacity_cycle {
        ensure!(
            completed_cycles > 0,
            "the run completed no full pressure and recovery cycle"
        );
        ensure!(
            final_stats.pressure.capacity_wait.waits > 0,
            "the run observed no capacity-blocked append"
        );
    }

    let final_timestamp_ms = unix_ms().max(last_metric_timestamp_ms.saturating_add(1));
    let final_sample = Sample::capture(
        final_timestamp_ms,
        started.elapsed(),
        last_sample.elapsed(),
        Phase::FinalDrain,
        completed_cycles,
        &log,
        &measurements,
        &data_directory,
        exporter.as_ref(),
    );
    write_sample(&mut local_metrics, &run_id, &final_sample)?;
    if let Some(exporter) = &exporter {
        exporter.submit(final_sample.prometheus(&run_id))?;
    }
    samples = samples.saturating_add(1);
    local_metrics.flush().context("flush local metrics")?;

    log.shutdown().await.context("shut down smoke root")?;
    drop(log);
    let reopened = Log::open(config)
        .await
        .context("reopen drained smoke root")?;
    let recovery_stats = reopened.stats();
    ensure!(
        recovery_stats.storage.pending_records == 0,
        "reopened smoke root recovered pending records after complete drain"
    );
    reopened
        .shutdown()
        .await
        .context("shut down reopened root")?;

    let validation = ValidationSummary {
        passed: true,
        completed_capacity_cycles: completed_cycles,
        appended_records: final_stats.commits.append_records,
        released_records: final_stats.commits.release_records,
        pending_records_after_drain: final_stats.storage.pending_records,
        pending_records_after_reopen: recovery_stats.storage.pending_records,
        capacity_waits: final_stats.pressure.capacity_wait.waits,
        integrity_errors: measurements.integrity_errors.load(Ordering::Acquire),
        reclaimed_segments: final_stats.maintenance.reclaimed_segments,
        reclaimed_bytes: final_stats.maintenance.reclaimed_bytes,
        recovery_records_scanned: recovery_stats.recovery.records_scanned,
        read_cancellations: final_stats.operations.read.cancelled,
    };
    let validation_metrics = telemetry::validation_metrics(&run_id, unix_ms(), &validation);
    local_metrics
        .write_all(validation_metrics.as_bytes())
        .context("write local validation metrics")?;
    local_metrics
        .flush()
        .context("flush local validation metrics")?;
    if let Some(exporter) = &exporter {
        exporter.submit(validation_metrics)?;
    }
    let exporter_summary = match exporter {
        Some(exporter) => exporter.finish().await?,
        None => ExporterSummary::disabled(),
    };
    ensure!(
        exporter_summary.failed_batches == 0 && exporter_summary.dropped_batches == 0,
        "VictoriaMetrics exporter lost one or more samples"
    );

    let ended_unix_ms = unix_ms();
    let metadata = RunMetadata {
        schema_version: 1,
        run_id: run_id.clone(),
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        git_dirty: command_output("git", &["status", "--porcelain"])
            .map(|status| !status.is_empty()),
        started_unix_ms,
        ended_unix_ms,
        elapsed_seconds: started.elapsed().as_secs_f64(),
        configuration: arguments.clone(),
        samples,
        exporter: exporter_summary,
        validation,
    };
    write_json(&output_directory.join("run.json"), &metadata)?;
    if !arguments.no_victoria_metrics {
        telemetry::write_victoria_report(
            &arguments.victoria_metrics_url,
            &output_directory,
            &metadata,
        )?;
    }

    if !arguments.keep_data {
        std::fs::remove_dir_all(&data_directory)
            .with_context(|| format!("remove drained smoke root {}", data_directory.display()))?;
    }
    println!("run_id: {run_id}");
    if arguments.no_victoria_metrics {
        println!("metrics: {}", metrics_path.display());
    } else {
        println!("report: {}", output_directory.join("report.md").display());
    }
    Ok(())
}

fn validate(arguments: &RunArgs) -> Result<()> {
    ensure!(arguments.duration_seconds > 0, "duration must be positive");
    ensure!(
        arguments.sample_interval_seconds > 0,
        "sample interval must be positive"
    );
    ensure!(arguments.steady_seconds > 0, "steady time must be positive");
    ensure!(
        arguments.pressure_hold_seconds > 0,
        "pressure hold time must be positive"
    );
    ensure!(arguments.streams > 0, "stream count must be positive");
    ensure!(arguments.producers > 0, "producer count must be positive");
    ensure!(
        arguments.append_batch_records > 0,
        "append batch must be positive"
    );
    ensure!(
        arguments.read_batch_records > 0,
        "read batch must be positive"
    );
    ensure!(
        arguments.metadata_bytes >= workload::METADATA_HEADER_BYTES,
        "metadata must be at least {} bytes",
        workload::METADATA_HEADER_BYTES
    );
    ensure!(arguments.payload_bytes > 0, "payload must be non-empty");
    ensure!(
        0.0 < arguments.recovery_low_ratio
            && arguments.recovery_low_ratio < arguments.steady_low_ratio
            && arguments.steady_low_ratio < arguments.steady_high_ratio
            && arguments.steady_high_ratio < arguments.pressure_high_ratio
            && arguments.pressure_high_ratio < 1.0,
        "waterline ratios must satisfy 0 < recovery < steady-low < steady-high < pressure-high < 1"
    );
    ensure!(
        arguments.capacity_bytes >= arguments.segment_bytes.saturating_mul(3),
        "capacity must be at least three segment sizes"
    );
    ensure!(
        arguments.command_queue_capacity > 0,
        "command queue capacity must be positive"
    );
    Ok(())
}

fn storage_config(arguments: &RunArgs, root: &Path) -> Result<Config> {
    let record_bound = u64::try_from(
        arguments
            .metadata_bytes
            .checked_add(arguments.payload_bytes)
            .and_then(|bytes| bytes.checked_add(128))
            .context("record bound overflow")?,
    )
    .context("record bound does not fit u64")?;
    let max_epoch_bytes = record_bound
        .checked_mul(
            u64::try_from(arguments.append_batch_records)
                .context("append batch does not fit u64")?,
        )
        .and_then(|bytes| bytes.checked_add(256))
        .context("epoch bound overflow")?;
    ensure!(
        max_epoch_bytes.saturating_add(1024) < arguments.segment_bytes,
        "configured record batch is too large for the segment size"
    );
    let release_bound = 72_u64
        .checked_add(
            u64::try_from(arguments.read_batch_records)
                .context("read batch does not fit u64")?
                .checked_mul(16)
                .context("release bound overflow")?,
        )
        .context("release bound overflow")?;
    let max_commit_bytes = (8 * MIB)
        .min(arguments.segment_bytes / 2)
        .max(max_epoch_bytes)
        .max(release_bound);
    let mut config = Config::new(
        root,
        Capacity::Bounded {
            total_bytes: arguments.capacity_bytes,
            when_full: FullPolicy::Block,
        },
    )
    .with_segment_bytes(arguments.segment_bytes)
    .with_max_epoch_bytes(max_epoch_bytes)
    .with_max_release_records(arguments.read_batch_records)
    .with_max_commit_units(arguments.producers.min(64))
    .with_max_commit_bytes(max_commit_bytes)
    .with_command_queue_capacity(arguments.command_queue_capacity)
    .with_detailed_observability();
    if arguments.steady_seconds > 0 {
        config = config.with_max_segment_age(Duration::from_secs(5 * 60));
    }
    Ok(config)
}

fn transition(
    phase: &mut Phase,
    next: Phase,
    phase_started: &mut Instant,
    control: &watch::Sender<Control>,
    producers_enabled: bool,
    consumers_enabled: bool,
) {
    eprintln!("phase transition: {} -> {}", phase.as_str(), next.as_str());
    *phase = next;
    *phase_started = Instant::now();
    control.send_replace(Control::new(producers_enabled, consumers_enabled));
}

fn capacity_utilization(stats: &RootStats) -> f64 {
    let Some(capacity) = stats.storage.configured_capacity_bytes else {
        return 0.0;
    };
    if capacity == 0 {
        return 0.0;
    }
    stats
        .storage
        .actual_file_bytes
        .saturating_add(stats.storage.maintenance_headroom_bytes) as f64
        / capacity as f64
}

fn write_sample(writer: &mut BufWriter<File>, run_id: &str, sample: &Sample) -> Result<()> {
    writer
        .write_all(sample.prometheus(run_id).as_bytes())
        .context("write local metrics sample")?;
    writer.flush().context("flush local metrics sample")?;
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    serde_json::to_writer_pretty(writer, value).with_context(|| format!("write {}", path.display()))
}

fn validate_run_id(value: &str) -> Result<()> {
    ensure!(!value.is_empty(), "run ID must not be empty");
    ensure!(value.len() <= 64, "run ID must be at most 64 bytes");
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
        "run ID may contain only ASCII letters, digits, '_' and '-'"
    );
    Ok(())
}

fn default_run_id() -> String {
    format!(
        "camus-{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        std::process::id()
    )
}

fn unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn command_output(program: &str, arguments: &[&str]) -> Option<String> {
    let output = ProcessCommand::new(program).args(arguments).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}
