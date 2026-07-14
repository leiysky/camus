use super::workload::{Measurements, OperationInterval, Phase, LATENCY_BUCKETS_NS};
use super::RunMetadata;
use anyhow::{ensure, Context, Result};
use camus::{DurationStats, Log, OperationCounters, RootHealth, RootStats, WaitStats};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const METRIC_PREFIX: &str = "camus_long_smoke_";

pub(crate) struct Sample {
    pub(crate) timestamp_ms: u64,
    pub(crate) run_elapsed: Duration,
    pub(crate) interval_elapsed: Duration,
    pub(crate) phase: Phase,
    pub(crate) completed_cycles: u64,
    pub(crate) stats: RootStats,
    pub(crate) health: RootHealth,
    pub(crate) append: OperationInterval,
    pub(crate) read: OperationInterval,
    pub(crate) release: OperationInterval,
    pub(crate) capacity_utilization_ratio: f64,
    filesystem_available_bytes: Option<u64>,
    process_resident_memory_bytes: Option<u64>,
    integrity_errors: u64,
    exporter: ExporterSummary,
}

impl Sample {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn capture(
        timestamp_ms: u64,
        run_elapsed: Duration,
        interval_elapsed: Duration,
        phase: Phase,
        completed_cycles: u64,
        log: &Log,
        measurements: &Measurements,
        data_directory: &Path,
        exporter: Option<&VmExporter>,
    ) -> Self {
        let stats = log.stats();
        let [append, read, release] = measurements.take_intervals();
        let capacity_utilization_ratio = stats
            .storage
            .configured_capacity_bytes
            .filter(|capacity| *capacity > 0)
            .map_or(0.0, |capacity| {
                stats
                    .storage
                    .actual_file_bytes
                    .saturating_add(stats.storage.maintenance_headroom_bytes) as f64
                    / capacity as f64
            });
        Self {
            timestamp_ms,
            run_elapsed,
            interval_elapsed,
            phase,
            completed_cycles,
            stats,
            health: log.health(),
            append,
            read,
            release,
            capacity_utilization_ratio,
            filesystem_available_bytes: fs2::available_space(data_directory).ok(),
            process_resident_memory_bytes: resident_memory_bytes(),
            integrity_errors: measurements.integrity_errors.load(Ordering::Acquire),
            exporter: exporter.map_or_else(ExporterSummary::disabled, VmExporter::summary),
        }
    }

    pub(crate) fn prometheus(&self, run_id: &str) -> String {
        let mut output = String::with_capacity(12 * 1024);
        metric(
            &mut output,
            "run_elapsed_seconds",
            run_id,
            &[],
            self.run_elapsed.as_secs_f64(),
            self.timestamp_ms,
        );
        metric(
            &mut output,
            "interval_duration_seconds",
            run_id,
            &[("phase", self.phase.as_str())],
            self.interval_elapsed.as_secs_f64(),
            self.timestamp_ms,
        );
        metric(
            &mut output,
            "phase_info",
            run_id,
            &[("phase", self.phase.as_str())],
            1,
            self.timestamp_ms,
        );
        metric(
            &mut output,
            "completed_capacity_cycles_total",
            run_id,
            &[],
            self.completed_cycles,
            self.timestamp_ms,
        );
        metric(
            &mut output,
            "root_state",
            run_id,
            &[("state", self.health.state.as_str())],
            1,
            self.timestamp_ms,
        );
        metric(
            &mut output,
            "root_failure",
            run_id,
            &[],
            u8::from(self.health.failure.is_some()),
            self.timestamp_ms,
        );
        metric(
            &mut output,
            "capacity_utilization_ratio",
            run_id,
            &[],
            self.capacity_utilization_ratio,
            self.timestamp_ms,
        );
        storage_metrics(&mut output, run_id, self.timestamp_ms, &self.stats);
        pressure_metrics(&mut output, run_id, self.timestamp_ms, &self.stats);
        operation_counters(&mut output, run_id, self.timestamp_ms, &self.stats);
        commit_metrics(&mut output, run_id, self.timestamp_ms, &self.stats);
        maintenance_metrics(&mut output, run_id, self.timestamp_ms, &self.stats);
        recovery_metrics(&mut output, run_id, self.timestamp_ms, &self.stats);
        for interval in [&self.append, &self.read, &self.release] {
            interval_metrics(
                &mut output,
                run_id,
                self.timestamp_ms,
                self.phase,
                self.interval_elapsed,
                interval,
            );
        }
        metric(
            &mut output,
            "integrity_errors_total",
            run_id,
            &[],
            self.integrity_errors,
            self.timestamp_ms,
        );
        if let Some(bytes) = self.filesystem_available_bytes {
            metric(
                &mut output,
                "filesystem_available_bytes",
                run_id,
                &[],
                bytes,
                self.timestamp_ms,
            );
        }
        if let Some(bytes) = self.process_resident_memory_bytes {
            metric(
                &mut output,
                "process_resident_memory_bytes",
                run_id,
                &[],
                bytes,
                self.timestamp_ms,
            );
        }
        metric(
            &mut output,
            "metrics_pushed_batches_total",
            run_id,
            &[],
            self.exporter.pushed_batches,
            self.timestamp_ms,
        );
        metric(
            &mut output,
            "metrics_failed_batches_total",
            run_id,
            &[],
            self.exporter.failed_batches,
            self.timestamp_ms,
        );
        metric(
            &mut output,
            "metrics_dropped_batches_total",
            run_id,
            &[],
            self.exporter.dropped_batches,
            self.timestamp_ms,
        );
        output
    }
}

fn storage_metrics(output: &mut String, run_id: &str, timestamp: u64, stats: &RootStats) {
    let storage = stats.storage;
    if let Some(capacity) = storage.configured_capacity_bytes {
        metric(output, "capacity_bytes", run_id, &[], capacity, timestamp);
    }
    metric(
        output,
        "actual_file_bytes",
        run_id,
        &[],
        storage.actual_file_bytes,
        timestamp,
    );
    metric(
        output,
        "maintenance_headroom_bytes",
        run_id,
        &[],
        storage.maintenance_headroom_bytes,
        timestamp,
    );
    if let Some(bytes) = storage.data_admissible_bytes {
        metric(
            output,
            "data_admissible_bytes",
            run_id,
            &[],
            bytes,
            timestamp,
        );
    }
    metric(
        output,
        "pending_records",
        run_id,
        &[],
        storage.pending_records,
        timestamp,
    );
    metric(
        output,
        "pending_payload_bytes",
        run_id,
        &[],
        storage.pending_payload_bytes,
        timestamp,
    );
    for (kind, value) in [
        ("live", storage.live_segments),
        ("sealed", storage.sealed_segments),
        ("reclaimable", storage.reclaimable_segments),
    ] {
        metric(
            output,
            "segments",
            run_id,
            &[("kind", kind)],
            value,
            timestamp,
        );
    }
    metric(
        output,
        "reclaimable_bytes",
        run_id,
        &[],
        storage.reclaimable_bytes,
        timestamp,
    );
}

fn pressure_metrics(output: &mut String, run_id: &str, timestamp: u64, stats: &RootStats) {
    let pressure = stats.pressure;
    metric(
        output,
        "command_queue_depth",
        run_id,
        &[],
        pressure.queue_depth,
        timestamp,
    );
    metric(
        output,
        "active_storage_jobs",
        run_id,
        &[],
        pressure.active_storage_jobs,
        timestamp,
    );
    metric(
        output,
        "admitted_commands_total",
        run_id,
        &[],
        pressure.admitted_commands,
        timestamp,
    );
    wait_metrics(output, run_id, timestamp, "queue", pressure.queue_wait);
    wait_metrics(
        output,
        run_id,
        timestamp,
        "readiness",
        pressure.readiness_wait,
    );
    wait_metrics(
        output,
        run_id,
        timestamp,
        "capacity",
        pressure.capacity_wait,
    );
    duration_metrics(
        output,
        run_id,
        timestamp,
        "storage_job",
        pressure.storage_job_elapsed,
    );
}

fn wait_metrics(output: &mut String, run_id: &str, timestamp: u64, kind: &str, wait: WaitStats) {
    metric(
        output,
        "waiters",
        run_id,
        &[("kind", kind)],
        wait.current,
        timestamp,
    );
    metric(
        output,
        "waits_total",
        run_id,
        &[("kind", kind)],
        wait.waits,
        timestamp,
    );
    duration_metrics(output, run_id, timestamp, kind, wait.elapsed);
}

fn duration_metrics(
    output: &mut String,
    run_id: &str,
    timestamp: u64,
    kind: &str,
    duration: DurationStats,
) {
    metric(
        output,
        "duration_observations_total",
        run_id,
        &[("kind", kind)],
        duration.observations,
        timestamp,
    );
    metric(
        output,
        "duration_seconds_total",
        run_id,
        &[("kind", kind)],
        duration.total.as_secs_f64(),
        timestamp,
    );
    metric(
        output,
        "duration_seconds_max",
        run_id,
        &[("kind", kind)],
        duration.max.as_secs_f64(),
        timestamp,
    );
}

fn operation_counters(output: &mut String, run_id: &str, timestamp: u64, stats: &RootStats) {
    for (operation, counters) in [
        ("append", stats.operations.append),
        ("read", stats.operations.read),
        ("release", stats.operations.release),
        ("reclaim", stats.operations.reclaim),
    ] {
        logical_operation_metrics(output, run_id, timestamp, operation, counters);
    }
}

fn logical_operation_metrics(
    output: &mut String,
    run_id: &str,
    timestamp: u64,
    operation: &str,
    counters: OperationCounters,
) {
    for (outcome, value) in [
        ("started", counters.started),
        ("succeeded", counters.succeeded),
        ("failed", counters.failed),
        ("cancelled", counters.cancelled),
    ] {
        metric(
            output,
            "logical_operations_total",
            run_id,
            &[("operation", operation), ("outcome", outcome)],
            value,
            timestamp,
        );
    }
    metric(
        output,
        "logical_operation_records_total",
        run_id,
        &[("operation", operation)],
        counters.records,
        timestamp,
    );
    metric(
        output,
        "logical_operation_payload_bytes_total",
        run_id,
        &[("operation", operation)],
        counters.payload_bytes,
        timestamp,
    );
    duration_metrics(output, run_id, timestamp, operation, counters.elapsed);
}

fn commit_metrics(output: &mut String, run_id: &str, timestamp: u64, stats: &RootStats) {
    let commits = stats.commits;
    for (kind, groups, units, records, bytes, max_units, max_bytes) in [
        (
            "append",
            commits.append_groups,
            commits.append_units,
            commits.append_records,
            commits.append_encoded_bytes,
            commits.max_append_units,
            commits.max_append_encoded_bytes,
        ),
        (
            "release",
            commits.release_groups,
            commits.release_units,
            commits.release_records,
            commits.release_encoded_bytes,
            commits.max_release_units,
            commits.max_release_encoded_bytes,
        ),
    ] {
        metric(
            output,
            "commit_groups_total",
            run_id,
            &[("kind", kind)],
            groups,
            timestamp,
        );
        metric(
            output,
            "commit_units_total",
            run_id,
            &[("kind", kind)],
            units,
            timestamp,
        );
        metric(
            output,
            "commit_records_total",
            run_id,
            &[("kind", kind)],
            records,
            timestamp,
        );
        metric(
            output,
            "commit_encoded_bytes_total",
            run_id,
            &[("kind", kind)],
            bytes,
            timestamp,
        );
        metric(
            output,
            "commit_units_max",
            run_id,
            &[("kind", kind)],
            max_units,
            timestamp,
        );
        metric(
            output,
            "commit_encoded_bytes_max",
            run_id,
            &[("kind", kind)],
            max_bytes,
            timestamp,
        );
    }
}

fn maintenance_metrics(output: &mut String, run_id: &str, timestamp: u64, stats: &RootStats) {
    let maintenance = stats.maintenance;
    for (kind, value) in [
        ("automatic_reclaim", maintenance.automatic_reclaim_passes),
        ("explicit_reclaim", maintenance.explicit_reclaim_passes),
        ("size_rollover", maintenance.size_rollovers),
        ("age_rollover", maintenance.age_rollovers),
        ("reclaim_rollover", maintenance.reclaim_rollovers),
        ("manifest_compaction", maintenance.manifest_compactions),
    ] {
        metric(
            output,
            "maintenance_actions_total",
            run_id,
            &[("kind", kind)],
            value,
            timestamp,
        );
    }
    metric(
        output,
        "maintenance_reclaimed_segments_total",
        run_id,
        &[],
        maintenance.reclaimed_segments,
        timestamp,
    );
    metric(
        output,
        "maintenance_reclaimed_bytes_total",
        run_id,
        &[],
        maintenance.reclaimed_bytes,
        timestamp,
    );
}

fn recovery_metrics(output: &mut String, run_id: &str, timestamp: u64, stats: &RootStats) {
    let recovery = stats.recovery;
    for (kind, value) in [
        ("manifest_frames", recovery.manifest_frames_scanned),
        ("segments", recovery.segments_scanned),
        ("epochs", recovery.epochs_scanned),
        ("records", recovery.records_scanned),
    ] {
        metric(
            output,
            "recovery_scanned_total",
            run_id,
            &[("kind", kind)],
            value,
            timestamp,
        );
    }
    metric(
        output,
        "recovery_elapsed_seconds",
        run_id,
        &[],
        recovery.elapsed.as_secs_f64(),
        timestamp,
    );
}

fn interval_metrics(
    output: &mut String,
    run_id: &str,
    timestamp: u64,
    phase: Phase,
    elapsed: Duration,
    interval: &OperationInterval,
) {
    let labels = [("operation", interval.name), ("phase", phase.as_str())];
    metric(
        output,
        "interval_operation_calls",
        run_id,
        &labels,
        interval.calls,
        timestamp,
    );
    metric(
        output,
        "interval_operation_records",
        run_id,
        &labels,
        interval.records,
        timestamp,
    );
    metric(
        output,
        "interval_operation_payload_bytes",
        run_id,
        &labels,
        interval.payload_bytes,
        timestamp,
    );
    metric(
        output,
        "interval_operation_failures",
        run_id,
        &labels,
        interval.failures,
        timestamp,
    );
    metric(
        output,
        "operation_records_per_second",
        run_id,
        &labels,
        interval.records_per_second(elapsed),
        timestamp,
    );
    for (quantile, nanos) in [
        ("0.50", interval.p50_ns),
        ("0.95", interval.p95_ns),
        ("0.99", interval.p99_ns),
    ] {
        metric(
            output,
            "interval_operation_latency_seconds",
            run_id,
            &[
                ("operation", interval.name),
                ("phase", phase.as_str()),
                ("quantile", quantile),
            ],
            nanos as f64 / 1_000_000_000.0,
            timestamp,
        );
    }
    metric(
        output,
        "interval_operation_latency_seconds_max",
        run_id,
        &labels,
        interval.max_ns as f64 / 1_000_000_000.0,
        timestamp,
    );
    for ((upper, _), count) in LATENCY_BUCKETS_NS.iter().zip(&interval.cumulative_buckets) {
        metric(
            output,
            "interval_operation_latency_bucket",
            run_id,
            &[
                ("operation", interval.name),
                ("phase", phase.as_str()),
                ("le", upper),
            ],
            *count,
            timestamp,
        );
    }
}

fn metric(
    output: &mut String,
    suffix: &str,
    run_id: &str,
    labels: &[(&str, &str)],
    value: impl std::fmt::Display,
    timestamp_ms: u64,
) {
    let _ = write!(
        output,
        "{METRIC_PREFIX}{suffix}{{run_id=\"{}\"",
        escape_label(run_id)
    );
    for (name, value) in labels {
        let _ = write!(output, ",{name}=\"{}\"", escape_label(value));
    }
    let _ = writeln!(output, "}} {value} {timestamp_ms}");
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct ExporterSummary {
    pub(crate) enabled: bool,
    pub(crate) pushed_batches: u64,
    pub(crate) failed_batches: u64,
    pub(crate) dropped_batches: u64,
}

impl ExporterSummary {
    pub(crate) const fn disabled() -> Self {
        Self {
            enabled: false,
            pushed_batches: 0,
            failed_batches: 0,
            dropped_batches: 0,
        }
    }
}

#[derive(Default)]
struct ExporterStatus {
    pushed: AtomicU64,
    failed: AtomicU64,
    dropped: AtomicU64,
}

pub(crate) struct VmExporter {
    sender: Option<mpsc::Sender<String>>,
    status: Arc<ExporterStatus>,
    task: Option<JoinHandle<()>>,
}

impl VmExporter {
    pub(crate) fn start(base_url: &str) -> Result<Self> {
        ensure!(!base_url.trim().is_empty(), "VictoriaMetrics URL is empty");
        let endpoint = format!(
            "{}/api/v1/import/prometheus",
            base_url.trim_end_matches('/')
        );
        let (sender, mut receiver) = mpsc::channel::<String>(64);
        let status = Arc::new(ExporterStatus::default());
        let task_status = status.clone();
        let task = tokio::spawn(async move {
            while let Some(body) = receiver.recv().await {
                let endpoint = endpoint.clone();
                let result =
                    tokio::task::spawn_blocking(move || push_batch(&endpoint, &body)).await;
                match result {
                    Ok(Ok(())) => {
                        task_status.pushed.fetch_add(1, Ordering::AcqRel);
                    }
                    Ok(Err(error)) => {
                        task_status.failed.fetch_add(1, Ordering::AcqRel);
                        eprintln!("VictoriaMetrics push failed: {error:#}");
                    }
                    Err(error) => {
                        task_status.failed.fetch_add(1, Ordering::AcqRel);
                        eprintln!("VictoriaMetrics push task failed: {error}");
                    }
                }
            }
        });
        Ok(Self {
            sender: Some(sender),
            status,
            task: Some(task),
        })
    }

    pub(crate) fn submit(&self, body: String) -> Result<()> {
        let Some(sender) = &self.sender else {
            anyhow::bail!("VictoriaMetrics exporter is closed");
        };
        match sender.try_send(body) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.status.dropped.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("VictoriaMetrics exporter task stopped")
            }
        }
    }

    pub(crate) fn summary(&self) -> ExporterSummary {
        ExporterSummary {
            enabled: true,
            pushed_batches: self.status.pushed.load(Ordering::Acquire),
            failed_batches: self.status.failed.load(Ordering::Acquire),
            dropped_batches: self.status.dropped.load(Ordering::Acquire),
        }
    }

    pub(crate) async fn finish(mut self) -> Result<ExporterSummary> {
        self.sender.take();
        if let Some(task) = self.task.take() {
            task.await.context("join VictoriaMetrics exporter")?;
        }
        Ok(self.summary())
    }
}

fn push_batch(endpoint: &str, body: &str) -> Result<()> {
    let mut child = Command::new("curl")
        .args([
            "-fsS",
            "--retry",
            "3",
            "--retry-all-errors",
            "--connect-timeout",
            "2",
            "--max-time",
            "10",
            "-H",
            "Content-Type: text/plain",
            "-X",
            "POST",
            "--data-binary",
            "@-",
            endpoint,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("start curl for VictoriaMetrics push")?;
    child
        .stdin
        .take()
        .context("open curl stdin")?
        .write_all(body.as_bytes())
        .context("write VictoriaMetrics request")?;
    let output = child
        .wait_with_output()
        .context("wait for VictoriaMetrics push")?;
    ensure!(
        output.status.success(),
        "curl exited with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ValidationSummary {
    pub(crate) passed: bool,
    pub(crate) completed_capacity_cycles: u64,
    pub(crate) appended_records: u64,
    pub(crate) released_records: u64,
    pub(crate) pending_records_after_drain: u64,
    pub(crate) pending_records_after_reopen: u64,
    pub(crate) capacity_waits: u64,
    pub(crate) integrity_errors: u64,
    pub(crate) reclaimed_segments: u64,
    pub(crate) reclaimed_bytes: u64,
    pub(crate) recovery_records_scanned: u64,
    pub(crate) read_cancellations: u64,
}

pub(crate) fn validation_metrics(
    run_id: &str,
    timestamp_ms: u64,
    validation: &ValidationSummary,
) -> String {
    let mut output = String::new();
    metric(
        &mut output,
        "validation_passed",
        run_id,
        &[],
        u8::from(validation.passed),
        timestamp_ms,
    );
    metric(
        &mut output,
        "validation_pending_records_after_reopen",
        run_id,
        &[],
        validation.pending_records_after_reopen,
        timestamp_ms,
    );
    output
}

#[derive(Debug, Deserialize)]
struct ExportSeries {
    metric: BTreeMap<String, String>,
    values: Vec<f64>,
    timestamps: Vec<i64>,
}

pub(crate) fn write_victoria_report(
    base_url: &str,
    output_directory: &Path,
    metadata: &RunMetadata,
) -> Result<()> {
    let mut raw = String::new();
    let mut series = Vec::new();
    for attempt in 0..=10 {
        raw = export_metrics(
            base_url,
            &metadata.run_id,
            metadata.started_unix_ms,
            metadata.ended_unix_ms.saturating_add(1_000),
        )?;
        series = parse_export(&raw)?;
        let expected_samples = usize::try_from(metadata.samples).unwrap_or(usize::MAX);
        let capacity_ready =
            values(&series, "capacity_utilization_ratio", &[]).len() >= expected_samples;
        let intervals_ready =
            values(&series, "interval_duration_seconds", &[]).len() >= expected_samples;
        let validation_ready = max_value(&series, "validation_passed", &[]) == 1.0;
        if (capacity_ready && intervals_ready && validation_ready) || attempt == 10 {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    let export_path = output_directory.join("victoria-export.jsonl");
    std::fs::write(&export_path, &raw)
        .with_context(|| format!("write {}", export_path.display()))?;
    let utilization = values(&series, "capacity_utilization_ratio", &[]);
    ensure!(
        utilization.len() >= usize::try_from(metadata.samples).unwrap_or(usize::MAX),
        "VictoriaMetrics exposed only {} of {} accepted capacity samples",
        utilization.len(),
        metadata.samples
    );
    ensure!(
        values(&series, "interval_duration_seconds", &[]).len()
            >= usize::try_from(metadata.samples).unwrap_or(usize::MAX),
        "VictoriaMetrics export is missing one or more interval samples"
    );
    ensure!(
        max_value(&series, "validation_passed", &[]) == 1.0,
        "VictoriaMetrics export is missing the successful validation sample"
    );
    for operation in ["append", "read", "release"] {
        ensure!(
            sum_values(
                &series,
                "interval_operation_latency_bucket",
                &[("operation", operation), ("le", "+Inf")],
            ) > 0.0,
            "VictoriaMetrics export contains no {operation} latency observations"
        );
    }
    let waiters = values(&series, "waiters", &[("kind", "capacity")]);
    let capacity = summarize(&utilization);
    let pressure_target = metadata.configuration.pressure_high_ratio;
    let high_water_fraction = fraction(&utilization, |value| value >= pressure_target);
    let capacity_wait_fraction = fraction(&waiters, |value| value > 0.0);
    let peak_actual_bytes = max_value(&series, "actual_file_bytes", &[]);
    let peak_headroom_bytes = max_value(&series, "maintenance_headroom_bytes", &[]);
    let minimum_filesystem_bytes = values(&series, "filesystem_available_bytes", &[])
        .into_iter()
        .fold(f64::INFINITY, f64::min);
    let peak_resident_bytes = max_value(&series, "process_resident_memory_bytes", &[]);

    let mut report = String::new();
    writeln!(report, "# Camus long-running smoke report\n")?;
    writeln!(report, "- Run ID: `{}`", metadata.run_id)?;
    writeln!(
        report,
        "- Verdict: **{}**",
        if metadata.validation.passed {
            "PASS"
        } else {
            "FAIL"
        }
    )?;
    writeln!(
        report,
        "- Measured/finalization elapsed: {:.1} s",
        metadata.elapsed_seconds
    )?;
    writeln!(
        report,
        "- Metrics source: VictoriaMetrics export ({} capacity samples; {} local samples)\n",
        utilization.len(),
        metadata.samples
    )?;

    writeln!(report, "## Configuration\n")?;
    writeln!(report, "| Setting | Value |")?;
    writeln!(report, "| --- | ---: |")?;
    writeln!(
        report,
        "| Capacity / segment | {:.2} GiB / {:.2} MiB |",
        metadata.configuration.capacity_bytes as f64 / 1024_f64.powi(3),
        metadata.configuration.segment_bytes as f64 / 1024_f64.powi(2)
    )?;
    writeln!(
        report,
        "| Streams / producers | {} / {} |",
        metadata.configuration.streams, metadata.configuration.producers
    )?;
    writeln!(
        report,
        "| Append / read-release batch | {} / {} records |",
        metadata.configuration.append_batch_records, metadata.configuration.read_batch_records
    )?;
    writeln!(
        report,
        "| Metadata / payload | {} / {} bytes |",
        metadata.configuration.metadata_bytes, metadata.configuration.payload_bytes
    )?;
    writeln!(
        report,
        "| Target waterlines | recovery {:.0}% · steady {:.0}–{:.0}% · pressure ≥{:.0}% |\n",
        metadata.configuration.recovery_low_ratio * 100.0,
        metadata.configuration.steady_low_ratio * 100.0,
        metadata.configuration.steady_high_ratio * 100.0,
        metadata.configuration.pressure_high_ratio * 100.0
    )?;

    writeln!(report, "## Capacity and backpressure\n")?;
    writeln!(report, "| Metric | Result |")?;
    writeln!(report, "| --- | ---: |")?;
    writeln!(
        report,
        "| Safe-capacity utilization min / avg / p95 / max | {:.1}% / {:.1}% / {:.1}% / {:.1}% |",
        capacity.min * 100.0,
        capacity.average * 100.0,
        capacity.p95 * 100.0,
        capacity.max * 100.0
    )?;
    writeln!(
        report,
        "| Samples at or above pressure target ({:.0}%) | {:.1}% |",
        pressure_target * 100.0,
        high_water_fraction * 100.0
    )?;
    writeln!(
        report,
        "| Samples with blocked appenders | {:.1}% |",
        capacity_wait_fraction * 100.0
    )?;
    writeln!(
        report,
        "| Capacity waits / completed cycles | {} / {} |",
        metadata.validation.capacity_waits, metadata.validation.completed_capacity_cycles
    )?;
    writeln!(
        report,
        "| Reclaimed during the run | {} segments / {:.2} MiB |",
        metadata.validation.reclaimed_segments,
        metadata.validation.reclaimed_bytes as f64 / 1024_f64.powi(2)
    )?;
    writeln!(
        report,
        "| Peak actual files / maintenance headroom | {:.2} MiB / {:.2} MiB |",
        peak_actual_bytes / 1024_f64.powi(2),
        peak_headroom_bytes / 1024_f64.powi(2)
    )?;
    if minimum_filesystem_bytes.is_finite() {
        writeln!(
            report,
            "| Minimum filesystem free space | {:.2} GiB |",
            minimum_filesystem_bytes / 1024_f64.powi(3)
        )?;
    }
    writeln!(
        report,
        "| Peak process RSS | {:.2} MiB |\n",
        peak_resident_bytes / 1024_f64.powi(2)
    )?;

    writeln!(report, "## Throughput\n")?;
    writeln!(
        report,
        "| Phase | Append records/s | Append logical MiB/s | Read records/s | Release records/s |"
    )?;
    writeln!(report, "| --- | ---: | ---: | ---: | ---: |")?;
    for phase in [
        "all",
        "warmup",
        "steady",
        "pressure_fill",
        "pressure_hold",
        "recovery",
        "final_drain",
    ] {
        let filter = (phase != "all").then_some(("phase", phase));
        let duration = sum_values(
            &series,
            "interval_duration_seconds",
            &filter.into_iter().collect::<Vec<_>>(),
        );
        let rate = |operation| {
            let mut filters = vec![("operation", operation)];
            if let Some(filter) = filter {
                filters.push(filter);
            }
            let records = sum_values(&series, "interval_operation_records", &filters);
            if duration > 0.0 {
                records / duration
            } else {
                0.0
            }
        };
        let append_rate = rate("append");
        let logical_mib_per_second = append_rate
            * metadata
                .configuration
                .metadata_bytes
                .saturating_add(metadata.configuration.payload_bytes) as f64
            / 1024_f64.powi(2);
        writeln!(
            report,
            "| {} | {:.0} | {:.2} | {:.0} | {:.0} |",
            phase,
            append_rate,
            logical_mib_per_second,
            rate("read"),
            rate("release")
        )?;
    }
    writeln!(report)?;

    writeln!(report, "## Reactor and wait latency\n")?;
    writeln!(
        report,
        "| Internal observation | Count | Average | Maximum |"
    )?;
    writeln!(report, "| --- | ---: | ---: | ---: |")?;
    for (label, kind) in [
        ("Command queue wait", "queue"),
        ("Readiness wait", "readiness"),
        ("Capacity wait", "capacity"),
        ("Storage job", "storage_job"),
    ] {
        let observations = max_value(&series, "duration_observations_total", &[("kind", kind)]);
        let total = max_value(&series, "duration_seconds_total", &[("kind", kind)]);
        let maximum = max_value(&series, "duration_seconds_max", &[("kind", kind)]);
        let average = if observations > 0.0 {
            total / observations
        } else {
            0.0
        };
        writeln!(
            report,
            "| {label} | {:.0} | {:.3} ms | {:.3} ms |",
            observations,
            average * 1_000.0,
            maximum * 1_000.0
        )?;
    }
    writeln!(report)?;

    writeln!(report, "## Group commit\n")?;
    writeln!(
        report,
        "| Kind | Groups | Units/group | Records/group | Max units |"
    )?;
    writeln!(report, "| --- | ---: | ---: | ---: | ---: |")?;
    for kind in ["append", "release"] {
        let groups = max_value(&series, "commit_groups_total", &[("kind", kind)]);
        let units = max_value(&series, "commit_units_total", &[("kind", kind)]);
        let records = max_value(&series, "commit_records_total", &[("kind", kind)]);
        let max_units = max_value(&series, "commit_units_max", &[("kind", kind)]);
        writeln!(
            report,
            "| {kind} | {:.0} | {:.2} | {:.2} | {:.0} |",
            groups,
            divide(units, groups),
            divide(records, groups),
            max_units
        )?;
    }
    writeln!(report)?;

    writeln!(report, "## Client-observed operation latency\n")?;
    writeln!(
        report,
        "| Operation | Approx. p50 | Approx. p95 | Approx. p99 | Worst interval p99 | Max |"
    )?;
    writeln!(report, "| --- | ---: | ---: | ---: | ---: | ---: |")?;
    for operation in ["append", "read", "release"] {
        let buckets = aggregate_buckets(&series, operation);
        let worst_interval_p99 = max_value(
            &series,
            "interval_operation_latency_seconds",
            &[("operation", operation), ("quantile", "0.99")],
        );
        let max = values(
            &series,
            "interval_operation_latency_seconds_max",
            &[("operation", operation)],
        )
        .into_iter()
        .fold(0.0_f64, f64::max);
        writeln!(
            report,
            "| {} | {} | {} | {} | {:.3} ms | {:.3} ms |",
            operation,
            bucket_quantile(&buckets, 0.50),
            bucket_quantile(&buckets, 0.95),
            bucket_quantile(&buckets, 0.99),
            worst_interval_p99 * 1_000.0,
            max * 1_000.0
        )?;
    }
    writeln!(report)?;

    writeln!(report, "## Durability and integrity\n")?;
    writeln!(report, "| Check | Result |")?;
    writeln!(report, "| --- | ---: |")?;
    writeln!(
        report,
        "| Durable append / release records | {} / {} |",
        metadata.validation.appended_records, metadata.validation.released_records
    )?;
    writeln!(
        report,
        "| Pending after drain / reopen | {} / {} |",
        metadata.validation.pending_records_after_drain,
        metadata.validation.pending_records_after_reopen
    )?;
    writeln!(
        report,
        "| Integrity errors | {} |",
        metadata.validation.integrity_errors
    )?;
    writeln!(
        report,
        "| Expected read cancellations during worker stop | {} |",
        metadata.validation.read_cancellations
    )?;
    writeln!(
        report,
        "| VictoriaMetrics pushed / failed / dropped batches | {} / {} / {} |",
        metadata.exporter.pushed_batches,
        metadata.exporter.failed_batches,
        metadata.exporter.dropped_batches
    )?;
    writeln!(
        report,
        "| Recovery records scanned after drained reopen | {} |",
        metadata.validation.recovery_records_scanned
    )?;

    let report_path = output_directory.join("report.md");
    std::fs::write(&report_path, report)
        .with_context(|| format!("write {}", report_path.display()))?;
    Ok(())
}

fn parse_export(raw: &str) -> Result<Vec<ExportSeries>> {
    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<ExportSeries>(line).context("decode VM export line"))
        .collect()
}

fn export_metrics(base_url: &str, run_id: &str, start_ms: u64, end_ms: u64) -> Result<String> {
    let endpoint = format!("{}/api/v1/export", base_url.trim_end_matches('/'));
    let matcher = format!(
        "{{__name__=~\"{METRIC_PREFIX}.*\",run_id=\"{}\"}}",
        escape_label(run_id)
    );
    let start = format!("{:.3}", start_ms as f64 / 1_000.0);
    let end = format!("{:.3}", end_ms as f64 / 1_000.0);
    let output = Command::new("curl")
        .args([
            "-fsS",
            "--max-time",
            "120",
            "-X",
            "POST",
            "--data-urlencode",
            &format!("match[]={matcher}"),
            "--data-urlencode",
            &format!("start={start}"),
            "--data-urlencode",
            &format!("end={end}"),
            &endpoint,
        ])
        .output()
        .context("export long-smoke metrics from VictoriaMetrics")?;
    ensure!(
        output.status.success(),
        "VictoriaMetrics export failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout).context("VictoriaMetrics export is not UTF-8")
}

fn values(series: &[ExportSeries], metric: &str, filters: &[(&str, &str)]) -> Vec<f64> {
    let full_name = format!("{METRIC_PREFIX}{metric}");
    series
        .iter()
        .filter(|item| item.metric.get("__name__") == Some(&full_name))
        .filter(|item| {
            filters
                .iter()
                .all(|(name, value)| item.metric.get(*name).map(String::as_str) == Some(*value))
        })
        .flat_map(|item| {
            item.values
                .iter()
                .zip(&item.timestamps)
                .map(|(value, _timestamp)| *value)
        })
        .filter(|value| value.is_finite())
        .collect()
}

fn sum_values(series: &[ExportSeries], metric: &str, filters: &[(&str, &str)]) -> f64 {
    values(series, metric, filters).into_iter().sum()
}

fn max_value(series: &[ExportSeries], metric: &str, filters: &[(&str, &str)]) -> f64 {
    values(series, metric, filters)
        .into_iter()
        .fold(0.0_f64, f64::max)
}

fn divide(numerator: f64, denominator: f64) -> f64 {
    if denominator > 0.0 {
        numerator / denominator
    } else {
        0.0
    }
}

#[derive(Clone, Copy)]
struct Summary {
    min: f64,
    average: f64,
    p95: f64,
    max: f64,
}

fn summarize(input: &[f64]) -> Summary {
    let mut values = input.to_vec();
    values.sort_by(f64::total_cmp);
    let sum: f64 = values.iter().sum();
    Summary {
        min: values.first().copied().unwrap_or_default(),
        average: if values.is_empty() {
            0.0
        } else {
            sum / values.len() as f64
        },
        p95: percentile_sorted(&values, 0.95),
        max: values.last().copied().unwrap_or_default(),
    }
}

fn percentile_sorted(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * quantile).ceil() as usize;
    values[index.min(values.len() - 1)]
}

fn fraction(input: &[f64], predicate: impl Fn(f64) -> bool) -> f64 {
    if input.is_empty() {
        return 0.0;
    }
    input.iter().filter(|value| predicate(**value)).count() as f64 / input.len() as f64
}

fn aggregate_buckets(series: &[ExportSeries], operation: &str) -> Vec<(&'static str, f64)> {
    LATENCY_BUCKETS_NS
        .iter()
        .map(|(upper, _)| {
            (
                *upper,
                sum_values(
                    series,
                    "interval_operation_latency_bucket",
                    &[("operation", operation), ("le", upper)],
                ),
            )
        })
        .collect()
}

fn bucket_quantile(buckets: &[(&str, f64)], quantile: f64) -> String {
    let total = buckets.last().map_or(0.0, |(_, count)| *count);
    if total == 0.0 {
        return "n/a".to_string();
    }
    let target = total * quantile;
    let upper = buckets
        .iter()
        .find_map(|(upper, count)| (*count >= target).then_some(*upper))
        .unwrap_or("+Inf");
    if upper == "+Inf" {
        return ">300 s".to_string();
    }
    let seconds = upper.parse::<f64>().unwrap_or_default();
    if seconds < 1.0 {
        format!("≤{:.3} ms", seconds * 1_000.0)
    } else {
        format!("≤{seconds:.3} s")
    }
}

pub(crate) fn resident_memory_bytes() -> Option<u64> {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(kilobytes) = status.lines().find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_ascii_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        }) {
            return kilobytes.checked_mul(1024);
        }
    }

    let process_id = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &process_id])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let kilobytes = String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    kilobytes.checked_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_are_escaped_for_prometheus() {
        assert_eq!(escape_label("a\\b\n\"c"), "a\\\\b\\n\\\"c");
    }

    #[test]
    fn bucket_quantile_uses_aggregated_cumulative_counts() {
        let buckets = vec![("0.001", 50.0), ("0.01", 95.0), ("0.1", 100.0)];
        assert_eq!(bucket_quantile(&buckets, 0.50), "≤1.000 ms");
        assert_eq!(bucket_quantile(&buckets, 0.99), "≤100.000 ms");
    }

    #[test]
    fn exported_timestamps_are_retained_for_schema_validation() {
        let item: ExportSeries =
            serde_json::from_str(r#"{"metric":{"__name__":"x"},"values":[1],"timestamps":[2]}"#)
                .unwrap();
        assert_eq!(item.timestamps, vec![2]);
    }
}
