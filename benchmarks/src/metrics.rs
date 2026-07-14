use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) const REPORT_SCHEMA_VERSION: u32 = 3;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct BenchmarkReport {
    pub(crate) schema_version: u32,
    pub(crate) environment: Environment,
    pub(crate) configuration: RunConfiguration,
    pub(crate) results: Vec<CaseResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Environment {
    pub(crate) unix_time_seconds: u64,
    pub(crate) os: String,
    pub(crate) architecture: String,
    pub(crate) logical_cpus: usize,
    pub(crate) cpu_model: Option<String>,
    pub(crate) kernel: Option<String>,
    pub(crate) rustc: Option<String>,
    pub(crate) git_commit: Option<String>,
    pub(crate) git_dirty: Option<bool>,
    pub(crate) data_directory: String,
    pub(crate) note: Option<String>,
    pub(crate) engine_versions: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RunConfiguration {
    pub(crate) profile: String,
    pub(crate) samples: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) concurrency: usize,
    pub(crate) logical_streams: usize,
    pub(crate) batch_records: usize,
    pub(crate) read_batch_records: usize,
    pub(crate) release_batch_records: usize,
    pub(crate) setup_batch_records: usize,
    pub(crate) sequential_records: usize,
    pub(crate) concurrent_records: usize,
    pub(crate) batch_total_records: usize,
    pub(crate) read_snapshot_records: usize,
    pub(crate) release_records: usize,
    pub(crate) drain_records: usize,
    pub(crate) warm_restart_records: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct CaseResult {
    pub(crate) engine: String,
    pub(crate) workload: String,
    pub(crate) samples: usize,
    pub(crate) records_per_sample: u64,
    pub(crate) operations_per_sample: u64,
    pub(crate) metadata_bytes: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) concurrency: usize,
    pub(crate) logical_streams: usize,
    pub(crate) batch_records: usize,
    pub(crate) total_elapsed_ns: u64,
    pub(crate) records_per_second: f64,
    pub(crate) operations_per_second: f64,
    pub(crate) logical_mib_per_second: f64,
    pub(crate) latency_ns: LatencySummary,
    pub(crate) median_storage_bytes: u64,
}

impl CaseResult {
    pub(crate) fn identity(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}|{}|{}",
            self.engine,
            self.workload,
            self.metadata_bytes,
            self.payload_bytes,
            self.concurrency,
            self.logical_streams,
            self.batch_records,
            self.records_per_sample
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(crate) struct LatencySummary {
    pub(crate) p50: u64,
    pub(crate) p95: u64,
    pub(crate) p99: u64,
    pub(crate) max: u64,
}

pub(crate) struct Measurement {
    pub(crate) records: u64,
    pub(crate) operations: u64,
    pub(crate) elapsed: Duration,
    pub(crate) latencies_ns: Vec<u64>,
    pub(crate) storage_bytes: u64,
}

pub(crate) struct CaseDescriptor<'a> {
    pub(crate) engine: &'a str,
    pub(crate) workload: &'a str,
    pub(crate) samples: usize,
    pub(crate) records_per_sample: u64,
    pub(crate) operations_per_sample: u64,
    pub(crate) metadata_bytes: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) concurrency: usize,
    pub(crate) logical_streams: usize,
    pub(crate) batch_records: usize,
}

pub(crate) fn aggregate(
    descriptor: CaseDescriptor<'_>,
    measurements: Vec<Measurement>,
) -> Result<CaseResult> {
    let mut histogram = Histogram::<u64>::new(3).context("create latency histogram")?;
    let mut elapsed_ns = 0_u64;
    let mut records = 0_u64;
    let mut operations = 0_u64;
    let mut storage_bytes = Vec::with_capacity(measurements.len());

    for measurement in measurements {
        elapsed_ns = elapsed_ns
            .checked_add(duration_ns(measurement.elapsed))
            .context("aggregate elapsed time overflow")?;
        records = records
            .checked_add(measurement.records)
            .context("aggregate record count overflow")?;
        operations = operations
            .checked_add(measurement.operations)
            .context("aggregate operation count overflow")?;
        for latency in measurement.latencies_ns {
            histogram
                .record(latency.max(1))
                .context("record operation latency")?;
        }
        storage_bytes.push(measurement.storage_bytes);
    }

    storage_bytes.sort_unstable();
    let median_storage_bytes = storage_bytes
        .get(storage_bytes.len() / 2)
        .copied()
        .unwrap_or_default();
    let elapsed_seconds = elapsed_ns as f64 / 1_000_000_000.0;
    let records_per_second = records as f64 / elapsed_seconds;
    let operations_per_second = operations as f64 / elapsed_seconds;
    let logical_bytes_per_record = descriptor
        .metadata_bytes
        .checked_add(descriptor.payload_bytes)
        .context("logical record byte size overflow")?;
    let logical_mib_per_second =
        records_per_second * logical_bytes_per_record as f64 / (1024.0 * 1024.0);

    Ok(CaseResult {
        engine: descriptor.engine.to_string(),
        workload: descriptor.workload.to_string(),
        samples: descriptor.samples,
        records_per_sample: descriptor.records_per_sample,
        operations_per_sample: descriptor.operations_per_sample,
        metadata_bytes: descriptor.metadata_bytes,
        payload_bytes: descriptor.payload_bytes,
        concurrency: descriptor.concurrency,
        logical_streams: descriptor.logical_streams,
        batch_records: descriptor.batch_records,
        total_elapsed_ns: elapsed_ns,
        records_per_second,
        operations_per_second,
        logical_mib_per_second,
        latency_ns: LatencySummary {
            p50: histogram.value_at_quantile(0.50),
            p95: histogram.value_at_quantile(0.95),
            p99: histogram.value_at_quantile(0.99),
            max: histogram.max(),
        },
        median_storage_bytes,
    })
}

pub(crate) fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn environment(data_directory: &Path, note: Option<String>) -> Environment {
    let unix_time_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut engine_versions = BTreeMap::new();
    engine_versions.insert("camus".to_string(), "git checkout".to_string());
    engine_versions.insert(
        "simple_append_file".to_string(),
        "built-in format 1; one sync_data per operation".to_string(),
    );
    engine_versions.insert(
        "rocksdb".to_string(),
        "rust-rocksdb 0.24 / RocksDB 10.4.2".to_string(),
    );
    engine_versions.insert("redb".to_string(), "4.1".to_string());

    Environment {
        unix_time_seconds,
        os: std::env::consts::OS.to_string(),
        architecture: std::env::consts::ARCH.to_string(),
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        cpu_model: cpu_model(),
        kernel: command_output("uname", &["-a"]),
        rustc: command_output("rustc", &["-Vv"]),
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        git_dirty: command_output("git", &["status", "--porcelain"])
            .map(|status| !status.is_empty()),
        data_directory: data_directory.display().to_string(),
        note,
        engine_versions,
    }
}

fn cpu_model() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        return command_output("sysctl", &["-n", "machdep.cpu.brand_string"]);
    }
    #[cfg(target_os = "linux")]
    {
        return std::fs::read_to_string("/proc/cpuinfo")
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("model name\t: ").map(str::to_string));
    }
    #[allow(unreachable_code)]
    None
}

fn command_output(program: &str, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program).args(arguments).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}
