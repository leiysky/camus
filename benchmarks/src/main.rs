mod diagnostics;
mod engines;
mod metrics;
mod model;
mod workloads;

use anyhow::{bail, ensure, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use engines::EngineKind;
use metrics::{
    aggregate, environment, BenchmarkReport, CaseDescriptor, CaseResult, RunConfiguration,
    REPORT_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use workloads::WorkloadConfig;

const TARGET_EPOCH_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "camus-bench",
    about = "Durable-buffer benchmarks for Camus, a simple append file, RocksDB, and redb"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run isolated benchmark samples and write a versioned JSON report.
    Run(RunArgs),
    /// Compare two reports and fail when configured regression limits are exceeded.
    Compare(CompareArgs),
    /// Measure foreground latency while Camus compacts a large manifest.
    ManifestCompaction(diagnostics::ManifestCompactionArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Engines to run.
    #[arg(long, value_enum, value_delimiter = ',')]
    engines: Option<Vec<EngineKind>>,

    /// Workloads to run.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "sequential-append,concurrent-append,batch-append,read-snapshot,release-batch,drain,warm-restart"
    )]
    workloads: Vec<WorkloadKind>,

    /// Workload-size preset.
    #[arg(long, value_enum, default_value = "baseline")]
    profile: Profile,

    /// Replace every profile-specific record count with one value.
    #[arg(long)]
    records: Option<usize>,

    /// Number of independent fresh-database samples per case.
    #[arg(long)]
    samples: Option<usize>,

    /// Opaque metadata bytes per record.
    #[arg(long, default_value_t = 32)]
    metadata_bytes: usize,

    /// Opaque payload bytes per record.
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,

    /// Concurrent callers in the concurrent append cases.
    #[arg(long)]
    concurrency: Option<usize>,

    /// Logical streams in the multi-stream concurrent append case.
    #[arg(long)]
    logical_streams: Option<usize>,

    /// Records per atomic append batch.
    #[arg(long)]
    batch_records: Option<usize>,

    /// Records per read plus durable release/delete cycle.
    #[arg(long)]
    read_batch_records: Option<usize>,

    /// IDs per isolated durable release/delete operation.
    #[arg(long)]
    release_batch_records: Option<usize>,

    /// Parent directory on the device under test. Fresh child directories are removed.
    #[arg(long, default_value = "target/benchmark-data")]
    data_directory: PathBuf,

    /// JSON report path. Defaults under target/benchmark-results.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Free-form environment note stored in the report.
    #[arg(long)]
    note: Option<String>,
}

#[derive(Debug, Args)]
struct CompareArgs {
    /// Trusted report from the same host and storage setup.
    #[arg(long)]
    baseline: PathBuf,

    /// Newly measured report.
    #[arg(long)]
    candidate: PathBuf,

    /// Maximum allowed records-per-second decrease.
    #[arg(long, default_value_t = 15.0)]
    max_throughput_drop_percent: f64,

    /// Maximum allowed p99 latency increase.
    #[arg(long, default_value_t = 25.0)]
    max_p99_increase_percent: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum WorkloadKind {
    SequentialAppend,
    ConcurrentAppend,
    BatchAppend,
    ReadSnapshot,
    ReleaseBatch,
    Drain,
    WarmRestart,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Profile {
    Smoke,
    Baseline,
    Soak,
}

impl Profile {
    const fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Baseline => "baseline",
            Self::Soak => "soak",
        }
    }

    const fn defaults(self) -> ProfileDefaults {
        match self {
            Self::Smoke => ProfileDefaults {
                samples: 1,
                concurrency: 4,
                logical_streams: 4,
                batch_records: 16,
                read_batch_records: 32,
                release_batch_records: 32,
                setup_batch_records: 64,
                sequential_records: 16,
                concurrent_records: 128,
                batch_total_records: 256,
                read_snapshot_records: 256,
                release_records: 256,
                drain_records: 256,
                warm_restart_records: 1_024,
            },
            Self::Baseline => ProfileDefaults {
                samples: 3,
                concurrency: 16,
                logical_streams: 16,
                batch_records: 64,
                read_batch_records: 256,
                release_batch_records: 256,
                setup_batch_records: 1_024,
                sequential_records: 256,
                concurrent_records: 4_096,
                batch_total_records: 8_192,
                read_snapshot_records: 8_192,
                release_records: 8_192,
                drain_records: 8_192,
                warm_restart_records: 16_384,
            },
            Self::Soak => ProfileDefaults {
                samples: 5,
                concurrency: 64,
                logical_streams: 64,
                batch_records: 256,
                read_batch_records: 1_024,
                release_batch_records: 1_024,
                setup_batch_records: 1_024,
                sequential_records: 2_048,
                concurrent_records: 65_536,
                batch_total_records: 131_072,
                read_snapshot_records: 131_072,
                release_records: 131_072,
                drain_records: 131_072,
                warm_restart_records: 262_144,
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ProfileDefaults {
    samples: usize,
    concurrency: usize,
    logical_streams: usize,
    batch_records: usize,
    read_batch_records: usize,
    release_batch_records: usize,
    setup_batch_records: usize,
    sequential_records: usize,
    concurrent_records: usize,
    batch_total_records: usize,
    read_snapshot_records: usize,
    release_records: usize,
    drain_records: usize,
    warm_restart_records: usize,
}

#[derive(Clone, Debug)]
struct Settings {
    report: RunConfiguration,
}

#[derive(Clone, Copy, Debug)]
enum CaseKind {
    SequentialAppend,
    ConcurrentAppend,
    BatchAppend,
    ReadSnapshot,
    ReleaseBatch,
    Drain,
    WarmRestart,
}

#[derive(Clone, Debug)]
struct Case {
    kind: CaseKind,
    name: String,
    records: usize,
    concurrency: usize,
    logical_streams: usize,
    batch_records: usize,
    operations: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Run(arguments) => run(arguments).await,
        Command::Compare(arguments) => compare(arguments),
        Command::ManifestCompaction(arguments) => diagnostics::manifest_compaction(arguments).await,
    }
}

async fn run(arguments: RunArgs) -> Result<()> {
    let engines = arguments.engines.clone().unwrap_or_else(default_engines);
    let settings = settings(&arguments, &engines)?;
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
    let cases = cases(&arguments.workloads, &settings.report);
    let workload_config = WorkloadConfig {
        metadata_bytes: settings.report.metadata_bytes,
        payload_bytes: settings.report.payload_bytes,
        concurrency: settings.report.concurrency,
        batch_records: settings.report.batch_records,
        read_batch_records: settings.report.read_batch_records,
        release_batch_records: settings.report.release_batch_records,
        setup_batch_records: settings.report.setup_batch_records,
    };

    let mut results = Vec::new();
    for case in &cases {
        for engine in &engines {
            let mut measurements = Vec::with_capacity(settings.report.samples);
            for sample in 1..=settings.report.samples {
                eprintln!(
                    "running engine={} workload={} sample={sample}/{}",
                    engine.name(),
                    case.name,
                    settings.report.samples
                );
                let measurement = match case.kind {
                    CaseKind::SequentialAppend => {
                        workloads::sequential_append(
                            *engine,
                            &data_directory,
                            case.records,
                            workload_config,
                        )
                        .await?
                    }
                    CaseKind::ConcurrentAppend => {
                        workloads::concurrent_append(
                            *engine,
                            &data_directory,
                            case.records,
                            case.logical_streams,
                            workload_config,
                        )
                        .await?
                    }
                    CaseKind::BatchAppend => {
                        workloads::batch_append(
                            *engine,
                            &data_directory,
                            case.records,
                            workload_config,
                        )
                        .await?
                    }
                    CaseKind::ReadSnapshot => {
                        workloads::read_snapshot(
                            *engine,
                            &data_directory,
                            case.records,
                            workload_config,
                        )
                        .await?
                    }
                    CaseKind::ReleaseBatch => {
                        workloads::release_batch(
                            *engine,
                            &data_directory,
                            case.records,
                            workload_config,
                        )
                        .await?
                    }
                    CaseKind::Drain => {
                        workloads::drain(*engine, &data_directory, case.records, workload_config)
                            .await?
                    }
                    CaseKind::WarmRestart => {
                        workloads::warm_restart(
                            *engine,
                            &data_directory,
                            case.records,
                            workload_config,
                        )
                        .await?
                    }
                };
                measurements.push(measurement);
            }
            results.push(aggregate(
                CaseDescriptor {
                    engine: engine.name(),
                    workload: &case.name,
                    samples: settings.report.samples,
                    records_per_sample: u64::try_from(case.records)
                        .context("case record count overflow")?,
                    operations_per_sample: u64::try_from(case.operations)
                        .context("case operation count overflow")?,
                    metadata_bytes: settings.report.metadata_bytes,
                    payload_bytes: settings.report.payload_bytes,
                    concurrency: case.concurrency,
                    logical_streams: case.logical_streams,
                    batch_records: case.batch_records,
                },
                measurements,
            )?);
        }
    }

    let report = BenchmarkReport {
        schema_version: REPORT_SCHEMA_VERSION,
        environment: environment(&data_directory, arguments.note),
        configuration: settings.report,
        results,
    };
    print_results(&report.results);
    let output = arguments.output.unwrap_or_else(default_output_path);
    write_report(&output, &report)?;
    println!("report: {}", output.display());
    Ok(())
}

fn default_engines() -> Vec<EngineKind> {
    let mut engines = vec![EngineKind::Camus, EngineKind::SimpleAppendFile];
    if cfg!(all(feature = "rocksdb-engine", not(target_os = "macos"))) {
        engines.push(EngineKind::Rocksdb);
    }
    if cfg!(feature = "redb-engine") {
        engines.push(EngineKind::Redb);
    }
    engines
}

fn settings(arguments: &RunArgs, engines: &[EngineKind]) -> Result<Settings> {
    ensure!(!engines.is_empty(), "at least one engine is required");
    #[cfg(target_os = "macos")]
    ensure!(
        !engines.contains(&EngineKind::Rocksdb),
        "RocksDB durability benchmarks are disabled on macOS because the bundled \
         librocksdb-sys build does not use F_FULLFSYNC; run RocksDB comparisons on Linux"
    );
    ensure!(
        !arguments.workloads.is_empty(),
        "at least one workload is required"
    );
    let mut defaults = arguments.profile.defaults();
    if let Some(records) = arguments.records {
        ensure!(records > 0, "--records must be greater than zero");
        defaults.sequential_records = records;
        defaults.concurrent_records = records;
        defaults.batch_total_records = records;
        defaults.read_snapshot_records = records;
        defaults.release_records = records;
        defaults.drain_records = records;
        defaults.warm_restart_records = records;
    }
    defaults.samples = arguments.samples.unwrap_or(defaults.samples);
    defaults.concurrency = arguments.concurrency.unwrap_or(defaults.concurrency);
    defaults.logical_streams = arguments
        .logical_streams
        .unwrap_or(defaults.logical_streams);
    defaults.batch_records = arguments.batch_records.unwrap_or(defaults.batch_records);
    defaults.read_batch_records = arguments
        .read_batch_records
        .unwrap_or(defaults.read_batch_records);
    defaults.release_batch_records = arguments
        .release_batch_records
        .unwrap_or(defaults.release_batch_records);
    ensure!(
        defaults.samples > 0,
        "sample count must be greater than zero"
    );
    ensure!(
        defaults.concurrency > 0,
        "concurrency must be greater than zero"
    );
    ensure!(
        defaults.logical_streams > 0,
        "logical stream count must be greater than zero"
    );
    ensure!(
        defaults.batch_records > 0,
        "batch size must be greater than zero"
    );
    ensure!(
        defaults.read_batch_records > 0,
        "read batch size must be greater than zero"
    );
    ensure!(
        defaults.release_batch_records > 0,
        "release batch size must be greater than zero"
    );

    let logical_record_bytes = arguments
        .metadata_bytes
        .checked_add(arguments.payload_bytes)
        .and_then(|bytes| bytes.checked_add(256))
        .context("logical record byte size overflow")?;
    ensure!(
        logical_record_bytes <= TARGET_EPOCH_BYTES,
        "one benchmark record must fit the 4 MiB comparison epoch target"
    );
    let max_batch_records = (TARGET_EPOCH_BYTES / logical_record_bytes).max(1);
    defaults.batch_records = defaults.batch_records.min(max_batch_records);
    defaults.setup_batch_records = defaults.setup_batch_records.min(max_batch_records);

    Ok(Settings {
        report: RunConfiguration {
            profile: arguments.profile.name().to_string(),
            samples: defaults.samples,
            metadata_bytes: arguments.metadata_bytes,
            payload_bytes: arguments.payload_bytes,
            concurrency: defaults.concurrency,
            logical_streams: defaults.logical_streams,
            batch_records: defaults.batch_records,
            read_batch_records: defaults.read_batch_records,
            release_batch_records: defaults.release_batch_records,
            setup_batch_records: defaults.setup_batch_records,
            sequential_records: defaults.sequential_records,
            concurrent_records: defaults.concurrent_records,
            batch_total_records: defaults.batch_total_records,
            read_snapshot_records: defaults.read_snapshot_records,
            release_records: defaults.release_records,
            drain_records: defaults.drain_records,
            warm_restart_records: defaults.warm_restart_records,
        },
    })
}

fn cases(workloads: &[WorkloadKind], settings: &RunConfiguration) -> Vec<Case> {
    let mut cases = Vec::new();
    for workload in workloads {
        match workload {
            WorkloadKind::SequentialAppend => cases.push(Case {
                kind: CaseKind::SequentialAppend,
                name: "append_sequential".to_string(),
                records: settings.sequential_records,
                concurrency: 1,
                logical_streams: 1,
                batch_records: 1,
                operations: settings.sequential_records,
            }),
            WorkloadKind::ConcurrentAppend => {
                let effective_streams = settings
                    .logical_streams
                    .min(settings.concurrency)
                    .min(settings.concurrent_records);
                cases.push(Case {
                    kind: CaseKind::ConcurrentAppend,
                    name: "append_concurrent_1_stream".to_string(),
                    records: settings.concurrent_records,
                    concurrency: settings.concurrency,
                    logical_streams: 1,
                    batch_records: 1,
                    operations: settings.concurrent_records,
                });
                if effective_streams > 1 {
                    cases.push(Case {
                        kind: CaseKind::ConcurrentAppend,
                        name: format!("append_concurrent_{effective_streams}_streams"),
                        records: settings.concurrent_records,
                        concurrency: settings.concurrency,
                        logical_streams: effective_streams,
                        batch_records: 1,
                        operations: settings.concurrent_records,
                    });
                }
            }
            WorkloadKind::BatchAppend => cases.push(Case {
                kind: CaseKind::BatchAppend,
                name: "append_batch".to_string(),
                records: settings.batch_total_records,
                concurrency: 1,
                logical_streams: 1,
                batch_records: settings.batch_records,
                operations: settings
                    .batch_total_records
                    .div_ceil(settings.batch_records),
            }),
            WorkloadKind::ReadSnapshot => cases.push(Case {
                kind: CaseKind::ReadSnapshot,
                name: "read_verified_snapshot".to_string(),
                records: settings.read_snapshot_records,
                concurrency: 1,
                logical_streams: 1,
                batch_records: settings.read_snapshot_records,
                operations: 1,
            }),
            WorkloadKind::ReleaseBatch => cases.push(Case {
                kind: CaseKind::ReleaseBatch,
                name: "release_batch".to_string(),
                records: settings.release_records,
                concurrency: 1,
                logical_streams: 1,
                batch_records: settings.release_batch_records,
                operations: settings
                    .release_records
                    .div_ceil(settings.release_batch_records),
            }),
            WorkloadKind::Drain => cases.push(Case {
                kind: CaseKind::Drain,
                name: "read_release_drain".to_string(),
                records: settings.drain_records,
                concurrency: 1,
                logical_streams: 1,
                batch_records: settings.read_batch_records,
                operations: settings.drain_records.div_ceil(settings.read_batch_records),
            }),
            WorkloadKind::WarmRestart => cases.push(Case {
                kind: CaseKind::WarmRestart,
                name: "warm_restart_first_batch".to_string(),
                records: settings.warm_restart_records,
                concurrency: 1,
                logical_streams: 1,
                batch_records: settings.read_batch_records,
                operations: 1,
            }),
        }
    }
    cases
}

fn write_report(path: &Path, report: &BenchmarkReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create report directory {}", parent.display()))?;
    }
    let writer = BufWriter::new(
        File::create(path).with_context(|| format!("create report {}", path.display()))?,
    );
    serde_json::to_writer_pretty(writer, report).context("serialize benchmark report")?;
    Ok(())
}

fn read_report(path: &Path) -> Result<BenchmarkReport> {
    let reader = BufReader::new(
        File::open(path).with_context(|| format!("open benchmark report {}", path.display()))?,
    );
    let value: serde_json::Value =
        serde_json::from_reader(reader).context("parse benchmark report JSON")?;
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .context("benchmark report has no numeric schema_version")?;
    ensure!(
        schema_version == u64::from(REPORT_SCHEMA_VERSION),
        "unsupported benchmark report schema {}",
        schema_version
    );
    serde_json::from_value(value).context("decode benchmark report schema")
}

fn compare(arguments: CompareArgs) -> Result<()> {
    ensure!(
        arguments.max_throughput_drop_percent >= 0.0,
        "throughput threshold must not be negative"
    );
    ensure!(
        arguments.max_p99_increase_percent >= 0.0,
        "latency threshold must not be negative"
    );
    let baseline = read_report(&arguments.baseline)?;
    let candidate = read_report(&arguments.candidate)?;
    let candidates = candidate
        .results
        .iter()
        .map(|result| (result.identity(), result))
        .collect::<HashMap<_, _>>();
    let mut regressions = Vec::new();

    println!(
        "{:<19} {:<36} {:>12} {:>12}",
        "engine", "workload", "throughput", "p99"
    );
    for expected in &baseline.results {
        let Some(actual) = candidates.get(&expected.identity()) else {
            regressions.push(format!(
                "missing candidate case {} {}",
                expected.engine, expected.workload
            ));
            continue;
        };
        let throughput_change =
            percent_change(expected.records_per_second, actual.records_per_second);
        let p99_change =
            percent_change(expected.latency_ns.p99 as f64, actual.latency_ns.p99 as f64);
        println!(
            "{:<19} {:<36} {:>+11.1}% {:>+11.1}%",
            expected.engine, expected.workload, throughput_change, p99_change
        );
        if throughput_change < -arguments.max_throughput_drop_percent {
            regressions.push(format!(
                "{} {} throughput changed {throughput_change:+.1}%",
                expected.engine, expected.workload
            ));
        }
        if p99_change > arguments.max_p99_increase_percent {
            regressions.push(format!(
                "{} {} p99 latency changed {p99_change:+.1}%",
                expected.engine, expected.workload
            ));
        }
    }

    if !regressions.is_empty() {
        for regression in &regressions {
            eprintln!("regression: {regression}");
        }
        bail!(
            "{} benchmark regression(s) exceeded limits",
            regressions.len()
        );
    }
    Ok(())
}

fn percent_change(baseline: f64, candidate: f64) -> f64 {
    if baseline == 0.0 {
        return 0.0;
    }
    (candidate / baseline - 1.0) * 100.0
}

fn print_results(results: &[CaseResult]) {
    println!(
        "{:<19} {:<36} {:>12} {:>12} {:>10} {:>10} {:>10}",
        "engine", "workload", "records/s", "ops/s", "p50 ms", "p99 ms", "MiB/s"
    );
    for result in results {
        println!(
            "{:<19} {:<36} {:>12.0} {:>12.0} {:>10.3} {:>10.3} {:>10.2}",
            result.engine,
            result.workload,
            result.records_per_second,
            result.operations_per_second,
            result.latency_ns.p50 as f64 / 1_000_000.0,
            result.latency_ns.p99 as f64 / 1_000_000.0,
            result.logical_mib_per_second,
        );
    }
}

fn default_output_path() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    PathBuf::from(format!("target/benchmark-results/report-{timestamp}.json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_engines_follow_the_platform_durability_boundary() {
        let cli = Cli::try_parse_from(["camus-bench", "run"]).unwrap();
        let Command::Run(arguments) = cli.command else {
            panic!("expected run command");
        };
        assert!(arguments.engines.is_none());
        let engines = default_engines();
        assert!(engines.contains(&EngineKind::Camus));
        assert!(engines.contains(&EngineKind::SimpleAppendFile));
        assert_eq!(
            engines.contains(&EngineKind::Rocksdb),
            cfg!(all(feature = "rocksdb-engine", not(target_os = "macos")))
        );
        assert_eq!(
            engines.contains(&EngineKind::Redb),
            cfg!(feature = "redb-engine")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn explicit_rocksdb_run_is_rejected_on_macos() {
        let cli = Cli::try_parse_from(["camus-bench", "run", "--engines", "rocksdb"]).unwrap();
        let Command::Run(arguments) = cli.command else {
            panic!("expected run command");
        };
        let engines = arguments.engines.clone().unwrap_or_else(default_engines);
        let error = settings(&arguments, &engines).unwrap_err();
        assert!(error.to_string().contains("disabled on macOS"));
    }
}
