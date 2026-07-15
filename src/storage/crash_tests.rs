use super::*;
use crate::config::FullPolicy;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

const SCENARIO_ENV: &str = "CAMUS_TEST_CRASH_SCENARIO";
const ROOT_ENV: &str = "CAMUS_TEST_CRASH_ROOT";
const STREAM: StreamId = StreamId::new(41);

#[derive(Clone, Copy)]
struct CrashCase {
    scenario: &'static str,
    point: &'static str,
    target: Option<&'static str>,
    expected_pending: u64,
    expected_highwater: Option<u64>,
    expected_segments: usize,
    expected_completed_seals: u64,
    expected_completed_deletions: u64,
    expected_removed_temporaries: u64,
}

#[derive(Clone, Copy)]
struct FaultCase {
    scenario: &'static str,
    point: &'static str,
    target: Option<&'static str>,
    kind: &'static str,
    expected_pending: u64,
    expected_highwater: Option<u64>,
    expected_segments: usize,
    expected_completed_seals: u64,
    expected_completed_deletions: u64,
    expected_removed_temporaries: u64,
    expected_repaired_active_tails: u64,
    expected_repaired_manifest_tails: u64,
}

#[test]
fn durable_boundaries_recover_after_deterministic_process_crashes() {
    let cases = [
        case("create", "segment.create.after_data_sync", 0, None, 0),
        case("create", "segment.create.after_rename", 1, Some(0), 1),
        case(
            "create",
            "segment.create.after_directory_sync",
            1,
            Some(0),
            1,
        ),
        case("append", "segment.append.after_data_sync", 2, Some(1), 1),
        CrashCase {
            expected_completed_seals: 1,
            ..case("seal", "segment.seal.after_data_sync", 1, Some(0), 1)
        },
        case("seal", "seal.after_manifest_sync", 1, Some(0), 1),
        case("release", "release.after_manifest_sync", 0, Some(0), 1),
        CrashCase {
            expected_completed_deletions: 1,
            ..case("reclaim", "reclaim.after_manifest_sync", 0, Some(0), 0)
        },
        case("reclaim", "reclaim.after_delete", 0, Some(0), 0),
        case("reclaim", "reclaim.after_directory_sync", 0, Some(0), 0),
        atomic_case("atomic_replace.after_data_sync", files::CHECKPOINT_FILE),
        atomic_case("atomic_replace.after_rename", files::CHECKPOINT_FILE),
        atomic_case(
            "atomic_replace.after_directory_sync",
            files::CHECKPOINT_FILE,
        ),
        atomic_case("atomic_replace.after_data_sync", files::MANIFEST_LOG_FILE),
        atomic_case("atomic_replace.after_rename", files::MANIFEST_LOG_FILE),
        atomic_case(
            "atomic_replace.after_directory_sync",
            files::MANIFEST_LOG_FILE,
        ),
    ];

    for crash_case in cases {
        run_case(crash_case);
    }
}

#[test]
fn io_failures_return_unknown_and_recover_the_observed_durable_state() {
    let cases = [
        FaultCase {
            kind: "enospc",
            ..fault_case("create", "segment.create.sync_data", 0, None, 0, 1)
        },
        fault_case("create", "segment.create.rename", 0, None, 0, 1),
        fault_case("create", "segment.create.directory_sync", 1, Some(0), 1, 0),
        fault_case("append", "segment.append.sync_data", 2, Some(1), 1, 0),
        FaultCase {
            expected_completed_seals: 1,
            ..fault_case("seal", "segment.seal.sync_data", 1, Some(0), 1, 0)
        },
        FaultCase {
            kind: "enospc",
            ..fault_case("release", "manifest.append.sync_data", 0, Some(0), 1, 0)
        },
        FaultCase {
            expected_completed_deletions: 1,
            ..fault_case("reclaim", "reclaim.delete", 0, Some(0), 0, 0)
        },
        fault_case("reclaim", "reclaim.directory_sync", 0, Some(0), 0, 0),
        atomic_fault("atomic_replace.sync_data", files::CHECKPOINT_FILE, 1),
        atomic_fault("atomic_replace.rename", files::CHECKPOINT_FILE, 1),
        atomic_fault("atomic_replace.directory_sync", files::CHECKPOINT_FILE, 0),
        atomic_fault("atomic_replace.sync_data", files::MANIFEST_LOG_FILE, 1),
        atomic_fault("atomic_replace.rename", files::MANIFEST_LOG_FILE, 1),
        atomic_fault("atomic_replace.directory_sync", files::MANIFEST_LOG_FILE, 0),
        short_write_fault("create", "segment.epoch.short_write", 0, None, 0, 1),
        FaultCase {
            expected_repaired_active_tails: 1,
            ..short_write_fault("append", "segment.epoch.short_write", 1, Some(0), 1, 0)
        },
        FaultCase {
            expected_repaired_active_tails: 1,
            ..short_write_fault("seal", "segment.footer.short_write", 1, Some(0), 1, 0)
        },
        FaultCase {
            expected_repaired_manifest_tails: 1,
            ..short_write_fault("release", "manifest.frame.short_write", 1, Some(0), 1, 0)
        },
        atomic_short_write(files::CHECKPOINT_FILE),
        atomic_short_write(files::MANIFEST_LOG_FILE),
    ];

    for fault_case in cases {
        run_fault_case(fault_case);
    }
}

fn case(
    scenario: &'static str,
    point: &'static str,
    expected_pending: u64,
    expected_highwater: Option<u64>,
    expected_segments: usize,
) -> CrashCase {
    CrashCase {
        scenario,
        point,
        target: None,
        expected_pending,
        expected_highwater,
        expected_segments,
        expected_completed_seals: 0,
        expected_completed_deletions: 0,
        expected_removed_temporaries: u64::from(matches!(point, "segment.create.after_data_sync")),
    }
}

fn atomic_case(point: &'static str, target: &'static str) -> CrashCase {
    CrashCase {
        target: Some(target),
        expected_removed_temporaries: u64::from(point == "atomic_replace.after_data_sync"),
        ..case("compact", point, 0, Some(0), 1)
    }
}

const fn fault_case(
    scenario: &'static str,
    point: &'static str,
    expected_pending: u64,
    expected_highwater: Option<u64>,
    expected_segments: usize,
    expected_removed_temporaries: u64,
) -> FaultCase {
    FaultCase {
        scenario,
        point,
        target: None,
        kind: "eio",
        expected_pending,
        expected_highwater,
        expected_segments,
        expected_completed_seals: 0,
        expected_completed_deletions: 0,
        expected_removed_temporaries,
        expected_repaired_active_tails: 0,
        expected_repaired_manifest_tails: 0,
    }
}

const fn atomic_fault(
    point: &'static str,
    target: &'static str,
    expected_removed_temporaries: u64,
) -> FaultCase {
    FaultCase {
        target: Some(target),
        ..fault_case(
            "compact",
            point,
            0,
            Some(0),
            1,
            expected_removed_temporaries,
        )
    }
}

const fn short_write_fault(
    scenario: &'static str,
    point: &'static str,
    expected_pending: u64,
    expected_highwater: Option<u64>,
    expected_segments: usize,
    expected_removed_temporaries: u64,
) -> FaultCase {
    FaultCase {
        kind: "write_zero",
        ..fault_case(
            scenario,
            point,
            expected_pending,
            expected_highwater,
            expected_segments,
            expected_removed_temporaries,
        )
    }
}

const fn atomic_short_write(target: &'static str) -> FaultCase {
    FaultCase {
        target: Some(target),
        ..short_write_fault("compact", "atomic_replace.short_write", 0, Some(0), 1, 1)
    }
}

fn run_case(crash_case: CrashCase) {
    let directory = TempDir::new().unwrap();
    prepare(directory.path(), crash_case.scenario);

    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .arg("--exact")
        .arg("storage::crash_tests::crash_child")
        .arg("--nocapture")
        .env(SCENARIO_ENV, crash_case.scenario)
        .env(ROOT_ENV, directory.path())
        .env(crate::test_crash::POINT_ENV, crash_case.point)
        .env("RUST_BACKTRACE", "0");
    if let Some(target) = crash_case.target {
        child.env(crate::test_crash::TARGET_ENV, target);
    }
    let output = child.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(crate::test_crash::EXIT_CODE),
        "scenario {} did not terminate at {} (target {:?}); stderr:\n{}",
        crash_case.scenario,
        crash_case.point,
        crash_case.target,
        stderr
    );
    assert!(
        stderr.contains(crate::test_crash::MARKER) && stderr.contains(crash_case.point),
        "scenario {} exited without the selected crash marker; stderr:\n{}",
        crash_case.scenario,
        stderr
    );

    let storage = Storage::open(crash_config(
        directory.path(),
        crash_case.scenario == "compact",
    ))
    .unwrap_or_else(|error| {
        panic!(
            "scenario {} failed recovery after {} (target {:?}): {error}",
            crash_case.scenario, crash_case.point, crash_case.target
        )
    });
    assert_eq!(
        storage.stream_stats(STREAM).pending_records,
        crash_case.expected_pending,
        "scenario {} at {} (target {:?})",
        crash_case.scenario,
        crash_case.point,
        crash_case.target
    );
    assert_eq!(
        storage.stream_highwater(STREAM),
        crash_case.expected_highwater,
        "scenario {} at {} (target {:?})",
        crash_case.scenario,
        crash_case.point,
        crash_case.target
    );
    assert_eq!(
        storage.segments.len(),
        crash_case.expected_segments,
        "scenario {} at {} (target {:?})",
        crash_case.scenario,
        crash_case.point,
        crash_case.target
    );
    assert_eq!(
        storage.recovery_stats().completed_segment_seals,
        crash_case.expected_completed_seals,
        "scenario {} at {} (target {:?})",
        crash_case.scenario,
        crash_case.point,
        crash_case.target
    );
    assert_eq!(
        storage.recovery_stats().completed_segment_deletions,
        crash_case.expected_completed_deletions,
        "scenario {} at {} (target {:?})",
        crash_case.scenario,
        crash_case.point,
        crash_case.target
    );
    assert_eq!(
        storage.recovery_stats().removed_temporary_files,
        crash_case.expected_removed_temporaries,
        "scenario {} at {} (target {:?})",
        crash_case.scenario,
        crash_case.point,
        crash_case.target
    );
}

fn run_fault_case(fault_case: FaultCase) {
    let directory = TempDir::new().unwrap();
    prepare(directory.path(), fault_case.scenario);

    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .arg("--exact")
        .arg("storage::crash_tests::fault_child")
        .arg("--nocapture")
        .env(SCENARIO_ENV, fault_case.scenario)
        .env(ROOT_ENV, directory.path())
        .env(crate::test_crash::FAULT_POINT_ENV, fault_case.point)
        .env(crate::test_crash::FAULT_KIND_ENV, fault_case.kind)
        .env("RUST_BACKTRACE", "0");
    if let Some(target) = fault_case.target {
        child.env(crate::test_crash::FAULT_TARGET_ENV, target);
    }
    let output = child.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "scenario {} did not return the injected error at {} (target {:?}); stderr:\n{}",
        fault_case.scenario,
        fault_case.point,
        fault_case.target,
        stderr
    );
    assert!(
        stderr.contains(crate::test_crash::FAULT_MARKER) && stderr.contains(fault_case.point),
        "scenario {} exited without the selected fault marker; stderr:\n{}",
        fault_case.scenario,
        stderr
    );

    let storage = Storage::open(crash_config(
        directory.path(),
        fault_case.scenario == "compact",
    ))
    .unwrap_or_else(|error| {
        panic!(
            "scenario {} failed recovery after {} (target {:?}): {error}",
            fault_case.scenario, fault_case.point, fault_case.target
        )
    });
    let context = || {
        format!(
            "scenario {} at {} (target {:?})",
            fault_case.scenario, fault_case.point, fault_case.target
        )
    };
    assert_eq!(
        storage.stream_stats(STREAM).pending_records,
        fault_case.expected_pending,
        "{}",
        context()
    );
    assert_eq!(
        storage.stream_highwater(STREAM),
        fault_case.expected_highwater,
        "{}",
        context()
    );
    assert_eq!(
        storage.segments.len(),
        fault_case.expected_segments,
        "{}",
        context()
    );
    assert_eq!(
        storage.recovery_stats().completed_segment_seals,
        fault_case.expected_completed_seals,
        "{}",
        context()
    );
    assert_eq!(
        storage.recovery_stats().completed_segment_deletions,
        fault_case.expected_completed_deletions,
        "{}",
        context()
    );
    assert_eq!(
        storage.recovery_stats().removed_temporary_files,
        fault_case.expected_removed_temporaries,
        "{}",
        context()
    );
    assert_eq!(
        storage.recovery_stats().repaired_active_tails,
        fault_case.expected_repaired_active_tails,
        "{}",
        context()
    );
    assert_eq!(
        storage.recovery_stats().repaired_manifest_tails,
        fault_case.expected_repaired_manifest_tails,
        "{}",
        context()
    );
}

fn prepare(root: &Path, scenario: &str) {
    let bounded = scenario == "compact";
    let mut storage = Storage::open(crash_config(root, bounded)).unwrap();
    match scenario {
        "create" => {}
        "append" | "release" | "reclaim" | "compact" => {
            append(&mut storage, Bytes::from_static(b"parent"));
            if scenario == "reclaim" {
                release_all(&mut storage);
            }
        }
        "seal" => append(&mut storage, Bytes::from(vec![0x5a; 300])),
        _ => panic!("unknown crash scenario: {scenario}"),
    }
}

#[test]
fn crash_child() {
    let Ok(scenario) = std::env::var(SCENARIO_ENV) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(ROOT_ENV).expect("crash root is set"));
    let mut storage = Storage::open(crash_config(&root, scenario == "compact")).unwrap();
    match scenario.as_str() {
        "create" | "append" => append(&mut storage, Bytes::from_static(b"child")),
        "seal" => append(&mut storage, Bytes::from(vec![0xa5; 300])),
        "release" => release_all(&mut storage),
        "compact" => {
            release_all(&mut storage);
            storage.compact_manifest().unwrap();
        }
        "reclaim" => {
            storage.reclaim(ReclaimKind::Explicit).unwrap();
        }
        _ => panic!("unknown crash scenario: {scenario}"),
    }
    panic!("scenario {scenario} completed without reaching the selected crash point");
}

#[test]
fn fault_child() {
    let Ok(scenario) = std::env::var(SCENARIO_ENV) else {
        return;
    };
    let point = std::env::var(crate::test_crash::FAULT_POINT_ENV).expect("fault point is set");
    let root = PathBuf::from(std::env::var_os(ROOT_ENV).expect("fault root is set"));
    let mut storage = Storage::open(crash_config(&root, scenario == "compact")).unwrap();
    let result = match scenario.as_str() {
        "create" | "append" => append_result(&mut storage, Bytes::from_static(b"child")),
        "seal" => append_result(&mut storage, Bytes::from(vec![0xa5; 300])),
        "release" => release_all_result(&mut storage),
        "compact" => release_all_result(&mut storage).and_then(|()| storage.compact_manifest()),
        "reclaim" => storage.reclaim(ReclaimKind::Explicit).map(|_| ()),
        _ => panic!("unknown fault scenario: {scenario}"),
    };
    let error = result.expect_err("selected operation must observe the injected I/O error");
    assert_eq!(error.kind(), crate::error::ErrorKind::Io);
    assert_eq!(error.durability_outcome(), DurabilityOutcome::Unknown);
    let source = match &error {
        Error::Io { source, .. } => source,
        _ => unreachable!("error kind was checked"),
    };
    match std::env::var(crate::test_crash::FAULT_KIND_ENV).as_deref() {
        Ok("enospc") => assert_eq!(source.raw_os_error(), Some(28)),
        Ok("eio") => assert_eq!(source.raw_os_error(), Some(5)),
        Ok("write_zero") => assert_eq!(source.kind(), std::io::ErrorKind::WriteZero),
        kind => panic!("unexpected injected I/O kind: {kind:?}"),
    }
    eprintln!("{}: {point}", crate::test_crash::FAULT_MARKER);
}

fn crash_config(root: &Path, bounded: bool) -> Config {
    let capacity = if bounded {
        Capacity::Bounded {
            total_bytes: 1024 * 1024,
            when_full: FullPolicy::RejectNew,
        }
    } else {
        Capacity::Unbounded
    };
    Config::new(root, capacity)
        .with_max_epoch_bytes(512)
        .with_segment_bytes(900)
        .with_max_release_records(8)
        .with_max_commit_bytes(512)
}

fn append(storage: &mut Storage, payload: Bytes) {
    append_result(storage, payload).unwrap();
}

fn append_result(storage: &mut Storage, payload: Bytes) -> Result<()> {
    storage
        .append_group(vec![AppendUnit {
            stream_id: STREAM,
            records: vec![Record::new(payload)],
        }])
        .map(|_| ())
}

fn release_all(storage: &mut Storage) {
    release_all_result(storage).unwrap();
}

fn release_all_result(storage: &mut Storage) -> Result<()> {
    let ids = storage
        .read(STREAM, ReadLimits::new(8, 1024))
        .unwrap()
        .expect("prepared record is pending")
        .iter()
        .map(|record| record.id)
        .collect();
    storage.release_group(vec![ReleaseUnit {
        stream_id: STREAM,
        ids,
    }])
}
