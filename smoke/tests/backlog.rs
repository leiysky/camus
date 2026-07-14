use serde_json::Value;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn killed_backlog_reopens_validates_and_drains() {
    let unique = format!(
        "camus-backlog-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let parent = std::env::temp_dir().join(unique);
    let data = parent.join("data");
    let reports = parent.join("reports");
    let output = Command::new(env!("CARGO_BIN_EXE_camus-long-smoke"))
        .args([
            "backlog",
            "--records",
            "512",
            "--streams",
            "2",
            "--append-batch-records",
            "32",
            "--read-batch-records",
            "64",
            "--payload-bytes",
            "64",
            "--segment-bytes",
            "1048576",
            "--run-id",
            "integration",
            "--data-directory",
        ])
        .arg(&data)
        .arg("--output-directory")
        .arg(&reports)
        .output()
        .expect("run backlog qualification binary");
    assert!(
        output.status.success(),
        "backlog qualification failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let report_path = reports.join("integration/backlog-report.json");
    let report: Value = serde_json::from_slice(
        &std::fs::read(&report_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", report_path.display())),
    )
    .expect("parse backlog report");
    assert_eq!(report["durable_records_before_kill"], 512);
    assert_eq!(report["validated_records"], 512);
    assert_eq!(report["pending_records_after_drain"], 0);
    assert_eq!(report["pending_records_after_final_reopen"], 0);

    std::fs::remove_dir_all(&parent)
        .unwrap_or_else(|error| panic!("remove {}: {error}", parent.display()));
}
