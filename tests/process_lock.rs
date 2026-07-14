use camus::{Config, Error, Log};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};

const HELPER_ROOT: &str = "CAMUS_PROCESS_LOCK_HELPER_ROOT";
const READY: &str = "CAMUS_PROCESS_LOCK_READY";

#[test]
fn storage_root_lock_is_exclusive_across_processes() {
    let directory = tempfile::tempdir().unwrap();
    let executable = std::env::current_exe().unwrap();
    let mut child = Command::new(executable)
        .args(["--exact", "lock_holder_helper", "--nocapture"])
        .env(HELPER_ROOT, directory.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut stdout = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        let read = stdout.read_line(&mut line).unwrap();
        assert_ne!(
            read, 0,
            "lock-holder child exited before acquiring the lock"
        );
        if line.contains(READY) {
            break;
        }
    }

    let second_open = Log::open(Config::new(directory.path()));
    child.stdin.take().unwrap().write_all(b"x").unwrap();
    let mut remaining_output = String::new();
    stdout.read_to_string(&mut remaining_output).unwrap();
    let status = child.wait().unwrap();
    assert!(
        status.success(),
        "lock-holder child failed: {remaining_output}"
    );

    let error = second_open
        .err()
        .expect("a second process must not acquire the root lock");
    assert!(matches!(error, Error::RootInUse(_)));

    drop(Log::open(Config::new(directory.path())).unwrap());
}

#[test]
fn lock_holder_helper() {
    let Some(root) = std::env::var_os(HELPER_ROOT) else {
        return;
    };
    let log = Log::open(Config::new(root)).unwrap();
    println!("{READY}");
    std::io::stdout().flush().unwrap();

    let mut release = [0_u8; 1];
    std::io::stdin().read_exact(&mut release).unwrap();
    drop(log);
}
