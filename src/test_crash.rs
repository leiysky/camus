use std::io::{self, Write};
use std::path::Path;

pub(crate) const POINT_ENV: &str = "CAMUS_TEST_CRASH_POINT";
pub(crate) const TARGET_ENV: &str = "CAMUS_TEST_CRASH_TARGET";
pub(crate) const EXIT_CODE: i32 = 86;
pub(crate) const MARKER: &str = "camus deterministic crash point reached";
pub(crate) const FAULT_POINT_ENV: &str = "CAMUS_TEST_IO_FAULT_POINT";
pub(crate) const FAULT_TARGET_ENV: &str = "CAMUS_TEST_IO_FAULT_TARGET";
pub(crate) const FAULT_KIND_ENV: &str = "CAMUS_TEST_IO_FAULT_KIND";
pub(crate) const FAULT_MARKER: &str = "camus deterministic I/O fault observed";

pub(crate) fn hit(point: &str) {
    hit_path(point, None);
}

pub(crate) fn hit_for_path(point: &str, path: &Path) {
    hit_path(point, Some(path));
}

fn hit_path(point: &str, path: Option<&Path>) {
    if std::env::var(POINT_ENV).as_deref() != Ok(point) {
        return;
    }
    if let Ok(target) = std::env::var(TARGET_ENV) {
        let matches = path
            .and_then(Path::file_name)
            .is_some_and(|name| name == target.as_str());
        if !matches {
            return;
        }
    }

    match path {
        Some(path) => eprintln!("{MARKER}: {point} ({})", path.display()),
        None => eprintln!("{MARKER}: {point}"),
    }
    let _ = io::stderr().flush();
    std::process::exit(EXIT_CODE);
}

pub(crate) fn inject_io(point: &str) -> io::Result<()> {
    injected_io_error_inner(point, None).map_or(Ok(()), Err)
}

pub(crate) fn inject_io_for_path(point: &str, path: &Path) -> io::Result<()> {
    injected_io_error_inner(point, Some(path)).map_or(Ok(()), Err)
}

pub(crate) fn injected_io_error(point: &str) -> Option<io::Error> {
    injected_io_error_inner(point, None)
}

pub(crate) fn injected_io_error_for_path(point: &str, path: &Path) -> Option<io::Error> {
    injected_io_error_inner(point, Some(path))
}

fn injected_io_error_inner(point: &str, path: Option<&Path>) -> Option<io::Error> {
    if std::env::var(FAULT_POINT_ENV).as_deref() != Ok(point) {
        return None;
    }
    if let Ok(target) = std::env::var(FAULT_TARGET_ENV) {
        let matches = path
            .and_then(Path::file_name)
            .is_some_and(|name| name == target.as_str());
        if !matches {
            return None;
        }
    }
    let error = match std::env::var(FAULT_KIND_ENV).as_deref() {
        Ok("enospc") => io::Error::from_raw_os_error(28),
        Ok("write_zero") => io::Error::new(
            io::ErrorKind::WriteZero,
            format!("injected partial write at {point}"),
        ),
        Ok("eio") | Err(_) => io::Error::from_raw_os_error(5),
        Ok(kind) => panic!("unknown deterministic I/O fault kind: {kind}"),
    };
    Some(error)
}
