use crate::error::{DurabilityOutcome, Error, Result};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, IoSlice, Read, Write};
use std::os::unix::fs::FileExt as UnixFileExt;
use std::path::{Path, PathBuf};

pub(super) const ROOT_FILE: &str = "ROOT";
pub(super) const ROOT_TEMP_FILE: &str = "ROOT.tmp";
pub(super) const LOCK_FILE: &str = "camus.lock";
pub(super) const CHECKPOINT_FILE: &str = "MANIFEST.chk";
pub(super) const CHECKPOINT_TEMP_FILE: &str = "MANIFEST.chk.tmp";
pub(super) const MANIFEST_LOG_FILE: &str = "MANIFEST.log";
pub(super) const MANIFEST_LOG_TEMP_FILE: &str = "MANIFEST.log.tmp";
pub(super) const SEGMENTS_DIRECTORY: &str = "segments";

pub(super) struct RootLock(File);

pub(super) fn write_all_vectored<W: Write>(
    writer: &mut W,
    path: &Path,
    slices: &mut [IoSlice<'_>],
    operation: &'static str,
    outcome: DurabilityOutcome,
) -> Result<()> {
    // `write_vectored` writes a platform-supported prefix when the slice count
    // exceeds one syscall's iovec bound. Advancing that prefix also handles
    // ordinary partial writes without imposing a smaller fixed limit.
    let mut remaining = slices;
    while !remaining.is_empty() {
        match writer.write_vectored(remaining) {
            Ok(0) => {
                return Err(Error::io(
                    operation,
                    path,
                    outcome,
                    io::Error::new(io::ErrorKind::WriteZero, "failed to write all buffers"),
                ));
            }
            Ok(written) => IoSlice::advance_slices(&mut remaining, written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(Error::io(operation, path, outcome, error)),
        }
    }
    Ok(())
}

impl Drop for RootLock {
    fn drop(&mut self) {
        // A concurrently forked process can briefly inherit this open file
        // description before exec closes it. Explicitly unlocking prevents an
        // inherited or duplicated descriptor from extending root ownership.
        let _ = self.0.unlock();
    }
}

pub(super) fn ensure_root_directory(root: &Path) -> Result<()> {
    if root.exists() {
        if !root.is_dir() {
            return Err(Error::invalid_config(format!(
                "root path is not a directory: {}",
                root.display()
            )));
        }
        return Ok(());
    }

    let mut created_directories = vec![root.to_path_buf()];
    let mut existing_ancestor = root
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    while !existing_ancestor.exists() {
        created_directories.push(existing_ancestor.to_path_buf());
        existing_ancestor = existing_ancestor
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
    }

    fs::create_dir_all(root).map_err(|error| {
        Error::io(
            "create root directory",
            root,
            DurabilityOutcome::Unknown,
            error,
        )
    })?;

    // Persist each newly created directory before the parent entry that names
    // it. The final sync covers the nearest ancestor that existed beforehand;
    // for a single-component relative root that ancestor is `.`.
    for directory in &created_directories {
        sync_directory(directory, DurabilityOutcome::Unknown)?;
    }
    sync_directory(existing_ancestor, DurabilityOutcome::Unknown)?;
    Ok(())
}

pub(super) fn acquire_lock(root: &Path) -> Result<RootLock> {
    let path = root.join(LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| {
            Error::io(
                "open root lock",
                &path,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })?;
    match file.try_lock() {
        Ok(()) => Ok(RootLock(file)),
        Err(TryLockError::WouldBlock) => Err(Error::RootInUse {
            path: root.to_path_buf(),
        }),
        Err(TryLockError::Error(error)) => Err(Error::io(
            "lock storage root",
            &path,
            DurabilityOutcome::NotApplicable,
            error,
        )),
    }
}

pub(super) fn ensure_segments_directory(root: &Path) -> Result<PathBuf> {
    let path = root.join(SEGMENTS_DIRECTORY);
    if path.exists() {
        if !path.is_dir() {
            return Err(Error::corruption(
                &path,
                0,
                "canonical segments path is not a directory",
            ));
        }
        return Ok(path);
    }
    fs::create_dir(&path).map_err(|error| {
        Error::io(
            "create segment directory",
            &path,
            DurabilityOutcome::Unknown,
            error,
        )
    })?;
    sync_directory(root, DurabilityOutcome::Unknown)?;
    Ok(path)
}

pub(super) fn sync_directory(path: &Path, outcome: DurabilityOutcome) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io("sync directory", path, outcome, error))?;
    Ok(())
}

pub(super) fn atomic_replace(
    temporary: &Path,
    canonical: &Path,
    bytes: &[u8],
    outcome: DurabilityOutcome,
) -> Result<()> {
    let directory = canonical.parent().ok_or_else(|| {
        Error::invalid_config(format!(
            "canonical path has no parent: {}",
            canonical.display()
        ))
    })?;

    if temporary.exists() {
        fs::remove_file(temporary)
            .map_err(|error| Error::io("remove stale temporary file", temporary, outcome, error))?;
        sync_directory(directory, outcome)?;
    }

    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)
        .map_err(|error| Error::io("create temporary file", temporary, outcome, error))?;
    #[cfg(test)]
    if let Some(error) =
        crate::test_crash::injected_io_error_for_path("atomic_replace.short_write", canonical)
    {
        let prefix = bytes.len().div_ceil(2);
        file.write_all(&bytes[..prefix])
            .map_err(|error| Error::io("write temporary file", temporary, outcome, error))?;
        return Err(Error::io("write temporary file", temporary, outcome, error));
    }
    file.write_all(bytes)
        .map_err(|error| Error::io("write temporary file", temporary, outcome, error))?;
    #[cfg(test)]
    crate::test_crash::inject_io_for_path("atomic_replace.sync_data", canonical)
        .map_err(|error| Error::io("sync temporary file", temporary, outcome, error))?;
    file.sync_data()
        .map_err(|error| Error::io("sync temporary file", temporary, outcome, error))?;
    #[cfg(test)]
    crate::test_crash::hit_for_path("atomic_replace.after_data_sync", canonical);
    drop(file);
    #[cfg(test)]
    crate::test_crash::inject_io_for_path("atomic_replace.rename", canonical)
        .map_err(|error| Error::io("publish temporary file", canonical, outcome, error))?;
    fs::rename(temporary, canonical)
        .map_err(|error| Error::io("publish temporary file", canonical, outcome, error))?;
    #[cfg(test)]
    crate::test_crash::hit_for_path("atomic_replace.after_rename", canonical);
    #[cfg(test)]
    crate::test_crash::inject_io_for_path("atomic_replace.directory_sync", canonical)
        .map_err(|error| Error::io("sync directory", directory, outcome, error))?;
    sync_directory(directory, outcome)?;
    #[cfg(test)]
    crate::test_crash::hit_for_path("atomic_replace.after_directory_sync", canonical);
    Ok(())
}

pub(super) fn read_complete_file(path: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(path)
        .map_err(|error| Error::io("open file", path, DurabilityOutcome::NotApplicable, error))?;
    let length = file
        .metadata()
        .map_err(|error| {
            Error::io(
                "read file metadata",
                path,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })?
        .len();
    let length = usize::try_from(length)
        .map_err(|_| Error::corruption(path, 0, "file length does not fit usize"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|error| Error::Runtime {
            message: format!(
                "cannot reserve {} bytes to read {}: {error}",
                length,
                path.display()
            ),
        })?;
    file.read_to_end(&mut bytes)
        .map_err(|error| Error::io("read file", path, DurabilityOutcome::NotApplicable, error))?;
    Ok(bytes)
}

pub(super) fn read_exact_at(
    file: &File,
    path: &Path,
    mut bytes: &mut [u8],
    mut offset: u64,
) -> Result<()> {
    while !bytes.is_empty() {
        match file.read_at(bytes, offset) {
            Ok(0) => {
                return Err(Error::io(
                    "read file",
                    path,
                    DurabilityOutcome::NotApplicable,
                    io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of file"),
                ));
            }
            Ok(read) => {
                offset = offset
                    .checked_add(read as u64)
                    .ok_or_else(|| Error::corruption(path, offset, "read offset overflow"))?;
                bytes = &mut bytes[read..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                return Err(Error::io(
                    "read file",
                    path,
                    DurabilityOutcome::NotApplicable,
                    error,
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn segment_path(directory: &Path, segment_id: u64) -> PathBuf {
    directory.join(format!("segment-{segment_id:020}.log"))
}

pub(super) fn segment_temporary_path(directory: &Path, segment_id: u64) -> PathBuf {
    directory.join(format!("segment-{segment_id:020}.log.tmp"))
}

pub(super) fn parse_segment_name(name: &OsStr) -> Option<u64> {
    parse_segment_name_suffix(name, ".log")
}

pub(super) fn parse_segment_temporary_name(name: &OsStr) -> Option<u64> {
    parse_segment_name_suffix(name, ".log.tmp")
}

fn parse_segment_name_suffix(name: &OsStr, suffix: &str) -> Option<u64> {
    let name = name.to_str()?;
    let digits = name.strip_prefix("segment-")?.strip_suffix(suffix)?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let segment_id = digits.parse().ok()?;
    (segment_id != u64::MAX).then_some(segment_id)
}

pub(super) fn file_len(path: &Path) -> Result<u64> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| {
            Error::io(
                "read file metadata",
                path,
                DurabilityOutcome::NotApplicable,
                error,
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn root_creation_handles_a_missing_directory_chain() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("missing").join("nested");

        ensure_root_directory(&root).unwrap();

        assert!(root.is_dir());
    }

    #[test]
    fn canonical_segment_names_are_exact() {
        let directory = Path::new("segments");
        let path = segment_path(directory, 42);
        assert_eq!(
            path.file_name().and_then(OsStr::to_str),
            Some("segment-00000000000000000042.log")
        );
        assert_eq!(parse_segment_name(path.file_name().unwrap()), Some(42));
        assert_eq!(
            parse_segment_temporary_name(OsStr::new("segment-00000000000000000042.log.tmp")),
            Some(42)
        );
        assert_eq!(parse_segment_name(OsStr::new("segment-42.log")), None);
    }

    #[test]
    fn root_lock_drop_unlocks_a_duplicated_descriptor() {
        let directory = TempDir::new().unwrap();
        let lock = acquire_lock(directory.path()).unwrap();
        let duplicate = lock.0.try_clone().unwrap();

        drop(lock);
        let reacquired = acquire_lock(directory.path()).unwrap();

        drop(reacquired);
        drop(duplicate);
    }
}
