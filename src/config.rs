use crate::error::{Error, Result};
use crate::format::{
    EPOCH_COMMIT_LEN, EPOCH_HEADER_LEN, MANIFEST_FRAME_HEADER_LEN, RECORD_DESCRIPTOR_LEN,
    SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN,
};
use crate::runtime::Runtime;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Default hard final size of one physical data segment.
pub const DEFAULT_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
/// Default hard encoded size of one append epoch.
pub const DEFAULT_MAX_EPOCH_BYTES: u64 = 8 * 1024 * 1024;
/// Default hard number of IDs in one release call.
pub const DEFAULT_MAX_RELEASE_RECORDS: usize = 65_536;
/// Default maximum commit units sharing one durability barrier.
pub const DEFAULT_MAX_COMMIT_UNITS: usize = 64;
/// Default maximum encoded bytes in one commit group.
pub const DEFAULT_MAX_COMMIT_BYTES: u64 = 8 * 1024 * 1024;
/// Default number of admitted commands buffered for one root.
pub const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 1_024;

/// Root-wide encoded storage capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Capacity {
    /// Do not impose a Camus byte budget.
    Unbounded,
    /// Enforce one total root budget and explicit full policy.
    Bounded {
        /// Total encoded Camus bytes, including maintenance headroom.
        total_bytes: u64,
        /// Behavior when a new append is not currently admissible.
        when_full: FullPolicy,
    },
}

/// Admission behavior for a full bounded root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FullPolicy {
    /// Wait asynchronously outside the command queue for capacity.
    Block,
    /// Return a typed rejection before operation admission.
    RejectNew,
}

/// Configuration used to open one storage root.
#[derive(Clone)]
pub struct Config {
    pub(crate) root: PathBuf,
    pub(crate) capacity: Capacity,
    pub(crate) segment_bytes: u64,
    pub(crate) max_segment_age: Option<Duration>,
    pub(crate) max_epoch_bytes: u64,
    pub(crate) max_release_records: usize,
    pub(crate) max_commit_units: usize,
    pub(crate) max_commit_bytes: u64,
    pub(crate) command_queue_capacity: usize,
    pub(crate) detailed_observability: bool,
    pub(crate) runtime: Option<Arc<dyn Runtime>>,
}

impl Config {
    /// Creates a root configuration with explicit capacity semantics.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, capacity: Capacity) -> Self {
        Self {
            root: root.into(),
            capacity,
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            max_segment_age: None,
            max_epoch_bytes: DEFAULT_MAX_EPOCH_BYTES,
            max_release_records: DEFAULT_MAX_RELEASE_RECORDS,
            max_commit_units: DEFAULT_MAX_COMMIT_UNITS,
            max_commit_bytes: DEFAULT_MAX_COMMIT_BYTES,
            command_queue_capacity: DEFAULT_COMMAND_QUEUE_CAPACITY,
            detailed_observability: false,
            runtime: None,
        }
    }

    /// Returns the configured root path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Replaces the hard final segment-size bound.
    #[must_use]
    pub fn with_segment_bytes(mut self, bytes: u64) -> Self {
        self.segment_bytes = bytes;
        self
    }

    /// Enables the soft reactor-driven segment-age deadline.
    #[must_use]
    pub fn with_max_segment_age(mut self, age: Duration) -> Self {
        self.max_segment_age = Some(age);
        self
    }

    /// Disables age-based segment rollover.
    #[must_use]
    pub fn without_max_segment_age(mut self) -> Self {
        self.max_segment_age = None;
        self
    }

    /// Replaces the hard encoded append-epoch bound.
    #[must_use]
    pub fn with_max_epoch_bytes(mut self, bytes: u64) -> Self {
        self.max_epoch_bytes = bytes;
        self
    }

    /// Replaces the hard record-count bound for one release request.
    #[must_use]
    pub fn with_max_release_records(mut self, records: usize) -> Self {
        self.max_release_records = records;
        self
    }

    /// Replaces the maximum commit units sharing one durability barrier.
    #[must_use]
    pub fn with_max_commit_units(mut self, units: usize) -> Self {
        self.max_commit_units = units;
        self
    }

    /// Replaces the maximum encoded bytes in one commit group.
    #[must_use]
    pub fn with_max_commit_bytes(mut self, bytes: u64) -> Self {
        self.max_commit_bytes = bytes;
        self
    }

    /// Replaces the admitted command-queue bound.
    #[must_use]
    pub fn with_command_queue_capacity(mut self, commands: usize) -> Self {
        self.command_queue_capacity = commands;
        self
    }

    /// Enables monotonic-clock timing for logical calls and storage jobs.
    ///
    /// Current gauges, counters, wait durations, recovery duration, and health
    /// transitions remain available when this option is disabled. The default
    /// avoids the additional end-to-end clock reads for every logical call and
    /// storage job. Time spent in an actual wait is always measured.
    #[must_use]
    pub fn with_detailed_observability(mut self) -> Self {
        self.detailed_observability = true;
        self
    }

    /// Disables per-operation and per-storage-job timing.
    #[must_use]
    pub fn without_detailed_observability(mut self) -> Self {
        self.detailed_observability = false;
        self
    }

    /// Uses a caller-provided runtime backend instead of the shared default.
    #[must_use]
    pub fn with_runtime(mut self, runtime: Arc<dyn Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.root.as_os_str().is_empty() {
            return Err(Error::invalid_config("root path must not be empty"));
        }
        if matches!(self.capacity, Capacity::Bounded { total_bytes: 0, .. }) {
            return Err(Error::invalid_config(
                "bounded capacity must be greater than zero",
            ));
        }
        if self.command_queue_capacity == 0 {
            return Err(Error::invalid_config(
                "command queue capacity must be greater than zero",
            ));
        }
        if self.max_commit_units == 0 {
            return Err(Error::invalid_config(
                "max_commit_units must be greater than zero",
            ));
        }
        if self.max_release_records == 0 {
            return Err(Error::invalid_config(
                "max_release_records must be greater than zero",
            ));
        }

        let minimum_epoch = EPOCH_HEADER_LEN
            .checked_add(RECORD_DESCRIPTOR_LEN)
            .and_then(|bytes| bytes.checked_add(EPOCH_COMMIT_LEN))
            .expect("fixed format sizes fit u64");
        if self.max_epoch_bytes < minimum_epoch {
            return Err(Error::invalid_config(format!(
                "max_epoch_bytes must be at least {minimum_epoch}"
            )));
        }

        let minimum_segment = SEGMENT_HEADER_LEN
            .checked_add(self.max_epoch_bytes)
            .and_then(|bytes| bytes.checked_add(SEGMENT_FOOTER_LEN))
            .ok_or_else(|| Error::invalid_config("segment size calculation overflowed"))?;
        if self.segment_bytes < minimum_segment {
            return Err(Error::invalid_config(format!(
                "segment_bytes must be at least {minimum_segment} for the configured epoch bound"
            )));
        }

        let release_records = u64::try_from(self.max_release_records)
            .map_err(|_| Error::invalid_config("max_release_records does not fit u64"))?;
        let maximal_release_frame = MANIFEST_FRAME_HEADER_LEN
            .checked_add(24)
            .and_then(|bytes| {
                release_records
                    .checked_mul(16)
                    .and_then(|n| bytes.checked_add(n))
            })
            .ok_or_else(|| Error::invalid_config("release frame size calculation overflowed"))?;
        let minimum_commit_bytes = self.max_epoch_bytes.max(maximal_release_frame);
        if self.max_commit_bytes < minimum_commit_bytes {
            return Err(Error::invalid_config(format!(
                "max_commit_bytes must be at least {minimum_commit_bytes}"
            )));
        }

        if let Some(age) = self.max_segment_age {
            if age.as_millis() == 0 {
                return Err(Error::invalid_config(
                    "max_segment_age must be at least one millisecond",
                ));
            }
            let milliseconds = u64::try_from(age.as_millis()).map_err(|_| {
                Error::invalid_config("max_segment_age milliseconds do not fit u64")
            })?;
            if Duration::from_millis(milliseconds) != age {
                return Err(Error::invalid_config(
                    "max_segment_age must use whole milliseconds",
                ));
            }
        }

        Ok(())
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("root", &self.root)
            .field("capacity", &self.capacity)
            .field("segment_bytes", &self.segment_bytes)
            .field("max_segment_age", &self.max_segment_age)
            .field("max_epoch_bytes", &self.max_epoch_bytes)
            .field("max_release_records", &self.max_release_records)
            .field("max_commit_units", &self.max_commit_units)
            .field("max_commit_bytes", &self.max_commit_bytes)
            .field("command_queue_capacity", &self.command_queue_capacity)
            .field("detailed_observability", &self.detailed_observability)
            .field("custom_runtime", &self.runtime.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_configuration_satisfies_progress_bounds() {
        let config = Config::new("root", Capacity::Unbounded);
        config.validate().unwrap();
    }

    #[test]
    fn segment_must_fit_header_epoch_and_footer() {
        let config = Config::new("root", Capacity::Unbounded)
            .with_max_epoch_bytes(1024)
            .with_segment_bytes(1024 + SEGMENT_HEADER_LEN + SEGMENT_FOOTER_LEN - 1);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfig { .. })
        ));
    }
}
