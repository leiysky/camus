use crate::config::{Capacity, Config, FullPolicy};
use crate::error::{DurabilityOutcome, Error, Result};
use crate::model::{
    CommitStats, DurationStats, FailureInfo, MaintenanceStats, OperationCounters, OperationKind,
    OperationStats, PendingSnapshot, PressureStats, ReadLimits, ReclaimReport, Record, RecordId,
    RecoveryStats, RootHealth, RootId, RootState, RootStats, StorageJobStats, StorageStats,
    StreamId, StreamStats, WaitStats,
};
use crate::runtime::{default_runtime, run_blocking, run_blocking_guarded, Runtime, RuntimeFuture};
use crate::storage::{
    encoded_epoch_bytes, AppendUnit, CapacityCheck, ReclaimKind, ReleaseUnit, Storage,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};

const RUNNING: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const CLOSED: u8 = 2;
const POISONED: u8 = 3;

/// A thread-safe client for one open Camus storage root.
#[derive(Clone)]
pub struct Log {
    shared: Arc<Shared>,
}

/// A lightweight logical-stream handle backed by one root reactor.
#[derive(Clone)]
pub struct Stream {
    shared: Arc<Shared>,
    id: StreamId,
}

/// A non-owning asynchronous watch of low-frequency root health transitions.
///
/// A watch coalesces intermediate changes and never backpressures the root
/// reactor. It is an observability primitive, not a reliable event stream.
#[derive(Clone)]
pub struct HealthWatch {
    receiver: watch::Receiver<RootHealth>,
}

#[derive(Clone, Copy)]
struct Limits {
    capacity: Capacity,
    max_epoch_bytes: u64,
    max_release_records: usize,
    max_commit_units: usize,
    max_commit_bytes: u64,
    max_append_group_bytes: u64,
    max_bounded_release_group_bytes: u64,
}

#[derive(Clone, Eq, PartialEq)]
struct View {
    storage: StorageStats,
    commits: CommitStats,
    maintenance: MaintenanceStats,
    recovery: RecoveryStats,
    known_streams: Vec<StreamId>,
    stream_stats: BTreeMap<StreamId, StreamStats>,
    highwaters: BTreeMap<StreamId, u64>,
}

struct Shared {
    sender: mpsc::Sender<Command>,
    shutdown: watch::Sender<bool>,
    events: watch::Sender<u64>,
    health: watch::Sender<RootHealth>,
    view: RwLock<View>,
    root_id: RootId,
    limits: Limits,
    lifecycle: AtomicU8,
    shutdown_started: AtomicBool,
    reactor_finished: AtomicBool,
    active_storage_jobs: AtomicUsize,
    queue_depth: AtomicUsize,
    admitted_commands: AtomicU64,
    command_queue_capacity: usize,
    queue_wait: WaitCounters,
    readiness_wait: WaitCounters,
    capacity_wait: WaitCounters,
    reactor_dispatch_wait: AtomicDurationStats,
    operations: AtomicOperationStats,
    storage_job_elapsed: AtomicDurationStats,
    storage_jobs: AtomicStorageJobStats,
    detailed_observability: bool,
}

#[derive(Default)]
struct AtomicDurationStats {
    observations: AtomicU64,
    total_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

#[derive(Default)]
struct WaitCounters {
    current: AtomicUsize,
    waits: AtomicU64,
    elapsed: AtomicDurationStats,
}

#[derive(Default)]
struct AtomicOperationCounters {
    started: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    records: AtomicU64,
    payload_bytes: AtomicU64,
    elapsed: AtomicDurationStats,
}

#[derive(Default)]
struct AtomicOperationStats {
    append: AtomicOperationCounters,
    read: AtomicOperationCounters,
    release: AtomicOperationCounters,
    reclaim: AtomicOperationCounters,
}

#[derive(Default)]
struct AtomicStorageJobStats {
    append: AtomicDurationStats,
    read: AtomicDurationStats,
    release: AtomicDurationStats,
    reclaim: AtomicDurationStats,
    segment_rollover: AtomicDurationStats,
}

#[derive(Clone, Copy)]
enum StorageJobKind {
    Append,
    Read,
    Release,
    Reclaim,
    SegmentRollover,
}

#[derive(Clone, Copy)]
enum WaitKind {
    Readiness,
    Capacity,
}

struct WaitActivity<'a> {
    counters: &'a WaitCounters,
    started: Instant,
}

struct OperationActivity<'a> {
    counters: &'a AtomicOperationCounters,
    started: Option<Instant>,
    finished: bool,
}

enum AppendReply {
    Complete(Result<Vec<RecordId>>),
    Wait { records: Vec<Record> },
}

type AppendEntry = (StreamId, Vec<Record>, u64, oneshot::Sender<AppendReply>);

enum AppendJob {
    Complete {
        outputs: Vec<Vec<RecordId>>,
        selected: usize,
    },
    Capacity(CapacityCheck),
    Failure {
        error: Error,
        selected: usize,
    },
}

enum Command {
    Append {
        queued_at: Option<Instant>,
        stream_id: StreamId,
        records: Vec<Record>,
        encoded_bytes: u64,
        reply: oneshot::Sender<AppendReply>,
    },
    Read {
        queued_at: Option<Instant>,
        stream_id: StreamId,
        limits: ReadLimits,
        reply: oneshot::Sender<Result<Option<PendingSnapshot>>>,
    },
    Release {
        queued_at: Option<Instant>,
        stream_id: StreamId,
        ids: Vec<RecordId>,
        encoded_bound: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    Reclaim {
        queued_at: Option<Instant>,
        reply: oneshot::Sender<Result<ReclaimReport>>,
    },
}

struct ReactorTask {
    future: Option<RuntimeFuture>,
    termination: ReactorTermination,
}

struct ReactorTermination {
    shared: Weak<Shared>,
    finished: bool,
}

struct StorageJobActivity {
    shared: Weak<Shared>,
    kind: StorageJobKind,
    armed: bool,
    started: Option<Instant>,
}

impl Log {
    /// Opens and recovers one storage root on the configured runtime.
    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let runtime = match &config.runtime {
            Some(runtime) => runtime.clone(),
            None => default_runtime()?,
        };
        let limits = Limits {
            capacity: config.capacity,
            max_epoch_bytes: config.max_epoch_bytes,
            max_release_records: config.max_release_records,
            max_commit_units: config.max_commit_units,
            max_commit_bytes: config.max_commit_bytes,
            max_append_group_bytes: config.max_commit_bytes.min(
                config.segment_bytes
                    - crate::format::SEGMENT_HEADER_LEN
                    - crate::format::SEGMENT_FOOTER_LEN,
            ),
            max_bounded_release_group_bytes: config.max_commit_bytes.min(
                72_u64
                    .checked_add(
                        u64::try_from(config.max_release_records)
                            .map_err(|_| {
                                Error::invalid_config("max_release_records does not fit u64")
                            })?
                            .checked_mul(16)
                            .ok_or_else(|| {
                                Error::invalid_config("release group byte bound overflow")
                            })?,
                    )
                    .ok_or_else(|| Error::invalid_config("release group byte bound overflow"))?,
            ),
        };
        let queue_capacity = config.command_queue_capacity;
        let detailed_observability = config.detailed_observability;
        let storage = run_blocking(runtime.clone(), move || Storage::open(config)).await??;
        let root_id = storage.root_id();
        let view = view_from_storage(&storage)?;
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (events, _) = watch::channel(0_u64);
        let (health, _) = watch::channel(RootHealth::default());
        let shared = Arc::new(Shared {
            sender,
            shutdown,
            events,
            health,
            view: RwLock::new(view),
            root_id,
            limits,
            lifecycle: AtomicU8::new(RUNNING),
            shutdown_started: AtomicBool::new(false),
            reactor_finished: AtomicBool::new(false),
            active_storage_jobs: AtomicUsize::new(0),
            queue_depth: AtomicUsize::new(0),
            admitted_commands: AtomicU64::new(0),
            command_queue_capacity: queue_capacity,
            queue_wait: WaitCounters::default(),
            readiness_wait: WaitCounters::default(),
            capacity_wait: WaitCounters::default(),
            reactor_dispatch_wait: AtomicDurationStats::default(),
            operations: AtomicOperationStats::default(),
            storage_job_elapsed: AtomicDurationStats::default(),
            storage_jobs: AtomicStorageJobStats::default(),
            detailed_observability,
        });
        let weak = Arc::downgrade(&shared);
        let reactor = ReactorTask {
            future: Some(Box::pin(reactor_loop(
                runtime.clone(),
                storage,
                receiver,
                shutdown_receiver,
                weak.clone(),
                limits,
            ))),
            termination: ReactorTermination {
                shared: weak,
                finished: false,
            },
        };
        runtime
            .spawn(Box::pin(reactor))
            .map_err(|error| Error::Runtime {
                message: error.to_string(),
            })?;
        if shared.reactor_finished.load(Ordering::Acquire) {
            return Err(Error::Runtime {
                message: "root reactor terminated during startup".to_string(),
            });
        }
        Ok(Self { shared })
    }

    /// Constructs a lightweight handle for a caller-selected logical stream.
    #[must_use]
    pub fn stream(&self, id: StreamId) -> Stream {
        Stream {
            shared: self.shared.clone(),
            id,
        }
    }

    /// Returns a sorted in-memory snapshot of every durably known stream.
    #[must_use]
    pub fn known_streams(&self) -> Vec<StreamId> {
        self.shared.read_view().known_streams.clone()
    }

    /// Returns a synchronous in-memory snapshot of root state.
    #[must_use]
    pub fn stats(&self) -> RootStats {
        self.shared.stats()
    }

    /// Returns the current low-frequency lifecycle and failure state.
    #[must_use]
    pub fn health(&self) -> RootHealth {
        self.shared.health.borrow().clone()
    }

    /// Creates a non-owning asynchronous watch of future health transitions.
    #[must_use]
    pub fn watch_health(&self) -> HealthWatch {
        HealthWatch {
            receiver: self.shared.health.subscribe(),
        }
    }

    /// Requests and awaits one physical maintenance pass.
    pub async fn reclaim(&self) -> Result<ReclaimReport> {
        let activity = OperationActivity::new(
            &self.shared.operations.reclaim,
            self.shared.detailed_observability,
        );
        let result = async {
            let (reply, response) = oneshot::channel();
            let permit = self.shared.reserve_running().await?;
            permit.send(Command::Reclaim {
                queued_at: self.shared.dispatch_timestamp(),
                reply,
            });
            receive_response(&self.shared, response).await
        }
        .await;
        match result {
            Ok(report) => {
                activity.succeed(0, 0);
                Ok(report)
            }
            Err(error) => {
                activity.fail();
                Err(error)
            }
        }
    }

    /// Closes operation admission and drains every already admitted command.
    pub async fn shutdown(&self) -> Result<()> {
        if self.shared.lifecycle.load(Ordering::Acquire) == CLOSED {
            return Ok(());
        }
        let mut events = self.shared.events.subscribe();
        if !self.shared.shutdown_started.swap(true, Ordering::AcqRel) {
            if self.shared.lifecycle.load(Ordering::Acquire) != POISONED {
                self.shared
                    .lifecycle
                    .store(SHUTTING_DOWN, Ordering::Release);
                self.shared.set_health_state(RootState::ShuttingDown);
            }
            self.shared.shutdown.send_replace(true);
            self.shared.notify();
        }
        loop {
            if self.shared.lifecycle.load(Ordering::Acquire) == CLOSED {
                return Ok(());
            }
            if self.shared.reactor_finished.load(Ordering::Acquire)
                && self.shared.active_storage_jobs.load(Ordering::Acquire) == 0
            {
                if self.shared.lifecycle.swap(CLOSED, Ordering::AcqRel) != CLOSED {
                    self.shared.set_health_state(RootState::Closed);
                    self.shared.notify();
                }
                return Ok(());
            }
            events.changed().await.map_err(|_| Error::Runtime {
                message: "root reactor terminated before completing shutdown".to_string(),
            })?;
        }
    }
}

impl HealthWatch {
    /// Returns the latest lifecycle value without waiting.
    #[must_use]
    pub fn current(&self) -> RootHealth {
        self.receiver.borrow().clone()
    }

    /// Waits for a later published lifecycle value.
    ///
    /// Returns `None` after the root state has been dropped and no further
    /// transition can be published.
    pub async fn changed(&mut self) -> Option<RootHealth> {
        self.receiver.changed().await.ok()?;
        Some(self.current())
    }
}

impl Stream {
    /// Returns this handle's caller-selected logical stream ID.
    #[must_use]
    pub const fn id(&self) -> StreamId {
        self.id
    }

    /// Returns a synchronous in-memory snapshot of this stream.
    #[must_use]
    pub fn stats(&self) -> StreamStats {
        self.shared
            .read_view()
            .stream_stats
            .get(&self.id)
            .cloned()
            .unwrap_or_default()
    }

    /// Appends one opaque record as one independent durability epoch.
    pub async fn append(&self, record: Record) -> Result<RecordId> {
        let mut ids = self.append_batch(vec![record]).await?;
        Ok(ids.remove(0))
    }

    /// Appends an ordered non-empty batch as one durability epoch.
    pub async fn append_batch(&self, mut records: Vec<Record>) -> Result<Vec<RecordId>> {
        let payload_bytes = records.iter().fold(0_u64, |total, record| {
            total.saturating_add(u64::try_from(record.payload.len()).unwrap_or(u64::MAX))
        });
        let activity = OperationActivity::new(
            &self.shared.operations.append,
            self.shared.detailed_observability,
        );
        let result = async {
            let encoded_bytes = encoded_epoch_bytes(&records)?;
            if encoded_bytes > self.shared.limits.max_epoch_bytes {
                return Err(Error::EpochTooLarge {
                    encoded_bytes,
                    max_bytes: self.shared.limits.max_epoch_bytes,
                });
            }
            self.preflight_sequence(records.len())?;
            let mut events = self.shared.events.subscribe();
            loop {
                let (reply, response) = oneshot::channel();
                let permit = self.shared.reserve_running().await?;
                permit.send(Command::Append {
                    queued_at: self.shared.dispatch_timestamp(),
                    stream_id: self.id,
                    records,
                    encoded_bytes,
                    reply,
                });
                match response.await {
                    Ok(AppendReply::Complete(result)) => break result,
                    Ok(AppendReply::Wait { records: returned }) => {
                        records = returned;
                        self.shared
                            .wait_for_change(&mut events, WaitKind::Capacity)
                            .await?;
                    }
                    Err(_) => break Err(self.shared.channel_error()),
                }
            }
        }
        .await;
        match result {
            Ok(ids) => {
                activity.succeed(u64::try_from(ids.len()).unwrap_or(u64::MAX), payload_bytes);
                Ok(ids)
            }
            Err(error) => {
                activity.fail();
                Err(error)
            }
        }
    }

    /// Waits for and returns a non-empty bounded snapshot of pending records.
    pub async fn read(&self, limits: ReadLimits) -> Result<PendingSnapshot> {
        let activity = OperationActivity::new(
            &self.shared.operations.read,
            self.shared.detailed_observability,
        );
        let result = async {
            if limits.max_records == 0 {
                return Err(Error::InvalidReadLimits);
            }
            let mut events = self.shared.events.subscribe();
            loop {
                self.shared.ensure_running()?;
                if self.stats().pending_records == 0 {
                    self.shared
                        .wait_for_change(&mut events, WaitKind::Readiness)
                        .await?;
                    continue;
                }
                let (reply, response) = oneshot::channel();
                let permit = self.shared.reserve_running().await?;
                permit.send(Command::Read {
                    queued_at: self.shared.dispatch_timestamp(),
                    stream_id: self.id,
                    limits,
                    reply,
                });
                match receive_response(&self.shared, response).await? {
                    Some(snapshot) => break Ok(snapshot),
                    None => {
                        self.shared
                            .wait_for_change(&mut events, WaitKind::Readiness)
                            .await?;
                    }
                }
            }
        }
        .await;
        match result {
            Ok(snapshot) => {
                let payload_bytes = snapshot.iter().fold(0_u64, |total, record| {
                    total.saturating_add(u64::try_from(record.payload.len()).unwrap_or(u64::MAX))
                });
                activity.succeed(
                    u64::try_from(snapshot.len()).unwrap_or(u64::MAX),
                    payload_bytes,
                );
                Ok(snapshot)
            }
            Err(error) => {
                activity.fail();
                Err(error)
            }
        }
    }

    /// Durably removes an exact record subset from the shared pending set.
    pub async fn release(&self, ids: Vec<RecordId>) -> Result<()> {
        let released_records = u64::try_from(ids.len()).unwrap_or(u64::MAX);
        let activity = OperationActivity::new(
            &self.shared.operations.release,
            self.shared.detailed_observability,
        );
        let result = async {
            if ids.len() > self.shared.limits.max_release_records {
                return Err(Error::ReleaseTooLarge {
                    records: ids.len(),
                    max_records: self.shared.limits.max_release_records,
                });
            }
            self.preflight_release(&ids)?;
            if ids.is_empty() {
                return Ok(());
            }
            let count = u64::try_from(ids.len())
                .map_err(|_| Error::invalid_config("release ID count does not fit u64"))?;
            let encoded_bound = 72_u64
                .checked_add(
                    count
                        .checked_mul(16)
                        .ok_or_else(|| Error::invalid_config("release bound overflow"))?,
                )
                .ok_or_else(|| Error::invalid_config("release bound overflow"))?;
            let (reply, response) = oneshot::channel();
            let permit = self.shared.reserve_running().await?;
            permit.send(Command::Release {
                queued_at: self.shared.dispatch_timestamp(),
                stream_id: self.id,
                ids,
                encoded_bound,
                reply,
            });
            receive_response(&self.shared, response).await
        }
        .await;
        match result {
            Ok(()) => {
                activity.succeed(released_records, 0);
                Ok(())
            }
            Err(error) => {
                activity.fail();
                Err(error)
            }
        }
    }

    fn preflight_sequence(&self, records: usize) -> Result<()> {
        let count = u64::try_from(records)
            .map_err(|_| Error::invalid_config("append record count does not fit u64"))?;
        if let Some(highwater) = self.shared.read_view().highwaters.get(&self.id).copied() {
            highwater
                .checked_add(count)
                .ok_or(Error::SequenceExhausted { stream_id: self.id })?;
        }
        Ok(())
    }

    fn preflight_release(&self, ids: &[RecordId]) -> Result<()> {
        let view = self.shared.read_view();
        let highwater = view.highwaters.get(&self.id).copied();
        for id in ids {
            if id.root_id() != self.shared.root_id || id.stream_id() != self.id {
                return Err(Error::RecordIdScopeMismatch {
                    id: *id,
                    expected_stream: self.id,
                });
            }
            if highwater.is_none_or(|value| id.sequence() > value) {
                return Err(Error::UnknownRecordId { id: *id });
            }
        }
        Ok(())
    }
}

impl fmt::Debug for Log {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Log")
            .field("known_streams", &self.known_streams().len())
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Stream")
            .field("id", &self.id)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl Shared {
    fn dispatch_timestamp(&self) -> Option<Instant> {
        self.detailed_observability.then(Instant::now)
    }

    fn observe_dispatch_wait(&self, queued_at: Option<Instant>) {
        if let Some(queued_at) = queued_at {
            self.reactor_dispatch_wait.observe(queued_at.elapsed());
        }
    }

    async fn reserve_running(&self) -> Result<mpsc::Permit<'_, Command>> {
        self.ensure_running()?;
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                let waiting = WaitActivity::new(&self.queue_wait);
                let permit = self
                    .sender
                    .reserve()
                    .await
                    .map_err(|_| self.channel_error())?;
                drop(waiting);
                permit
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(self.channel_error()),
        };
        self.ensure_running()?;
        self.mark_admitted();
        Ok(permit)
    }

    async fn wait_for_change(
        &self,
        events: &mut watch::Receiver<u64>,
        kind: WaitKind,
    ) -> Result<()> {
        self.ensure_running()?;
        let counters = match kind {
            WaitKind::Readiness => &self.readiness_wait,
            WaitKind::Capacity => &self.capacity_wait,
        };
        let waiting = WaitActivity::new(counters);
        let changed = events.changed().await;
        drop(waiting);
        if changed.is_err() {
            return Err(self.channel_error());
        }
        self.ensure_running()
    }

    fn ensure_running(&self) -> Result<()> {
        match self.lifecycle.load(Ordering::Acquire) {
            RUNNING => Ok(()),
            POISONED => Err(Error::Poisoned),
            SHUTTING_DOWN | CLOSED => Err(Error::Closed),
            _ => Err(Error::Runtime {
                message: "invalid root lifecycle state".to_string(),
            }),
        }
    }

    fn channel_error(&self) -> Error {
        match self.lifecycle.load(Ordering::Acquire) {
            POISONED => Error::Poisoned,
            SHUTTING_DOWN | CLOSED => Error::Closed,
            _ => Error::Runtime {
                message: "root reactor terminated unexpectedly".to_string(),
            },
        }
    }

    fn read_view(&self) -> std::sync::RwLockReadGuard<'_, View> {
        self.view
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn stats(&self) -> RootStats {
        let (storage, commits, maintenance, recovery) = {
            let view = self.read_view();
            (view.storage, view.commits, view.maintenance, view.recovery)
        };
        RootStats {
            detailed_timings: self.detailed_observability,
            storage,
            pressure: PressureStats {
                command_queue_capacity: self.command_queue_capacity,
                queue_depth: self.queue_depth.load(Ordering::Acquire),
                active_storage_jobs: self.active_storage_jobs.load(Ordering::Acquire),
                admitted_commands: self.admitted_commands.load(Ordering::Acquire),
                reactor_dispatch_wait: self.reactor_dispatch_wait.snapshot(),
                storage_job_elapsed: self.storage_job_elapsed.snapshot(),
                storage_jobs: self.storage_jobs.snapshot(),
                queue_wait: self.queue_wait.snapshot(),
                readiness_wait: self.readiness_wait.snapshot(),
                capacity_wait: self.capacity_wait.snapshot(),
            },
            operations: self.operations.snapshot(),
            commits,
            maintenance,
            recovery,
        }
    }

    fn mark_admitted(&self) {
        self.queue_depth.fetch_add(1, Ordering::AcqRel);
        atomic_saturating_add(&self.admitted_commands, 1);
    }

    fn mark_completed(&self, count: usize) {
        let _ = self
            .queue_depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(count))
            });
    }

    fn notify(&self) {
        self.events
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    fn set_health_state(&self, state: RootState) {
        let _ = self.health.send_if_modified(|health| {
            if health.state != state {
                health.generation = health.generation.saturating_add(1);
                health.state = state;
                true
            } else {
                false
            }
        });
    }

    fn poison(&self, operation: OperationKind, error: &Error) {
        if self.lifecycle.load(Ordering::Acquire) == CLOSED {
            return;
        }
        self.lifecycle.store(POISONED, Ordering::Release);
        let failure = FailureInfo {
            operation,
            error_kind: error.kind(),
            durability_outcome: match error.durability_outcome() {
                DurabilityOutcome::Unknown => DurabilityOutcome::Unknown,
                DurabilityOutcome::NotApplicable
                    if matches!(
                        operation,
                        OperationKind::Append
                            | OperationKind::Release
                            | OperationKind::Reclaim
                            | OperationKind::SegmentRollover
                            | OperationKind::StatePublication
                    ) || (operation == OperationKind::Reactor
                        && self.active_storage_jobs.load(Ordering::Acquire) != 0) =>
                {
                    DurabilityOutcome::Unknown
                }
                DurabilityOutcome::NotApplicable => DurabilityOutcome::NotApplicable,
            },
            message: error.to_string(),
        };
        let _ = self.health.send_if_modified(|health| {
            let mut changed = false;
            if health.state != RootState::Poisoned {
                health.generation = health.generation.saturating_add(1);
                health.state = RootState::Poisoned;
                changed = true;
            }
            if health.failure.is_none() {
                health.failure = Some(failure.clone());
                changed = true;
            }
            changed
        });
        self.notify();
    }
}

impl Command {
    fn queued_at(&self) -> Option<Instant> {
        match self {
            Self::Append { queued_at, .. }
            | Self::Read { queued_at, .. }
            | Self::Release { queued_at, .. }
            | Self::Reclaim { queued_at, .. } => *queued_at,
        }
    }
}

impl AtomicDurationStats {
    fn observe(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        atomic_saturating_add(&self.observations, 1);
        atomic_saturating_add(&self.total_nanos, nanos);
        self.max_nanos.fetch_max(nanos, Ordering::AcqRel);
    }

    fn snapshot(&self) -> DurationStats {
        DurationStats {
            observations: self.observations.load(Ordering::Acquire),
            total: Duration::from_nanos(self.total_nanos.load(Ordering::Acquire)),
            max: Duration::from_nanos(self.max_nanos.load(Ordering::Acquire)),
        }
    }
}

impl WaitCounters {
    fn snapshot(&self) -> WaitStats {
        WaitStats {
            current: self.current.load(Ordering::Acquire),
            waits: self.waits.load(Ordering::Acquire),
            elapsed: self.elapsed.snapshot(),
        }
    }
}

impl AtomicOperationCounters {
    fn snapshot(&self) -> OperationCounters {
        OperationCounters {
            started: self.started.load(Ordering::Acquire),
            succeeded: self.succeeded.load(Ordering::Acquire),
            failed: self.failed.load(Ordering::Acquire),
            cancelled: self.cancelled.load(Ordering::Acquire),
            records: self.records.load(Ordering::Acquire),
            payload_bytes: self.payload_bytes.load(Ordering::Acquire),
            elapsed: self.elapsed.snapshot(),
        }
    }
}

impl AtomicOperationStats {
    fn snapshot(&self) -> OperationStats {
        OperationStats {
            append: self.append.snapshot(),
            read: self.read.snapshot(),
            release: self.release.snapshot(),
            reclaim: self.reclaim.snapshot(),
        }
    }
}

impl AtomicStorageJobStats {
    fn observe(&self, kind: StorageJobKind, duration: Duration) {
        match kind {
            StorageJobKind::Append => &self.append,
            StorageJobKind::Read => &self.read,
            StorageJobKind::Release => &self.release,
            StorageJobKind::Reclaim => &self.reclaim,
            StorageJobKind::SegmentRollover => &self.segment_rollover,
        }
        .observe(duration);
    }

    fn snapshot(&self) -> StorageJobStats {
        StorageJobStats {
            append: self.append.snapshot(),
            read: self.read.snapshot(),
            release: self.release.snapshot(),
            reclaim: self.reclaim.snapshot(),
            segment_rollover: self.segment_rollover.snapshot(),
        }
    }
}

impl<'a> WaitActivity<'a> {
    fn new(counters: &'a WaitCounters) -> Self {
        counters.current.fetch_add(1, Ordering::AcqRel);
        atomic_saturating_add(&counters.waits, 1);
        Self {
            counters,
            started: Instant::now(),
        }
    }
}

impl Drop for WaitActivity<'_> {
    fn drop(&mut self) {
        self.counters.current.fetch_sub(1, Ordering::AcqRel);
        self.counters.elapsed.observe(self.started.elapsed());
    }
}

impl<'a> OperationActivity<'a> {
    fn new(counters: &'a AtomicOperationCounters, detailed: bool) -> Self {
        atomic_saturating_add(&counters.started, 1);
        Self {
            counters,
            started: detailed.then(Instant::now),
            finished: false,
        }
    }

    fn succeed(mut self, records: u64, payload_bytes: u64) {
        atomic_saturating_add(&self.counters.succeeded, 1);
        atomic_saturating_add(&self.counters.records, records);
        atomic_saturating_add(&self.counters.payload_bytes, payload_bytes);
        self.finish_timing();
        self.finished = true;
    }

    fn fail(mut self) {
        atomic_saturating_add(&self.counters.failed, 1);
        self.finish_timing();
        self.finished = true;
    }

    fn finish_timing(&self) {
        if let Some(started) = self.started {
            self.counters.elapsed.observe(started.elapsed());
        }
    }
}

impl Drop for OperationActivity<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        atomic_saturating_add(&self.counters.cancelled, 1);
        self.finish_timing();
    }
}

fn atomic_saturating_add(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(amount))
    });
}

impl Future for ReactorTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(future) = this.future.as_mut() else {
            return Poll::Ready(());
        };
        if future.as_mut().poll(context).is_pending() {
            return Poll::Pending;
        }

        drop(this.future.take());
        this.termination.finish(true);
        Poll::Ready(())
    }
}

impl Drop for ReactorTask {
    fn drop(&mut self) {
        // Storage and the root lock live in the inner Future. Drop them before
        // publishing reactor termination so shutdown cannot return too early.
        drop(self.future.take());
        self.termination.finish(false);
    }
}

impl ReactorTermination {
    fn finish(&mut self, completed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        if completed {
            shared.lifecycle.store(CLOSED, Ordering::Release);
            shared.set_health_state(RootState::Closed);
        } else if shared.lifecycle.load(Ordering::Acquire) != CLOSED {
            shared.poison(
                OperationKind::Reactor,
                &Error::Runtime {
                    message: "root reactor terminated before completing its lifecycle".to_string(),
                },
            );
        }
        shared.reactor_finished.store(true, Ordering::Release);
        shared.notify();
    }
}

impl StorageJobActivity {
    fn new(shared: &Weak<Shared>, kind: StorageJobKind) -> Self {
        let (armed, started) = if let Some(shared) = shared.upgrade() {
            shared.active_storage_jobs.fetch_add(1, Ordering::AcqRel);
            (true, shared.detailed_observability.then(Instant::now))
        } else {
            (false, None)
        };
        Self {
            shared: shared.clone(),
            kind,
            armed,
            started,
        }
    }
}

impl Drop for StorageJobActivity {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        if let Some(started) = self.started {
            let elapsed = started.elapsed();
            shared.storage_job_elapsed.observe(elapsed);
            shared.storage_jobs.observe(self.kind, elapsed);
        }
        let previous = shared.active_storage_jobs.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous, 0);
        if previous == 1 && shared.reactor_finished.load(Ordering::Acquire) {
            shared.notify();
        }
    }
}

async fn receive_response<T>(shared: &Shared, response: oneshot::Receiver<Result<T>>) -> Result<T> {
    response
        .await
        .unwrap_or_else(|_| Err(shared.channel_error()))
}

async fn reactor_loop(
    runtime: Arc<dyn Runtime>,
    storage: Storage,
    mut receiver: mpsc::Receiver<Command>,
    mut shutdown: watch::Receiver<bool>,
    shared: Weak<Shared>,
    limits: Limits,
) {
    let mut storage = Some(storage);
    let mut backlog = VecDeque::new();
    let mut closing = false;
    let mut age_timer = observed_age_timer(&runtime, storage.as_ref(), &shared);

    loop {
        if lifecycle(&shared) == RUNNING
            && backlog.is_empty()
            && receiver.is_empty()
            && storage
                .as_ref()
                .is_some_and(Storage::has_automatic_reclaim_work)
        {
            let result = storage_job(
                runtime.clone(),
                &mut storage,
                &shared,
                StorageJobKind::Reclaim,
                |storage| storage.reclaim(ReclaimKind::Automatic),
            )
            .await;
            match result {
                Ok(_) => publish_storage(&shared, storage.as_ref(), &[]),
                Err(error) => {
                    if error.poisons_root() {
                        mark_poisoned(&shared, OperationKind::Reclaim, &error);
                    }
                    publish_storage(&shared, storage.as_ref(), &[]);
                }
            }
            age_timer = observed_age_timer(&runtime, storage.as_ref(), &shared);
            continue;
        }

        let command = if let Some(command) = backlog.pop_front() {
            Some(command)
        } else if closing {
            receiver.recv().await
        } else if lifecycle(&shared) == RUNNING {
            if let Some(timer) = age_timer.as_mut() {
                tokio::select! {
                    command = receiver.recv() => command,
                    _ = shutdown.changed() => {
                        closing = true;
                        receiver.close();
                        age_timer = None;
                        continue;
                    }
                    () = timer.as_mut() => {
                        let result = storage_job(
                            runtime.clone(),
                            &mut storage,
                            &shared,
                            StorageJobKind::SegmentRollover,
                            |storage| storage.seal_expired(),
                        )
                        .await;
                        match result {
                            Ok(_) => publish_storage(&shared, storage.as_ref(), &[]),
                            Err(error) if error.poisons_root() => {
                                mark_poisoned(&shared, OperationKind::SegmentRollover, &error);
                                publish_storage(&shared, storage.as_ref(), &[]);
                            }
                            Err(_) => publish_storage(&shared, storage.as_ref(), &[]),
                        }
                        age_timer = observed_age_timer(&runtime, storage.as_ref(), &shared);
                        continue;
                    }
                }
            } else {
                tokio::select! {
                    command = receiver.recv() => command,
                    _ = shutdown.changed() => {
                        closing = true;
                        receiver.close();
                        continue;
                    }
                }
            }
        } else {
            tokio::select! {
                command = receiver.recv() => command,
                _ = shutdown.changed() => {
                    closing = true;
                    receiver.close();
                    age_timer = None;
                    continue;
                }
            }
        };
        let Some(command) = command else {
            break;
        };
        if let Some(shared) = shared.upgrade() {
            shared.observe_dispatch_wait(command.queued_at());
        }

        if lifecycle(&shared) == POISONED {
            reject_poisoned(command);
            mark_completed(&shared, 1);
            continue;
        }

        match command {
            Command::Append {
                queued_at: _,
                stream_id,
                records,
                encoded_bytes,
                reply,
            } => {
                let capacity = limits.capacity;
                let (max_units, max_bytes) =
                    (limits.max_commit_units, limits.max_append_group_bytes);
                let mut entries: Vec<AppendEntry> =
                    vec![(stream_id, records, encoded_bytes, reply)];
                let mut group_bytes = encoded_bytes;
                let highwaters = shared
                    .upgrade()
                    .map(|shared| shared.read_view().highwaters.clone())
                    .unwrap_or_default();
                let mut group_records = BTreeMap::new();
                group_records.insert(
                    stream_id,
                    u64::try_from(entries[0].1.len()).unwrap_or(u64::MAX),
                );
                while entries.len() < max_units {
                    let Some(next) = backlog.pop_front().or_else(|| receiver.try_recv().ok())
                    else {
                        break;
                    };
                    match next {
                        Command::Append {
                            queued_at,
                            stream_id,
                            records,
                            encoded_bytes,
                            reply,
                        } => {
                            let record_count = u64::try_from(records.len()).unwrap_or(u64::MAX);
                            let cumulative = group_records
                                .get(&stream_id)
                                .copied()
                                .unwrap_or(0)
                                .checked_add(record_count);
                            let sequence_fits = cumulative.is_some_and(|count| {
                                highwaters
                                    .get(&stream_id)
                                    .is_none_or(|highwater| highwater.checked_add(count).is_some())
                            });
                            let bytes_fit = group_bytes
                                .checked_add(encoded_bytes)
                                .is_some_and(|bytes| bytes <= max_bytes);
                            if sequence_fits && bytes_fit {
                                if let Some(shared) = shared.upgrade() {
                                    shared.observe_dispatch_wait(queued_at);
                                }
                                group_bytes += encoded_bytes;
                                group_records.insert(
                                    stream_id,
                                    cumulative.expect("checked cumulative record count"),
                                );
                                entries.push((stream_id, records, encoded_bytes, reply));
                            } else {
                                backlog.push_front(Command::Append {
                                    queued_at,
                                    stream_id,
                                    records,
                                    encoded_bytes,
                                    reply,
                                });
                                break;
                            }
                        }
                        other => {
                            backlog.push_front(other);
                            break;
                        }
                    }
                }
                let units = entries
                    .iter()
                    .map(|(stream_id, records, _, _)| AppendUnit {
                        stream_id: *stream_id,
                        records: records.clone(),
                    })
                    .collect::<Vec<_>>();
                let changed_streams = entries
                    .iter()
                    .map(|(stream_id, _, _, _)| *stream_id)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let count = entries.len();
                let bounded = !matches!(capacity, Capacity::Unbounded);
                let result = storage_job(
                    runtime.clone(),
                    &mut storage,
                    &shared,
                    StorageJobKind::Append,
                    move |storage| Ok(execute_append_job(storage, units, bounded)),
                )
                .await;
                match &result {
                    Ok(AppendJob::Failure { error, selected })
                        if *selected != 0 && *selected <= count && error.poisons_root() =>
                    {
                        mark_poisoned(&shared, OperationKind::Append, error);
                    }
                    Err(error) if error.poisons_root() => {
                        mark_poisoned(&shared, OperationKind::Append, error);
                    }
                    _ => {}
                }
                publish_storage(&shared, storage.as_ref(), &changed_streams);
                age_timer = observed_age_timer(&runtime, storage.as_ref(), &shared);
                let completed = match result {
                    Ok(AppendJob::Complete { outputs, selected })
                        if selected != 0 && selected <= count && outputs.len() == selected =>
                    {
                        let deferred = entries.split_off(selected);
                        requeue_appends(&mut backlog, deferred, &shared);
                        for ((_, _, _, reply), ids) in entries.into_iter().zip(outputs) {
                            let _ = reply.send(AppendReply::Complete(Ok(ids)));
                        }
                        selected
                    }
                    Ok(AppendJob::Complete { .. }) => {
                        let error = Error::Runtime {
                            message: "storage returned the wrong append result count".to_string(),
                        };
                        mark_poisoned(&shared, OperationKind::Append, &error);
                        let mut entries = entries.into_iter();
                        if let Some((_, _, _, reply)) = entries.next() {
                            let _ = reply.send(AppendReply::Complete(Err(error)));
                        }
                        for (_, _, _, reply) in entries {
                            let _ = reply.send(AppendReply::Complete(Err(Error::Poisoned)));
                        }
                        count
                    }
                    Ok(AppendJob::Capacity(CapacityCheck::Wait {
                        needed_bytes,
                        available_bytes,
                    })) => {
                        let deferred = entries.split_off(1);
                        requeue_appends(&mut backlog, deferred, &shared);
                        let (_, records, _, reply) = entries.remove(0);
                        match capacity {
                            Capacity::Bounded {
                                when_full: FullPolicy::Block,
                                ..
                            } => {
                                let _ = reply.send(AppendReply::Wait { records });
                            }
                            Capacity::Bounded {
                                when_full: FullPolicy::RejectNew,
                                ..
                            } => {
                                let _ = reply.send(AppendReply::Complete(Err(
                                    Error::RejectedCapacity {
                                        needed_bytes,
                                        available_bytes,
                                    },
                                )));
                            }
                            Capacity::Unbounded => unreachable!(),
                        }
                        1
                    }
                    Ok(AppendJob::Capacity(CapacityCheck::Exceeds {
                        needed_bytes,
                        total_bytes,
                    })) => {
                        let deferred = entries.split_off(1);
                        requeue_appends(&mut backlog, deferred, &shared);
                        let (_, _, _, reply) = entries.remove(0);
                        let _ = reply.send(AppendReply::Complete(Err(Error::ExceedsCapacity {
                            needed_bytes,
                            total_bytes,
                        })));
                        1
                    }
                    Ok(AppendJob::Capacity(CapacityCheck::Admit)) => unreachable!(),
                    Ok(AppendJob::Failure { error, selected })
                        if selected != 0 && selected <= count =>
                    {
                        let deferred = entries.split_off(selected);
                        requeue_appends(&mut backlog, deferred, &shared);
                        reply_append_error(entries, error);
                        selected
                    }
                    Ok(AppendJob::Failure { .. }) => {
                        let error = Error::Runtime {
                            message: "storage returned an invalid append failure scope".to_string(),
                        };
                        mark_poisoned(&shared, OperationKind::Append, &error);
                        reply_append_error(entries, error);
                        count
                    }
                    Err(error) => {
                        reply_append_error(entries, error);
                        count
                    }
                };
                mark_completed(&shared, completed);
            }
            Command::Read {
                queued_at: _,
                stream_id,
                limits,
                reply,
            } => {
                let result = storage_job(
                    runtime.clone(),
                    &mut storage,
                    &shared,
                    StorageJobKind::Read,
                    move |storage| storage.read(stream_id, limits),
                )
                .await;
                let poisons = result.as_ref().err().is_some_and(Error::poisons_root);
                if poisons {
                    mark_poisoned(
                        &shared,
                        OperationKind::Read,
                        result.as_ref().expect_err("poisoning read has error"),
                    );
                }
                let _ = reply.send(result);
                mark_completed(&shared, 1);
            }
            Command::Release {
                queued_at: _,
                stream_id,
                ids,
                encoded_bound,
                reply,
            } => {
                let max_units = limits.max_commit_units;
                let max_bytes = if matches!(limits.capacity, Capacity::Bounded { .. }) {
                    limits.max_bounded_release_group_bytes
                } else {
                    limits.max_commit_bytes
                };
                let mut entries = vec![(stream_id, ids, encoded_bound, reply)];
                let mut group_bytes = encoded_bound;
                while entries.len() < max_units {
                    let Some(next) = backlog.pop_front().or_else(|| receiver.try_recv().ok())
                    else {
                        break;
                    };
                    match next {
                        Command::Release {
                            queued_at,
                            stream_id,
                            ids,
                            encoded_bound,
                            reply,
                        } if group_bytes
                            .checked_add(encoded_bound)
                            .is_some_and(|bytes| bytes <= max_bytes) =>
                        {
                            if let Some(shared) = shared.upgrade() {
                                shared.observe_dispatch_wait(queued_at);
                            }
                            group_bytes += encoded_bound;
                            entries.push((stream_id, ids, encoded_bound, reply));
                        }
                        other => {
                            backlog.push_front(other);
                            break;
                        }
                    }
                }
                let count = entries.len();
                let changed_streams = entries
                    .iter()
                    .map(|(stream_id, _, _, _)| *stream_id)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let units = entries
                    .iter()
                    .map(|(stream_id, ids, _, _)| ReleaseUnit {
                        stream_id: *stream_id,
                        ids: ids.clone(),
                    })
                    .collect();
                let result = storage_job(
                    runtime.clone(),
                    &mut storage,
                    &shared,
                    StorageJobKind::Release,
                    move |storage| storage.release_group(units),
                )
                .await;
                let poisons = result.as_ref().err().is_some_and(Error::poisons_root);
                if poisons {
                    mark_poisoned(
                        &shared,
                        OperationKind::Release,
                        result.as_ref().expect_err("poisoning release has error"),
                    );
                }
                publish_storage(&shared, storage.as_ref(), &changed_streams);
                match result {
                    Ok(()) => {
                        for (_, _, _, reply) in entries {
                            let _ = reply.send(Ok(()));
                        }
                    }
                    Err(error) => {
                        let copies = (1..count)
                            .map(|_| error.copy_nonpoisoning().unwrap_or(Error::Poisoned))
                            .collect::<Vec<_>>();
                        let mut entries = entries.into_iter();
                        if let Some((_, _, _, reply)) = entries.next() {
                            let _ = reply.send(Err(error));
                        }
                        for ((_, _, _, reply), copy) in entries.zip(copies) {
                            let _ = reply.send(Err(copy));
                        }
                    }
                }
                mark_completed(&shared, count);
            }
            Command::Reclaim {
                queued_at: _,
                reply,
            } => {
                let result = storage_job(
                    runtime.clone(),
                    &mut storage,
                    &shared,
                    StorageJobKind::Reclaim,
                    |storage| storage.reclaim(ReclaimKind::Explicit),
                )
                .await;
                let poisons = result.as_ref().err().is_some_and(Error::poisons_root);
                if poisons {
                    mark_poisoned(
                        &shared,
                        OperationKind::Reclaim,
                        result.as_ref().expect_err("poisoning reclaim has error"),
                    );
                }
                publish_storage(&shared, storage.as_ref(), &[]);
                let _ = reply.send(result);
                mark_completed(&shared, 1);
                age_timer = observed_age_timer(&runtime, storage.as_ref(), &shared);
            }
        }
    }
}

fn execute_append_job(
    storage: &mut Storage,
    mut units: Vec<AppendUnit>,
    bounded: bool,
) -> AppendJob {
    let physical_limit = match storage.append_prefix_for_active_segment(&units) {
        Ok(limit) => limit,
        Err(error) => return AppendJob::Failure { error, selected: 1 },
    };
    if !bounded {
        let selected_units = units.drain(..physical_limit).collect();
        return match storage.append_group(selected_units) {
            Ok(outputs) => AppendJob::Complete {
                outputs,
                selected: physical_limit,
            },
            Err(error) => AppendJob::Failure {
                error,
                selected: physical_limit,
            },
        };
    }

    let selection = (|| -> Result<(Option<usize>, CapacityCheck)> {
        let mut physical_limit = physical_limit;
        let mut selected = largest_admissible_append_prefix(storage, &units[..physical_limit])?;
        if matches!(selected.1, CapacityCheck::Wait { .. }) && storage.has_automatic_reclaim_work()
        {
            storage.reclaim(ReclaimKind::Automatic)?;
            physical_limit = storage.append_prefix_for_active_segment(&units)?;
            selected = largest_admissible_append_prefix(storage, &units[..physical_limit])?;
        }
        Ok(selected)
    })();
    let (selected, capacity) = match selection {
        Ok(selection) => selection,
        Err(error) => return AppendJob::Failure { error, selected: 1 },
    };
    let Some(selected) = selected else {
        return AppendJob::Capacity(capacity);
    };
    let selected_units = units.drain(..selected).collect();
    match storage.append_group(selected_units) {
        Ok(outputs) => AppendJob::Complete { outputs, selected },
        Err(error) => AppendJob::Failure { error, selected },
    }
}

fn largest_admissible_append_prefix(
    storage: &Storage,
    units: &[AppendUnit],
) -> Result<(Option<usize>, CapacityCheck)> {
    let first = storage.check_append_capacity(&units[..1])?;
    if !matches!(first, CapacityCheck::Admit) {
        return Ok((None, first));
    }
    for selected in (1..=units.len()).rev() {
        if matches!(
            storage.check_append_capacity(&units[..selected])?,
            CapacityCheck::Admit
        ) {
            return Ok((Some(selected), CapacityCheck::Admit));
        }
    }
    unreachable!("the first append unit was already admissible")
}

fn requeue_appends(
    backlog: &mut VecDeque<Command>,
    entries: Vec<AppendEntry>,
    shared: &Weak<Shared>,
) {
    for (stream_id, records, encoded_bytes, reply) in entries.into_iter().rev() {
        backlog.push_front(Command::Append {
            queued_at: shared
                .upgrade()
                .and_then(|shared| shared.dispatch_timestamp()),
            stream_id,
            records,
            encoded_bytes,
            reply,
        });
    }
}

fn reply_append_error(entries: Vec<AppendEntry>, error: Error) {
    let copies = (1..entries.len())
        .map(|_| error.copy_nonpoisoning().unwrap_or(Error::Poisoned))
        .collect::<Vec<_>>();
    let mut entries = entries.into_iter();
    if let Some((_, _, _, reply)) = entries.next() {
        let _ = reply.send(AppendReply::Complete(Err(error)));
    }
    for ((_, _, _, reply), copy) in entries.zip(copies) {
        let _ = reply.send(AppendReply::Complete(Err(copy)));
    }
}

async fn storage_job<T, F>(
    runtime: Arc<dyn Runtime>,
    storage: &mut Option<Storage>,
    shared: &Weak<Shared>,
    kind: StorageJobKind,
    job: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut Storage) -> Result<T> + Send + 'static,
{
    let current = storage.take().ok_or(Error::Poisoned)?;
    let activity = StorageJobActivity::new(shared, kind);
    match run_blocking_guarded(
        runtime,
        move || {
            let mut storage = current;
            let result = job(&mut storage);
            (storage, result)
        },
        activity,
    )
    .await
    {
        Ok((returned, result)) => {
            *storage = Some(returned);
            result
        }
        Err(error) => Err(error),
    }
}

fn view_from_storage(storage: &Storage) -> Result<View> {
    let known_streams = storage.known_streams();
    let stream_stats = known_streams
        .iter()
        .map(|stream_id| (*stream_id, storage.stream_stats(*stream_id)))
        .collect();
    Ok(View {
        storage: storage.storage_stats()?,
        commits: storage.commit_stats(),
        maintenance: storage.maintenance_stats(),
        recovery: storage.recovery_stats(),
        known_streams,
        stream_stats,
        highwaters: storage.stream_highwaters(),
    })
}

fn publish_storage(shared: &Weak<Shared>, storage: Option<&Storage>, streams: &[StreamId]) {
    let (Some(shared), Some(storage)) = (shared.upgrade(), storage) else {
        return;
    };
    let commits = storage.commit_stats();
    let maintenance = storage.maintenance_stats();
    let recovery = storage.recovery_stats();
    let storage_stats = match storage.storage_stats() {
        Ok(stats) => stats,
        Err(error) => {
            let mut current = shared
                .view
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            current.commits = commits;
            current.maintenance = maintenance;
            current.recovery = recovery;
            drop(current);
            mark_poisoned(
                &Arc::downgrade(&shared),
                OperationKind::StatePublication,
                &error,
            );
            return;
        }
    };
    let mut current = shared
        .view
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut state_changed = current.storage != storage_stats;
    current.storage = storage_stats;
    current.commits = commits;
    current.maintenance = maintenance;
    current.recovery = recovery;
    for stream_id in streams {
        let stats = storage.stream_stats(*stream_id);
        if stats.durable_known || current.stream_stats.contains_key(stream_id) {
            state_changed |= current.stream_stats.get(stream_id) != Some(&stats);
            current.stream_stats.insert(*stream_id, stats);
        }
        if let Some(highwater) = storage.stream_highwater(*stream_id) {
            state_changed |= current.highwaters.get(stream_id) != Some(&highwater);
            current.highwaters.insert(*stream_id, highwater);
            if let Err(index) = current.known_streams.binary_search(stream_id) {
                current.known_streams.insert(index, *stream_id);
                state_changed = true;
            }
        }
    }
    drop(current);
    if state_changed {
        shared.notify();
    }
}

fn lifecycle(shared: &Weak<Shared>) -> u8 {
    shared
        .upgrade()
        .map_or(CLOSED, |shared| shared.lifecycle.load(Ordering::Acquire))
}

fn mark_poisoned(shared: &Weak<Shared>, operation: OperationKind, error: &Error) {
    if let Some(shared) = shared.upgrade() {
        shared.poison(operation, error);
    }
}

fn mark_completed(shared: &Weak<Shared>, count: usize) {
    if let Some(shared) = shared.upgrade() {
        shared.mark_completed(count);
    }
}

fn reject_poisoned(command: Command) {
    match command {
        Command::Append { reply, .. } => {
            let _ = reply.send(AppendReply::Complete(Err(Error::Poisoned)));
        }
        Command::Read { reply, .. } => {
            let _ = reply.send(Err(Error::Poisoned));
        }
        Command::Release { reply, .. } => {
            let _ = reply.send(Err(Error::Poisoned));
        }
        Command::Reclaim { reply, .. } => {
            let _ = reply.send(Err(Error::Poisoned));
        }
    }
}

fn make_age_timer(
    runtime: &Arc<dyn Runtime>,
    storage: Option<&Storage>,
) -> Result<Option<RuntimeFuture>> {
    let Some(storage) = storage else {
        return Ok(None);
    };
    Ok(storage
        .next_age_delay()?
        .map(|duration| runtime.sleep(duration)))
}

fn observed_age_timer(
    runtime: &Arc<dyn Runtime>,
    storage: Option<&Storage>,
    shared: &Weak<Shared>,
) -> Option<RuntimeFuture> {
    match make_age_timer(runtime, storage) {
        Ok(timer) => timer,
        Err(error) => {
            mark_poisoned(shared, OperationKind::SegmentRollover, &error);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::RuntimeError;
    use bytes::Bytes;
    use std::sync::{Condvar, Mutex};
    use tempfile::TempDir;

    #[derive(Default)]
    struct HeldRuntime {
        reactor: Mutex<Option<RuntimeFuture>>,
    }

    #[derive(Default)]
    struct BlockingGate {
        blocked: AtomicBool,
        entered: AtomicBool,
        mutex: Mutex<()>,
        ready: Condvar,
    }

    #[derive(Default)]
    struct GatedRuntime {
        gate: Arc<BlockingGate>,
    }

    impl GatedRuntime {
        fn block_storage(&self) {
            self.gate.entered.store(false, Ordering::Release);
            self.gate.blocked.store(true, Ordering::Release);
        }

        fn unblock_storage(&self) {
            self.gate.blocked.store(false, Ordering::Release);
            self.gate.ready.notify_all();
        }
    }

    impl HeldRuntime {
        fn start_reactor(&self) {
            let reactor = self
                .reactor
                .lock()
                .unwrap()
                .take()
                .expect("stored reactor task");
            drop(tokio::spawn(reactor));
        }

        fn terminate_reactor(&self) {
            let reactor = self.reactor.lock().unwrap().take();
            drop(reactor);
        }
    }

    impl Runtime for HeldRuntime {
        fn spawn(&self, future: RuntimeFuture) -> std::result::Result<(), RuntimeError> {
            let mut reactor = self.reactor.lock().unwrap();
            if reactor.is_some() {
                return Err(RuntimeError::new("reactor task already stored"));
            }
            *reactor = Some(future);
            Ok(())
        }

        fn spawn_blocking(
            &self,
            job: Box<dyn FnOnce() + Send + 'static>,
        ) -> std::result::Result<(), RuntimeError> {
            let handle = std::thread::Builder::new()
                .name("camus-test-blocking".to_string())
                .spawn(job)
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            drop(handle);
            Ok(())
        }

        fn sleep(&self, _duration: Duration) -> RuntimeFuture {
            Box::pin(std::future::pending())
        }
    }

    impl Runtime for GatedRuntime {
        fn spawn(&self, future: RuntimeFuture) -> std::result::Result<(), RuntimeError> {
            drop(tokio::spawn(future));
            Ok(())
        }

        fn spawn_blocking(
            &self,
            job: Box<dyn FnOnce() + Send + 'static>,
        ) -> std::result::Result<(), RuntimeError> {
            let gate = self.gate.clone();
            let handle = std::thread::Builder::new()
                .name("camus-test-gated-blocking".to_string())
                .spawn(move || {
                    if gate.blocked.load(Ordering::Acquire) {
                        gate.entered.store(true, Ordering::Release);
                        let guard = gate.mutex.lock().unwrap();
                        drop(
                            gate.ready
                                .wait_while(guard, |_| gate.blocked.load(Ordering::Acquire))
                                .unwrap(),
                        );
                    }
                    job();
                })
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            drop(handle);
            Ok(())
        }

        fn sleep(&self, duration: Duration) -> RuntimeFuture {
            Box::pin(tokio::time::sleep(duration))
        }
    }

    fn config(directory: &TempDir) -> Config {
        Config::new(directory.path(), Capacity::Unbounded)
            .with_max_epoch_bytes(1024 * 1024)
            .with_segment_bytes(2 * 1024 * 1024)
            .with_max_commit_bytes(2 * 1024 * 1024)
    }

    #[tokio::test]
    async fn async_lifecycle_replays_until_release() {
        let directory = TempDir::new().unwrap();
        let stream_id = StreamId::new(9);
        let id = {
            let log = Log::open(config(&directory)).await.unwrap();
            let stream = log.stream(stream_id);
            let id = stream
                .append(Record::new(Bytes::from_static(b"payload")))
                .await
                .unwrap();
            log.shutdown().await.unwrap();
            id
        };

        let log = Log::open(config(&directory)).await.unwrap();
        let stream = log.stream(stream_id);
        let snapshot = stream.read(ReadLimits::new(4, 1024)).await.unwrap();
        assert_eq!(snapshot[0].id, id);
        stream.release(vec![id]).await.unwrap();
        log.shutdown().await.unwrap();

        let log = Log::open(config(&directory)).await.unwrap();
        assert_eq!(log.stream(stream_id).stats().pending_records, 0);
        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_keeps_progressing_if_the_initiating_future_is_dropped() {
        let directory = TempDir::new().unwrap();
        let log = Log::open(config(&directory)).await.unwrap();
        let mut initiating = Box::pin(log.shutdown());
        tokio::select! {
            biased;
            result = &mut initiating => result.unwrap(),
            () = tokio::task::yield_now() => {}
        }
        drop(initiating);

        tokio::time::timeout(Duration::from_secs(5), log.shutdown())
            .await
            .expect("reactor did not finish a cancellation-safe shutdown")
            .unwrap();
        assert_eq!(log.shared.lifecycle.load(Ordering::Acquire), CLOSED);
    }

    #[tokio::test]
    async fn runtime_termination_poisons_the_root_and_shutdown_releases_the_lock() {
        let directory = TempDir::new().unwrap();
        let runtime = Arc::new(HeldRuntime::default());
        let log = Log::open(config(&directory).with_runtime(runtime.clone()))
            .await
            .unwrap();
        let mut health_watch = log.watch_health();
        let activity =
            StorageJobActivity::new(&Arc::downgrade(&log.shared), StorageJobKind::Reclaim);

        runtime.terminate_reactor();

        let poisoned = tokio::time::timeout(Duration::from_secs(5), health_watch.changed())
            .await
            .expect("health watch did not observe reactor termination")
            .expect("health channel closed before the poison transition");
        assert_eq!(poisoned.state, RootState::Poisoned);
        assert_eq!(
            poisoned.failure.as_ref().unwrap().operation,
            OperationKind::Reactor
        );
        assert_eq!(
            poisoned.failure.as_ref().map(|failure| failure.error_kind),
            Some(crate::ErrorKind::Runtime)
        );
        assert_eq!(
            poisoned.failure.as_ref().unwrap().durability_outcome,
            DurabilityOutcome::Unknown
        );
        let mut shutdown = Box::pin(log.shutdown());
        tokio::select! {
            biased;
            result = &mut shutdown => panic!("shutdown completed before the storage job ended: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        drop(activity);
        tokio::time::timeout(Duration::from_secs(5), &mut shutdown)
            .await
            .expect("shutdown did not observe reactor termination")
            .unwrap();
        assert_eq!(log.shared.lifecycle.load(Ordering::Acquire), CLOSED);
        let closed = tokio::time::timeout(Duration::from_secs(5), health_watch.changed())
            .await
            .expect("health watch did not observe closure")
            .expect("health channel closed before the closed transition");
        assert_eq!(closed.state, RootState::Closed);
        assert_eq!(closed.failure, poisoned.failure);

        drop(shutdown);
        drop(log);
        assert!(health_watch.changed().await.is_none());

        let reopened = Log::open(config(&directory)).await.unwrap();
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn commit_stats_report_units_combined_by_group_commit() {
        let directory = TempDir::new().unwrap();
        let runtime = Arc::new(HeldRuntime::default());
        let log = Log::open(config(&directory).with_runtime(runtime.clone()))
            .await
            .unwrap();
        let first_stream = log.stream(StreamId::new(27));
        let second_stream = log.stream(StreamId::new(28));
        let first = tokio::spawn(async move {
            first_stream
                .append(Record::new(Bytes::from_static(b"first")))
                .await
        });
        let second = tokio::spawn(async move {
            second_stream
                .append(Record::new(Bytes::from_static(b"second")))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while log.stats().pressure.queue_depth != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("append commands did not enter the held reactor queue");

        runtime.start_reactor();
        let first_id = first.await.unwrap().unwrap();
        let second_id = second.await.unwrap().unwrap();
        let stats = log.stats();
        assert_eq!(stats.operations.append.succeeded, 2);
        assert_eq!(stats.commits.append_groups, 1);
        assert_eq!(stats.commits.append_units, 2);
        assert_eq!(stats.commits.append_records, 2);
        assert_eq!(stats.commits.max_append_units, 2);

        log.stream(StreamId::new(27))
            .release(vec![first_id])
            .await
            .unwrap();
        log.stream(StreamId::new(28))
            .release(vec![second_id])
            .await
            .unwrap();
        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn automatic_reclaim_drains_all_bounded_batches_without_an_edge_signal() {
        let directory = TempDir::new().unwrap();
        let log = Log::open(
            Config::new(directory.path(), Capacity::Unbounded)
                .with_max_epoch_bytes(128 * 1024)
                .with_segment_bytes(160 * 1024)
                .with_max_release_records(1024)
                .with_max_commit_bytes(256 * 1024)
                .with_detailed_observability(),
        )
        .await
        .unwrap();
        let stream = log.stream(StreamId::new(28));
        let mut ids = Vec::new();
        for value in 0_u8..10 {
            ids.push(
                stream
                    .append(Record::new(Bytes::from(vec![value; 120 * 1024])))
                    .await
                    .unwrap(),
            );
        }
        let mut events = log.shared.events.subscribe();
        stream.release(ids).await.unwrap();

        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let stats = log.stats();
                if stats.storage.live_segments == 0 {
                    assert_eq!(stats.storage.reclaimable_segments, 0);
                    assert_eq!(stats.maintenance.reclaimed_segments, 10);
                    assert_eq!(stats.maintenance.automatic_reclaim_passes, 3);
                    assert_eq!(stats.pressure.storage_jobs.reclaim.observations, 3);
                    break;
                }
                events.changed().await.unwrap();
            }
        })
        .await;
        assert!(
            drained.is_ok(),
            "automatic reclaim did not drain every bounded batch: {:?}",
            log.stats()
        );

        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queue_pressure_counts_only_reservations_that_find_a_full_queue() {
        let directory = TempDir::new().unwrap();
        let runtime = Arc::new(HeldRuntime::default());
        let log = Log::open(
            config(&directory)
                .with_runtime(runtime.clone())
                .with_command_queue_capacity(1),
        )
        .await
        .unwrap();
        let first_stream = log.stream(StreamId::new(29));
        let second_stream = log.stream(StreamId::new(30));
        let first = tokio::spawn(async move {
            first_stream
                .append(Record::new(Bytes::from_static(b"first")))
                .await
        });
        let second = tokio::spawn(async move {
            second_stream
                .append(Record::new(Bytes::from_static(b"second")))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let pressure = log.stats().pressure;
                if pressure.queue_depth == 1 && pressure.queue_wait.current == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second append did not wait on the full held queue");

        runtime.start_reactor();
        let first_id = first.await.unwrap().unwrap();
        let second_id = second.await.unwrap().unwrap();
        let wait = log.stats().pressure.queue_wait;
        assert_eq!(wait.current, 0);
        assert_eq!(wait.waits, 1);
        assert_eq!(wait.elapsed.observations, 1);

        log.stream(StreamId::new(29))
            .release(vec![first_id])
            .await
            .unwrap();
        log.stream(StreamId::new(30))
            .release(vec![second_id])
            .await
            .unwrap();
        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_admitted_append_is_separate_from_its_durable_commit() {
        let directory = TempDir::new().unwrap();
        let runtime = Arc::new(GatedRuntime::default());
        let log = Log::open(
            config(&directory)
                .with_runtime(runtime.clone())
                .with_detailed_observability(),
        )
        .await
        .unwrap();
        let stream = log.stream(StreamId::new(31));
        runtime.block_storage();

        let append_stream = stream.clone();
        let append = tokio::spawn(async move {
            append_stream
                .append(Record::new(Bytes::from_static(b"committed after cancel")))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !runtime.gate.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("append storage job did not reach the gate");
        assert_eq!(log.stats().pressure.active_storage_jobs, 1);

        append.abort();
        assert!(append.await.unwrap_err().is_cancelled());
        assert_eq!(log.stats().operations.append.cancelled, 1);
        runtime.unblock_storage();

        tokio::time::timeout(Duration::from_secs(5), async {
            while log.stats().commits.append_groups == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled append did not finish durably");
        let stats = log.stats();
        assert_eq!(stats.operations.append.succeeded, 0);
        assert_eq!(stats.operations.append.cancelled, 1);
        assert_eq!(stats.operations.append.elapsed.observations, 1);
        assert_eq!(stats.commits.append_groups, 1);
        assert_eq!(stats.commits.append_records, 1);

        let pending = stream.read(ReadLimits::new(1, 1024)).await.unwrap();
        stream.release(vec![pending[0].id]).await.unwrap();
        log.shutdown().await.unwrap();
    }

    #[test]
    fn append_group_selection_uses_the_remaining_segment_prefix() {
        let directory = TempDir::new().unwrap();
        let config = Config::new(directory.path(), Capacity::Unbounded)
            .with_max_epoch_bytes(256)
            .with_segment_bytes(500)
            .with_max_release_records(8)
            .with_max_commit_bytes(404);
        let mut storage = Storage::open(config).unwrap();
        storage
            .append_group(vec![AppendUnit {
                stream_id: StreamId::new(29),
                records: vec![Record::new(Bytes::from(vec![0_u8; 72]))],
            }])
            .unwrap();

        let next = vec![
            AppendUnit {
                stream_id: StreamId::new(29),
                records: vec![Record::new(Bytes::from(vec![1_u8; 22]))],
            },
            AppendUnit {
                stream_id: StreamId::new(30),
                records: vec![Record::new(Bytes::from(vec![2_u8; 22]))],
            },
        ];
        assert!(matches!(
            execute_append_job(&mut storage, next, false),
            AppendJob::Complete { selected: 1, .. }
        ));
        assert_eq!(storage.stream_stats(StreamId::new(29)).pending_records, 2);
        assert_eq!(storage.stream_stats(StreamId::new(30)).pending_records, 0);

        assert!(matches!(
            execute_append_job(
                &mut storage,
                vec![AppendUnit {
                    stream_id: StreamId::new(30),
                    records: vec![Record::new(Bytes::from(vec![2_u8; 22]))],
                }],
                false,
            ),
            AppendJob::Complete { selected: 1, .. }
        ));
        assert_eq!(storage.stream_stats(StreamId::new(30)).pending_records, 1);
    }
}
