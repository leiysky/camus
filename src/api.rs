use crate::config::{Capacity, Config, FullPolicy};
use crate::error::{Error, Result};
use crate::model::{
    PendingSnapshot, ReadLimits, ReclaimReport, Record, RecordId, RootId, Stats, StreamId,
    StreamStats,
};
use crate::runtime::{default_runtime, run_blocking, run_blocking_guarded, Runtime, RuntimeFuture};
use crate::storage::{encoded_epoch_bytes, AppendUnit, CapacityCheck, ReleaseUnit, Storage};
use std::collections::{BTreeMap, VecDeque};
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
    stats: Stats,
    known_streams: Vec<StreamId>,
    stream_stats: BTreeMap<StreamId, StreamStats>,
    highwaters: BTreeMap<StreamId, u64>,
}

struct Shared {
    sender: mpsc::Sender<Command>,
    shutdown: watch::Sender<bool>,
    events: watch::Sender<u64>,
    view: RwLock<View>,
    root_id: RootId,
    limits: Limits,
    lifecycle: AtomicU8,
    shutdown_started: AtomicBool,
    reactor_finished: AtomicBool,
    active_storage_jobs: AtomicUsize,
    queue_depth: AtomicUsize,
    admission_waiters: AtomicUsize,
    admitted_operations: AtomicU64,
    total_admission_wait_nanos: AtomicU64,
    max_admission_wait_nanos: AtomicU64,
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
        stream_id: StreamId,
        records: Vec<Record>,
        encoded_bytes: u64,
        reply: oneshot::Sender<AppendReply>,
    },
    Read {
        stream_id: StreamId,
        limits: ReadLimits,
        reply: oneshot::Sender<Result<Option<PendingSnapshot>>>,
    },
    Release {
        stream_id: StreamId,
        ids: Vec<RecordId>,
        encoded_bound: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    Reclaim {
        reply: oneshot::Sender<Result<ReclaimReport>>,
    },
}

struct AdmissionWait<'a> {
    shared: &'a Shared,
    started: Instant,
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
    armed: bool,
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
        let storage = run_blocking(runtime.clone(), move || Storage::open(config)).await??;
        let root_id = storage.root_id();
        let view = view_from_storage(&storage)?;
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (events, _) = watch::channel(0_u64);
        let shared = Arc::new(Shared {
            sender,
            shutdown,
            events,
            view: RwLock::new(view),
            root_id,
            limits,
            lifecycle: AtomicU8::new(RUNNING),
            shutdown_started: AtomicBool::new(false),
            reactor_finished: AtomicBool::new(false),
            active_storage_jobs: AtomicUsize::new(0),
            queue_depth: AtomicUsize::new(0),
            admission_waiters: AtomicUsize::new(0),
            admitted_operations: AtomicU64::new(0),
            total_admission_wait_nanos: AtomicU64::new(0),
            max_admission_wait_nanos: AtomicU64::new(0),
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
    pub fn stats(&self) -> Stats {
        self.shared.stats()
    }

    /// Requests and awaits one physical maintenance pass.
    pub async fn reclaim(&self) -> Result<ReclaimReport> {
        let (reply, response) = oneshot::channel();
        let permit = self.shared.reserve_running().await?;
        permit.send(Command::Reclaim { reply });
        receive_response(&self.shared, response).await
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
                stream_id: self.id,
                records,
                encoded_bytes,
                reply,
            });
            match response.await {
                Ok(AppendReply::Complete(result)) => return result,
                Ok(AppendReply::Wait { records: returned }) => {
                    records = returned;
                    self.shared.wait_for_change(&mut events).await?;
                }
                Err(_) => return Err(self.shared.channel_error()),
            }
        }
    }

    /// Waits for and returns a non-empty bounded snapshot of pending records.
    pub async fn read(&self, limits: ReadLimits) -> Result<PendingSnapshot> {
        if limits.max_records == 0 {
            return Err(Error::InvalidReadLimits);
        }
        let mut events = self.shared.events.subscribe();
        loop {
            self.shared.ensure_running()?;
            if self.stats().pending_records == 0 {
                self.shared.wait_for_change(&mut events).await?;
                continue;
            }
            let (reply, response) = oneshot::channel();
            let permit = self.shared.reserve_running().await?;
            permit.send(Command::Read {
                stream_id: self.id,
                limits,
                reply,
            });
            match receive_response(&self.shared, response).await? {
                Some(snapshot) => return Ok(snapshot),
                None => self.shared.wait_for_change(&mut events).await?,
            }
        }
    }

    /// Durably removes an exact record subset from the shared pending set.
    pub async fn release(&self, ids: Vec<RecordId>) -> Result<()> {
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
            stream_id: self.id,
            ids,
            encoded_bound,
            reply,
        });
        receive_response(&self.shared, response).await
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
    async fn reserve_running(&self) -> Result<mpsc::Permit<'_, Command>> {
        self.ensure_running()?;
        let waiting = AdmissionWait::new(self);
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| self.channel_error())?;
        drop(waiting);
        self.ensure_running()?;
        self.mark_admitted();
        Ok(permit)
    }

    async fn wait_for_change(&self, events: &mut watch::Receiver<u64>) -> Result<()> {
        self.ensure_running()?;
        let waiting = AdmissionWait::new(self);
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

    fn stats(&self) -> Stats {
        let mut stats = self.read_view().stats.clone();
        stats.queue_depth = self.queue_depth.load(Ordering::Acquire);
        stats.admission_waiters = self.admission_waiters.load(Ordering::Acquire);
        stats.admitted_operations = self.admitted_operations.load(Ordering::Acquire);
        stats.total_admission_wait =
            Duration::from_nanos(self.total_admission_wait_nanos.load(Ordering::Acquire));
        stats.max_admission_wait =
            Duration::from_nanos(self.max_admission_wait_nanos.load(Ordering::Acquire));
        stats.poisoned = self.lifecycle.load(Ordering::Acquire) == POISONED;
        stats
    }

    fn mark_admitted(&self) {
        self.queue_depth.fetch_add(1, Ordering::AcqRel);
        self.admitted_operations.fetch_add(1, Ordering::AcqRel);
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
}

impl<'a> AdmissionWait<'a> {
    fn new(shared: &'a Shared) -> Self {
        shared.admission_waiters.fetch_add(1, Ordering::AcqRel);
        Self {
            shared,
            started: Instant::now(),
        }
    }
}

impl Drop for AdmissionWait<'_> {
    fn drop(&mut self) {
        self.shared.admission_waiters.fetch_sub(1, Ordering::AcqRel);
        let nanos = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let _ = self.shared.total_admission_wait_nanos.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_add(nanos)),
        );
        self.shared
            .max_admission_wait_nanos
            .fetch_max(nanos, Ordering::AcqRel);
    }
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
        } else if shared.lifecycle.load(Ordering::Acquire) != CLOSED {
            shared.lifecycle.store(POISONED, Ordering::Release);
        }
        shared.reactor_finished.store(true, Ordering::Release);
        shared.notify();
    }
}

impl StorageJobActivity {
    fn new(shared: &Weak<Shared>) -> Self {
        let armed = if let Some(shared) = shared.upgrade() {
            shared.active_storage_jobs.fetch_add(1, Ordering::AcqRel);
            true
        } else {
            false
        };
        Self {
            shared: shared.clone(),
            armed,
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
    let mut maintenance_requested = false;
    let mut closing = false;
    let mut age_timer = make_age_timer(&runtime, storage.as_ref()).unwrap_or_else(|_| {
        mark_poisoned(&shared);
        None
    });

    loop {
        if maintenance_requested
            && lifecycle(&shared) == RUNNING
            && backlog.is_empty()
            && receiver.is_empty()
        {
            maintenance_requested = false;
            let result = storage_job(runtime.clone(), &mut storage, &shared, |storage| {
                storage.reclaim()
            })
            .await;
            match result {
                Ok(_) => publish_storage(&shared, storage.as_ref()),
                Err(error) => {
                    if error.poisons_root() {
                        mark_poisoned(&shared);
                    }
                }
            }
            age_timer = make_age_timer(&runtime, storage.as_ref()).unwrap_or_else(|_| {
                mark_poisoned(&shared);
                None
            });
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
                        maintenance_requested = false;
                        age_timer = None;
                        continue;
                    }
                    () = timer.as_mut() => {
                        let result = storage_job(runtime.clone(), &mut storage, &shared, |storage| storage.seal_expired()).await;
                        match result {
                            Ok(_) => publish_storage(&shared, storage.as_ref()),
                            Err(error) if error.poisons_root() => mark_poisoned(&shared),
                            Err(_) => {}
                        }
                        age_timer = make_age_timer(&runtime, storage.as_ref()).unwrap_or_else(|_| {
                            mark_poisoned(&shared);
                            None
                        });
                        continue;
                    }
                }
            } else {
                tokio::select! {
                    command = receiver.recv() => command,
                    _ = shutdown.changed() => {
                        closing = true;
                        receiver.close();
                        maintenance_requested = false;
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
                    maintenance_requested = false;
                    age_timer = None;
                    continue;
                }
            }
        };
        let Some(command) = command else {
            break;
        };

        if lifecycle(&shared) == POISONED {
            reject_poisoned(command);
            mark_completed(&shared, 1);
            continue;
        }

        match command {
            Command::Append {
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
                                group_bytes += encoded_bytes;
                                group_records.insert(
                                    stream_id,
                                    cumulative.expect("checked cumulative record count"),
                                );
                                entries.push((stream_id, records, encoded_bytes, reply));
                            } else {
                                backlog.push_front(Command::Append {
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
                let count = entries.len();
                let bounded = !matches!(capacity, Capacity::Unbounded);
                let result = storage_job(runtime.clone(), &mut storage, &shared, move |storage| {
                    Ok(execute_append_job(storage, units, bounded))
                })
                .await;
                publish_storage(&shared, storage.as_ref());
                age_timer = make_age_timer(&runtime, storage.as_ref()).unwrap_or_else(|_| {
                    mark_poisoned(&shared);
                    None
                });
                let completed = match result {
                    Ok(AppendJob::Complete { outputs, selected })
                        if selected != 0 && selected <= count && outputs.len() == selected =>
                    {
                        let deferred = entries.split_off(selected);
                        requeue_appends(&mut backlog, deferred);
                        for ((_, _, _, reply), ids) in entries.into_iter().zip(outputs) {
                            let _ = reply.send(AppendReply::Complete(Ok(ids)));
                        }
                        selected
                    }
                    Ok(AppendJob::Complete { .. }) => {
                        let mut entries = entries.into_iter();
                        if let Some((_, _, _, reply)) = entries.next() {
                            let _ = reply.send(AppendReply::Complete(Err(Error::Runtime {
                                message: "storage returned the wrong append result count"
                                    .to_string(),
                            })));
                        }
                        for (_, _, _, reply) in entries {
                            let _ = reply.send(AppendReply::Complete(Err(Error::Poisoned)));
                        }
                        mark_poisoned(&shared);
                        count
                    }
                    Ok(AppendJob::Capacity(CapacityCheck::Wait {
                        needed_bytes,
                        available_bytes,
                    })) => {
                        let deferred = entries.split_off(1);
                        requeue_appends(&mut backlog, deferred);
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
                        requeue_appends(&mut backlog, deferred);
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
                        requeue_appends(&mut backlog, deferred);
                        let poisons = error.poisons_root();
                        reply_append_error(entries, error);
                        if poisons {
                            mark_poisoned(&shared);
                        }
                        selected
                    }
                    Ok(AppendJob::Failure { .. }) => {
                        reply_append_error(
                            entries,
                            Error::Runtime {
                                message: "storage returned an invalid append failure scope"
                                    .to_string(),
                            },
                        );
                        mark_poisoned(&shared);
                        count
                    }
                    Err(error) => {
                        let poisons = error.poisons_root();
                        reply_append_error(entries, error);
                        if poisons {
                            mark_poisoned(&shared);
                        }
                        count
                    }
                };
                mark_completed(&shared, completed);
            }
            Command::Read {
                stream_id,
                limits,
                reply,
            } => {
                let result = storage_job(runtime.clone(), &mut storage, &shared, move |storage| {
                    storage.read(stream_id, limits)
                })
                .await;
                let poisons = result.as_ref().err().is_some_and(Error::poisons_root);
                publish_storage(&shared, storage.as_ref());
                let _ = reply.send(result);
                if poisons {
                    mark_poisoned(&shared);
                }
                mark_completed(&shared, 1);
            }
            Command::Release {
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
                            stream_id,
                            ids,
                            encoded_bound,
                            reply,
                        } if group_bytes
                            .checked_add(encoded_bound)
                            .is_some_and(|bytes| bytes <= max_bytes) =>
                        {
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
                let units = entries
                    .iter()
                    .map(|(stream_id, ids, _, _)| ReleaseUnit {
                        stream_id: *stream_id,
                        ids: ids.clone(),
                    })
                    .collect();
                let result = storage_job(runtime.clone(), &mut storage, &shared, move |storage| {
                    storage.release_group(units)
                })
                .await;
                let poisons = result.as_ref().err().is_some_and(Error::poisons_root);
                publish_storage(&shared, storage.as_ref());
                match result {
                    Ok(()) => {
                        for (_, _, _, reply) in entries {
                            let _ = reply.send(Ok(()));
                        }
                        maintenance_requested = true;
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
                        if poisons {
                            mark_poisoned(&shared);
                        }
                    }
                }
                mark_completed(&shared, count);
            }
            Command::Reclaim { reply } => {
                let result = storage_job(runtime.clone(), &mut storage, &shared, |storage| {
                    storage.reclaim()
                })
                .await;
                let poisons = result.as_ref().err().is_some_and(Error::poisons_root);
                publish_storage(&shared, storage.as_ref());
                let _ = reply.send(result);
                if poisons {
                    mark_poisoned(&shared);
                }
                mark_completed(&shared, 1);
                age_timer = make_age_timer(&runtime, storage.as_ref()).unwrap_or_else(|_| {
                    mark_poisoned(&shared);
                    None
                });
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
        if matches!(selected.1, CapacityCheck::Wait { .. }) {
            storage.reclaim()?;
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

fn requeue_appends(backlog: &mut VecDeque<Command>, entries: Vec<AppendEntry>) {
    for (stream_id, records, encoded_bytes, reply) in entries.into_iter().rev() {
        backlog.push_front(Command::Append {
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
    job: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut Storage) -> Result<T> + Send + 'static,
{
    let current = storage.take().ok_or(Error::Poisoned)?;
    let activity = StorageJobActivity::new(shared);
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
        stats: storage.stats()?,
        known_streams,
        stream_stats,
        highwaters: storage.stream_highwaters(),
    })
}

fn publish_storage(shared: &Weak<Shared>, storage: Option<&Storage>) {
    let (Some(shared), Some(storage)) = (shared.upgrade(), storage) else {
        return;
    };
    match view_from_storage(storage) {
        Ok(view) => {
            let mut current = shared
                .view
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if *current != view {
                *current = view;
                shared.notify();
            }
        }
        Err(_) => mark_poisoned(&Arc::downgrade(&shared)),
    }
}

fn lifecycle(shared: &Weak<Shared>) -> u8 {
    shared
        .upgrade()
        .map_or(CLOSED, |shared| shared.lifecycle.load(Ordering::Acquire))
}

fn mark_poisoned(shared: &Weak<Shared>) {
    if let Some(shared) = shared.upgrade() {
        shared.lifecycle.store(POISONED, Ordering::Release);
        shared.notify();
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
        Command::Reclaim { reply } => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::RuntimeError;
    use bytes::Bytes;
    use std::sync::Mutex;
    use tempfile::TempDir;

    #[derive(Default)]
    struct HeldRuntime {
        reactor: Mutex<Option<RuntimeFuture>>,
    }

    impl HeldRuntime {
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
        let activity = StorageJobActivity::new(&Arc::downgrade(&log.shared));

        runtime.terminate_reactor();

        assert!(log.stats().poisoned);
        let mut shutdown = Box::pin(log.shutdown());
        tokio::select! {
            biased;
            result = &mut shutdown => panic!("shutdown completed before the storage job ended: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        drop(activity);
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("shutdown did not observe reactor termination")
            .unwrap();
        assert_eq!(log.shared.lifecycle.load(Ordering::Acquire), CLOSED);

        let reopened = Log::open(config(&directory)).await.unwrap();
        reopened.shutdown().await.unwrap();
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
