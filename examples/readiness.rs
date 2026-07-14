//! Wait for durable stream readiness without callbacks or polling.
//!
//! Real async applications can await `readiness.wait_for(stream)` directly.
//! This example uses a tiny standard-library executor so Camus does not need
//! to depend on or start an async runtime.

use camus::{Config, Log, Record, Result, StreamId};
use std::future::Future;
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

const UPLOADS: StreamId = StreamId::new(7);

struct ThreadWake(mpsc::SyncSender<()>);

impl ThreadWake {
    fn notify(&self) {
        let _ = self.0.try_send(());
    }
}

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let (sender, receiver) = mpsc::sync_channel(1);
    let waker = Waker::from(Arc::new(ThreadWake(sender)));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => receiver.recv().expect("readiness waker was dropped"),
        }
    }
}

fn main() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut log = Log::open(Config::new(directory.path()))?;
    let readiness = log.readiness();

    // This is an application-owned thread. Camus itself starts no thread.
    let waiter_readiness = readiness.clone();
    let waiter = thread::spawn(move || block_on(waiter_readiness.wait_for(UPLOADS)));

    // No registration handshake or sleep is needed. If append wins the race,
    // the first Future poll observes level-triggered readiness immediately.
    log.append_to(
        UPLOADS,
        Record::new("upload-1", b"opaque upload".as_slice()),
    )?;
    waiter.join().expect("readiness thread panicked")?;
    assert!(readiness.is_ready(UPLOADS));

    // A wake is only notification. The Log owner still enumerates, reads, and
    // releases records; the waiter did not claim or reserve anything.
    let pending = log.recovery().pending_records_for(UPLOADS);
    assert_eq!(pending.len(), 1);
    assert_eq!(log.read(&pending[0].location)?.as_ref(), b"opaque upload");
    log.release_from(UPLOADS, ["upload-1"])?;
    assert!(!readiness.is_ready(UPLOADS));

    // Rearm wait_for only after attempting to drain. While a stream remains
    // pending, another wait completes immediately and a loop could spin.
    Ok(())
}
