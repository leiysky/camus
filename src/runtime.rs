use crate::error::{Error, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// A boxed runtime-neutral task Future.
pub type RuntimeFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Failure to submit work to a configured runtime backend.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{message}")]
pub struct RuntimeError {
    message: String,
}

impl RuntimeError {
    /// Creates a runtime submission error.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Minimal executor capabilities required by one Camus root reactor.
pub trait Runtime: Send + Sync + 'static {
    /// Spawns a long-lived async coordination task.
    fn spawn(&self, future: RuntimeFuture) -> std::result::Result<(), RuntimeError>;

    /// Spawns one finite blocking filesystem job.
    fn spawn_blocking(
        &self,
        job: Box<dyn FnOnce() + Send + 'static>,
    ) -> std::result::Result<(), RuntimeError>;

    /// Returns a Future that becomes ready after the requested duration.
    fn sleep(&self, duration: Duration) -> RuntimeFuture;
}

struct TokioBackend {
    runtime: tokio::runtime::Runtime,
}

impl TokioBackend {
    fn new() -> std::result::Result<Self, RuntimeError> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .thread_name("camus-runtime")
            .build()
            .map(|runtime| Self { runtime })
            .map_err(|error| RuntimeError::new(error.to_string()))
    }
}

impl Runtime for TokioBackend {
    fn spawn(&self, future: RuntimeFuture) -> std::result::Result<(), RuntimeError> {
        drop(self.runtime.spawn(future));
        Ok(())
    }

    fn spawn_blocking(
        &self,
        job: Box<dyn FnOnce() + Send + 'static>,
    ) -> std::result::Result<(), RuntimeError> {
        drop(self.runtime.spawn_blocking(job));
        Ok(())
    }

    fn sleep(&self, duration: Duration) -> RuntimeFuture {
        Box::pin(async move {
            tokio::time::sleep(duration).await;
        })
    }
}

pub(crate) fn default_runtime() -> Result<Arc<dyn Runtime>> {
    static DEFAULT: OnceLock<std::result::Result<Arc<TokioBackend>, RuntimeError>> =
        OnceLock::new();

    match DEFAULT.get_or_init(|| TokioBackend::new().map(Arc::new)) {
        Ok(runtime) => Ok(runtime.clone()),
        Err(error) => Err(Error::Runtime {
            message: error.to_string(),
        }),
    }
}

pub(crate) async fn run_blocking<T, F>(runtime: Arc<dyn Runtime>, job: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    run_blocking_guarded(runtime, job, ()).await
}

pub(crate) async fn run_blocking_guarded<T, F, G>(
    runtime: Arc<dyn Runtime>,
    job: F,
    completion_guard: G,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
    G: Send + 'static,
{
    let (sender, receiver) = tokio::sync::oneshot::channel();
    runtime
        .spawn_blocking(Box::new(move || {
            let _completion_guard = completion_guard;
            let result = job();
            let _ = sender.send(result);
        }))
        .map_err(|error| Error::Runtime {
            message: error.to_string(),
        })?;

    receiver.await.map_err(|_| Error::Runtime {
        message: "blocking runtime job terminated without a result".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_runtime_is_process_shared() {
        let first = default_runtime().unwrap();
        let second = default_runtime().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }
}
