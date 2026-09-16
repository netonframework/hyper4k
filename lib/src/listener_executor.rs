use std::future::Future;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Hyper spawns HTTP/2 stream tasks separately from the connection future.
/// Track them per listener so stopping one server also fences its callbacks.
#[derive(Clone)]
pub(crate) struct ListenerExecutor {
    tasks: TaskTracker,
    shutdown: CancellationToken,
}

impl ListenerExecutor {
    pub(crate) fn new() -> Self {
        Self {
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
        }
    }

    /// Call only after every connection task has joined (no new stream producers).
    pub(crate) async fn stop(&self) {
        self.shutdown.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }
}

impl<F> hyper::rt::Executor<F> for ListenerExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        let shutdown = self.shutdown.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {},
                _ = future => {},
            }
        });
    }
}
