use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

const CLOSING: u64 = 1 << 63;

struct State {
    active: AtomicU64,
    idle: Notify,
}

/// Hyper spawns HTTP/2 stream tasks separately from the connection future.
/// Track them per listener without a cancellation wrapper around every task.
#[derive(Clone)]
pub(crate) struct ListenerExecutor {
    state: Arc<State>,
}

impl ListenerExecutor {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(State {
                active: AtomicU64::new(0),
                idle: Notify::new(),
            }),
        }
    }

    /// Call only after every connection task has joined (no new stream producers).
    pub(crate) async fn stop(&self) {
        self.state.active.fetch_or(CLOSING, Ordering::AcqRel);
        loop {
            let notified = self.state.idle.notified();
            if self.state.active.load(Ordering::Acquire) == CLOSING {
                return;
            }
            notified.await;
        }
    }
}

impl<F> hyper::rt::Executor<F> for ListenerExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        let state = self.state.clone();
        let mut current = state.active.load(Ordering::Acquire);
        loop {
            if current & CLOSING != 0 {
                return;
            }
            match state.active.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
        tokio::spawn(async move {
            future.await;
            let previous = state.active.fetch_sub(1, Ordering::AcqRel);
            if previous == 1 || previous == CLOSING + 1 {
                state.idle.notify_waiters();
            }
        });
    }
}
