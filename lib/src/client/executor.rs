//! Fixed-size, fairly-scheduled bridge executor.
//!
//! Replaces "one blocking worker per request". That model cost an OS thread per
//! in-flight request and, worse, a paused request kept its thread parked on a
//! condvar — so a handful of slow consumers could exhaust the blocking pool and
//! stall every other request in the process.
//!
//! Here a paused request holds no thread: it leaves the ready queue and returns
//! when it is resumed or when more events arrive.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// One unit of schedulable work.
pub(crate) trait Runnable: Send + Sync + 'static {
    /// Deliver at most `budget` events. Returns true if the slot still has work
    /// and should be re-queued; false if it is paused or finished.
    fn run_slice(&self, budget: usize) -> bool;
}

struct Shared {
    ready: Mutex<VecDeque<Arc<dyn Runnable>>>,
    cv: Condvar,
    stopped: AtomicBool,
}

pub(crate) struct BridgeExecutor {
    shared: Arc<Shared>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

/// Events one slot may deliver before yielding its worker.
///
/// Bounded on purpose: without it a single high-throughput stream would hold a
/// worker indefinitely and starve everyone else sharing it.
const SLICE: usize = 8;

impl BridgeExecutor {
    pub(crate) fn new(threads: usize) -> Arc<Self> {
        let shared = Arc::new(Shared {
            ready: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            stopped: AtomicBool::new(false),
        });
        let mut workers = Vec::with_capacity(threads);
        for _ in 0..threads {
            let s = shared.clone();
            workers.push(std::thread::spawn(move || worker_loop(s)));
        }
        Arc::new(BridgeExecutor {
            shared,
            workers: Mutex::new(workers),
        })
    }

    pub(crate) fn schedule(&self, slot: Arc<dyn Runnable>) {
        let mut q = self.shared.ready.lock().unwrap();
        q.push_back(slot);
        self.shared.cv.notify_one();
    }

    pub(crate) fn shutdown(&self) {
        self.shared.stopped.store(true, Ordering::SeqCst);
        self.shared.cv.notify_all();
        for h in self.workers.lock().unwrap().drain(..) {
            let _ = h.join();
        }
    }
}

fn worker_loop(shared: Arc<Shared>) {
    loop {
        // Checked every iteration, not only when the queue drains: a slot that
        // always has more work would otherwise keep this thread alive forever
        // and shutdown() would never join it.
        if shared.stopped.load(Ordering::SeqCst) {
            return;
        }
        let slot = {
            let mut q = shared.ready.lock().unwrap();
            loop {
                if let Some(s) = q.pop_front() {
                    break Some(s);
                }
                if shared.stopped.load(Ordering::SeqCst) {
                    break None;
                }
                q = shared.cv.wait(q).unwrap();
            }
        };
        let Some(slot) = slot else { return };
        // Round-robin: a slot with more work goes to the BACK, so one busy
        // stream cannot monopolise a worker.
        if slot.run_slice(SLICE) {
            let mut q = shared.ready.lock().unwrap();
            q.push_back(slot);
            shared.cv.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct Counter {
        remaining: Mutex<usize>,
        delivered: AtomicUsize,
        paused: AtomicBool,
    }

    impl Runnable for Counter {
        fn run_slice(&self, budget: usize) -> bool {
            if self.paused.load(Ordering::SeqCst) {
                return false;
            }
            let mut n = self.remaining.lock().unwrap();
            let take = budget.min(*n);
            *n -= take;
            self.delivered.fetch_add(take, Ordering::SeqCst);
            *n > 0
        }
    }

    #[test]
    fn a_paused_slot_holds_no_worker() {
        // The property the old model could not provide: parked work must not
        // occupy a thread. With one worker thread, a paused slot must still let
        // another slot make progress.
        let ex = BridgeExecutor::new(1);
        let stuck = Arc::new(Counter {
            remaining: Mutex::new(1000),
            delivered: AtomicUsize::new(0),
            paused: AtomicBool::new(true),
        });
        let other = Arc::new(Counter {
            remaining: Mutex::new(16),
            delivered: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
        });
        ex.schedule(stuck.clone());
        ex.schedule(other.clone());

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while other.delivered.load(Ordering::SeqCst) < 16 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            other.delivered.load(Ordering::SeqCst),
            16,
            "a paused slot blocked the only worker"
        );
        assert_eq!(stuck.delivered.load(Ordering::SeqCst), 0);
        ex.shutdown();
    }

    #[test]
    fn a_busy_slot_does_not_starve_its_peers() {
        let ex = BridgeExecutor::new(1);
        let hog = Arc::new(Counter {
            // Effectively unbounded: the point is that  finishes
            // WITHOUT waiting for this one, which a fixed 10_000 could not
            // show because the hog also completes in the test window.
            remaining: Mutex::new(usize::MAX),
            delivered: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
        });
        let small = Arc::new(Counter {
            remaining: Mutex::new(8),
            delivered: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
        });
        ex.schedule(hog.clone());
        ex.schedule(small.clone());

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while small.delivered.load(Ordering::SeqCst) < 8 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(
            small.delivered.load(Ordering::SeqCst),
            8,
            "a high-throughput slot starved a small one"
        );
        assert!(
            *hog.remaining.lock().unwrap() > 0,
            "the hog ran to completion, so this proves nothing about fairness"
        );
        ex.shutdown();
    }

    #[test]
    fn shutdown_joins_every_worker() {
        let ex = BridgeExecutor::new(4);
        std::thread::sleep(std::time::Duration::from_millis(20));
        let started = std::time::Instant::now();
        ex.shutdown();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }
}
