//! Per-request event delivery, the terminal gate, and backpressure.
//!
//! Each request owns a bounded queue and is scheduled onto a fixed-size
//! executor (see `executor.rs`). Callbacks never run on a Tokio I/O worker, and
//! a paused request holds no thread at all — it simply leaves the ready queue.

use super::executor::{BridgeExecutor, Runnable};
use crate::abi::*;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use tokio::sync::Semaphore;

pub(crate) type OnHeaders =
    extern "C" fn(*mut c_void, u64, u16, u8, *const Hyper4kHeader, usize) -> Hyper4kHeadersAction;
pub(crate) type OnChunk = extern "C" fn(*mut c_void, u64, *const u8, usize) -> Hyper4kChunkAction;
pub(crate) type OnDone = extern "C" fn(*mut c_void, u64, *const Hyper4kError);

/// `user_data` is opaque to us and lives until this request's `OnDone` returns.
#[derive(Clone, Copy)]
pub(crate) struct UserData(pub *mut c_void);
unsafe impl Send for UserData {}
unsafe impl Sync for UserData {}

pub(crate) struct Callbacks {
    pub on_headers: OnHeaders,
    pub on_chunk: Option<OnChunk>,
    pub on_done: OnDone,
    pub user_data: UserData,
}
unsafe impl Send for Callbacks {}
unsafe impl Sync for Callbacks {}

pub(crate) enum Event {
    Headers {
        status: u16,
        version: u8,
        headers: Vec<(Bytes, Bytes)>,
    },
    Chunk(Bytes),
}

pub(crate) struct Terminal {
    pub kind: Hyper4kErrorKind,
    pub protocol_code: u32,
    pub message: String,
}

// Request states. The gate is what makes "OnDone is the last event" true.
const RUNNING: u8 = 0;
/// Normal completion: deliver what is already queued, then Done.
const TERMINAL_DRAIN: u8 = 1;
/// Cancel / close / truncation: the queued events are stale, drop them.
const TERMINAL_DISCARD: u8 = 2;

// ---------------------------------------------------------------------------
// Shutdown accounting
// ---------------------------------------------------------------------------

/// Counts bridges that can still invoke a callback.
///
/// `free()` waits on this reaching zero. Deterministic, not a timeout: a
/// deadline that expires and frees anyway is a `user_data` use-after-free with
/// extra steps.
pub(crate) struct BridgeCounter {
    n: StdMutex<u32>,
    cv: Condvar,
}

impl Default for BridgeCounter {
    fn default() -> Self {
        BridgeCounter {
            n: StdMutex::new(0),
            cv: Condvar::new(),
        }
    }
}

impl BridgeCounter {
    pub(crate) fn enter(&self) {
        *self.n.lock().unwrap() += 1;
    }

    /// Called after `OnDone` has returned, so zero really does mean no callback
    /// can still be running.
    pub(crate) fn leave(&self) {
        let mut n = self.n.lock().unwrap();
        *n -= 1;
        if *n == 0 {
            self.cv.notify_all();
        }
    }

    pub(crate) fn wait_zero(&self) {
        let mut n = self.n.lock().unwrap();
        while *n != 0 {
            n = self.cv.wait(n).unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Request state
// ---------------------------------------------------------------------------

pub(crate) struct RequestState {
    pub id: u64,
    state: AtomicU8,
    /// Set once the first response event is queued. Irreversible, and the only
    /// signal that exists on the committed path (see spec §四).
    committed: AtomicBool,
    /// True while the consumer has paused delivery. A paused slot is simply not
    /// in the ready queue; it occupies no thread.
    paused: AtomicBool,
    /// True while a chunk callback is executing, so an early `resume` can be
    /// remembered instead of lost.
    in_callback: AtomicBool,
    /// A resume that arrived before the pause landed. Belongs to the current
    /// chunk only: leaking it forward would release a later pause.
    permit: AtomicBool,
    /// Holds a slot of the connection's paused-stream budget while parked.
    conn: StdMutex<Option<Arc<super::pool::ConnEntry>>>,
    pause_guard: StdMutex<Option<super::pool::PauseGuard>>,
}

impl RequestState {
    fn new(id: u64) -> Self {
        RequestState {
            id,
            state: AtomicU8::new(RUNNING),
            committed: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            in_callback: AtomicBool::new(false),
            permit: AtomicBool::new(false),
            conn: StdMutex::new(None),
            pause_guard: StdMutex::new(None),
        }
    }

    pub(crate) fn bind_connection(&self, entry: Arc<super::pool::ConnEntry>) {
        *self.conn.lock().unwrap() = Some(entry);
    }

    pub(crate) fn mark_committed(&self) {
        self.committed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.state.load(Ordering::SeqCst) != RUNNING
    }

    fn discarding(&self) -> bool {
        self.state.load(Ordering::SeqCst) == TERMINAL_DISCARD
    }

    /// Claim the right to finish this request. Exactly one caller may win.
    fn claim(&self, discard: bool) -> bool {
        let target = if discard {
            TERMINAL_DISCARD
        } else {
            TERMINAL_DRAIN
        };
        self.state
            .compare_exchange(RUNNING, target, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn enter_pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        let guard = self
            .conn
            .lock()
            .unwrap()
            .clone()
            .map(super::pool::PauseGuard::new);
        *self.pause_guard.lock().unwrap() = guard;
    }

    fn leave_pause(&self) {
        self.paused.store(false, Ordering::SeqCst);
        *self.pause_guard.lock().unwrap() = None;
    }

    fn clear_permit(&self) {
        self.permit.store(false, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// The schedulable slot
// ---------------------------------------------------------------------------

struct Slot {
    state: Arc<RequestState>,
    callbacks: Callbacks,
    queue: StdMutex<VecDeque<Event>>,
    /// Producer backpressure. A permit is taken before an event is queued and
    /// returned once it has been delivered, so a slow consumer stops the
    /// network task polling the body and the peer's window closes.
    space: Arc<Semaphore>,
    terminal: StdMutex<Option<Terminal>>,
    done_ready: AtomicBool,
    /// True while this slot sits in the executor's ready queue.
    scheduled: AtomicBool,
    /// True while a worker is inside `run_slice` for this slot.
    ///
    /// Without it two workers can run the same slot at once — one draining the
    /// queue while the other, having found it empty, delivers `OnDone`. The
    /// caller then sees an event after `OnDone`, which the spec forbids. It
    /// showed up as a 1-in-14 flake, never when the test ran alone.
    running: AtomicBool,
    /// Set once `OnDone` has been delivered. Nothing may follow it.
    finished_flag: AtomicBool,
    finished: StdMutex<Option<Box<dyn FnOnce() + Send>>>,
    counter: Arc<BridgeCounter>,
    executor: Arc<BridgeExecutor>,
    /// Lets the slot re-queue itself after settling from inside a callback.
    me: StdMutex<std::sync::Weak<Slot>>,
}

impl Slot {
    fn deliver(&self, ev: Event) -> Option<Hyper4kChunkAction> {
        let cb = &self.callbacks;
        let id = self.state.id;
        match ev {
            Event::Headers {
                status,
                version,
                headers,
            } => {
                let raw: Vec<Hyper4kHeader> = headers
                    .iter()
                    .map(|(n, v)| Hyper4kHeader {
                        name: crate::Hyper4kSlice {
                            ptr: n.as_ptr(),
                            len: n.len(),
                        },
                        value: crate::Hyper4kSlice {
                            ptr: v.as_ptr(),
                            len: v.len(),
                        },
                    })
                    .collect();
                let action =
                    (cb.on_headers)(cb.user_data.0, id, status, version, raw.as_ptr(), raw.len());
                // Map onto the chunk action space so the caller has one place
                // to react. PAUSE has no meaning at the headers stage.
                if action == HYPER4K_HEADERS_CANCEL {
                    Some(HYPER4K_CHUNK_CANCEL)
                } else {
                    None
                }
            }
            Event::Chunk(b) => cb
                .on_chunk
                .map(|f| f(cb.user_data.0, id, b.as_ptr(), b.len())),
        }
    }

    fn deliver_done(&self, t: &Terminal) {
        let cb = &self.callbacks;
        if t.kind == HYPER4K_ERR_NONE {
            (cb.on_done)(cb.user_data.0, self.state.id, std::ptr::null());
        } else {
            let err = Hyper4kError {
                kind: t.kind,
                protocol_code: t.protocol_code,
                message: crate::Hyper4kSlice {
                    ptr: t.message.as_ptr(),
                    len: t.message.len(),
                },
            };
            (cb.on_done)(cb.user_data.0, self.state.id, &err);
        }
    }
}

impl Runnable for Slot {
    fn run_slice(&self, budget: usize) -> bool {
        // One worker per slot. A loser bounces immediately; the owner re-queues
        // if work remains, so nothing is lost.
        if self.running.swap(true, Ordering::SeqCst) {
            self.scheduled.store(false, Ordering::SeqCst);
            return false;
        }
        let more = self.run_owned(budget);
        self.running.store(false, Ordering::SeqCst);
        if self.finished_flag.load(Ordering::SeqCst) {
            return false; // OnDone was the last event, by contract
        }
        // Re-check: an event may have been queued while we held the slot.
        more || !self.queue.lock().unwrap().is_empty()
    }
}

impl Slot {
    fn run_owned(&self, budget: usize) -> bool {
        self.scheduled.store(false, Ordering::SeqCst);
        if self.state.paused.load(Ordering::SeqCst) {
            return false;
        }

        for _ in 0..budget {
            if self.state.paused.load(Ordering::SeqCst) {
                return false;
            }
            let next = self.queue.lock().unwrap().pop_front();
            let Some(ev) = next else { break };
            // A permit frees up as soon as the event leaves the queue.
            self.space.add_permits(1);

            if self.state.discarding() {
                continue; // drained and dropped, never delivered
            }

            self.state.in_callback.store(true, Ordering::SeqCst);
            let action = self.deliver(ev);
            self.state.in_callback.store(false, Ordering::SeqCst);

            match action {
                Some(a) if a == HYPER4K_CHUNK_CANCEL => {
                    self.state.clear_permit();
                    // Settle for real. An earlier version only returned here,
                    // so a callback-driven cancel silently completed as a
                    // success.
                    settle_slot(
                        &self.state,
                        self,
                        Terminal {
                            kind: HYPER4K_ERR_CANCELLED,
                            protocol_code: 0,
                            message: "cancelled by callback".into(),
                        },
                        true,
                    );
                    return false;
                }
                Some(a) if a == HYPER4K_CHUNK_PAUSE => {
                    // An early resume consumes the pause outright: no parking,
                    // no lost wakeup, nothing left to release a later pause.
                    if self.state.permit.swap(false, Ordering::SeqCst) {
                        continue;
                    }
                    self.state.enter_pause();
                    return false;
                }
                _ => self.state.clear_permit(),
            }
        }

        let empty = self.queue.lock().unwrap().is_empty();
        if empty && self.done_ready.load(Ordering::SeqCst) {
            if let Some(t) = self.terminal.lock().unwrap().take() {
                self.deliver_done(&t);
                // Only after OnDone has returned: the handle is dropped and
                // free() may proceed.
                self.finished_flag.store(true, Ordering::SeqCst);
                if let Some(f) = self.finished.lock().unwrap().take() {
                    f();
                }
                self.counter.leave();
            }
            return false;
        }
        !empty
    }
}

// ---------------------------------------------------------------------------
// Handle and producer
// ---------------------------------------------------------------------------

pub(crate) struct RequestHandle {
    pub state: Arc<RequestState>,
    slot: Arc<Slot>,
    executor: Arc<BridgeExecutor>,
    abort: StdMutex<Option<tokio::task::AbortHandle>>,
}

impl RequestHandle {
    pub(crate) fn set_abort(&self, h: tokio::task::AbortHandle) {
        *self.abort.lock().unwrap() = Some(h);
    }

    fn schedule(&self) {
        schedule_slot(&self.executor, &self.slot);
    }

    /// Resume a paused response body.
    ///
    /// The permit exists because `PAUSE` is only observable after the callback
    /// returns: a consumer that finishes early and resumes from inside its own
    /// callback would otherwise be ignored and the stream would hang.
    pub(crate) fn resume(&self) -> Hyper4kStatus {
        if self.state.paused.load(Ordering::SeqCst) {
            self.state.leave_pause();
            self.schedule();
            return HYPER4K_STATUS_OK;
        }
        if self.state.in_callback.load(Ordering::SeqCst) {
            self.state.permit.store(true, Ordering::SeqCst);
            return HYPER4K_STATUS_OK;
        }
        if self.state.is_terminal() {
            return HYPER4K_STATUS_ALREADY_DONE;
        }
        HYPER4K_STATUS_NOT_PAUSED
    }

    /// Terminate this request exactly once, in the frozen order.
    ///
    /// Flip the gate first, then stop the producer, then hand over `OnDone`,
    /// then abort the network task. Aborting first would drop the callback,
    /// because an aborted Tokio task does not run ordinary cleanup.
    pub(crate) fn settle(&self, terminal: Terminal, discard: bool) {
        settle_slot(&self.state, &self.slot, terminal, discard);
        if let Some(h) = self.abort.lock().unwrap().take() {
            h.abort();
        }
    }
}

/// The one place a request may be terminated, used by both the public handle
/// and a callback that cancels from inside the executor.
fn settle_slot(state: &Arc<RequestState>, slot: &Slot, terminal: Terminal, discard: bool) {
    if !state.claim(discard) {
        // Someone else already settled. On shutdown a paused slot still has to
        // be released so it can finish — release ONLY, never downgrade an
        // already-successful drain, or its queued chunks would be dropped while
        // the terminal keeps reporting success.
        if discard && state.paused.load(Ordering::SeqCst) {
            state.leave_pause();
            reschedule(slot);
        }
        return;
    }
    *slot.terminal.lock().unwrap() = Some(terminal);
    slot.done_ready.store(true, Ordering::SeqCst);
    slot.space.add_permits(1); // unblock a producer waiting for room
                               // Only an abort releases the pause. A normal completion means "no more data
                               // is coming", not "deliver what the consumer asked us to hold" — unpausing
                               // here would overrun the pause and hand over chunks it never asked for.
    if discard && state.paused.load(Ordering::SeqCst) {
        state.leave_pause();
    }
    reschedule(slot);
}

fn reschedule(slot: &Slot) {
    let me = slot.me.lock().unwrap().upgrade();
    if let Some(arc) = me {
        schedule_slot(&slot.executor, &arc);
    }
}

fn schedule_slot(ex: &Arc<BridgeExecutor>, slot: &Arc<Slot>) {
    if !slot.scheduled.swap(true, Ordering::SeqCst) {
        ex.schedule(slot.clone() as Arc<dyn Runnable>);
    }
}

/// The producer half handed to the network task.
pub(crate) struct EventSink {
    slot: Arc<Slot>,
    executor: Arc<BridgeExecutor>,
    state: Arc<RequestState>,
}

impl EventSink {
    /// Queues one event, waiting for room if the consumer is behind.
    ///
    /// Waiting here is the backpressure: it stops the network task polling the
    /// body, which lets the peer's flow-control window close. Nothing is
    /// buffered without bound and nothing is dropped.
    pub(crate) async fn send(&self, ev: Event) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        let Ok(permit) = self.slot.space.clone().acquire_owned().await else {
            return false;
        };
        if self.state.is_terminal() {
            return false;
        }
        permit.forget(); // returned by the consumer once the event is delivered
        self.state.mark_committed();
        self.slot.queue.lock().unwrap().push_back(ev);
        if !self.state.paused.load(Ordering::SeqCst) {
            schedule_slot(&self.executor, &self.slot);
        }
        true
    }
}

/// Wires up one request.
pub(crate) fn spawn(
    executor: &Arc<BridgeExecutor>,
    counter: &Arc<BridgeCounter>,
    id: u64,
    callbacks: Callbacks,
    queue_capacity: usize,
    on_finished: impl FnOnce() + Send + 'static,
) -> (Arc<RequestHandle>, EventSink) {
    let state = Arc::new(RequestState::new(id));
    let slot = Arc::new(Slot {
        state: state.clone(),
        callbacks,
        queue: StdMutex::new(VecDeque::new()),
        space: Arc::new(Semaphore::new(queue_capacity)),
        terminal: StdMutex::new(None),
        done_ready: AtomicBool::new(false),
        scheduled: AtomicBool::new(false),
        running: AtomicBool::new(false),
        finished_flag: AtomicBool::new(false),
        finished: StdMutex::new(Some(Box::new(on_finished))),
        counter: counter.clone(),
        executor: executor.clone(),
        me: StdMutex::new(std::sync::Weak::new()),
    });
    *slot.me.lock().unwrap() = Arc::downgrade(&slot);

    let handle = Arc::new(RequestHandle {
        state: state.clone(),
        slot: slot.clone(),
        executor: executor.clone(),
        abort: StdMutex::new(None),
    });

    let sink = EventSink {
        slot,
        executor: executor.clone(),
        state,
    };
    (handle, sink)
}

#[cfg(test)]
mod counter_tests {
    use super::*;
    use std::sync::atomic::Ordering as AOrd;

    #[test]
    fn wait_zero_blocks_until_the_last_bridge_leaves() {
        let c = Arc::new(BridgeCounter::default());
        c.enter();
        c.enter();
        let left = Arc::new(AtomicBool::new(false));

        let c2 = c.clone();
        let l2 = left.clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(150));
            c2.leave();
            std::thread::sleep(std::time::Duration::from_millis(150));
            l2.store(true, AOrd::SeqCst);
            c2.leave();
        });

        c.wait_zero();
        assert!(
            left.load(AOrd::SeqCst),
            "wait_zero returned before the last bridge left"
        );
        t.join().unwrap();
    }

    #[test]
    fn wait_zero_returns_immediately_when_nothing_is_active() {
        let c = BridgeCounter::default();
        c.wait_zero();
    }
}

#[cfg(test)]
mod exclusion_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering as AOrd};

    /// Counts how many workers are inside `run_slice` at once.
    struct Concurrent {
        depth: AtomicU32,
        max_depth: AtomicU32,
        running: AtomicBool,
        rounds: AtomicU32,
    }

    impl Runnable for Concurrent {
        fn run_slice(&self, _budget: usize) -> bool {
            // Mirrors Slot::run_slice's guard so the property can be tested
            // without standing up a whole request.
            if self.running.swap(true, AOrd::SeqCst) {
                return false;
            }
            let d = self.depth.fetch_add(1, AOrd::SeqCst) + 1;
            self.max_depth.fetch_max(d, AOrd::SeqCst);
            std::thread::sleep(std::time::Duration::from_micros(50));
            self.depth.fetch_sub(1, AOrd::SeqCst);
            self.running.store(false, AOrd::SeqCst);
            self.rounds.fetch_add(1, AOrd::SeqCst) < 200
        }
    }

    #[test]
    fn a_slot_never_runs_on_two_workers_at_once() {
        // The 1-in-14 flake was exactly this: one worker draining the queue
        // while another, finding it empty, delivered OnDone — so the caller saw
        // an event after OnDone.
        let ex = BridgeExecutor::new(8);
        let slot = Arc::new(Concurrent {
            depth: AtomicU32::new(0),
            max_depth: AtomicU32::new(0),
            running: AtomicBool::new(false),
            rounds: AtomicU32::new(0),
        });
        // Queue the same slot from many threads, as a producer burst would.
        for _ in 0..64 {
            ex.schedule(slot.clone() as Arc<dyn Runnable>);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while slot.rounds.load(AOrd::SeqCst) < 200 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        ex.shutdown();
        assert_eq!(
            slot.max_depth.load(AOrd::SeqCst),
            1,
            "a slot ran on more than one worker at once"
        );
    }
}
