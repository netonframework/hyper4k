//! Per-request event queue and the terminal gate.
//!
//! Callbacks never run on a Tokio I/O worker: each request owns a bounded queue
//! drained by its own bridge task. A slow consumer therefore stalls only its own
//! stream.

use crate::abi::*;
use bytes::Bytes;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use tokio::sync::{mpsc, oneshot, Notify};

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

pub(crate) struct RequestState {
    pub id: u64,
    state: AtomicU8,
    /// Set once the first response event is queued. Irreversible, and the only
    /// signal that exists on the committed path (see spec §四).
    committed: AtomicBool,
    pub finished: Arc<Notify>,
    /// True while a chunk callback is executing. An early `resume` arriving in
    /// that window must be remembered, or the stream parks forever.
    in_callback: AtomicBool,
    /// A resume that arrived before the pause landed. It belongs to the current
    /// chunk only: leaking it forward would silently release a later pause.
    permit: AtomicBool,
    parked: StdMutex<bool>,
    unpark: Condvar,
}

impl RequestState {
    pub(crate) fn new(id: u64) -> Self {
        RequestState {
            id,
            state: AtomicU8::new(RUNNING),
            committed: AtomicBool::new(false),
            finished: Arc::new(Notify::new()),
            in_callback: AtomicBool::new(false),
            permit: AtomicBool::new(false),
            parked: StdMutex::new(false),
            unpark: Condvar::new(),
        }
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

    /// Claim the right to finish this request. Returns false if someone else
    /// already did — every caller races, and exactly one must win.
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

    fn discarding(&self) -> bool {
        self.state.load(Ordering::SeqCst) == TERMINAL_DISCARD
    }

    /// Resume a paused response body.
    ///
    /// The permit exists because `PAUSE` is only observable after the callback
    /// returns: a consumer that finishes early and calls `resume` from inside
    /// its own callback would otherwise be ignored and the stream would hang.
    pub(crate) fn resume(&self) -> Hyper4kStatus {
        // Parked is checked FIRST. After a normal completion the request is
        // "terminal" on the network side while the consumer still has queued
        // data waiting behind its own pause; reporting ALREADY_DONE there would
        // strand that data with no way to ask for it.
        let mut parked = self.parked.lock().unwrap();
        if *parked {
            *parked = false;
            self.unpark.notify_all();
            return HYPER4K_STATUS_OK;
        }
        drop(parked);
        if self.in_callback.load(Ordering::SeqCst) {
            // Remembered for THIS chunk only; the worker clears it otherwise.
            self.permit.store(true, Ordering::SeqCst);
            return HYPER4K_STATUS_OK;
        }
        if self.is_terminal() {
            return HYPER4K_STATUS_ALREADY_DONE;
        }
        HYPER4K_STATUS_NOT_PAUSED
    }

    /// Park until resumed or aborted. Returns false only when aborted.
    ///
    /// A *normal* completion must NOT release the pause. The network side
    /// finishing only means no more data is coming; whatever is already queued
    /// still belongs to the consumer, who asked us to hold it. Treating any
    /// terminal state as a release made backpressure vanish for small
    /// responses, because the body ended before the consumer resumed.
    fn park(&self) -> bool {
        // An early resume consumes the pause outright: no parking, no lost
        // wakeup, and nothing left over to release a future pause.
        if self.permit.swap(false, Ordering::SeqCst) {
            return true;
        }
        let mut parked = self.parked.lock().unwrap();
        *parked = true;
        while *parked && !self.discarding() {
            parked = self.unpark.wait(parked).unwrap();
        }
        *parked = false;
        !self.discarding()
    }

    fn release_park(&self) {
        let mut parked = self.parked.lock().unwrap();
        *parked = false;
        self.unpark.notify_all();
    }

    fn clear_permit(&self) {
        self.permit.store(false, Ordering::SeqCst);
    }
}

/// The producer half handed to the network task.
pub(crate) struct EventSink {
    tx: mpsc::Sender<Event>,
    state: Arc<RequestState>,
}

impl EventSink {
    /// Applies backpressure: when the queue is full this waits, which stops the
    /// network task polling the body, which lets the peer's flow-control window
    /// close. It never buffers without bound and never drops data.
    pub(crate) async fn send(&self, ev: Event) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        self.state.mark_committed();
        self.tx.send(ev).await.is_ok()
    }
}

pub(crate) struct RequestHandle {
    pub state: Arc<RequestState>,
    /// Kept separate from the event producer so the producer can be closed
    /// before `OnDone` is enqueued.
    done_tx: std::sync::Mutex<Option<oneshot::Sender<Terminal>>>,
    event_tx: std::sync::Mutex<Option<mpsc::Sender<Event>>>,
    abort: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
}

impl RequestHandle {
    pub(crate) fn set_abort(&self, h: tokio::task::AbortHandle) {
        *self.abort.lock().unwrap() = Some(h);
    }

    /// Terminate this request exactly once, in the frozen order.
    ///
    /// The order is the contract, not an implementation detail:
    /// flip the gate first, then stop the producer, then discard if this is an
    /// abort, then enqueue `OnDone`, then abort the network task. Enqueueing
    /// `OnDone` before closing the producer would let a late chunk arrive after
    /// it; aborting first would drop the callback entirely, because an aborted
    /// Tokio task does not run ordinary cleanup.
    pub(crate) fn settle(&self, terminal: Terminal, discard: bool) {
        if !self.state.claim(discard) {
            return;
        }
        // 3. close the event producer
        drop(self.event_tx.lock().unwrap().take());
        // 4/5. hand OnDone to the bridge through its own channel
        if let Some(tx) = self.done_tx.lock().unwrap().take() {
            let _ = tx.send(terminal);
        }
        // Abort paths must release a paused request: waiting for a consumer that
        // has walked away would let one stream block hyper4k_client_free
        // forever. A normal completion deliberately does not — see `park`.
        if discard {
            self.state.release_park();
        }
        // 6. only now stop the network task
        if let Some(h) = self.abort.lock().unwrap().take() {
            h.abort();
        }
    }
}

/// Wires up one request: returns the handle, the producer, and the bridge task.
pub(crate) fn spawn(
    rt: &tokio::runtime::Handle,
    id: u64,
    callbacks: Callbacks,
    queue_capacity: usize,
    on_finished: impl FnOnce() + Send + 'static,
) -> (Arc<RequestHandle>, EventSink, tokio::task::JoinHandle<()>) {
    let state = Arc::new(RequestState::new(id));
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(queue_capacity);
    let (done_tx, done_rx) = oneshot::channel::<Terminal>();

    let handle = Arc::new(RequestHandle {
        state: state.clone(),
        done_tx: std::sync::Mutex::new(Some(done_tx)),
        event_tx: std::sync::Mutex::new(Some(event_tx.clone())),
        abort: std::sync::Mutex::new(None),
    });

    let sink = EventSink {
        tx: event_tx,
        state: state.clone(),
    };

    let worker_state = state.clone();
    let worker_handle = handle.clone();
    // Explicit handle: send() runs on the caller's thread, which has no
    // ambient runtime, and callbacks must not run on an I/O worker either.
    let worker = rt.spawn_blocking(move || {
        let cb = callbacks;
        // Deliver queued events unless the gate says they are stale.
        while let Some(ev) = event_rx.blocking_recv() {
            if worker_state.discarding() {
                continue; // drained and dropped, never delivered
            }
            worker_state.in_callback.store(true, Ordering::SeqCst);
            let action = deliver(&cb, id, ev);
            worker_state.in_callback.store(false, Ordering::SeqCst);

            match action {
                Some(a) if a == HYPER4K_CHUNK_CANCEL => {
                    worker_state.clear_permit();
                    worker_handle.settle(
                        Terminal {
                            kind: HYPER4K_ERR_CANCELLED,
                            protocol_code: 0,
                            message: "cancelled by callback".into(),
                        },
                        true,
                    );
                }
                Some(a) if a == HYPER4K_CHUNK_PAUSE => {
                    // Pause means "this chunk is consumed, hold the next one".
                    // Nothing is replayed on resume.
                    if !worker_state.park() {
                        continue;
                    }
                }
                _ => {
                    // CONTINUE, or a headers action: a permit taken during this
                    // callback does not survive it.
                    worker_state.clear_permit();
                }
            }
        }
        let terminal = done_rx.blocking_recv().unwrap_or(Terminal {
            kind: HYPER4K_ERR_CANCELLED,
            protocol_code: 0,
            message: "client dropped".into(),
        });
        deliver_done(&cb, id, &terminal);
        // The handle is removed only after OnDone has returned, so user_data
        // stays alive for the whole callback.
        on_finished();
        worker_state.finished.notify_waiters();
    });

    (handle, sink, worker)
}

fn deliver(cb: &Callbacks, id: u64, ev: Event) -> Option<Hyper4kChunkAction> {
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
            // Map the headers action onto the chunk action space so the worker
            // has one place to react. PAUSE has no meaning at this stage.
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

fn deliver_done(cb: &Callbacks, id: u64, t: &Terminal) {
    if t.kind == HYPER4K_ERR_NONE {
        (cb.on_done)(cb.user_data.0, id, std::ptr::null());
    } else {
        let err = Hyper4kError {
            kind: t.kind,
            protocol_code: t.protocol_code,
            message: crate::Hyper4kSlice {
                ptr: t.message.as_ptr(),
                len: t.message.len(),
            },
        };
        (cb.on_done)(cb.user_data.0, id, &err);
    }
}
