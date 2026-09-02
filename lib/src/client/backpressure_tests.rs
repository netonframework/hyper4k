//! Backpressure: pause, resume, permit ownership and lost wakeups.
//!
//! These are the tests that would catch a stream parking forever, which only
//! shows up under load and is close to undiagnosable after the fact. Every wait
//! here is bounded so a hang fails the test instead of blocking CI.

use super::handle::*;
use super::*;
use crate::abi::*;
use std::ffi::c_void;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// What a chunk callback should do, decided per chunk index.
#[derive(Clone, Copy, PartialEq)]
pub enum Plan {
    Continue,
    Pause,
    /// Call `resume` from inside the callback, then return PAUSE. Without a
    /// permit this deadlocks.
    ResumeThenPause,
    /// Call `resume` from inside the callback, then return CONTINUE. The permit
    /// must be discarded, not carried into a later pause.
    ResumeThenContinue,
    Cancel,
}

pub struct Ctl {
    pub chunks: Mutex<Vec<Vec<u8>>>,
    pub done: Mutex<Option<i32>>,
    pub done_calls: AtomicU32,
    pub plan: Mutex<Vec<Plan>>,
    pub client: AtomicU64,
    pub request_id: AtomicU64,
}

impl Default for Ctl {
    fn default() -> Self {
        Ctl {
            chunks: Mutex::new(Vec::new()),
            done: Mutex::new(None),
            done_calls: AtomicU32::new(0),
            plan: Mutex::new(Vec::new()),
            client: AtomicU64::new(0),
            request_id: AtomicU64::new(0),
        }
    }
}

impl Ctl {
    fn plan_for(&self, idx: usize) -> Plan {
        self.plan
            .lock()
            .unwrap()
            .get(idx)
            .copied()
            .unwrap_or(Plan::Continue)
    }
    pub fn chunk_count(&self) -> usize {
        self.chunks.lock().unwrap().len()
    }
}

extern "C" fn on_headers(
    _ud: *mut c_void,
    _id: u64,
    _status: u16,
    _version: u8,
    _h: *const Hyper4kHeader,
    _n: usize,
) -> Hyper4kHeadersAction {
    HYPER4K_HEADERS_CONTINUE
}

extern "C" fn on_chunk(ud: *mut c_void, id: u64, ptr: *const u8, len: usize) -> Hyper4kChunkAction {
    let ctl = unsafe { &*(ud as *const Ctl) };
    let idx = {
        let mut c = ctl.chunks.lock().unwrap();
        c.push(unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec());
        c.len() - 1
    };
    match ctl.plan_for(idx) {
        Plan::Continue => HYPER4K_CHUNK_CONTINUE,
        Plan::Pause => HYPER4K_CHUNK_PAUSE,
        Plan::Cancel => HYPER4K_CHUNK_CANCEL,
        Plan::ResumeThenPause => {
            let c = ctl.client.load(Ordering::SeqCst) as *mut Hyper4kClient;
            unsafe { hyper4k_client_resume(c, id) };
            HYPER4K_CHUNK_PAUSE
        }
        Plan::ResumeThenContinue => {
            let c = ctl.client.load(Ordering::SeqCst) as *mut Hyper4kClient;
            unsafe { hyper4k_client_resume(c, id) };
            HYPER4K_CHUNK_CONTINUE
        }
    }
}

extern "C" fn on_done(ud: *mut c_void, _id: u64, error: *const Hyper4kError) {
    let ctl = unsafe { &*(ud as *const Ctl) };
    ctl.done_calls.fetch_add(1, Ordering::SeqCst);
    *ctl.done.lock().unwrap() = Some(if error.is_null() {
        -999
    } else {
        unsafe { (*error).kind }
    });
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for: {what}");
}

/// Server sending each part as its own chunk.
async fn chunked_server(parts: &'static [&'static [u8]]) -> SocketAddr {
    use http_body_util::StreamBody;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(sock);
                let svc = hyper::service::service_fn(move |_r| async move {
                    let stream = futures_lite_stream(parts);
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(StreamBody::new(stream)))
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    addr
}

fn futures_lite_stream(
    parts: &'static [&'static [u8]],
) -> impl futures_core::Stream<Item = Result<hyper::body::Frame<bytes::Bytes>, std::convert::Infallible>>
{
    async_stream_of(parts)
}

fn async_stream_of(
    parts: &'static [&'static [u8]],
) -> tokio_stream::wrappers::ReceiverStream<
    Result<hyper::body::Frame<bytes::Bytes>, std::convert::Infallible>,
> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        for p in parts {
            // A small gap keeps the parts in separate frames.
            tokio::time::sleep(Duration::from_millis(10)).await;
            if tx
                .send(Ok(hyper::body::Frame::data(bytes::Bytes::from_static(p))))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

fn new_client() -> *mut Hyper4kClient {
    let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
    };
    let mut c = std::ptr::null_mut();
    assert_eq!(unsafe { hyper4k_client_new(&o, &mut c) }, HYPER4K_STATUS_OK);
    c
}

fn start(client: *mut Hyper4kClient, url: &str, ctl: &Arc<Ctl>) -> u64 {
    ctl.client.store(client as u64, Ordering::SeqCst);
    let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_request_init(&mut r, std::mem::size_of::<Hyper4kClientRequest>() as u32)
    };
    let m = b"GET";
    r.method = crate::Hyper4kSlice {
        ptr: m.as_ptr(),
        len: m.len(),
    };
    r.url = crate::Hyper4kSlice {
        ptr: url.as_ptr(),
        len: url.len(),
    };
    let mut id = 0u64;
    let st = unsafe {
        hyper4k_client_send(
            client,
            &r,
            Some(on_headers),
            Some(on_chunk),
            Some(on_done),
            Arc::as_ptr(ctl) as *mut c_void,
            &mut id,
        )
    };
    assert_eq!(st, HYPER4K_STATUS_OK);
    ctl.request_id.store(id, Ordering::SeqCst);
    id
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

const PARTS: &[&[u8]] = &[b"aaa", b"bbb", b"ccc"];

// --- tests -----------------------------------------------------------------

#[test]
fn pause_stops_delivery_until_resume_and_never_repeats_a_chunk() {
    let r = rt();
    let addr = r.block_on(chunked_server(PARTS));
    let ctl = Arc::new(Ctl::default());
    *ctl.plan.lock().unwrap() = vec![Plan::Pause];
    let client = new_client();
    let url = format!("http://{addr}/x");
    let id = start(client, &url, &ctl);

    wait_until("first chunk", || ctl.chunk_count() >= 1);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(ctl.chunk_count(), 1, "delivery continued while paused");

    assert_eq!(
        unsafe { hyper4k_client_resume(client, id) },
        HYPER4K_STATUS_OK
    );
    wait_until("all chunks", || ctl.chunk_count() == 3);
    let got = ctl.chunks.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![b"aaa".to_vec(), b"bbb".to_vec(), b"ccc".to_vec()],
        "resume must not replay the paused chunk"
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn resume_arriving_before_pause_lands_is_not_lost() {
    // The callback resumes itself and *then* returns PAUSE. Without a permit
    // the wakeup is lost and this hangs until the 5s deadline.
    let r = rt();
    let addr = r.block_on(chunked_server(PARTS));
    let ctl = Arc::new(Ctl::default());
    *ctl.plan.lock().unwrap() = vec![Plan::ResumeThenPause, Plan::ResumeThenPause];
    let client = new_client();
    let url = format!("http://{addr}/x");
    start(client, &url, &ctl);

    wait_until("all chunks despite self-resume", || ctl.chunk_count() == 3);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_permit_does_not_leak_into_a_later_pause() {
    // Chunk 1 resumes but returns CONTINUE, so its permit must be discarded.
    // Chunk 2 pauses for real and must stay paused.
    let r = rt();
    let addr = r.block_on(chunked_server(PARTS));
    let ctl = Arc::new(Ctl::default());
    *ctl.plan.lock().unwrap() = vec![Plan::ResumeThenContinue, Plan::Pause];
    let client = new_client();
    let url = format!("http://{addr}/x");
    let id = start(client, &url, &ctl);

    wait_until("two chunks", || ctl.chunk_count() >= 2);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        ctl.chunk_count(),
        2,
        "a stale permit released a later pause"
    );
    assert_eq!(
        unsafe { hyper4k_client_resume(client, id) },
        HYPER4K_STATUS_OK
    );
    wait_until("all chunks", || ctl.chunk_count() == 3);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn resume_on_a_running_request_that_is_not_paused_reports_not_paused() {
    let r = rt();
    let addr = r.block_on(chunked_server(PARTS));
    let ctl = Arc::new(Ctl::default());
    let client = new_client();
    let url = format!("http://{addr}/x");
    let id = start(client, &url, &ctl);
    // Immediately: nothing is paused and no chunk callback is running.
    let st = unsafe { hyper4k_client_resume(client, id) };
    assert!(
        st == HYPER4K_STATUS_NOT_PAUSED || st == HYPER4K_STATUS_ALREADY_DONE,
        "unexpected status {st}"
    );
    assert_eq!(
        unsafe { hyper4k_client_resume(client, 999_999) },
        HYPER4K_STATUS_NOT_FOUND
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn chunk_cancel_terminates_with_cancelled() {
    let r = rt();
    let addr = r.block_on(chunked_server(PARTS));
    let ctl = Arc::new(Ctl::default());
    *ctl.plan.lock().unwrap() = vec![Plan::Cancel];
    let client = new_client();
    let url = format!("http://{addr}/x");
    start(client, &url, &ctl);

    wait_until("done", || ctl.done.lock().unwrap().is_some());
    assert_eq!(*ctl.done.lock().unwrap(), Some(HYPER4K_ERR_CANCELLED));
    assert_eq!(ctl.done_calls.load(Ordering::SeqCst), 1);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn close_releases_paused_requests_so_free_returns() {
    // A consumer that pauses and walks away must not be able to block shutdown.
    let r = rt();
    let addr = r.block_on(chunked_server(PARTS));
    let ctl = Arc::new(Ctl::default());
    *ctl.plan.lock().unwrap() = vec![Plan::Pause];
    let client = new_client();
    let url = format!("http://{addr}/x");
    start(client, &url, &ctl);

    wait_until("paused", || ctl.chunk_count() >= 1);
    unsafe { hyper4k_client_close(client) };
    wait_until("done after close", || ctl.done.lock().unwrap().is_some());
    assert_eq!(*ctl.done.lock().unwrap(), Some(HYPER4K_ERR_CANCELLED));

    let started = Instant::now();
    unsafe { hyper4k_client_free(client) };
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "free() blocked on a paused request"
    );
}

#[test]
fn repeated_pause_and_resume_does_not_deadlock() {
    // Every chunk pauses; the driver resumes each time. This is the shape that
    // would expose a lost wakeup under load.
    let r = rt();
    let addr = r.block_on(chunked_server(PARTS));
    let ctl = Arc::new(Ctl::default());
    *ctl.plan.lock().unwrap() = vec![Plan::Pause, Plan::Pause, Plan::Pause];
    let client = new_client();
    let url = format!("http://{addr}/x");
    let id = start(client, &url, &ctl);

    for expect in 1..=3usize {
        wait_until(&format!("chunk {expect}"), || ctl.chunk_count() >= expect);
        if expect < 3 {
            wait_until("resume accepted", || unsafe {
                hyper4k_client_resume(client, id) == HYPER4K_STATUS_OK
            });
        }
    }
    assert_eq!(ctl.chunk_count(), 3);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_paused_request_consumes_its_connection_paused_budget() {
    // Without this the reservation invariant in spec §2.5 is decoration: the
    // pool would keep handing new streams to a connection whose streams are all
    // parked, and the connection-level window would starve the live ones.
    // clippy caught the gap first — PauseGuard was never constructed outside
    // the pool's own tests.
    let r = rt();
    let addr = r.block_on(chunked_server(PARTS));
    let ctl = Arc::new(Ctl::default());
    *ctl.plan.lock().unwrap() = vec![Plan::Pause];
    let client = new_client();
    let url = format!("http://{addr}/x");
    let id = start(client, &url, &ctl);

    wait_until("paused", || ctl.chunk_count() >= 1);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        unsafe { hyper4k_client_paused_stream_count(client) },
        1,
        "a parked stream did not take a slot of its connection's budget"
    );

    assert_eq!(
        unsafe { hyper4k_client_resume(client, id) },
        HYPER4K_STATUS_OK
    );
    wait_until("all chunks", || ctl.chunk_count() == 3);
    wait_until("budget returned", || unsafe {
        hyper4k_client_paused_stream_count(client) == 0
    });
    unsafe { hyper4k_client_free(client) };
}
