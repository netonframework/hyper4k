//! Lifecycle, callback-ordering and terminal-gate tests.

use super::bridge::{OnChunk, OnDone, OnHeaders};
use super::handle::*;
use super::*;
use crate::abi::*;
use std::ffi::c_void;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// --- capture ---------------------------------------------------------------

#[derive(Default)]
pub struct Capture {
    pub status: AtomicU32,
    pub version: AtomicU32,
    pub body: Mutex<Vec<u8>>,
    pub headers_calls: AtomicU32,
    pub done_calls: AtomicU32,
    /// `Some(-999)` marks success so a plain integer can carry both outcomes.
    pub done: Mutex<Option<i32>>,
    pub events_after_done: AtomicU32,
    pub forbidden_thread: Mutex<Option<std::thread::ThreadId>>,
    pub reentered: AtomicU32,
}

impl Capture {
    fn note_thread(&self) {
        if let Some(t) = *self.forbidden_thread.lock().unwrap() {
            if std::thread::current().id() == t {
                self.reentered.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    fn note_event(&self) {
        if self.done.lock().unwrap().is_some() {
            self.events_after_done.fetch_add(1, Ordering::SeqCst);
        }
    }
}

extern "C" fn on_headers(
    ud: *mut c_void,
    _id: u64,
    status: u16,
    version: u8,
    _h: *const Hyper4kHeader,
    _n: usize,
) -> Hyper4kHeadersAction {
    let cap = unsafe { &*(ud as *const Capture) };
    cap.note_thread();
    cap.note_event();
    cap.status.store(status as u32, Ordering::SeqCst);
    cap.version.store(version as u32, Ordering::SeqCst);
    cap.headers_calls.fetch_add(1, Ordering::SeqCst);
    HYPER4K_HEADERS_CONTINUE
}

extern "C" fn on_chunk(
    ud: *mut c_void,
    _id: u64,
    ptr: *const u8,
    len: usize,
) -> Hyper4kChunkAction {
    let cap = unsafe { &*(ud as *const Capture) };
    cap.note_thread();
    cap.note_event();
    cap.body
        .lock()
        .unwrap()
        .extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, len) });
    HYPER4K_CHUNK_CONTINUE
}

extern "C" fn on_done(ud: *mut c_void, _id: u64, error: *const Hyper4kError) {
    let cap = unsafe { &*(ud as *const Capture) };
    cap.note_thread();
    cap.done_calls.fetch_add(1, Ordering::SeqCst);
    *cap.done.lock().unwrap() = Some(if error.is_null() {
        -999
    } else {
        unsafe { (*error).kind }
    });
}

// --- helpers ---------------------------------------------------------------

fn wait_until(mut f: impl FnMut() -> bool) {
    // Bounded so a hang fails the test instead of blocking CI.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("condition not met within 5s");
}

fn slice_of(b: &[u8]) -> crate::Hyper4kSlice {
    crate::Hyper4kSlice {
        ptr: b.as_ptr(),
        len: b.len(),
    }
}

pub fn new_client_with_ca(ca_pem: &str, flags: u64) -> *mut Hyper4kClient {
    let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
    };
    o.flags = flags;
    o.custom_ca_pem = ca_pem.as_ptr();
    o.custom_ca_pem_len = ca_pem.len();
    let mut c = std::ptr::null_mut();
    assert_eq!(unsafe { hyper4k_client_new(&o, &mut c) }, HYPER4K_STATUS_OK);
    c
}

/// Send one request and block until it settles.
pub fn send_and_wait(client: *mut Hyper4kClient, url: &str) -> Arc<Capture> {
    let cap = Arc::new(Capture::default());
    let (st, _) = send(client, url, &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_OK, "submission failed for {url}");
    wait_until(|| cap.done.lock().unwrap().is_some());
    cap
}

fn new_client(flags: u64) -> *mut Hyper4kClient {
    let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
    };
    o.flags = flags;
    let mut c = std::ptr::null_mut();
    assert_eq!(unsafe { hyper4k_client_new(&o, &mut c) }, HYPER4K_STATUS_OK);
    c
}

fn send(
    client: *mut Hyper4kClient,
    url: &str,
    cap: &Arc<Capture>,
    chunk: Option<OnChunk>,
) -> (Hyper4kStatus, u64) {
    let url_owned = url.to_string();
    let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_request_init(&mut r, std::mem::size_of::<Hyper4kClientRequest>() as u32)
    };
    r.method = slice_of(b"GET");
    r.url = slice_of(url_owned.as_bytes());
    let mut id = 0u64;
    let st = unsafe {
        hyper4k_client_send(
            client,
            &r,
            Some(on_headers),
            chunk,
            Some(on_done),
            Arc::as_ptr(cap) as *mut c_void,
            &mut id,
        )
    };
    (st, id)
}

// --- peers -----------------------------------------------------------------

async fn echo_server(body: &'static [u8]) -> SocketAddr {
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
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(
                        http_body_util::Full::new(bytes::Bytes::from_static(body)),
                    ))
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    addr
}

/// Accepts, then never answers. Used to hold a request in flight.
async fn silent_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            held.push(sock); // keep the socket open, answer nothing
        }
    });
    addr
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

// --- tests -----------------------------------------------------------------

#[test]
fn plaintext_get_delivers_headers_body_and_done_once() {
    let r = rt();
    let addr = r.block_on(echo_server(b"pong"));
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    let (st, id) = send(client, &format!("http://{addr}/ping"), &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_OK);
    assert_ne!(id, 0);

    wait_until(|| cap.done.lock().unwrap().is_some());
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    assert_eq!(cap.status.load(Ordering::SeqCst), 200);
    assert_eq!(cap.version.load(Ordering::SeqCst), 1);
    assert_eq!(&*cap.body.lock().unwrap(), b"pong");
    assert_eq!(cap.done_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        cap.events_after_done.load(Ordering::SeqCst),
        0,
        "event after OnDone"
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_normal_completion_drains_the_queued_body_before_done() {
    // Teeth for DrainThenDone: the response is fully queued before the request
    // settles. Discarding it would leave the caller with an empty 200.
    let r = rt();
    let addr = r.block_on(echo_server(b"abcdef"));
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    let (st, _) = send(client, &format!("http://{addr}/x"), &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_OK);
    wait_until(|| cap.done.lock().unwrap().is_some());
    assert_eq!(
        cap.headers_calls.load(Ordering::SeqCst),
        1,
        "queued headers dropped on success"
    );
    assert_eq!(
        &*cap.body.lock().unwrap(),
        b"abcdef",
        "queued body dropped on success"
    );
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn null_on_chunk_discards_the_body_but_still_completes() {
    let r = rt();
    let addr = r.block_on(echo_server(b"pong"));
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    let (st, _) = send(client, &format!("http://{addr}/ping"), &cap, None);
    assert_eq!(st, HYPER4K_STATUS_OK);
    wait_until(|| cap.done.lock().unwrap().is_some());
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    assert!(cap.body.lock().unwrap().is_empty());
    assert_eq!(cap.headers_calls.load(Ordering::SeqCst), 1);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn send_after_close_is_rejected_synchronously_without_callbacks() {
    let r = rt();
    let addr = r.block_on(echo_server(b"pong"));
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    unsafe { hyper4k_client_close(client) };
    let (st, _) = send(client, &format!("http://{addr}/ping"), &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_CLIENT_CLOSED);
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        cap.done.lock().unwrap().is_none(),
        "refused request produced a callback"
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn close_drives_inflight_requests_to_exactly_one_done() {
    let r = rt();
    let addr = r.block_on(silent_server());
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    let (st, _) = send(client, &format!("http://{addr}/slow"), &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_OK);
    std::thread::sleep(Duration::from_millis(50));
    unsafe { hyper4k_client_close(client) };
    wait_until(|| cap.done.lock().unwrap().is_some());
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_CANCELLED));
    assert_eq!(cap.done_calls.load(Ordering::SeqCst), 1);
    unsafe { hyper4k_client_free(client) }; // must return, not hang
}

#[test]
fn cancel_reports_three_distinct_states() {
    let r = rt();
    let addr = r.block_on(silent_server());
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    let (st, id) = send(client, &format!("http://{addr}/slow"), &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_OK);
    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(
        unsafe { hyper4k_client_cancel(client, id) },
        HYPER4K_STATUS_OK
    );
    wait_until(|| cap.done.lock().unwrap().is_some());
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_CANCELLED));
    // Cancelling again is ALREADY_DONE while the handle lives, NOT_FOUND after
    // it is reaped. Both are legal; anything else is not.
    let second = unsafe { hyper4k_client_cancel(client, id) };
    assert!(
        second == HYPER4K_STATUS_ALREADY_DONE || second == HYPER4K_STATUS_NOT_FOUND,
        "unexpected status {second}"
    );
    assert_eq!(
        unsafe { hyper4k_client_cancel(client, 999_999) },
        HYPER4K_STATUS_NOT_FOUND
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn cancelling_with_a_backlog_discards_it_and_reports_once() {
    // Teeth for DiscardThenDone. The response is already queued when cancel
    // lands, so a wrong mode would flush stale headers to the caller.
    let r = rt();
    let addr = r.block_on(echo_server(b"stale-body"));
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    let (st, id) = send(client, &format!("http://{addr}/x"), &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_OK);
    // Cancel immediately: the race is the point, and either outcome is a valid
    // observation as long as the invariants below hold.
    let _ = unsafe { hyper4k_client_cancel(client, id) };
    wait_until(|| cap.done.lock().unwrap().is_some());

    assert_eq!(
        cap.done_calls.load(Ordering::SeqCst),
        1,
        "OnDone fired more than once"
    );
    assert_eq!(
        cap.events_after_done.load(Ordering::SeqCst),
        0,
        "an event was delivered after OnDone"
    );
    if *cap.done.lock().unwrap() == Some(HYPER4K_ERR_CANCELLED) {
        assert_eq!(
            cap.headers_calls.load(Ordering::SeqCst),
            0,
            "stale headers delivered after a cancel won the race"
        );
        assert!(cap.body.lock().unwrap().is_empty(), "stale body delivered");
    }
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_callback_never_re_enters_the_send_thread() {
    // Spec §2.6 contract point 2.
    let r = rt();
    let addr = r.block_on(echo_server(b"pong"));
    let cap = Arc::new(Capture::default());
    *cap.forbidden_thread.lock().unwrap() = Some(std::thread::current().id());
    let client = new_client(0);
    let (st, _) = send(client, &format!("http://{addr}/ping"), &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_OK);
    wait_until(|| cap.done.lock().unwrap().is_some());
    assert_eq!(
        cap.reentered.load(Ordering::SeqCst),
        0,
        "a callback ran synchronously on the send() thread"
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn send_racing_close_yields_either_a_refusal_or_exactly_one_done() {
    // The frozen either/or from spec §2.3, hammered rather than assumed.
    let r = rt();
    let addr = r.block_on(echo_server(b"pong"));
    for _ in 0..50 {
        let cap = Arc::new(Capture::default());
        let client = new_client(0);
        let c2 = client as usize;
        let closer = std::thread::spawn(move || {
            unsafe { hyper4k_client_close(c2 as *mut Hyper4kClient) };
        });
        let (st, _) = send(client, &format!("http://{addr}/ping"), &cap, Some(on_chunk));
        closer.join().unwrap();

        if st == HYPER4K_STATUS_OK {
            wait_until(|| cap.done.lock().unwrap().is_some());
            assert_eq!(
                cap.done_calls.load(Ordering::SeqCst),
                1,
                "an accepted request must settle exactly once"
            );
        } else {
            assert_eq!(st, HYPER4K_STATUS_CLIENT_CLOSED);
            std::thread::sleep(Duration::from_millis(20));
            assert!(
                cap.done.lock().unwrap().is_none(),
                "a refused request must produce no callback"
            );
        }
        unsafe { hyper4k_client_free(client) };
    }
}

#[test]
fn http_scheme_with_http2_required_is_refused_at_submit() {
    let cap = Arc::new(Capture::default());
    let client = new_client(HYPER4K_CLIENT_HTTP2_REQUIRED);
    let (st, _) = send(client, "http://127.0.0.1:1/x", &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_UNSUPPORTED);
    assert!(cap.done.lock().unwrap().is_none());
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn an_unreachable_peer_reports_connect_failure_once() {
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    // Port 1 on loopback: refused immediately, no DNS involved.
    let (st, _) = send(client, "http://127.0.0.1:1/x", &cap, Some(on_chunk));
    assert_eq!(st, HYPER4K_STATUS_OK, "submission itself is fine");
    wait_until(|| cap.done.lock().unwrap().is_some());
    assert_ne!(*cap.done.lock().unwrap(), Some(-999));
    assert_eq!(cap.done_calls.load(Ordering::SeqCst), 1);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn an_illegal_header_is_rejected_instead_of_aborting_the_process() {
    // The crate is built with panic = "abort", so a builder `expect` on a bad
    // header name would kill the host process rather than return a status.
    // A caller must never be able to do that to us.
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    let url = "http://127.0.0.1:1/x";
    let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_request_init(&mut r, std::mem::size_of::<Hyper4kClientRequest>() as u32)
    };
    r.method = slice_of(b"GET");
    r.url = slice_of(url.as_bytes());
    // A space is not legal in a header name.
    let bad_name = b"in valid";
    let value = b"x";
    let hdr = [crate::abi::Hyper4kHeader {
        name: slice_of(bad_name),
        value: slice_of(value),
    }];
    r.headers = hdr.as_ptr();
    r.header_count = 1;

    let mut id = 0u64;
    let st = unsafe {
        hyper4k_client_send(
            client,
            &r,
            Some(on_headers),
            Some(on_chunk),
            Some(on_done),
            Arc::as_ptr(&cap) as *mut c_void,
            &mut id,
        )
    };
    assert_eq!(st, HYPER4K_STATUS_INVALID_ARG);
    assert!(cap.done.lock().unwrap().is_none());
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn an_illegal_header_value_is_also_rejected() {
    let cap = Arc::new(Capture::default());
    let client = new_client(0);
    let url = "http://127.0.0.1:1/x";
    let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_request_init(&mut r, std::mem::size_of::<Hyper4kClientRequest>() as u32)
    };
    r.method = slice_of(b"GET");
    r.url = slice_of(url.as_bytes());
    let hdr = [crate::abi::Hyper4kHeader {
        name: slice_of(b"x-test"),
        value: slice_of(b"bad\nvalue"), // a newline would split the message
    }];
    r.headers = hdr.as_ptr();
    r.header_count = 1;
    let mut id = 0u64;
    let st = unsafe {
        hyper4k_client_send(
            client,
            &r,
            Some(on_headers),
            Some(on_chunk),
            Some(on_done),
            Arc::as_ptr(&cap) as *mut c_void,
            &mut id,
        )
    };
    assert_eq!(st, HYPER4K_STATUS_INVALID_ARG);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_short_caller_struct_is_not_read_past_its_allocation() {
    // A caller built against an older, smaller struct. Reading their buffer as
    // a full one would run off the end of their allocation.
    let prefix = OPTIONS_MIN_SIZE as usize;
    let layout =
        std::alloc::Layout::from_size_align(prefix, std::mem::align_of::<Hyper4kClientOptions>())
            .unwrap();
    let base = unsafe { std::alloc::alloc_zeroed(layout) };
    assert!(!base.is_null());
    unsafe {
        std::ptr::write_unaligned(base as *mut u32, hyper4k_abi_version());
        std::ptr::write_unaligned(base.add(4) as *mut u32, OPTIONS_MIN_SIZE);
    }
    let mut client = std::ptr::null_mut();
    let st = unsafe { hyper4k_client_new(base as *const Hyper4kClientOptions, &mut client) };
    assert_eq!(
        st, HYPER4K_STATUS_OK,
        "a minimal-prefix caller was rejected"
    );
    unsafe {
        hyper4k_client_free(client);
        std::alloc::dealloc(base, layout);
    }
}

#[test]
fn a_bad_ca_bundle_fails_at_construction_not_on_first_request() {
    let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
    };
    let junk = b"not a certificate at all";
    o.custom_ca_pem = junk.as_ptr();
    o.custom_ca_pem_len = junk.len();
    let mut client = std::ptr::null_mut();
    assert_eq!(
        unsafe { hyper4k_client_new(&o, &mut client) },
        HYPER4K_STATUS_INVALID_ARG,
        "a bad CA bundle was accepted and deferred to the first request"
    );
}

#[test]
fn null_required_callbacks_are_rejected() {
    // Taken as Option precisely so a C NULL is a value we can inspect. With a
    // non-nullable fn pointer the parameter would already be an invalid Rust
    // value before any check could run.
    let client = new_client(0);
    let url = "http://127.0.0.1:1/x";
    let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_request_init(&mut r, std::mem::size_of::<Hyper4kClientRequest>() as u32)
    };
    r.method = slice_of(b"GET");
    r.url = slice_of(url.as_bytes());
    let mut id = 0u64;

    let no_headers = unsafe {
        hyper4k_client_send(
            client,
            &r,
            None,
            Some(on_chunk),
            Some(on_done),
            std::ptr::null_mut(),
            &mut id,
        )
    };
    assert_eq!(no_headers, HYPER4K_STATUS_INVALID_ARG);

    let no_done = unsafe {
        hyper4k_client_send(
            client,
            &r,
            Some(on_headers),
            Some(on_chunk),
            None,
            std::ptr::null_mut(),
            &mut id,
        )
    };
    assert_eq!(no_done, HYPER4K_STATUS_INVALID_ARG);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn free_waits_for_a_slow_callback_instead_of_racing_it() {
    // The whole point of a deterministic wait: a callback that outlives free()
    // would touch a user_data the caller is entitled to have reclaimed.
    //
    // Caveat, verified by injecting the bug: this test alone does NOT prove
    // wait_zero is what provides the guarantee. Dropping the tokio runtime also
    // joins blocking tasks, so replacing wait_zero with a short sleep still
    // passes here. BridgeCounter is therefore unit-tested directly in
    // bridge::counter_tests; this case covers the end-to-end property.
    use std::sync::atomic::AtomicBool;
    static IN_CALLBACK: AtomicBool = AtomicBool::new(false);
    static CALLBACK_FINISHED: AtomicBool = AtomicBool::new(false);

    extern "C" fn slow_done(_ud: *mut c_void, _id: u64, _e: *const Hyper4kError) {
        IN_CALLBACK.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(400));
        CALLBACK_FINISHED.store(true, Ordering::SeqCst);
    }

    let r = rt();
    let addr = r.block_on(echo_server(b"pong"));
    let client = new_client(0);
    let cap = Arc::new(Capture::default());

    let url = format!("http://{addr}/ping");
    let mut req: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_request_init(&mut req, std::mem::size_of::<Hyper4kClientRequest>() as u32)
    };
    req.method = slice_of(b"GET");
    req.url = slice_of(url.as_bytes());
    let mut id = 0u64;
    assert_eq!(
        unsafe {
            hyper4k_client_send(
                client,
                &req,
                Some(on_headers),
                Some(on_chunk),
                Some(slow_done),
                Arc::as_ptr(&cap) as *mut c_void,
                &mut id,
            )
        },
        HYPER4K_STATUS_OK
    );

    wait_until(|| IN_CALLBACK.load(Ordering::SeqCst));
    unsafe { hyper4k_client_free(client) };
    assert!(
        CALLBACK_FINISHED.load(Ordering::SeqCst),
        "free() returned while a callback was still running"
    );
}
