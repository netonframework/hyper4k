//! Retry and timeout behaviour against real peers.
//!
//! `retry.rs` unit-tests the decision table; these prove the decision is wired
//! to the wire.

use super::handle::*;
use super::handle_tests::{new_client_with_ca, Capture};
use super::*;
use crate::abi::*;
use std::ffi::c_void;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for: {what}");
}

struct Peer {
    addr: SocketAddr,
    requests: Arc<AtomicU32>,
}

impl Peer {
    fn request_count(&self) -> u32 {
        self.requests.load(Ordering::SeqCst)
    }
}

/// Accepts, reads the request, then drops without answering. Every attempt
/// therefore reaches the wire — the not-provably-unsent case.
async fn read_then_drop() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicU32::new(0));
    let counter = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let counter = counter.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                if let Ok(n) = sock.read(&mut buf).await {
                    if n > 0 {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
                drop(sock);
            });
        }
    });
    Peer { addr, requests }
}

/// Reads the request, waits, then drops. The delay makes each attempt cost
/// real time, which is what lets a total deadline be told apart from one that
/// is re-armed per attempt.
async fn slow_read_then_drop(delay: Duration) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicU32::new(0));
    let counter = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let counter = counter.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                if let Ok(n) = sock.read(&mut buf).await {
                    if n > 0 {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
                tokio::time::sleep(delay).await;
                drop(sock);
            });
        }
    });
    Peer { addr, requests }
}

/// Answers the first request, then closes; later attempts hit a dead
/// connection before anything is written — the provably-unsent case.
async fn answer_then_close(body: &'static str) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicU32::new(0));
    let counter = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let counter = counter.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                counter.fetch_add(1, Ordering::SeqCst);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    Peer { addr, requests }
}

/// Sends headers, one chunk, then stalls forever.
async fn stall_after_first_chunk() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicU32::new(0));
    let counter = requests.clone();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n")
                .await;
            let _ = sock.flush().await;
            held.push(sock); // never finishes the body
        }
    });
    Peer { addr, requests }
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
    cap.body
        .lock()
        .unwrap()
        .extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, len) });
    HYPER4K_CHUNK_CONTINUE
}

extern "C" fn on_done(ud: *mut c_void, _id: u64, error: *const Hyper4kError) {
    let cap = unsafe { &*(ud as *const Capture) };
    cap.done_calls.fetch_add(1, Ordering::SeqCst);
    *cap.done.lock().unwrap() = Some(if error.is_null() {
        -999
    } else {
        unsafe { (*error).kind }
    });
}

struct Cfg {
    max_retries: u32,
    request_timeout_ms: u64,
    read_idle_timeout_ms: u64,
}

impl Default for Cfg {
    fn default() -> Self {
        Cfg {
            max_retries: 2,
            request_timeout_ms: 30_000,
            read_idle_timeout_ms: 0,
        }
    }
}

fn client_with(cfg: Cfg) -> *mut Hyper4kClient {
    let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
    };
    o.max_retries = cfg.max_retries;
    o.request_timeout_ms = cfg.request_timeout_ms;
    o.read_idle_timeout_ms = cfg.read_idle_timeout_ms;
    let mut c = std::ptr::null_mut();
    assert_eq!(unsafe { hyper4k_client_new(&o, &mut c) }, HYPER4K_STATUS_OK);
    c
}

fn send_method(
    client: *mut Hyper4kClient,
    method: &str,
    url: &str,
    cap: &Arc<Capture>,
) -> Hyper4kStatus {
    let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_request_init(&mut r, std::mem::size_of::<Hyper4kClientRequest>() as u32)
    };
    r.method = crate::Hyper4kSlice {
        ptr: method.as_ptr(),
        len: method.len(),
    };
    r.url = crate::Hyper4kSlice {
        ptr: url.as_ptr(),
        len: url.len(),
    };
    let mut id = 0u64;
    unsafe {
        hyper4k_client_send(
            client,
            &r,
            Some(on_headers),
            Some(on_chunk),
            Some(on_done),
            Arc::as_ptr(cap) as *mut c_void,
            &mut id,
        )
    }
}

// --- tests -----------------------------------------------------------------

#[test]
fn an_inflight_post_that_may_have_run_is_not_retried() {
    // The peer reads the request, so it may well have acted on it. Replaying a
    // POST here is how duplicate payments happen.
    let r = rt();
    let peer = r.block_on(read_then_drop());
    let cap = Arc::new(Capture::default());
    let client = client_with(Cfg {
        max_retries: 3,
        ..Default::default()
    });
    let url = format!("http://{}/x", peer.addr);
    assert_eq!(send_method(client, "POST", &url, &cap), HYPER4K_STATUS_OK);

    wait_until("done", || cap.done.lock().unwrap().is_some());
    assert_eq!(
        *cap.done.lock().unwrap(),
        Some(HYPER4K_ERR_OUTCOME_UNKNOWN),
        "a POST that may have run must report an unknown outcome"
    );
    assert_eq!(peer.request_count(), 1, "the POST was replayed");
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn an_inflight_get_is_retried_within_the_budget() {
    // Same peer, idempotent method: retrying is allowed, and the budget bounds
    // it. 1 attempt + 2 retries = 3 requests seen.
    let r = rt();
    let peer = r.block_on(read_then_drop());
    let cap = Arc::new(Capture::default());
    let client = client_with(Cfg {
        max_retries: 2,
        ..Default::default()
    });
    let url = format!("http://{}/x", peer.addr);
    assert_eq!(send_method(client, "GET", &url, &cap), HYPER4K_STATUS_OK);

    wait_until("done", || cap.done.lock().unwrap().is_some());
    assert_eq!(peer.request_count(), 3, "expected 1 attempt + 2 retries");
    assert_ne!(*cap.done.lock().unwrap(), Some(-999));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_retry_budget_of_zero_means_one_attempt() {
    let r = rt();
    let peer = r.block_on(read_then_drop());
    let cap = Arc::new(Capture::default());
    let client = client_with(Cfg {
        max_retries: 0,
        ..Default::default()
    });
    let url = format!("http://{}/x", peer.addr);
    assert_eq!(send_method(client, "GET", &url, &cap), HYPER4K_STATUS_OK);
    wait_until("done", || cap.done.lock().unwrap().is_some());
    assert_eq!(
        peer.request_count(),
        1,
        "budget 0 must mean a single attempt"
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_stale_pooled_connection_is_replaced_without_bothering_the_caller() {
    // The provably-unsent case: the peer closed the keep-alive connection, so
    // the second request never reaches the wire on it. The caller should see a
    // clean success, not a transport error.
    let r = rt();
    let peer = r.block_on(answer_then_close("hello"));
    let client = client_with(Cfg::default());
    let url = format!("http://{}/x", peer.addr);

    let first = Arc::new(Capture::default());
    assert_eq!(send_method(client, "GET", &url, &first), HYPER4K_STATUS_OK);
    wait_until("first done", || first.done.lock().unwrap().is_some());
    assert_eq!(*first.done.lock().unwrap(), Some(-999));

    std::thread::sleep(Duration::from_millis(150)); // let the close land

    let second = Arc::new(Capture::default());
    assert_eq!(send_method(client, "GET", &url, &second), HYPER4K_STATUS_OK);
    wait_until("second done", || second.done.lock().unwrap().is_some());
    assert_eq!(
        *second.done.lock().unwrap(),
        Some(-999),
        "a stale pooled connection surfaced as a caller-visible failure"
    );
    assert_eq!(&*second.body.lock().unwrap(), b"hello");
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn request_timeout_covers_all_retries() {
    // With a short total budget and a large retry budget, the deadline must
    // still fire once. A per-attempt reset would let this run for retries x
    // timeout instead.
    let r = rt();
    // Each attempt costs 200ms, so a 300ms total budget must expire during the
    // second attempt. A per-attempt reset would never fire at all.
    let peer = r.block_on(slow_read_then_drop(Duration::from_millis(200)));
    let cap = Arc::new(Capture::default());
    let client = client_with(Cfg {
        max_retries: 50,
        request_timeout_ms: 300,
        ..Default::default()
    });
    let url = format!("http://{}/x", peer.addr);
    let started = Instant::now();
    assert_eq!(send_method(client, "GET", &url, &cap), HYPER4K_STATUS_OK);
    wait_until("done", || cap.done.lock().unwrap().is_some());
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the total timeout was reset per attempt ({:?})",
        started.elapsed()
    );
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TIMEOUT));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn read_idle_timeout_fires_on_a_stalled_stream() {
    let r = rt();
    let peer = r.block_on(stall_after_first_chunk());
    let cap = Arc::new(Capture::default());
    let client = client_with(Cfg {
        request_timeout_ms: 0, // disabled: only the idle limit should act
        read_idle_timeout_ms: 200,
        ..Default::default()
    });
    let url = format!("http://{}/x", peer.addr);
    assert_eq!(send_method(client, "GET", &url, &cap), HYPER4K_STATUS_OK);
    wait_until("done", || cap.done.lock().unwrap().is_some());
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_IDLE_TIMEOUT));
    // The chunk that did arrive was still delivered.
    assert_eq!(&*cap.body.lock().unwrap(), b"abc");
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_healthy_stream_is_not_killed_by_the_idle_timeout() {
    // The positive counterpart: the same limit must not fire when data keeps
    // arriving, or the previous test would pass for the wrong reason.
    let r = rt();
    let peer = r.block_on(answer_then_close("hello"));
    let cap = Arc::new(Capture::default());
    let client = client_with(Cfg {
        request_timeout_ms: 0,
        read_idle_timeout_ms: 500,
        ..Default::default()
    });
    let url = format!("http://{}/x", peer.addr);
    assert_eq!(send_method(client, "GET", &url, &cap), HYPER4K_STATUS_OK);
    wait_until("done", || cap.done.lock().unwrap().is_some());
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn zero_timeouts_disable_rather_than_expire_immediately() {
    let r = rt();
    let peer = r.block_on(answer_then_close("ok"));
    let cap = Arc::new(Capture::default());
    let client = client_with(Cfg {
        request_timeout_ms: 0,
        read_idle_timeout_ms: 0,
        ..Default::default()
    });
    let url = format!("http://{}/x", peer.addr);
    assert_eq!(send_method(client, "GET", &url, &cap), HYPER4K_STATUS_OK);
    wait_until("done", || cap.done.lock().unwrap().is_some());
    assert_eq!(
        *cap.done.lock().unwrap(),
        Some(-999),
        "0 must mean disabled, not expire-immediately"
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_committed_response_that_truncates_reports_truncated_not_unknown() {
    // Headers arrive, then the peer vanishes. The request certainly ran, so the
    // caller must be able to tell this apart from "we do not know".
    let r = rt();
    let peer = r.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicU32::new(0));
        let counter = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                // Promise 100 bytes, send 3, then disappear.
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nabc")
                    .await;
                let _ = sock.flush().await;
                drop(sock);
            }
        });
        Peer { addr, requests }
    });

    let cap = Arc::new(Capture::default());
    let client = client_with(Cfg {
        max_retries: 3,
        ..Default::default()
    });
    let url = format!("http://{}/x", peer.addr);
    assert_eq!(send_method(client, "GET", &url, &cap), HYPER4K_STATUS_OK);
    wait_until("done", || cap.done.lock().unwrap().is_some());

    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TRUNCATED));
    assert_eq!(
        cap.headers_calls.load(Ordering::SeqCst),
        1,
        "a committed response was replayed"
    );
    assert_eq!(peer.request_count(), 1, "a committed response was replayed");
    unsafe { hyper4k_client_free(client) };
}
