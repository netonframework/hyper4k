//! `Hyper4kClient`: the exported lifecycle and request functions.

use super::bridge::{
    self, Callbacks, Event, OnChunk, OnDone, OnHeaders, RequestHandle, Terminal, UserData,
};
use super::plaintext::PlaintextConnector;
use super::pool::{Pool, PoolKey, Sender};
use super::{
    copy_prefix, defaults_options, defaults_request, validate_header, Hyper4kClientOptions,
    Hyper4kClientRequest, HYPER4K_CLIENT_CA_REPLACE_SYSTEM, HYPER4K_CLIENT_HTTP2_REQUIRED,
    KNOWN_CLIENT_FLAGS, OPTIONS_MIN_SIZE, REQUEST_MIN_SIZE,
};
use crate::abi::*;
use bytes::Bytes;
use dashmap::DashMap;
use http_body_util::{BodyExt, Full};
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

pub struct Hyper4kClient {
    runtime: Option<Runtime>,
    /// One pool per transport. A connection is never shared across schemes.
    pool: Arc<Pool>,
    tls_pool: Option<Arc<Pool>>,
    requests: Arc<DashMap<u64, Arc<RequestHandle>>>,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
    #[allow(dead_code)]
    connect_timeout: Option<Duration>,
    queue_capacity: usize,
    h2_required: bool,
    bridges: Arc<bridge::BridgeCounter>,
    /// Fixed thread count: callbacks never run on an I/O worker, and a paused
    /// request occupies none of these.
    executor: Arc<super::executor::BridgeExecutor>,
    limits: Arc<Limits>,
    max_retries: u32,
    request_timeout: Option<Duration>,
    read_idle_timeout: Option<Duration>,
    /// Set when the client was built with a proxy: plaintext requests must then
    /// keep absolute-form targets (that is how a proxy learns the origin).
    via_proxy: bool,
}

/// Client-wide ceilings.
///
/// Each request's queue is bounded, but nothing bounded the number of requests,
/// so N of them could still grow without limit. Refusing here is deliberate
/// throttling and reports `RESOURCE_EXHAUSTED`, which is a different problem
/// from a real allocation failure and must not be reported as `OOM`.
pub(crate) struct Limits {
    max_inflight: u32,
    max_bytes: u64,
    inflight: std::sync::atomic::AtomicU32,
    bytes: std::sync::atomic::AtomicU64,
}

impl Limits {
    fn new(max_inflight: u32, max_bytes: u64) -> Self {
        Limits {
            max_inflight: if max_inflight == 0 {
                super::DEFAULT_MAX_INFLIGHT
            } else {
                max_inflight
            },
            max_bytes: if max_bytes == 0 {
                super::DEFAULT_MAX_BUFFERED_BYTES
            } else {
                max_bytes
            },
            inflight: std::sync::atomic::AtomicU32::new(0),
            bytes: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Reserve room for one request, or refuse. Reservation and release are
    /// paired through `Reservation`'s Drop so no exit path can leak either.
    fn try_reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        let n = self.inflight.fetch_add(1, Ordering::SeqCst);
        if n >= self.max_inflight {
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        let b = self.bytes.fetch_add(bytes, Ordering::SeqCst);
        if b + bytes > self.max_bytes {
            self.bytes.fetch_sub(bytes, Ordering::SeqCst);
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(Reservation {
            limits: self.clone(),
            bytes,
        })
    }

    pub(crate) fn inflight(&self) -> u32 {
        self.inflight.load(Ordering::SeqCst)
    }
}

pub(crate) struct Reservation {
    limits: Arc<Limits>,
    bytes: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.limits.bytes.fetch_sub(self.bytes, Ordering::SeqCst);
        self.limits.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Parsed once at `send`, then reused for every attempt.
///
/// A retry cannot reuse the original `hyper::Request`: `try_send_request`
/// consumes it, and on the not-provably-unsent path it is not handed back at
/// all. `Bytes` clones are refcount bumps, so replaying costs no body copy.
#[derive(Clone)]
pub(crate) struct RequestTemplate {
    pub method: hyper::Method,
    pub uri: hyper::Uri,
    pub headers: Vec<(Bytes, Bytes)>,
    pub body: Bytes,
    pub authority: String,
    /// Keep the full URL on the wire (request goes to a proxy).
    pub absolute_form: bool,
}

impl RequestTemplate {
    /// Rejects anything the builder would later refuse.
    ///
    /// Validation has to happen at submit time, not at build time: the crate is
    /// compiled with `panic = "abort"`, so a builder `expect` on an illegal
    /// header name would take the host process down instead of returning
    /// INVALID_ARG. A caller must never be able to kill us with a bad header.
    pub(crate) fn validate(&self) -> bool {
        for (n, v) in &self.headers {
            if hyper::header::HeaderName::from_bytes(n).is_err() {
                return false;
            }
            if hyper::header::HeaderValue::from_bytes(v).is_err() {
                return false;
            }
        }
        true
    }

    pub(crate) fn build(&self) -> Option<hyper::Request<Full<Bytes>>> {
        let mut b = hyper::Request::builder()
            .method(self.method.clone())
            .uri(self.uri.clone());
        let mut saw_host = false;
        for (n, v) in &self.headers {
            if n.eq_ignore_ascii_case(b"host") {
                saw_host = true;
            }
            b = b.header(n.as_ref(), v.as_ref());
        }
        if !saw_host {
            b = b.header("host", self.authority.as_str());
        }
        b.body(Full::new(self.body.clone())).ok()
    }
}

/// Rewrites the request-target to origin-form for HTTP/1.1 (RFC 9112 §3.2.1).
fn to_origin_form(mut req: hyper::Request<Full<Bytes>>) -> hyper::Request<Full<Bytes>> {
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "/".to_owned());
    if let Ok(uri) = hyper::Uri::try_from(path_and_query) {
        *req.uri_mut() = uri;
    }
    req
}

unsafe fn slice_bytes(s: &crate::Hyper4kSlice) -> Option<Bytes> {
    if s.len == 0 {
        return Some(Bytes::new());
    }
    if s.ptr.is_null() {
        return None;
    }
    Some(Bytes::copy_from_slice(std::slice::from_raw_parts(
        s.ptr, s.len,
    )))
}

/// Create a client.
///
/// # Safety
/// `opts` and `out_client` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_new(
    opts: *const Hyper4kClientOptions,
    out_client: *mut *mut Hyper4kClient,
) -> Hyper4kStatus {
    if opts.is_null() || out_client.is_null() {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    // Read only the prefix the caller allocated. Dereferencing their shorter
    // buffer as a full struct would read past the end of their allocation —
    // exactly the bug the init functions are careful to avoid on the write side.
    let raw_size =
        std::ptr::read_unaligned((opts as *const u8).add(std::mem::size_of::<u32>()) as *const u32);
    let raw_abi = std::ptr::read_unaligned(opts as *const u32);
    let st = validate_header(raw_abi, raw_size, OPTIONS_MIN_SIZE);
    if st != HYPER4K_STATUS_OK {
        return st;
    }
    let o = &copy_prefix::<Hyper4kClientOptions>(opts as *const u8, raw_size, defaults_options());
    // An unknown flag is refused, never ignored: the bit we drop could be the
    // one carrying a security decision.
    if o.flags & !KNOWN_CLIENT_FLAGS != 0 {
        return HYPER4K_STATUS_UNKNOWN_FLAGS;
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return HYPER4K_STATUS_OOM,
    };
    let connect_timeout = match o.connect_timeout_ms {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    };
    let custom_ca = if o.custom_ca_pem.is_null() || o.custom_ca_pem_len == 0 {
        None
    } else {
        Some(std::slice::from_raw_parts(o.custom_ca_pem, o.custom_ca_pem_len).to_vec())
    };
    // A proxy that cannot be honoured is refused here, not on the first
    // request: a client that silently connects directly when it was told to
    // use a proxy is leaking traffic, not degrading gracefully.
    let proxy = if o.proxy_url.is_null() || o.proxy_url_len == 0 {
        None
    } else {
        let raw = std::slice::from_raw_parts(o.proxy_url, o.proxy_url_len);
        let Ok(text) = std::str::from_utf8(raw) else {
            return HYPER4K_STATUS_INVALID_ARG;
        };
        match super::proxy::ProxyTarget::parse(text) {
            Some(p) => Some(p),
            None => return HYPER4K_STATUS_INVALID_ARG,
        }
    };
    let tls_opts = super::tls::TlsOptions {
        custom_ca_pem: custom_ca,
        replace_system_roots: o.flags & HYPER4K_CLIENT_CA_REPLACE_SYSTEM != 0,
        require_h2: o.flags & HYPER4K_CLIENT_HTTP2_REQUIRED != 0,
        connect_timeout,
        proxy: proxy.clone(),
    };
    // Built eagerly: a bad CA bundle should fail at construction, not on the
    // first request where it looks like a network problem.
    // A bad CA bundle fails here, loudly. Turning it into `None` and reporting
    // success would defer the error to the first HTTPS request, where it reads
    // as a network problem instead of a configuration one.
    let tls_pool = match super::tls::TlsClientConnector::new(&tls_opts) {
        Ok(c) => Some(Arc::new(Pool::new(Arc::new(c)))),
        Err(HYPER4K_ERR_TLS_CA) => return HYPER4K_STATUS_INVALID_ARG,
        Err(_) => None, // no platform trust store: plaintext still works
    };

    let client = Hyper4kClient {
        runtime: Some(runtime),
        pool: Arc::new(Pool::new(Arc::new(PlaintextConnector {
            connect_timeout,
            proxy: proxy.clone(),
        }))),
        tls_pool,
        requests: Arc::new(DashMap::new()),
        next_id: AtomicU64::new(1),
        closed: Arc::new(AtomicBool::new(false)),
        connect_timeout,
        queue_capacity: 8,
        h2_required: o.flags & HYPER4K_CLIENT_HTTP2_REQUIRED != 0,
        bridges: Arc::new(bridge::BridgeCounter::default()),
        executor: super::executor::BridgeExecutor::new(
            std::thread::available_parallelism()
                .map(|n| n.get().clamp(2, 8))
                .unwrap_or(4),
        ),
        limits: Arc::new(Limits::new(o.max_inflight_requests, o.max_buffered_bytes)),
        max_retries: o.max_retries,
        via_proxy: proxy.is_some(),
        request_timeout: (o.request_timeout_ms != 0)
            .then(|| Duration::from_millis(o.request_timeout_ms)),
        read_idle_timeout: (o.read_idle_timeout_ms != 0)
            .then(|| Duration::from_millis(o.read_idle_timeout_ms)),
    };
    *out_client = Box::into_raw(Box::new(client));
    HYPER4K_STATUS_OK
}

/// Stop accepting work and cancel everything in flight. Idempotent, non-blocking.
///
/// # Safety
/// `client` must come from `hyper4k_client_new` and not yet be freed.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_close(client: *mut Hyper4kClient) {
    if client.is_null() {
        return;
    }
    let c = &*client;
    c.closed.store(true, Ordering::SeqCst);
    // Every accepted request still gets exactly one OnDone.
    let ids: Vec<u64> = c.requests.iter().map(|e| *e.key()).collect();
    for id in ids {
        if let Some(h) = c.requests.get(&id) {
            h.settle(
                Terminal {
                    kind: HYPER4K_ERR_CANCELLED,
                    protocol_code: 0,
                    message: "client closed".into(),
                },
                true,
            );
        }
    }
    let pool = c.pool.clone();
    let tls_pool = c.tls_pool.clone();
    if let Some(rt) = c.runtime.as_ref() {
        rt.spawn(async move {
            pool.shutdown().await;
            if let Some(p) = tls_pool {
                p.shutdown().await;
            }
        });
    }
}

/// Block until every request has settled and no callback can fire, then free.
///
/// # Safety
/// Requires exclusive ownership. MUST NOT be called from a callback thread —
/// it would wait on the very bridge that is running the callback.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_free(client: *mut Hyper4kClient) {
    if client.is_null() {
        return;
    }
    hyper4k_client_close(client);
    let mut boxed = Box::from_raw(client);

    // Wait for every bridge to finish its OnDone. No timeout: freeing while a
    // callback is still running would hand the caller a dangling user_data,
    // and no deadline makes that safe.
    boxed.bridges.wait_zero();

    // Only then tear down the transport and the runtime.
    let pool = boxed.pool.clone();
    let tls_pool = boxed.tls_pool.clone();
    if let Some(rt) = boxed.runtime.as_ref() {
        rt.block_on(async {
            pool.shutdown().await;
            if let Some(p) = tls_pool {
                p.shutdown().await;
            }
        });
    }
    drop(boxed.runtime.take());
    boxed.executor.shutdown();
}

/// Submit a request.
///
/// Contract (only what a callee can actually guarantee):
///   1. `*out_request_id` is written before the request can produce any event;
///   2. no callback re-enters the calling thread synchronously;
///   3. a callback may run on another thread concurrently with this returning.
///
/// # Safety
/// All pointers must be valid; slices are copied before returning.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_send(
    client: *mut Hyper4kClient,
    request: *const Hyper4kClientRequest,
    on_headers: Option<OnHeaders>,
    on_chunk: Option<OnChunk>,
    on_done: Option<OnDone>,
    user_data: *mut c_void,
    out_request_id: *mut u64,
) -> Hyper4kStatus {
    if client.is_null() || request.is_null() || out_request_id.is_null() {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    // Taken as Option so a C NULL is a value we can inspect. A non-nullable
    // `extern "C" fn` would already be an invalid Rust value on entry — the
    // check would come too late to mean anything.
    let (Some(on_headers), Some(on_done)) = (on_headers, on_done) else {
        return HYPER4K_STATUS_INVALID_ARG;
    };
    let c = &*client;
    let raw_size = std::ptr::read_unaligned(
        (request as *const u8).add(std::mem::size_of::<u32>()) as *const u32
    );
    let raw_abi = std::ptr::read_unaligned(request as *const u32);
    let st = validate_header(raw_abi, raw_size, REQUEST_MIN_SIZE);
    if st != HYPER4K_STATUS_OK {
        return st;
    }
    let r =
        &copy_prefix::<Hyper4kClientRequest>(request as *const u8, raw_size, defaults_request());
    if c.closed.load(Ordering::SeqCst) {
        return HYPER4K_STATUS_CLIENT_CLOSED;
    }

    let (Some(method_b), Some(url_b)) = (slice_bytes(&r.method), slice_bytes(&r.url)) else {
        return HYPER4K_STATUS_INVALID_ARG;
    };
    let Ok(method) = hyper::Method::from_bytes(&method_b) else {
        return HYPER4K_STATUS_INVALID_ARG;
    };
    let Ok(url_str) = std::str::from_utf8(&url_b) else {
        return HYPER4K_STATUS_INVALID_ARG;
    };
    let Ok(uri) = url_str.parse::<hyper::Uri>() else {
        return HYPER4K_STATUS_INVALID_ARG;
    };
    let scheme = uri.scheme_str().unwrap_or("").to_string();
    let Some(host) = uri.host().map(|h| h.to_string()) else {
        return HYPER4K_STATUS_INVALID_ARG;
    };
    let port = uri.port_u16().unwrap_or(match scheme.as_str() {
        "https" => 443,
        "http" => 80,
        _ => return HYPER4K_STATUS_INVALID_ARG,
    });
    if scheme != "http" && scheme != "https" {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    // v4 ships no h2c client, so this combination has no implementation at all.
    if scheme == "http" && c.http2_required() {
        return HYPER4K_STATUS_UNSUPPORTED;
    }

    let mut headers = Vec::with_capacity(r.header_count);
    if r.header_count > 0 {
        if r.headers.is_null() {
            return HYPER4K_STATUS_INVALID_ARG;
        }
        for i in 0..r.header_count {
            let h = &*r.headers.add(i);
            let (Some(n), Some(v)) = (slice_bytes(&h.name), slice_bytes(&h.value)) else {
                return HYPER4K_STATUS_INVALID_ARG;
            };
            headers.push((n, v));
        }
    }
    let body = if r.body_len == 0 {
        Bytes::new()
    } else if r.body_ptr.is_null() {
        return HYPER4K_STATUS_INVALID_ARG;
    } else {
        Bytes::copy_from_slice(std::slice::from_raw_parts(r.body_ptr, r.body_len))
    };

    // Decide the transport BEFORE anything is registered. Creating the bridge
    // first and refusing afterwards would leave a registered request that no
    // one will ever settle.
    let pool = if scheme == "https" {
        match c.tls_pool.clone() {
            Some(p) => p,
            None => return HYPER4K_STATUS_UNSUPPORTED,
        }
    } else {
        c.pool.clone()
    };

    let template = RequestTemplate {
        method,
        uri,
        headers,
        body,
        authority: format!("{host}:{port}"),
        // Only plaintext needs absolute-form: a TLS target is reached through a
        // CONNECT tunnel, and inside the tunnel the origin sees a direct request.
        absolute_form: c.via_proxy && scheme == "http",
    };
    if !template.validate() {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    let key = PoolKey::new(&scheme, &host, port);
    let id = c.next_id.fetch_add(1, Ordering::SeqCst);

    // Queue capacity is per request; this is the ceiling across all of them.
    let queue_budget = (c.queue_capacity as u64) * 64 * 1024;
    let Some(reservation) = c
        .limits
        .try_reserve(template.body.len() as u64 + queue_budget)
    else {
        return HYPER4K_STATUS_RESOURCE_EXHAUSTED;
    };

    let requests = c.requests.clone();
    let bridges = c.bridges.clone();
    bridges.enter();
    let cleanup_id = id;
    let cleanup = move || {
        // Held until the request is fully done, then released by Drop.
        drop(reservation);
        requests.remove(&cleanup_id);
    };
    let _ = &bridges;
    let rt = c.runtime.as_ref().expect("runtime present until free");
    let (handle, sink) = bridge::spawn(
        &c.executor,
        &c.bridges,
        id,
        Callbacks {
            on_headers,
            on_chunk,
            on_done,
            user_data: UserData(user_data),
        },
        c.queue_capacity,
        cleanup,
    );
    c.requests.insert(id, handle.clone());

    // Written before anything can fire, per contract point 1.
    *out_request_id = id;

    let h = handle.clone();
    // UINT64_MAX inherits, 0 disables, anything else overrides.
    let idle = match r.read_idle_timeout_ms {
        u64::MAX => c.read_idle_timeout,
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    };
    let policy = RequestPolicy {
        max_retries: c.max_retries,
        request_timeout: c.request_timeout,
        read_idle_timeout: idle,
    };
    let task = rt.spawn(async move {
        run_request(pool, key, template, sink, h, policy).await;
    });
    handle.set_abort(task.abort_handle());
    HYPER4K_STATUS_OK
}

/// Resume a paused response body. Idempotent, callback-thread safe, non-blocking.
///
/// # Safety
/// `client` must be a live client.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_resume(
    client: *mut Hyper4kClient,
    request_id: u64,
) -> Hyper4kStatus {
    if client.is_null() {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    let c = &*client;
    let Some(h) = c.requests.get(&request_id) else {
        return HYPER4K_STATUS_NOT_FOUND;
    };
    h.resume()
}

/// Requests currently in flight. Diagnostics only.
///
/// # Safety
/// `client` must be a live client.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_inflight_count(client: *mut Hyper4kClient) -> u32 {
    if client.is_null() {
        return 0;
    }
    (*client).limits.inflight()
}

/// Total parked streams across every pooled connection.
///
/// Diagnostics only — it exists so the reservation invariant can be observed
/// rather than assumed.
///
/// # Safety
/// `client` must be a live client.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_paused_stream_count(client: *mut Hyper4kClient) -> u32 {
    if client.is_null() {
        return 0;
    }
    let c = &*client;
    let mut n = c.pool.total_paused();
    if let Some(p) = c.tls_pool.as_ref() {
        n += p.total_paused();
    }
    n
}

/// Cancel a request. Idempotent, callback-thread safe, non-blocking.
///
/// # Safety
/// `client` must be a live client.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_cancel(
    client: *mut Hyper4kClient,
    request_id: u64,
) -> Hyper4kStatus {
    if client.is_null() {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    let c = &*client;
    let Some(h) = c.requests.get(&request_id) else {
        return HYPER4K_STATUS_NOT_FOUND;
    };
    if h.state.is_terminal() {
        return HYPER4K_STATUS_ALREADY_DONE;
    }
    h.settle(
        Terminal {
            kind: HYPER4K_ERR_CANCELLED,
            protocol_code: 0,
            message: "cancelled".into(),
        },
        true,
    );
    HYPER4K_STATUS_OK
}

impl Hyper4kClient {
    fn http2_required(&self) -> bool {
        // Stored implicitly: only reachable via flags at construction. Task 5
        // gives this a real home when TLS options arrive.
        self.h2_required
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RequestPolicy {
    pub max_retries: u32,
    /// Covers every attempt together. Not re-armed per retry: a "total" limit
    /// that resets is not a limit at all.
    pub request_timeout: Option<Duration>,
    /// Inter-chunk idle limit, re-armed on every delivered chunk. This is what
    /// keeps a long stream alive without an overall deadline.
    pub read_idle_timeout: Option<Duration>,
}

async fn run_request(
    pool: Arc<Pool>,
    key: PoolKey,
    template: RequestTemplate,
    sink: bridge::EventSink,
    handle: Arc<RequestHandle>,
    policy: RequestPolicy,
) {
    let work = attempt_loop(pool, key, template, sink, handle.clone(), policy);
    match policy.request_timeout {
        Some(d) => {
            if tokio::time::timeout(d, work).await.is_err() {
                handle.settle(
                    Terminal {
                        kind: HYPER4K_ERR_TIMEOUT,
                        protocol_code: 0,
                        message: "request timeout".into(),
                    },
                    true,
                );
            }
        }
        None => work.await,
    }
}

async fn attempt_loop(
    pool: Arc<Pool>,
    key: PoolKey,
    template: RequestTemplate,
    sink: bridge::EventSink,
    handle: Arc<RequestHandle>,
    policy: RequestPolicy,
) {
    let idempotent = super::retry::is_idempotent(&template.method);
    let mut budget = policy.max_retries;

    loop {
        let mut lease = match pool.acquire(&key).await {
            Ok(l) => l,
            Err(kind) => {
                handle.settle(
                    Terminal {
                        kind,
                        protocol_code: 0,
                        message: "connect failed".into(),
                    },
                    true,
                );
                return;
            }
        };

        // Task 0 finding: `take_message` reports "hyper had not written it yet",
        // not "the peer refused it". Handing work to a connection that is
        // already going away gets it serialised before the GOAWAY is processed,
        // which downgrades a safely retryable failure to OUTCOME_UNKNOWN.
        if lease.sender_mut().is_closed() {
            drop(lease);
            if budget > 0 {
                budget -= 1;
                continue;
            }
            handle.settle(
                Terminal {
                    kind: HYPER4K_ERR_CONNECT,
                    protocol_code: 0,
                    message: "no live connection".into(),
                },
                true,
            );
            return;
        }

        // Tie the request to this connection so a pause consumes that
        // connection's paused-stream budget rather than an abstract one.
        handle.state.bind_connection(lease.entry.clone());

        // Every attempt builds a fresh request: try_send_request consumes it,
        // and on the not-provably-unsent path it is not handed back at all.
        let Some(req) = template.build() else {
            handle.settle(
                Terminal {
                    kind: HYPER4K_ERR_PROTOCOL,
                    protocol_code: 0,
                    message: "request could not be built".into(),
                },
                true,
            );
            return;
        };
        type Sent = Result<
            hyper::Response<hyper::body::Incoming>,
            hyper::client::conn::TrySendError<hyper::Request<Full<Bytes>>>,
        >;
        let sent: Sent = match lease.sender_mut() {
            // The low-level conn API writes the URI verbatim. HTTP/1.1 on a
            // direct connection must use origin-form ("/path?q"); absolute-form
            // is for proxies. hyper's high-level Client does this rewrite for
            // you, and skipping it here was invisible against hyper's own server,
            // which accepts either. HTTP/2 keeps the full URI: the :scheme and
            // :authority pseudo-headers are derived from it.
            Sender::H1(s) => {
                let req = if template.absolute_form { req } else { to_origin_form(req) };
                s.try_send_request(req).await
            }
            Sender::H2(s) => s.try_send_request(req).await,
        };

        let response = match sent {
            Ok(resp) => resp,
            Err(mut e) => {
                let provably_unsent = e.take_message().is_some();
                let committed = handle.state.is_committed();
                if super::retry::may_retry(committed, provably_unsent, idempotent, budget) {
                    budget -= 1;
                    drop(lease);
                    continue;
                }
                let kind = if committed {
                    HYPER4K_ERR_TRUNCATED
                } else {
                    HYPER4K_ERR_OUTCOME_UNKNOWN
                };
                handle.settle(
                    Terminal {
                        kind,
                        protocol_code: 0,
                        message: "send failed".into(),
                    },
                    true,
                );
                return;
            }
        };

        deliver_response(response, &sink, &handle, policy).await;
        return;
    }
}

async fn deliver_response(
    response: hyper::Response<hyper::body::Incoming>,
    sink: &bridge::EventSink,
    handle: &Arc<RequestHandle>,
    policy: RequestPolicy,
) {
    let version = match response.version() {
        hyper::Version::HTTP_2 => 2u8,
        _ => 1u8,
    };
    let status = response.status().as_u16();
    let headers: Vec<(Bytes, Bytes)> = response
        .headers()
        .iter()
        .map(|(n, v)| {
            (
                Bytes::copy_from_slice(n.as_str().as_bytes()),
                Bytes::copy_from_slice(v.as_bytes()),
            )
        })
        .collect();

    // From here the response is visible to the caller: no replay is possible.
    if !sink
        .send(Event::Headers {
            status,
            version,
            headers,
        })
        .await
    {
        return;
    }

    let mut body = response.into_body();
    loop {
        let next = match policy.read_idle_timeout {
            Some(d) => match tokio::time::timeout(d, body.frame()).await {
                Ok(v) => v,
                Err(_) => {
                    handle.settle(
                        Terminal {
                            kind: HYPER4K_ERR_IDLE_TIMEOUT,
                            protocol_code: 0,
                            message: "idle timeout between chunks".into(),
                        },
                        true,
                    );
                    return;
                }
            },
            None => body.frame().await,
        };

        match next {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if !data.is_empty() && !sink.send(Event::Chunk(data)).await {
                        return;
                    }
                }
            }
            Some(Err(_)) => {
                // The response had started: this is truncation, and truncation
                // is never a candidate for replay.
                handle.settle(
                    Terminal {
                        kind: HYPER4K_ERR_TRUNCATED,
                        protocol_code: 0,
                        message: "response truncated".into(),
                    },
                    true,
                );
                return;
            }
            None => break,
        }
    }

    handle.settle(
        Terminal {
            kind: HYPER4K_ERR_NONE,
            protocol_code: 0,
            message: String::new(),
        },
        false,
    );
}
