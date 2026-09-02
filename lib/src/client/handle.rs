//! `Hyper4kClient`: the exported lifecycle and request functions.

use super::bridge::{
    self, Callbacks, Event, OnChunk, OnDone, OnHeaders, RequestHandle, Terminal, UserData,
};
use super::plaintext::PlaintextConnector;
use super::pool::{Pool, PoolKey, Sender};
use super::{
    validate_header, Hyper4kClientOptions, Hyper4kClientRequest, HYPER4K_CLIENT_CA_REPLACE_SYSTEM,
    HYPER4K_CLIENT_HTTP2_REQUIRED, KNOWN_CLIENT_FLAGS, OPTIONS_MIN_SIZE, REQUEST_MIN_SIZE,
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
    max_retries: u32,
    request_timeout: Option<Duration>,
    read_idle_timeout: Option<Duration>,
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
}

impl RequestTemplate {
    pub(crate) fn build(&self) -> hyper::Request<Full<Bytes>> {
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
        b.body(Full::new(self.body.clone()))
            .expect("validated at send")
    }
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
    let o = &*opts;
    let st = validate_header(o.abi_version, o.struct_size, OPTIONS_MIN_SIZE);
    if st != HYPER4K_STATUS_OK {
        return st;
    }
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
    let tls_opts = super::tls::TlsOptions {
        custom_ca_pem: custom_ca,
        replace_system_roots: o.flags & HYPER4K_CLIENT_CA_REPLACE_SYSTEM != 0,
        require_h2: o.flags & HYPER4K_CLIENT_HTTP2_REQUIRED != 0,
        connect_timeout,
    };
    // Built eagerly: a bad CA bundle should fail at construction, not on the
    // first request where it looks like a network problem.
    let tls_pool = match super::tls::TlsClientConnector::new(&tls_opts) {
        Ok(c) => Some(Arc::new(Pool::new(Arc::new(c)))),
        Err(_) => None,
    };

    let client = Hyper4kClient {
        runtime: Some(runtime),
        pool: Arc::new(Pool::new(Arc::new(PlaintextConnector { connect_timeout }))),
        tls_pool,
        requests: Arc::new(DashMap::new()),
        next_id: AtomicU64::new(1),
        closed: Arc::new(AtomicBool::new(false)),
        connect_timeout,
        queue_capacity: 8,
        h2_required: o.flags & HYPER4K_CLIENT_HTTP2_REQUIRED != 0,
        max_retries: o.max_retries,
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
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !boxed.requests.is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    if let Some(rt) = boxed.runtime.take() {
        rt.shutdown_timeout(Duration::from_secs(5));
    }
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
    on_headers: OnHeaders,
    on_chunk: Option<OnChunk>,
    on_done: OnDone,
    user_data: *mut c_void,
    out_request_id: *mut u64,
) -> Hyper4kStatus {
    if client.is_null() || request.is_null() || out_request_id.is_null() {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    let c = &*client;
    let r = &*request;
    let st = validate_header(r.abi_version, r.struct_size, REQUEST_MIN_SIZE);
    if st != HYPER4K_STATUS_OK {
        return st;
    }
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

    let template = RequestTemplate {
        method,
        uri,
        headers,
        body,
        authority: format!("{host}:{port}"),
    };
    let key = PoolKey::new(&scheme, &host, port);
    let id = c.next_id.fetch_add(1, Ordering::SeqCst);

    let requests = c.requests.clone();
    let cleanup_id = id;
    let cleanup = move || {
        requests.remove(&cleanup_id);
    };
    let rt = c.runtime.as_ref().expect("runtime present until free");
    let (handle, sink, _worker) = bridge::spawn(
        rt.handle(),
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

    let pool = if scheme == "https" {
        match c.tls_pool.clone() {
            Some(p) => p,
            None => return HYPER4K_STATUS_UNSUPPORTED,
        }
    } else {
        c.pool.clone()
    };
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
    h.state.resume()
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

        // Every attempt builds a fresh request: try_send_request consumes it,
        // and on the not-provably-unsent path it is not handed back at all.
        let req = template.build();
        type Sent = Result<
            hyper::Response<hyper::body::Incoming>,
            hyper::client::conn::TrySendError<hyper::Request<Full<Bytes>>>,
        >;
        let sent: Sent = match lease.sender_mut() {
            Sender::H1(s) => s.try_send_request(req).await,
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
