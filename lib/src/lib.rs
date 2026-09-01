//! hyper4k —— Tokio + Hyper HTTP 引擎，通过借用切片 C ABI 暴露。
//!
//! 这一层**只做协议与传输**：accept / parse / body 聚合 / 写回 / 连接生命周期。
//! 路由、中间件、handler 一律由上层（Kotlin / Neton）负责。
//!
//! 详细 ABI 契约见 `include/hyper4k.h`。

use std::cell::Cell;
use std::convert::Infallible;
use std::ffi::{c_char, c_void, CStr};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use bytes::Bytes;
use dashmap::DashMap;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{mpsc, oneshot};

const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Streaming body channel capacity. Kept small: a bigger buffer only piles bytes
/// up on the Rust side instead of letting backpressure reach the writer.
const STREAM_CHANNEL_CAPACITY: usize = 4;

// ---------------------------------------------------------------------------
// ABI v3 return codes
// ---------------------------------------------------------------------------

/// Success. Same value as v2 `hyper4k_respond`'s "delivered".
pub const HYPER4K_OK: i32 = 1;
/// Responder is stale or already completed (v2 semantics).
pub const HYPER4K_FAILED: i32 = 0;
/// The responder's state does not allow this call (one-shot mixed with streaming).
pub const HYPER4K_ERR_WRONG_STATE: i32 = -4;
/// Client is gone: stop producing data and call finish. Not an error path.
pub const HYPER4K_ERR_CLIENT_GONE: i32 = -5;
/// The write needs to block but the caller is an engine thread. Streaming writes
/// must run on a dispatcher where blocking is safe.
pub const HYPER4K_ERR_WOULD_BLOCK: i32 = -6;

// ---------------------------------------------------------------------------
// C 可见的数据布局
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct Hyper4kSlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl Hyper4kSlice {
    #[inline]
    fn borrow(b: &[u8]) -> Self {
        Hyper4kSlice {
            ptr: b.as_ptr(),
            len: b.len(),
        }
    }
}

#[repr(C)]
pub struct Hyper4kRequest {
    pub method: Hyper4kSlice,
    pub path: Hyper4kSlice,
    pub query: Hyper4kSlice,
    pub headers: Hyper4kSlice,
    pub body: Hyper4kSlice,
    pub responder: u64,
}

pub type Hyper4kRequestCallback = extern "C" fn(user_data: *mut c_void, req: *const Hyper4kRequest);

// ---------------------------------------------------------------------------
// 内部句柄
// ---------------------------------------------------------------------------

/// 跨 await/线程携带的回调上下文。指针由 Kotlin 侧保证存活（StableRef）。
struct CallbackCtx {
    cb: Hyper4kRequestCallback,
    user_data: *mut c_void,
}
// 安全性：user_data 是 Kotlin 的 StableRef，在 server 生命周期内有效；
// cb 是普通函数指针。两者实际可跨线程安全使用。
unsafe impl Send for CallbackCtx {}
unsafe impl Sync for CallbackCtx {}

struct ResponseData {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

/// Head of a streaming response: status and headers go out now, body arrives over `rx`.
struct StreamStart {
    status: u16,
    headers: Vec<(String, String)>,
    rx: mpsc::Receiver<Bytes>,
}

/// What the connection task receives: a finished response, or the head of a stream.
enum Delivery {
    Buffered(ResponseData),
    Stream(StreamStart),
}

/// Response body. An enum rather than `BoxBody` so the one-shot path stays monomorphic.
pub struct Hyper4kBody(BodyKind);

enum BodyKind {
    Full(Full<Bytes>),
    Stream(mpsc::Receiver<Bytes>),
}

impl Hyper4kBody {
    fn full(bytes: Bytes) -> Self {
        Hyper4kBody(BodyKind::Full(Full::new(bytes)))
    }

    fn stream(rx: mpsc::Receiver<Bytes>) -> Self {
        Hyper4kBody(BodyKind::Stream(rx))
    }
}

impl Body for Hyper4kBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        match &mut self.get_mut().0 {
            BodyKind::Full(full) => Pin::new(full).poll_frame(cx),
            // recv yields None once every sender is dropped, which ends the stream.
            BodyKind::Stream(rx) => rx
                .poll_recv(cx)
                .map(|chunk| chunk.map(|bytes| Ok(Frame::data(bytes)))),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            BodyKind::Full(full) => full.is_end_stream(),
            BodyKind::Stream(_) => false,
        }
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        match &self.0 {
            // Known length: hyper emits Content-Length.
            BodyKind::Full(full) => full.size_hint(),
            // Unknown length: hyper picks chunked (HTTP/1.1) or DATA frames (HTTP/2).
            BodyKind::Stream(_) => hyper::body::SizeHint::default(),
        }
    }
}

static NEXT_RESPONDER_ID: AtomicU64 = AtomicU64::new(1);
static PENDING_RESPONSES: OnceLock<DashMap<u64, oneshot::Sender<Delivery>>> = OnceLock::new();
/// Responders that have entered streaming: id -> body sender. Presence in this
/// map is the single source of truth for the Streaming state.
static ACTIVE_STREAMS: OnceLock<DashMap<u64, mpsc::Sender<Bytes>>> = OnceLock::new();

// ABI v2 同步快路径：回调在 Tokio worker 线程上执行，若 handler 在回调内同步完成，
// hyper4k_respond 直接把响应写入线程本地槽，省掉 DashMap 注册 + oneshot 交接。
thread_local! {
    static IN_CALLBACK: Cell<bool> = const { Cell::new(false) };
    static SYNC_RESPONSE: std::cell::RefCell<Option<Delivery>> = const { std::cell::RefCell::new(None) };
}

fn take_sync_response() -> Option<Delivery> {
    SYNC_RESPONSE.with(|slot| slot.borrow_mut().take())
}

/// Parks a delivery in the thread-local slot; false if the slot is already taken.
fn set_sync_response(delivery: Delivery) -> bool {
    SYNC_RESPONSE.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_some() {
            return false;
        }
        *slot = Some(delivery);
        true
    })
}

fn pending_responses() -> &'static DashMap<u64, oneshot::Sender<Delivery>> {
    PENDING_RESPONSES.get_or_init(DashMap::new)
}

fn active_streams() -> &'static DashMap<u64, mpsc::Sender<Bytes>> {
    ACTIVE_STREAMS.get_or_init(DashMap::new)
}

fn next_responder_id() -> u64 {
    loop {
        let id = NEXT_RESPONDER_ID.fetch_add(1, Ordering::Relaxed);
        if id != 0 {
            return id;
        }
    }
}

/// 未交付的通道条目守卫：回调未响应就返回（或连接断开）时移除注册，
/// 防止 DashMap 条目泄漏。
struct PendingResponse {
    id: u64,
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        pending_responses().remove(&self.id);
    }
}

/// 注册一个异步响应通道。
fn register_response() -> (u64, oneshot::Receiver<Delivery>, PendingResponse) {
    let (tx, rx) = oneshot::channel::<Delivery>();
    let id = next_responder_id();
    pending_responses().insert(id, tx);
    (id, rx, PendingResponse { id })
}

fn error_response(status: u16, message: &'static [u8]) -> Response<Hyper4kBody> {
    Response::builder()
        .status(status)
        .body(Hyper4kBody::full(Bytes::from_static(message)))
        .expect("static error response must build")
}

/// Builds the hyper response for one delivery.
fn build_response(delivery: Delivery) -> Response<Hyper4kBody> {
    let (status, headers, body) = match delivery {
        Delivery::Buffered(data) => (
            data.status,
            data.headers,
            Hyper4kBody::full(Bytes::from(data.body)),
        ),
        Delivery::Stream(start) => (
            start.status,
            start.headers,
            // No Content-Length: size_hint is unknown, so hyper frames the body itself.
            Hyper4kBody::stream(start.rx),
        ),
    };

    let mut builder = Response::builder().status(status);
    for (k, v) in &headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    builder
        .body(body)
        .unwrap_or_else(|_| error_response(500, b"hyper4k: bad response"))
}

pub struct Hyper4kServer {
    // drop 时关闭 runtime
    _runtime: Runtime,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

// ---------------------------------------------------------------------------
// 请求处理（Tokio 端）
// ---------------------------------------------------------------------------

async fn handle(
    req: Request<Incoming>,
    ctx: Arc<CallbackCtx>,
) -> Result<Response<Hyper4kBody>, Infallible> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();

    let mut header_buf = String::new();
    for (name, value) in req.headers() {
        header_buf.push_str(name.as_str());
        header_buf.push_str(": ");
        header_buf.push_str(value.to_str().unwrap_or(""));
        header_buf.push('\n');
    }

    // v1：聚合 body（流式版本未来用 Incoming 的 frame stream 实现）
    let body: Bytes = match http_body_util::Limited::new(req.into_body(), MAX_REQUEST_BODY_BYTES)
        .collect()
        .await
    {
        Ok(c) => c.to_bytes(),
        Err(_) => {
            return Ok(error_response(413, b"hyper4k: request body too large"));
        }
    };

    let (responder, rx, _pending) = register_response();

    // 这些局部变量（method/path/query/header_buf/body）在 await 期间保持存活，
    // 因此借用给 Kotlin 的切片在 hyper4k_respond 被调用前始终有效。
    IN_CALLBACK.set(true);
    {
        let creq = Hyper4kRequest {
            method: Hyper4kSlice::borrow(method.as_bytes()),
            path: Hyper4kSlice::borrow(path.as_bytes()),
            query: Hyper4kSlice::borrow(query.as_bytes()),
            headers: Hyper4kSlice::borrow(header_buf.as_bytes()),
            body: Hyper4kSlice::borrow(&body),
            responder,
        };

        // 调进 Kotlin。同步完成的 handler 会在回调内直接调 hyper4k_respond，
        // 响应落入线程本地槽；异步路径照旧走 responder 通道。
        (ctx.cb)(ctx.user_data, &creq as *const Hyper4kRequest);
    }
    IN_CALLBACK.set(false);

    let delivery = match take_sync_response() {
        // Sync path: the _pending guard clears the registry when handle returns.
        // begin() lands in this same slot when it runs inside the callback.
        Some(delivery) => delivery,
        None => match rx.await {
            Ok(d) => d,
            Err(_) => {
                return Ok(error_response(500, b"hyper4k: handler dropped responder"));
            }
        },
    };

    Ok(build_response(delivery))
}

// ---------------------------------------------------------------------------
// C ABI 导出
// ---------------------------------------------------------------------------

/// 启动服务器。失败返回 NULL。
///
/// # Safety
/// `host` 必须是合法的 NUL 结尾 C 字符串；`user_data` 在 server 存活期间必须有效。
#[no_mangle]
pub unsafe extern "C" fn hyper4k_server_start(
    host: *const c_char,
    port: u16,
    on_request: Hyper4kRequestCallback,
    user_data: *mut c_void,
) -> *mut Hyper4kServer {
    let host = if host.is_null() {
        "0.0.0.0".to_owned()
    } else {
        CStr::from_ptr(host)
            .to_str()
            .unwrap_or("0.0.0.0")
            .to_owned()
    };

    let addr: SocketAddr = match format!("{host}:{port}").parse() {
        Ok(a) => a,
        Err(_) => return std::ptr::null_mut(),
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(_) => return std::ptr::null_mut(),
    };

    // 同步绑定，便于把“端口占用”作为 NULL 返回报告给上层。
    let listener = match runtime.block_on(async { TcpListener::bind(addr).await }) {
        Ok(l) => l,
        Err(_) => return std::ptr::null_mut(),
    };

    let ctx = Arc::new(CallbackCtx {
        cb: on_request,
        user_data,
    });
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    runtime.spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accept = listener.accept() => {
                    let (stream, _peer) = match accept {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let io = TokioIo::new(stream);
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        let service = service_fn(move |req| handle(req, ctx.clone()));
                        // auto::Builder sniffs the connection preface, so a single
                        // port serves h1 and h2c. No ALPN: TLS terminates upstream.
                        let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .await;
                    });
                }
            }
        }
    });

    Box::into_raw(Box::new(Hyper4kServer {
        _runtime: runtime,
        shutdown_tx: Some(shutdown_tx),
    }))
}

/// 完成一个请求（拷贝版，保留给不便移交所有权的调用方）。返回 1 表示响应已交付，
/// 0 表示句柄已失效或已经完成。
///
/// # Safety
/// 响应缓冲必须在本次调用期间保持有效。
#[no_mangle]
pub unsafe extern "C" fn hyper4k_respond(
    responder: u64,
    status: u16,
    headers_ptr: *const u8,
    headers_len: usize,
    body_ptr: *const u8,
    body_len: usize,
) -> i32 {
    if responder == 0 {
        return 0;
    }

    let headers = parse_headers(headers_ptr, headers_len);
    let body = if body_ptr.is_null() || body_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(body_ptr, body_len).to_vec()
    };

    if active_streams().contains_key(&responder) {
        // Already streaming: one-shot and streaming are mutually exclusive.
        return HYPER4K_ERR_WRONG_STATE;
    }

    deliver_response(
        responder,
        Delivery::Buffered(ResponseData {
            status,
            headers,
            body,
        }),
    )
}

fn deliver_response(responder: u64, delivery: Delivery) -> i32 {
    if IN_CALLBACK.get() {
        // 同步快路径：响应直接交给 handle()，跳过通道唤醒。
        return i32::from(set_sync_response(delivery));
    }
    let sender = pending_responses().remove(&responder);
    i32::from(
        sender
            .map(|(_, tx)| tx.send(delivery).is_ok())
            .unwrap_or(false),
    )
}

/// Starts a streaming response: status and headers go out now, the body follows
/// as write() chunks.
///
/// Returns `HYPER4K_OK`, or `HYPER4K_ERR_WRONG_STATE` if the responder is already
/// completed, already streaming, or stale.
///
/// # Safety
/// The header buffer must stay valid for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_response_begin(
    responder: u64,
    status: u16,
    headers_ptr: *const u8,
    headers_len: usize,
) -> i32 {
    if responder == 0 {
        return HYPER4K_ERR_WRONG_STATE;
    }

    let headers = parse_headers(headers_ptr, headers_len);
    let (tx, rx) = mpsc::channel::<Bytes>(STREAM_CHANNEL_CAPACITY);

    // Claim the state before delivering: another thread can call write() before
    // begin() returns, and a missing entry would be misread as WRONG_STATE.
    // entry() rather than insert(): a repeated begin must be refused, never replace
    // the sender of a stream already in flight.
    match active_streams().entry(responder) {
        dashmap::mapref::entry::Entry::Occupied(_) => return HYPER4K_ERR_WRONG_STATE,
        dashmap::mapref::entry::Entry::Vacant(slot) => {
            slot.insert(tx);
        }
    }

    let delivered = deliver_response(
        responder,
        Delivery::Stream(StreamStart {
            status,
            headers,
            rx,
        }),
    );

    if delivered != HYPER4K_OK {
        active_streams().remove(&responder);
        return HYPER4K_ERR_WRONG_STATE;
    }
    HYPER4K_OK
}

/// Writes one body chunk. The data is copied during the call, so the caller may
/// release its buffer on return.
///
/// Blocks the calling thread while the downstream is not writable — that is the
/// backpressure. Call it only where blocking is safe; on an engine thread it
/// returns `HYPER4K_ERR_WOULD_BLOCK` instead of stalling the runtime.
///
/// `HYPER4K_ERR_CLIENT_GONE` means the client is gone: stop producing data and
/// call finish. That is a normal path, not an error.
///
/// # Safety
/// The chunk buffer must stay valid for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_response_write(
    responder: u64,
    chunk_ptr: *const u8,
    chunk_len: usize,
) -> i32 {
    if responder == 0 {
        return HYPER4K_ERR_WRONG_STATE;
    }
    // Clone the sender and drop the guard: never block while holding a shard lock.
    let sender = match active_streams().get(&responder) {
        Some(entry) => entry.value().clone(),
        None => return HYPER4K_ERR_WRONG_STATE,
    };

    if chunk_len == 0 {
        // An empty chunk carries no meaning, and a zero-length HTTP/1.1 chunk ends a stream.
        return HYPER4K_OK;
    }
    if chunk_ptr.is_null() {
        return HYPER4K_ERR_WRONG_STATE;
    }
    let chunk = Bytes::copy_from_slice(std::slice::from_raw_parts(chunk_ptr, chunk_len));

    match sender.try_send(chunk) {
        Ok(()) => HYPER4K_OK,
        Err(mpsc::error::TrySendError::Closed(_)) => HYPER4K_ERR_CLIENT_GONE,
        Err(mpsc::error::TrySendError::Full(chunk)) => {
            // A full channel means the client is behind and we have to wait for room.
            // Blocking a runtime thread would starve the engine, and blocking_send
            // panics in async context, which under panic = "abort" kills the process.
            // Report it instead, so a misplaced write fails loudly rather than slowly.
            if Handle::try_current().is_ok() {
                return HYPER4K_ERR_WOULD_BLOCK;
            }
            match sender.blocking_send(chunk) {
                Ok(()) => HYPER4K_OK,
                Err(_) => HYPER4K_ERR_CLIENT_GONE,
            }
        }
    }
}

/// Ends the streaming response and releases the responder.
///
/// Idempotent: a repeated call returns `HYPER4K_ERR_WRONG_STATE`, not UB.
#[no_mangle]
pub extern "C" fn hyper4k_response_finish(responder: u64) -> i32 {
    if responder == 0 {
        return HYPER4K_ERR_WRONG_STATE;
    }
    // Removing the entry drops the last sender, so the body's recv ends the stream.
    match active_streams().remove(&responder) {
        Some(_) => HYPER4K_OK,
        None => HYPER4K_ERR_WRONG_STATE,
    }
}

/// 优雅停止并释放服务器。
///
/// # Safety
/// `server` 必须是 `hyper4k_server_start` 返回且未被 stop 过的指针。
#[no_mangle]
pub unsafe extern "C" fn hyper4k_server_stop(server: *mut Hyper4kServer) {
    if server.is_null() {
        return;
    }
    let mut s = Box::from_raw(server);
    if let Some(tx) = s.shutdown_tx.take() {
        let _ = tx.send(());
    }
    // drop(s) -> drop(runtime) 关闭所有 worker。
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

/// 解析 "Name: Value\n" 文本块为 (name, value) 列表。
unsafe fn parse_headers(ptr: *const u8, len: usize) -> Vec<(String, String)> {
    if ptr.is_null() || len == 0 {
        return Vec::new();
    }
    let raw = std::slice::from_raw_parts(ptr, len);
    let text = std::str::from_utf8(raw).unwrap_or("");
    text.lines()
        .filter_map(|line| {
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                return None;
            }
            let idx = line.find(':')?;
            let name = line[..idx].trim().to_owned();
            let value = line[idx + 1..].trim().to_owned();
            if name.is_empty() {
                None
            } else {
                Some((name, value))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        active_streams, hyper4k_respond, hyper4k_response_begin, hyper4k_response_finish,
        hyper4k_response_write, hyper4k_server_start, hyper4k_server_stop, parse_headers,
        register_response, take_sync_response, Delivery, Hyper4kRequest, ResponseData,
        HYPER4K_ERR_CLIENT_GONE, HYPER4K_ERR_WRONG_STATE, HYPER4K_OK, IN_CALLBACK,
    };
    use std::ffi::{c_void, CString};
    use std::io::{Read, Write};
    use std::net::{TcpListener as StdTcpListener, TcpStream};
    use std::sync::mpsc as std_mpsc;
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::Duration;

    /// Unwraps a one-shot delivery; a stream is a failure in these cases.
    fn buffered(delivery: Delivery) -> ResponseData {
        match delivery {
            Delivery::Buffered(data) => data,
            Delivery::Stream(_) => panic!("expected a buffered delivery, got a stream"),
        }
    }

    /// Picks a free port and releases it for the server under test.
    fn free_port() -> u16 {
        let probe = StdTcpListener::bind("127.0.0.1:0").expect("allocate test port");
        let port = probe.local_addr().expect("test address").port();
        drop(probe);
        port
    }

    #[test]
    fn parses_header_block() {
        let raw = b"Content-Type: application/json\nX-Request-Id: abc\r\n";
        let headers = unsafe { parse_headers(raw.as_ptr(), raw.len()) };

        assert_eq!(
            headers,
            vec![
                ("Content-Type".to_owned(), "application/json".to_owned()),
                ("X-Request-Id".to_owned(), "abc".to_owned()),
            ]
        );
    }

    #[test]
    fn ignores_invalid_header_lines() {
        let raw = b"invalid\n: empty-name\nValid: value\n";
        let headers = unsafe { parse_headers(raw.as_ptr(), raw.len()) };

        assert_eq!(headers, vec![("Valid".to_owned(), "value".to_owned())]);
    }

    #[test]
    fn accepts_empty_header_block() {
        let headers = unsafe { parse_headers(std::ptr::null(), 0) };
        assert!(headers.is_empty());
    }

    #[test]
    fn responder_is_single_use() {
        let (responder, mut receiver, registration) = register_response();
        let body = b"ok";

        let first = unsafe {
            hyper4k_respond(
                responder,
                200,
                std::ptr::null(),
                0,
                body.as_ptr(),
                body.len(),
            )
        };
        let second =
            unsafe { hyper4k_respond(responder, 500, std::ptr::null(), 0, std::ptr::null(), 0) };

        assert_eq!(first, 1);
        assert_eq!(second, 0);
        assert_eq!(buffered(receiver.try_recv().expect("response")).status, 200);
        drop(registration);
    }

    #[test]
    fn late_response_is_rejected_after_request_is_dropped() {
        // 连接断开后没有接收方：用非零但不存在的 responder 模拟。
        let delivered = unsafe {
            hyper4k_respond(
                u64::MAX,
                200,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
            )
        };

        assert_eq!(delivered, 0);
    }

    #[test]
    fn sync_respond_inside_callback_is_captured_by_slot() {
        let body = b"sync-body";
        let delivered = unsafe {
            IN_CALLBACK.set(true);
            let r = hyper4k_respond(
                42,
                200,
                std::ptr::null(),
                0,
                body.as_ptr(),
                body.len(),
            );
            IN_CALLBACK.set(false);
            r
        };
        assert_eq!(delivered, 1);

        let data = buffered(take_sync_response().expect("sync response"));
        assert_eq!(data.status, 200);
        assert_eq!(data.body, b"sync-body");

        // 槽位已清空后，同线程再次同步响应仍能落入。
        let delivered2 = unsafe {
            IN_CALLBACK.set(true);
            let r = hyper4k_respond(7, 201, std::ptr::null(), 0, b"x".as_ptr(), 1);
            IN_CALLBACK.set(false);
            r
        };
        assert_eq!(delivered2, 1);
        assert_eq!(buffered(take_sync_response().unwrap()).status, 201);
    }

    // -----------------------------------------------------------------------
    // ABI v3: state machine
    // -----------------------------------------------------------------------

    #[test]
    fn oneshot_and_streaming_are_mutually_exclusive() {
        let (responder, mut receiver, registration) = register_response();

        assert_eq!(
            unsafe { hyper4k_response_begin(responder, 200, std::ptr::null(), 0) },
            HYPER4K_OK
        );
        // Already streaming: the one-shot path is refused, and it is not UB.
        assert_eq!(
            unsafe { hyper4k_respond(responder, 500, std::ptr::null(), 0, std::ptr::null(), 0) },
            HYPER4K_ERR_WRONG_STATE
        );
        // A repeated begin is refused and must not replace the stream in flight.
        assert_eq!(
            unsafe { hyper4k_response_begin(responder, 204, std::ptr::null(), 0) },
            HYPER4K_ERR_WRONG_STATE
        );

        match receiver.try_recv().expect("stream start") {
            Delivery::Stream(start) => assert_eq!(start.status, 200),
            Delivery::Buffered(_) => panic!("expected a stream start"),
        }

        assert_eq!(hyper4k_response_finish(responder), HYPER4K_OK);
        // finish is idempotent: the second call is WRONG_STATE, not a double free.
        assert_eq!(
            hyper4k_response_finish(responder),
            HYPER4K_ERR_WRONG_STATE
        );
        // Writing after finish fails safely.
        assert_eq!(
            unsafe { hyper4k_response_write(responder, b"x".as_ptr(), 1) },
            HYPER4K_ERR_WRONG_STATE
        );
        drop(registration);
    }

    #[test]
    fn write_without_begin_is_rejected() {
        assert_eq!(
            unsafe { hyper4k_response_write(u64::MAX, b"x".as_ptr(), 1) },
            HYPER4K_ERR_WRONG_STATE
        );
        assert_eq!(
            hyper4k_response_finish(u64::MAX),
            HYPER4K_ERR_WRONG_STATE
        );
    }

    #[test]
    fn write_after_client_disconnect_reports_client_gone() {
        let (responder, mut receiver, registration) = register_response();
        assert_eq!(
            unsafe { hyper4k_response_begin(responder, 200, std::ptr::null(), 0) },
            HYPER4K_OK
        );

        // Dropping the receiver stands in for a dead connection.
        let start = match receiver.try_recv().expect("stream start") {
            Delivery::Stream(start) => start,
            Delivery::Buffered(_) => panic!("expected a stream start"),
        };
        drop(start);

        assert_eq!(
            unsafe { hyper4k_response_write(responder, b"late".as_ptr(), 4) },
            HYPER4K_ERR_CLIENT_GONE
        );
        // finish is still required once the client is gone, or the responder leaks.
        assert_eq!(hyper4k_response_finish(responder), HYPER4K_OK);
        assert!(!active_streams().contains_key(&responder));
        drop(registration);
    }

    // -----------------------------------------------------------------------
    // ABI v3: end-to-end streaming
    // -----------------------------------------------------------------------

    /// Gate between the handler thread and the test thread: `first_sent` reports
    /// that event one is out, `release` allows the last event to be written.
    struct SseGate {
        first_sent: std_mpsc::Sender<()>,
        release: Mutex<std_mpsc::Receiver<()>>,
    }
    static SSE_GATE: OnceLock<SseGate> = OnceLock::new();

    extern "C" fn sse_handler(_user_data: *mut c_void, request: *const Hyper4kRequest) {
        let responder = unsafe { (*request).responder };
        // Writes must leave the engine thread: hyper4k_response_write blocks.
        thread::spawn(move || {
            let headers = b"Content-Type: text/event-stream\nCache-Control: no-cache\n";
            assert_eq!(
                unsafe {
                    hyper4k_response_begin(responder, 200, headers.as_ptr(), headers.len())
                },
                HYPER4K_OK
            );

            let gate = SSE_GATE.get().expect("gate installed");
            let first = b"data: event-1\n\n";
            assert_eq!(
                unsafe { hyper4k_response_write(responder, first.as_ptr(), first.len()) },
                HYPER4K_OK
            );
            gate.first_sent.send(()).expect("signal first event");

            // The last event is not written until the test confirms it read the first.
            gate.release
                .lock()
                .expect("gate lock")
                .recv()
                .expect("await release");

            let last = b"data: event-2\n\n";
            assert_eq!(
                unsafe { hyper4k_response_write(responder, last.as_ptr(), last.len()) },
                HYPER4K_OK
            );
            assert_eq!(hyper4k_response_finish(responder), HYPER4K_OK);
        });
    }

    #[test]
    fn streams_first_event_before_last_event_is_produced() {
        let (first_tx, first_rx) = std_mpsc::channel::<()>();
        let (release_tx, release_rx) = std_mpsc::channel::<()>();
        SSE_GATE
            .set(SseGate {
                first_sent: first_tx,
                release: Mutex::new(release_rx),
            })
            .unwrap_or_else(|_| panic!("gate installed twice"));

        let port = free_port();
        let host = CString::new("127.0.0.1").expect("host");
        let server =
            unsafe { hyper4k_server_start(host.as_ptr(), port, sse_handler, std::ptr::null_mut()) };
        assert!(!server.is_null());

        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        stream
            .write_all(b"GET /events HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .expect("write request");

        // Read until the first event arrives; the response is still open at that point.
        let mut seen = Vec::new();
        let mut buf = [0u8; 1024];
        while !String::from_utf8_lossy(&seen).contains("data: event-1") {
            let n = stream.read(&mut buf).expect("read first event");
            assert_ne!(n, 0, "connection closed before the first event arrived");
            seen.extend_from_slice(&buf[..n]);
        }

        let head = String::from_utf8_lossy(&seen).to_string();
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        // The test for real streaming: the client holds event one while the server
        // has not sent the last one. The handler is parked on the gate, so this holds
        // structurally rather than by timing.
        assert!(
            !head.contains("data: event-2"),
            "response was buffered, not streamed: {head}"
        );
        first_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("handler reported the first event");

        release_tx.send(()).expect("release handler");
        let mut rest = String::new();
        stream.read_to_string(&mut rest).expect("read remainder");
        assert!(rest.contains("data: event-2"), "{rest}");

        unsafe { hyper4k_server_stop(server) };
    }

    // -----------------------------------------------------------------------
    // ABI v3: h2c
    // -----------------------------------------------------------------------

    #[test]
    fn accepts_http2_prior_knowledge_connections() {
        let port = free_port();
        let host = CString::new("127.0.0.1").expect("host");
        let server =
            unsafe { hyper4k_server_start(host.as_ptr(), port, test_handler, std::ptr::null_mut()) };
        assert!(!server.is_null());

        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        // h2 preface plus an empty SETTINGS frame: what --http2-prior-knowledge sends.
        stream
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .expect("write preface");
        stream
            .write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0])
            .expect("write SETTINGS");

        let mut frame_header = [0u8; 9];
        stream
            .read_exact(&mut frame_header)
            .expect("server must answer the h2 preface");
        // The first frame must be SETTINGS(0x04); an h1 parse would answer "HTTP/1.1 400".
        assert_eq!(
            frame_header[3], 0x04,
            "expected a SETTINGS frame, got {frame_header:?}"
        );

        unsafe { hyper4k_server_stop(server) };
    }

    extern "C" fn test_handler(_user_data: *mut c_void, request: *const Hyper4kRequest) {
        let body = b"hello from hyper4k";
        let headers = b"Content-Type: text/plain\nConnection: close\n";
        unsafe {
            hyper4k_respond(
                (*request).responder,
                201,
                headers.as_ptr(),
                headers.len(),
                body.as_ptr(),
                body.len(),
            );
        }
    }

    extern "C" fn async_test_handler(_user_data: *mut c_void, request: *const Hyper4kRequest) {
        let responder = unsafe { (*request).responder };
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let body = b"async response";
            let delivered = unsafe {
                hyper4k_respond(
                    responder,
                    200,
                    std::ptr::null(),
                    0,
                    body.as_ptr(),
                    body.len(),
                )
            };
            assert_eq!(delivered, 1);
        });
    }

    #[test]
    fn serves_request_through_c_abi() {
        let probe = StdTcpListener::bind("127.0.0.1:0").expect("allocate test port");
        let port = probe.local_addr().expect("test address").port();
        drop(probe);

        let host = CString::new("127.0.0.1").expect("host");
        let server = unsafe {
            hyper4k_server_start(host.as_ptr(), port, test_handler, std::ptr::null_mut())
        };
        assert!(!server.is_null());

        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .expect("write request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");

        assert!(response.starts_with("HTTP/1.1 201 Created"), "{response}");
        assert!(response.ends_with("hello from hyper4k"), "{response}");
        unsafe { hyper4k_server_stop(server) };
    }

    #[test]
    fn responder_remains_valid_after_callback_returns() {
        let probe = StdTcpListener::bind("127.0.0.1:0").expect("allocate test port");
        let port = probe.local_addr().expect("test address").port();
        drop(probe);

        let host = CString::new("127.0.0.1").expect("host");
        let server = unsafe {
            hyper4k_server_start(
                host.as_ptr(),
                port,
                async_test_handler,
                std::ptr::null_mut(),
            )
        };
        assert!(!server.is_null());

        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .write_all(b"GET /async HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .expect("write request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.ends_with("async response"), "{response}");
        unsafe { hyper4k_server_stop(server) };
    }
}
