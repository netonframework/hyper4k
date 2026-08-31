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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use dashmap::DashMap;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

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

static NEXT_RESPONDER_ID: AtomicU64 = AtomicU64::new(1);
static PENDING_RESPONSES: OnceLock<DashMap<u64, oneshot::Sender<ResponseData>>> = OnceLock::new();

// ABI v2 同步快路径：回调在 Tokio worker 线程上执行，若 handler 在回调内同步完成，
// hyper4k_respond 直接把响应写入线程本地槽，省掉 DashMap 注册 + oneshot 交接。
thread_local! {
    static IN_CALLBACK: Cell<bool> = const { Cell::new(false) };
    static SYNC_RESPONSE: std::cell::RefCell<Option<ResponseData>> = const { std::cell::RefCell::new(None) };
}

fn take_sync_response() -> Option<ResponseData> {
    SYNC_RESPONSE.with(|slot| slot.borrow_mut().take())
}

/// 尝试把同步响应写入线程本地槽；槽已被占用（重复响应）时返回 false。
fn set_sync_response(data: ResponseData) -> bool {
    SYNC_RESPONSE.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_some() {
            return false;
        }
        *slot = Some(data);
        true
    })
}

fn pending_responses() -> &'static DashMap<u64, oneshot::Sender<ResponseData>> {
    PENDING_RESPONSES.get_or_init(DashMap::new)
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
fn register_response() -> (u64, oneshot::Receiver<ResponseData>, PendingResponse) {
    let (tx, rx) = oneshot::channel::<ResponseData>();
    let id = next_responder_id();
    pending_responses().insert(id, tx);
    (id, rx, PendingResponse { id })
}

/// 由 ResponseData 构造 hyper 响应（单态 Full 体，避免动态分发开销）。
fn build_response(resp_data: ResponseData) -> Response<Full<Bytes>> {
    let mut builder = Response::builder().status(resp_data.status);
    for (k, v) in &resp_data.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    builder
        .body(Full::new(Bytes::from(resp_data.body)))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(500)
                .body(Full::new(Bytes::from_static(b"hyper4k: bad response")))
                .expect("static 500 response must build")
        })
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
) -> Result<Response<Full<Bytes>>, Infallible> {
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
            return Ok(Response::builder()
                .status(413)
                .body(Full::new(Bytes::from_static(
                    b"hyper4k: request body too large",
                )))
                .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))));
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

    let resp_data = match take_sync_response() {
        // 同步路径：通道注册条目由 _pending 守卫在 handle 结束时清理，
        // 这里不再额外碰 DashMap。
        Some(data) => data,
        None => match rx.await {
            Ok(d) => d,
            Err(_) => ResponseData {
                status: 500,
                headers: Vec::new(),
                body: b"hyper4k: handler dropped responder".to_vec(),
            },
        },
    };

    Ok(build_response(resp_data))
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
                        let _ = hyper::server::conn::http1::Builder::new()
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

    deliver_response(responder, ResponseData { status, headers, body })
}

fn deliver_response(responder: u64, data: ResponseData) -> i32 {
    if IN_CALLBACK.get() {
        // 同步快路径：响应直接交给 handle()，跳过通道唤醒。
        return i32::from(set_sync_response(data));
    }
    let sender = pending_responses().remove(&responder);
    i32::from(sender.map(|(_, tx)| tx.send(data).is_ok()).unwrap_or(false))
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
        hyper4k_respond, hyper4k_server_start, hyper4k_server_stop, parse_headers,
        register_response, take_sync_response, Hyper4kRequest, IN_CALLBACK,
    };
    use std::ffi::{c_void, CString};
    use std::io::{Read, Write};
    use std::net::{TcpListener as StdTcpListener, TcpStream};
    use std::thread;
    use std::time::Duration;

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
        assert_eq!(receiver.try_recv().expect("response").status, 200);
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

        let data = take_sync_response().expect("sync response");
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
        assert_eq!(take_sync_response().unwrap().status, 201);
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
