//! hyper4k —— Tokio + Hyper HTTP 引擎，通过零拷贝 C ABI 暴露。
//!
//! 这一层**只做协议与传输**：accept / parse / body 聚合 / 写回 / 连接生命周期。
//! 路由、中间件、handler 一律由上层（Kotlin / Neton）负责。
//!
//! 详细 ABI 契约见 `include/hyper4k.h`。

use std::convert::Infallible;
use std::ffi::{c_char, c_void, CStr};
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

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
        Hyper4kSlice { ptr: b.as_ptr(), len: b.len() }
    }
}

#[repr(C)]
pub struct Hyper4kRequest {
    pub method: Hyper4kSlice,
    pub path: Hyper4kSlice,
    pub query: Hyper4kSlice,
    pub headers: Hyper4kSlice,
    pub body: Hyper4kSlice,
    pub responder: *mut Hyper4kResponder,
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

/// 每请求一个，承载把响应送回等待中的连接 future 的 oneshot。
pub struct Hyper4kResponder {
    tx: Option<oneshot::Sender<ResponseData>>,
}

pub struct Hyper4kServer {
    // drop 时关闭 runtime
    runtime: Runtime,
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
    let body: Bytes = match req.into_body().collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => Bytes::new(),
    };

    let (tx, rx) = oneshot::channel::<ResponseData>();
    let responder: *mut Hyper4kResponder =
        Box::into_raw(Box::new(Hyper4kResponder { tx: Some(tx) }));

    // 这些局部变量（method/path/query/header_buf/body）在 await 期间保持存活，
    // 因此借用给 Kotlin 的切片在 hyper4k_respond 被调用前始终有效。
    let creq = Hyper4kRequest {
        method: Hyper4kSlice::borrow(method.as_bytes()),
        path: Hyper4kSlice::borrow(path.as_bytes()),
        query: Hyper4kSlice::borrow(query.as_bytes()),
        headers: Hyper4kSlice::borrow(header_buf.as_bytes()),
        body: Hyper4kSlice::borrow(&body),
        responder,
    };

    // 调进 Kotlin。约定：尽快返回，稍后（可在另一线程）调用 hyper4k_respond。
    (ctx.cb)(ctx.user_data, &creq as *const Hyper4kRequest);

    let resp_data = match rx.await {
        Ok(d) => d,
        Err(_) => ResponseData {
            status: 500,
            headers: Vec::new(),
            body: b"hyper4k: handler dropped responder".to_vec(),
        },
    };

    // Kotlin 已经 respond 完毕，可以安全回收 responder。
    unsafe { drop(Box::from_raw(responder)); }

    let mut builder = Response::builder().status(resp_data.status);
    for (k, v) in &resp_data.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    let resp = builder
        .body(Full::new(Bytes::from(resp_data.body)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from_static(b"hyper4k: bad response"))));

    Ok(resp)
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
        CStr::from_ptr(host).to_str().unwrap_or("0.0.0.0").to_owned()
    };

    let addr: SocketAddr = match format!("{host}:{port}").parse() {
        Ok(a) => a,
        Err(_) => return std::ptr::null_mut(),
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(r) => r,
        Err(_) => return std::ptr::null_mut(),
    };

    // 同步绑定，便于把“端口占用”作为 NULL 返回报告给上层。
    let listener = match runtime.block_on(async { TcpListener::bind(addr).await }) {
        Ok(l) => l,
        Err(_) => return std::ptr::null_mut(),
    };

    let ctx = Arc::new(CallbackCtx { cb: on_request, user_data });
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

    Box::into_raw(Box::new(Hyper4kServer { runtime, shutdown_tx: Some(shutdown_tx) }))
}

/// 完成一个请求。每个 responder 只能调用一次。
///
/// # Safety
/// `responder` 必须是回调中收到的、尚未被 respond 过的有效指针。
#[no_mangle]
pub unsafe extern "C" fn hyper4k_respond(
    responder: *mut Hyper4kResponder,
    status: u16,
    headers_ptr: *const u8,
    headers_len: usize,
    body_ptr: *const u8,
    body_len: usize,
) {
    if responder.is_null() {
        return;
    }
    let r = &mut *responder;

    let headers = parse_headers(headers_ptr, headers_len);
    let body = if body_ptr.is_null() || body_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(body_ptr, body_len).to_vec()
    };

    if let Some(tx) = r.tx.take() {
        let _ = tx.send(ResponseData { status, headers, body });
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
            if name.is_empty() { None } else { Some((name, value)) }
        })
        .collect()
}
