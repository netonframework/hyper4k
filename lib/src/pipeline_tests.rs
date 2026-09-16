//! Controlled HTTP/1 flush-policy experiment through the production C callback path.
use super::*;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;

extern "C" fn echo(_data: *mut c_void, request: *const Hyper4kRequest) {
    unsafe {
        let request = &*request;
        let mut body = std::slice::from_raw_parts(request.path.ptr, request.path.len).to_vec();
        body.extend_from_slice(std::slice::from_raw_parts(
            request.body.ptr,
            request.body.len,
        ));
        hyper4k_respond(
            request.responder,
            200,
            std::ptr::null(),
            0,
            body.as_ptr(),
            body.len(),
        );
    }
}

struct Server {
    addr: SocketAddr,
    task: JoinHandle<()>,
    release: Arc<Notify>,
}

impl Server {
    async fn start(flush: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let release = Arc::new(Notify::new());
        let gate = release.clone();
        let task = tokio::spawn(async move {
            let ctx = Arc::new(CallbackCtx {
                cb: echo,
                user_data: std::ptr::null_mut(),
            });
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, peer) = accepted.unwrap();
                        let ctx = ctx.clone();
                        let gate = gate.clone();
                        let peer: Arc<str> = Arc::from(peer.to_string());
                        connections.spawn(async move {
                            let service = service_fn(move |req: Request<Incoming>| {
                                let ctx = ctx.clone();
                                let peer = peer.clone();
                                let gate = gate.clone();
                                async move {
                                    if req.uri().path() == "/slow" { gate.notified().await; }
                                    handle(req, ctx, peer).await
                                }
                            });
                            let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                            builder.http1().pipeline_flush(flush);
                            let _ = builder.serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            addr,
            task,
            release,
        }
    }

    async fn connect(&self) -> BufReader<TcpStream> {
        BufReader::new(TcpStream::connect(self.addr).await.unwrap())
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn response(client: &mut BufReader<TcpStream>) -> Vec<u8> {
    timeout(Duration::from_secs(5), async {
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        assert!(line.starts_with("HTTP/1.1 200 "), "{line}");
        let mut length = None;
        loop {
            line.clear();
            assert_ne!(client.read_line(&mut line).await.unwrap(), 0);
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = Some(value.trim().parse::<usize>().unwrap());
            }
        }
        let mut body = vec![0; length.expect("buffered response length")];
        client.read_exact(&mut body).await.unwrap();
        body
    })
    .await
    .expect("response must not wait for another request")
}

#[tokio::test]
async fn flush_policy_preserves_mixed_pipeline_order_and_bodies() {
    for flush in [false, true] {
        let server = Server::start(flush).await;
        let mut client = server.connect().await;
        let batch = b"GET /get HTTP/1.1\r\nHost: local\r\n\r\nPOST /cl HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\n\r\nabcPOST /chunk HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n3\r\ndef\r\n0\r\n\r\n";
        for _ in 0..16 {
            client.get_mut().write_all(batch).await.unwrap();
            for expected in [b"/get".as_slice(), b"/clabc", b"/chunkdef"] {
                assert_eq!(response(&mut client).await, expected);
            }
        }
        drop(client);
        server.stop().await;
    }
}

#[tokio::test]
async fn flush_policy_does_not_hold_response_behind_slow_handler() {
    for flush in [false, true] {
        let server = Server::start(flush).await;
        let mut client = server.connect().await;
        client.get_mut().write_all(b"GET /first HTTP/1.1\r\nHost: local\r\n\r\nGET /slow HTTP/1.1\r\nHost: local\r\n\r\n").await.unwrap();
        assert_eq!(response(&mut client).await, b"/first");
        server.release.notify_one();
        assert_eq!(response(&mut client).await, b"/slow");
        drop(client);
        server.stop().await;
    }
}

#[tokio::test]
async fn flush_policy_does_not_wait_for_incomplete_next_request() {
    {
        let server = Server::start(false).await;
        let mut client = server.connect().await;
        client
            .get_mut()
            .write_all(b"GET /first HTTP/1.1\r\nHost: local\r\n\r\nGET /next HTTP/1.1\r\nHost:")
            .await
            .unwrap();
        assert_eq!(response(&mut client).await, b"/first");
        client.get_mut().write_all(b" local\r\n\r\n").await.unwrap();
        assert_eq!(response(&mut client).await, b"/next");
        drop(client);
        server.stop().await;
    }
}

#[tokio::test]
#[ignore = "documents why experimental pipeline_flush is not enabled in production"]
async fn experimental_flush_stalls_until_the_partial_request_is_completed() {
    let server = Server::start(true).await;
    let mut client = server.connect().await;
    client
        .get_mut()
        .write_all(b"GET /first HTTP/1.1\r\nHost: local\r\n\r\nGET /next HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(250), client.fill_buf())
        .await
        .is_err());
    client.get_mut().write_all(b" local\r\n\r\n").await.unwrap();
    assert_eq!(response(&mut client).await, b"/first");
    assert_eq!(response(&mut client).await, b"/next");
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local release-mode TCP experiment, not an Arena throughput claim"]
async fn compare_flush_policies() {
    for depth in [1, 16] {
        for (round, flush) in [false, true, true, false, false, true]
            .into_iter()
            .enumerate()
        {
            let server = Server::start(flush).await;
            let mut clients = Vec::new();
            for _ in 0..16 {
                clients.push(server.connect().await);
            }
            let start = Instant::now();
            let mut workers = JoinSet::new();
            for mut client in clients {
                workers.spawn(async move {
                    let request = b"GET /7 HTTP/1.1\r\nHost: local\r\n\r\n".repeat(depth);
                    for _ in 0..200 {
                        client.get_mut().write_all(&request).await.unwrap();
                        for _ in 0..depth {
                            assert_eq!(response(&mut client).await, b"/7");
                        }
                    }
                });
            }
            while let Some(result) = workers.join_next().await {
                result.unwrap();
            }
            let elapsed = start.elapsed().as_secs_f64();
            println!("depth={depth} round={round} flush={flush} requests={} seconds={elapsed:.6} rps={:.0}", 16 * 200 * depth, (16 * 200 * depth) as f64 / elapsed);
            server.stop().await;
        }
    }
}
