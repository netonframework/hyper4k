//! Proxy tests (ABI 4.1). A tiny forward proxy runs in-process: absolute-form
//! requests are forwarded to their origin, CONNECT opens a tunnel.
use super::handle::*;
use super::handle_tests::{send_and_wait, Capture};
use super::tls_tests::{spawn_tls_server, TlsFixture};
use super::*;
use crate::abi::*;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What the proxy saw: one request line per accepted connection.
type Seen = Arc<Mutex<Vec<String>>>;

async fn read_head(sock: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    while let Ok(n) = sock.read(&mut tmp).await {
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    buf
}

/// `refuse_connect` answers every CONNECT with 403 instead of tunnelling.
async fn spawn_proxy(refuse_connect: bool) -> (SocketAddr, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut client, _)) = listener.accept().await else {
                break;
            };
            let log = log.clone();
            tokio::spawn(async move {
                let head = read_head(&mut client).await;
                let text = String::from_utf8_lossy(&head).into_owned();
                let line = text.lines().next().unwrap_or("").to_owned();
                log.lock().unwrap().push(line.clone());
                let mut parts = line.split_whitespace();
                let method = parts.next().unwrap_or("");
                let target = parts.next().unwrap_or("");
                if method == "CONNECT" {
                    if refuse_connect {
                        let _ = client.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n").await;
                        return;
                    }
                    let Ok(mut origin) = TcpStream::connect(target).await else {
                        let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                        return;
                    };
                    let _ = client.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n").await;
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut origin).await;
                } else {
                    // absolute-form: "GET http://host:port/path HTTP/1.1"
                    let uri: hyper::Uri = match target.parse() {
                        Ok(u) => u,
                        Err(_) => {
                            let _ = client.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
                            return;
                        }
                    };
                    let authority = format!("{}:{}", uri.host().unwrap_or(""), uri.port_u16().unwrap_or(80));
                    let Ok(mut origin) = TcpStream::connect(&authority).await else {
                        let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                        return;
                    };
                    // Forward the bytes we already read, then pipe the rest.
                    let _ = origin.write_all(&head).await;
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut origin).await;
                }
            });
        }
    });
    (addr, seen)
}

async fn origin_server(body: &'static [u8]) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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

fn client_with(proxy: &str, ca_pem: Option<&str>) -> (Hyper4kStatus, *mut Hyper4kClient) {
    let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
    };
    o.max_retries = 0;
    o.proxy_url = proxy.as_ptr();
    o.proxy_url_len = proxy.len();
    if let Some(ca) = ca_pem {
        o.custom_ca_pem = ca.as_ptr();
        o.custom_ca_pem_len = ca.len();
    }
    let mut c = std::ptr::null_mut();
    let st = unsafe { hyper4k_client_new(&o, &mut c) };
    (st, c)
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

#[test]
fn capability_bit_is_lit() {
    assert_ne!(hyper4k_client_capabilities() & HYPER4K_CLIENT_CAP_PROXY, 0);
    assert_eq!(hyper4k_abi_version(), (4 << 16) | 1);
}

#[test]
fn plaintext_goes_through_the_proxy_in_absolute_form() {
    let r = rt();
    let origin = r.block_on(origin_server(b"via-proxy"));
    let (proxy, seen) = r.block_on(spawn_proxy(false));
    let (st, client) = client_with(&format!("http://{proxy}"), None);
    assert_eq!(st, HYPER4K_STATUS_OK);

    let cap = send_and_wait(client, &format!("http://{origin}/resource?x=1"));
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    assert_eq!(cap.status.load(std::sync::atomic::Ordering::SeqCst), 200);
    assert_eq!(&*cap.body.lock().unwrap(), b"via-proxy");

    let lines = seen.lock().unwrap().clone();
    assert_eq!(lines.len(), 1, "one proxied connection: {lines:?}");
    assert_eq!(lines[0], format!("GET http://{origin}/resource?x=1 HTTP/1.1"));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn tls_goes_through_a_connect_tunnel_and_still_validates() {
    let r = rt();
    let fx = TlsFixture::valid();
    let ca = fx.ca_pem.clone();
    let peer = r.block_on(spawn_tls_server(fx, &["http/1.1"]));
    let (proxy, seen) = r.block_on(spawn_proxy(false));
    let (st, client) = client_with(&format!("http://{proxy}"), Some(&ca));
    assert_eq!(st, HYPER4K_STATUS_OK);

    let cap = send_and_wait(client, &peer.url("/tunnelled"));
    assert_eq!(*cap.done.lock().unwrap(), Some(-999), "kind={:?}", cap.done.lock().unwrap());
    assert_eq!(cap.status.load(std::sync::atomic::Ordering::SeqCst), 200);
    assert_eq!(peer.accept_count(), 1, "the origin must see exactly one tunnelled connection");

    let lines = seen.lock().unwrap().clone();
    assert_eq!(lines, vec![format!("CONNECT localhost:{} HTTP/1.1", peer.addr.port())]);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn a_refused_connect_is_a_protocol_error_not_a_hang() {
    let r = rt();
    let fx = TlsFixture::valid();
    let ca = fx.ca_pem.clone();
    let peer = r.block_on(spawn_tls_server(fx, &["http/1.1"]));
    let (proxy, _) = r.block_on(spawn_proxy(true));
    let (st, client) = client_with(&format!("http://{proxy}"), Some(&ca));
    assert_eq!(st, HYPER4K_STATUS_OK);

    let cap = send_and_wait(client, &peer.url("/nope"));
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_PROTOCOL));
    assert_eq!(peer.accept_count(), 0);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn an_unusable_proxy_url_is_refused_at_construction() {
    for bad in ["https://proxy:443", "http://u:p@proxy:3128", "socks5://proxy:1080", "proxy:3128"] {
        let (st, client) = client_with(bad, None);
        assert_eq!(st, HYPER4K_STATUS_INVALID_ARG, "{bad} must be refused");
        assert!(client.is_null());
    }
}

#[test]
fn a_dead_proxy_is_a_connect_failure() {
    let (st, client) = client_with("http://127.0.0.1:1", None);
    assert_eq!(st, HYPER4K_STATUS_OK);
    let cap: Arc<Capture> = send_and_wait(client, "http://example.invalid/x");
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_CONNECT));
    unsafe { hyper4k_client_free(client) };
}
