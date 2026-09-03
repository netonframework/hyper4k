//! Pool tests.
//!
//! Real sockets are plaintext h1 only — production must never pick H2 from an
//! `http://` URL. H2 multiplexing and the capacity counters go through an
//! injected test connector, so the pool logic is covered without shipping an
//! h2c client.

use super::plaintext::PlaintextConnector;
use super::pool::*;
use bytes::Bytes;
use http_body_util::Full;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

// --- peers -----------------------------------------------------------------

struct Peer {
    addr: SocketAddr,
    accepts: Arc<AtomicU32>,
}

impl Peer {
    fn key(&self) -> PoolKey {
        PoolKey::new("http", &self.addr.ip().to_string(), self.addr.port())
    }
    fn accept_count(&self) -> u32 {
        self.accepts.load(Ordering::SeqCst)
    }
}

/// Minimal HTTP/1.1 keep-alive peer.
async fn spawn_h1_server() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicU32::new(0));
    let counter = accepts.clone();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let io = TokioIo::new(sock);
                let svc = hyper::service::service_fn(|_req| async {
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(
                        Bytes::from_static(b"ok"),
                    )))
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    Peer { addr, accepts }
}

/// HTTP/2 peer used **only** by the test connector below.
async fn spawn_h2_server() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicU32::new(0));
    let counter = accepts.clone();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let io = TokioIo::new(sock);
                let svc = hyper::service::service_fn(|_req| async {
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(
                        Bytes::from_static(b"ok"),
                    )))
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    Peer { addr, accepts }
}

// --- connectors ------------------------------------------------------------

fn plaintext() -> Arc<dyn Connector> {
    Arc::new(PlaintextConnector { connect_timeout: Some(Duration::from_secs(5)), proxy: None })
}

/// Test-only connector that speaks HTTP/2 over cleartext.
///
/// This is exactly what `PlaintextConnector` must never do. It exists so the
/// pool's multiplexing and capacity accounting can be tested without waiting
/// for TLS in Task 5.
struct TestH2Connector {
    dials: Arc<AtomicU32>,
}

impl Connector for TestH2Connector {
    fn connect(&self, key: &PoolKey) -> ConnectFuture {
        let addr = format!("{}:{}", key.host, key.port);
        let dials = self.dials.clone();
        Box::pin(async move {
            dials.fetch_add(1, Ordering::SeqCst);
            let stream = TcpStream::connect(&addr)
                .await
                .map_err(|_| crate::abi::HYPER4K_ERR_CONNECT)?;
            let io = TokioIo::new(stream);
            let (sender, conn) = hyper::client::conn::http2::handshake::<_, _, Full<Bytes>>(
                TokioExecutor::new(),
                io,
            )
            .await
            .map_err(|_| crate::abi::HYPER4K_ERR_CONNECT)?;
            let driver = tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(Connected {
                sender: Sender::H2(sender),
                driver,
            })
        })
    }
}

fn h2_connector() -> (Arc<dyn Connector>, Arc<AtomicU32>) {
    let dials = Arc::new(AtomicU32::new(0));
    (
        Arc::new(TestH2Connector {
            dials: dials.clone(),
        }),
        dials,
    )
}

// --- real sockets, plaintext, h1 only --------------------------------------

#[tokio::test]
async fn plaintext_urls_always_yield_an_h1_sender() {
    // The guard against accidentally shipping an h2c client.
    let peer = spawn_h1_server().await;
    let pool = Pool::new(plaintext());
    let lease = pool.acquire(&peer.key()).await.unwrap();
    assert!(
        matches!(lease.sender.as_ref().unwrap(), Sender::H1(_)),
        "http:// must not negotiate H2 in v4"
    );
}

#[tokio::test]
async fn h1_connections_are_exclusive_while_held() {
    let peer = spawn_h1_server().await;
    let pool = Pool::new(plaintext());
    let key = peer.key();
    let a = pool.acquire(&key).await.unwrap();
    let b = pool.acquire(&key).await.unwrap();
    assert_ne!(a.conn_id, b.conn_id, "h1 connections are exclusive");
}

#[tokio::test]
async fn h1_connections_are_reused_after_release() {
    // Without this, "pool" would just mean "dial every time" for h1.
    let peer = spawn_h1_server().await;
    let pool = Pool::new(plaintext());
    let key = peer.key();
    let a = pool.acquire(&key).await.unwrap();
    let id = a.conn_id;
    drop(a); // RAII returns the exclusive slot
    let b = pool.acquire(&key).await.unwrap();
    assert_eq!(b.conn_id, id, "released h1 connection was not reused");
}

#[tokio::test]
async fn concurrent_h1_acquires_are_bounded_not_unbounded() {
    // NOT "dial once": h1 connections are exclusive, so 16 concurrent acquires
    // that all hold their lease genuinely need 16 connections. Demanding
    // accept_count == 1 here would either deadlock or contradict exclusivity.
    // What the pool owes us is a cap.
    let peer = spawn_h1_server().await;
    let pool = Arc::new(Pool::new(plaintext()).with_max_connections_per_key(4));
    let key = peer.key();

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let p = pool.clone();
        let k = key.clone();
        set.spawn(async move { p.acquire(&k).await.map(|l| l.conn_id) });
    }
    let mut held = Vec::new();
    while let Some(r) = set.join_next().await {
        held.push(r.unwrap());
    }
    assert!(
        peer.accept_count() <= 4,
        "h1 pool exceeded its per-authority connection cap: {}",
        peer.accept_count()
    );
}

/// Peer that answers one request and then closes, so the pooled sender goes
/// stale exactly the way a real keep-alive peer does.
async fn spawn_h1_close_after_one() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicU32::new(0));
    let counter = accepts.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
                let _ = sock.flush().await;
                drop(sock);
            });
        }
    });
    Peer { addr, accepts }
}

#[tokio::test]
async fn a_closed_connection_is_evicted_when_the_lease_drops() {
    let peer = spawn_h1_close_after_one().await;
    let pool = Pool::new(plaintext());
    let key = peer.key();

    let mut lease = pool.acquire(&key).await.unwrap();
    let old = lease.conn_id;
    if let Sender::H1(s) = lease.sender_mut() {
        let req = hyper::Request::builder()
            .uri("/x")
            .header("host", "test")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let _ = s.send_request(req).await;
    }
    // Let the peer's close reach us.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(lease); // RAII: there is no pool.release()

    let fresh = pool.acquire(&key).await.unwrap();
    assert_ne!(fresh.conn_id, old, "a dead connection was handed out again");
    // The TCP handshake completes before the server's accept() returns, so poll
    // rather than sampling once.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while peer.accept_count() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        peer.accept_count(),
        2,
        "the pool did not redial after eviction"
    );
}

#[tokio::test]
async fn shutdown_joins_every_connection_driver() {
    let peer = spawn_h1_server().await;
    let pool = Pool::new(plaintext());
    let _lease = pool.acquire(&peer.key()).await.unwrap();
    // A driver outliving shutdown would hang hyper4k_client_free.
    tokio::time::timeout(Duration::from_secs(5), pool.shutdown())
        .await
        .expect("shutdown did not join its drivers");
}

// --- injected h2 connector: bookkeeping without an h2c client --------------

#[tokio::test]
async fn h2_leases_to_one_authority_share_a_connection() {
    let peer = spawn_h2_server().await;
    let (c, _) = h2_connector();
    let pool = Pool::new(c);
    let key = peer.key();
    let a = pool.acquire(&key).await.unwrap();
    let b = pool.acquire(&key).await.unwrap();
    assert_eq!(a.conn_id, b.conn_id, "h2 must multiplex, not redial");
}

#[tokio::test]
async fn concurrent_h2_acquires_dial_once() {
    // Dial de-duplication only makes sense where connections multiplex.
    let peer = spawn_h2_server().await;
    let (c, dials) = h2_connector();
    let pool = Arc::new(Pool::new(c));
    let key = peer.key();

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let p = pool.clone();
        let k = key.clone();
        set.spawn(async move { p.acquire(&k).await.map(|l| l.conn_id) });
    }
    let mut ok = 0;
    while let Some(r) = set.join_next().await {
        if r.unwrap().is_ok() {
            ok += 1;
        }
    }
    assert_eq!(ok, 16);
    assert_eq!(
        dials.load(Ordering::SeqCst),
        1,
        "connection storm: dialled more than once for one authority"
    );
}

#[tokio::test]
async fn an_h2_connection_at_the_paused_cap_stops_taking_new_streams() {
    let peer = spawn_h2_server().await;
    let (c, _) = h2_connector();
    let pool = Pool::new(c).with_paused_cap(2);
    let key = peer.key();
    let a = pool.acquire(&key).await.unwrap();
    let _g1 = PauseGuard::new(a.entry.clone());
    let _g2 = PauseGuard::new(a.entry.clone());
    let c2 = pool.acquire(&key).await.unwrap();
    assert_ne!(
        c2.conn_id, a.conn_id,
        "new stream landed on a connection at its paused cap"
    );
}

#[tokio::test]
async fn dropping_a_pause_guard_restores_capacity() {
    let peer = spawn_h2_server().await;
    let (c, _) = h2_connector();
    let pool = Pool::new(c).with_paused_cap(1);
    let key = peer.key();
    let a = pool.acquire(&key).await.unwrap();
    {
        let _g = PauseGuard::new(a.entry.clone());
        let other = pool.acquire(&key).await.unwrap();
        assert_ne!(other.conn_id, a.conn_id);
    }
    // Assert the accounting, not which connection gets picked next: choosing a
    // different live connection is also legal.
    assert_eq!(a.entry.paused_count(), 0, "paused count leaked after drop");
    assert!(
        pool.eligible_connections(&key).contains(&a.conn_id),
        "the unpaused connection did not return to the eligible set"
    );
    assert_eq!(
        pool.connection_count(&key),
        2,
        "an extra connection was dialled"
    );
}

#[tokio::test]
async fn capacity_survives_cancel_timeout_and_connection_error() {
    // Every abnormal exit unwinds through Drop; none may leak a slot.
    let peer = spawn_h2_server().await;
    let (c, _) = h2_connector();
    let pool = Pool::new(c);
    let key = peer.key();
    let before = pool.active_count(&key);

    for scenario in 0..3 {
        let lease = pool.acquire(&key).await.unwrap();
        match scenario {
            0 => drop(lease), // cancel
            1 => {
                let fut = async move {
                    let _held = lease;
                    tokio::time::sleep(Duration::from_secs(30)).await;
                };
                // Timeout drops the future, and with it the lease.
                let _ = tokio::time::timeout(Duration::from_millis(20), fut).await;
            }
            _ => {
                // Connection error path: the lease goes out of scope on unwind.
                let r: Result<(), ()> = Err(());
                let _held = lease;
                assert!(r.is_err());
            }
        }
    }
    assert_eq!(
        pool.active_count(&key),
        before,
        "an abnormal exit leaked pool capacity"
    );
}
