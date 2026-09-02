//! Three-layer baseline (spec §五).
//!
//! The point is attribution, not a headline number: without measuring the same
//! workload at each layer there is no way to tell whether a slow request is
//! Hyper's cost, the FFI boundary's, or Neton's dispatcher.
//!
//!   Layer 1  pure Hyper, Rust handler          — the floor
//!   Layer 2  hyper4k C ABI, extern "C" handler — adds the FFI boundary
//!   Layer 3  Neton full chain                  — adds routing/security/envelope
//!
//! Layer 3 lives in the Neton repository and is driven separately; this harness
//! covers 1 and 2, which is where the ABI's own overhead shows up.
//!
//! Run: cargo bench --bench three_layer -- --nocapture
//! (or: cargo run --release --bench three_layer)

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use std::ffi::{c_void, CString};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};

const BODY: &[u8] = b"{\"ok\":true}";
const WARMUP: usize = 2_000;
const SAMPLES: usize = 20_000;
const CONCURRENCY: usize = 32;
/// A single run is not evidence: the first run of this benchmark showed layer 2
/// at +17% cost with a 2.6x p99, and the very next run had it 8% *faster*. The
/// harness therefore repeats and reports the range, so nobody can read a
/// conclusion out of one sample.
const ROUNDS: usize = 5;

// --- layer 1: pure hyper ---------------------------------------------------

async fn spawn_pure_hyper() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = TokioIo::new(sock);
                let svc = hyper::service::service_fn(|_req| async {
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(
                        Bytes::from_static(BODY),
                    )))
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    addr
}

// --- layer 2: hyper4k C ABI ------------------------------------------------

extern "C" fn abi_handler(_ud: *mut c_void, request: *const hyper4k::Hyper4kRequest) {
    let headers = b"Content-Type: application/json\n";
    unsafe {
        hyper4k::hyper4k_respond(
            (*request).responder,
            200,
            headers.as_ptr(),
            headers.len(),
            BODY.as_ptr(),
            BODY.len(),
        );
    }
}

fn spawn_hyper4k(port: u16) -> *mut hyper4k::Hyper4kServer {
    let host = CString::new("127.0.0.1").unwrap();
    let s = unsafe {
        hyper4k::hyper4k_server_start(host.as_ptr(), port, abi_handler, std::ptr::null_mut())
    };
    assert!(!s.is_null(), "hyper4k failed to bind {port}");
    s
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

// --- load generator (identical for both layers) ----------------------------

struct Stats {
    rps: f64,
    p50_us: u64,
    p99_us: u64,
    rss_mb: f64,
}

async fn drive(addr: SocketAddr, n: usize) -> Vec<u64> {
    let mut latencies = Vec::with_capacity(n);
    let per_conn = n / CONCURRENCY;
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        set.spawn(async move {
            let mut out = Vec::with_capacity(per_conn);
            let io = TokioIo::new(TcpStream::connect(addr).await.unwrap());
            let (mut sender, conn) = hyper::client::conn::http1::handshake::<_, Full<Bytes>>(io)
                .await
                .unwrap();
            tokio::spawn(async move {
                let _ = conn.await;
            });
            for _ in 0..per_conn {
                let req = hyper::Request::builder()
                    .uri("/bench")
                    .header("host", "bench")
                    .body(Full::new(Bytes::new()))
                    .unwrap();
                let t0 = Instant::now();
                let Ok(resp) = sender.send_request(req).await else {
                    break;
                };
                let _ = resp.into_body().collect().await;
                out.push(t0.elapsed().as_micros() as u64);
            }
            out
        });
    }
    while let Some(r) = set.join_next().await {
        latencies.extend(r.unwrap());
    }
    latencies
}

/// Resident set size in MiB. macOS and Linux report it differently.
fn rss_mb() -> f64 {
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok();
        if let Some(o) = out {
            if let Ok(s) = String::from_utf8(o.stdout) {
                if let Ok(kb) = s.trim().parse::<f64>() {
                    return kb / 1024.0;
                }
            }
        }
        0.0
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| {
                s.split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse::<f64>().ok())
            })
            .map(|pages| pages * 4096.0 / 1024.0 / 1024.0)
            .unwrap_or(0.0)
    }
}

fn summarise(mut lat: Vec<u64>, elapsed: Duration) -> Stats {
    lat.sort_unstable();
    let n = lat.len();
    Stats {
        rps: n as f64 / elapsed.as_secs_f64(),
        p50_us: lat[n / 2],
        p99_us: lat[n * 99 / 100],
        rss_mb: rss_mb(),
    }
}

#[allow(dead_code)]
fn report(name: &str, s: &Stats, floor: Option<&Stats>) {
    let overhead = match floor {
        Some(f) => format!("{:+.1}%", (f.rps / s.rps - 1.0) * 100.0),
        None => "—".to_string(),
    };
    println!(
        "{name:<22} {:>10.0} rps   p50 {:>6} us   p99 {:>6} us   RSS {:>6.1} MiB   cost vs floor {overhead}",
        s.rps, s.p50_us, s.p99_us, s.rss_mb
    );
}

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    println!(
        "three-layer baseline — {SAMPLES} requests, {CONCURRENCY} connections, \
         keep-alive h1, {ROUNDS} rounds\n"
    );

    let mut l1: Vec<Stats> = Vec::new();
    let mut l2: Vec<Stats> = Vec::new();

    for round in 1..=ROUNDS {
        let a = rt.block_on(async {
            let addr = spawn_pure_hyper().await;
            drive(addr, WARMUP).await;
            let t0 = Instant::now();
            let lat = drive(addr, SAMPLES).await;
            summarise(lat, t0.elapsed())
        });

        let port = free_port();
        let server = spawn_hyper4k(port);
        std::thread::sleep(Duration::from_millis(200));
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let b = rt.block_on(async {
            drive(addr, WARMUP).await;
            let t0 = Instant::now();
            let lat = drive(addr, SAMPLES).await;
            summarise(lat, t0.elapsed())
        });
        unsafe { hyper4k::hyper4k_server_stop(server) };

        println!(
            "round {round}: pure {:>8.0} rps / p99 {:>5} us    abi {:>8.0} rps / p99 {:>5} us    {:+.1}%",
            a.rps,
            a.p99_us,
            b.rps,
            b.p99_us,
            (a.rps / b.rps - 1.0) * 100.0
        );
        l1.push(a);
        l2.push(b);
    }

    println!();
    summary("layer 1 pure hyper", &l1);
    summary("layer 2 hyper4k ABI", &l2);
    println!("layer 3 Neton chain    driven from the Neton repository, not here");

    let deltas: Vec<f64> = l1
        .iter()
        .zip(&l2)
        .map(|(a, b)| (a.rps / b.rps - 1.0) * 100.0)
        .collect();
    let lo = deltas.iter().cloned().fold(f64::INFINITY, f64::min);
    let hi = deltas.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    println!("\nABI cost across rounds: {lo:+.1}% .. {hi:+.1}% (negative = hyper4k faster)");
    if lo < 0.0 && hi > 0.0 {
        println!(
            "The range spans zero, so at this load the ABI's cost is inside run-to-run\n\
             noise and cannot be measured. Read that as \"not detectable here\", not as\n\
             \"free\": more headers, larger bodies or higher concurrency may expose it."
        );
    }
}

fn summary(name: &str, runs: &[Stats]) {
    let rps_lo = runs.iter().map(|s| s.rps).fold(f64::INFINITY, f64::min);
    let rps_hi = runs.iter().map(|s| s.rps).fold(f64::NEG_INFINITY, f64::max);
    let p99_lo = runs.iter().map(|s| s.p99_us).min().unwrap();
    let p99_hi = runs.iter().map(|s| s.p99_us).max().unwrap();
    let rss = runs.iter().map(|s| s.rss_mb).sum::<f64>() / runs.len() as f64;
    println!(
        "{name:<22} {rps_lo:>8.0}-{rps_hi:<8.0} rps   p99 {p99_lo}-{p99_hi} us   RSS {rss:.1} MiB"
    );
}
