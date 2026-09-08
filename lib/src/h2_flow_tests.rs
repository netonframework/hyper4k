//! HTTP/2 flow-control tests against the server, using the low-level `h2` crate
//! as a client so a test can withhold receive capacity by hand and observe the
//! server pause and resume.
//!
//! Scope, stated plainly. These verify the server's send-side flow control under
//! the conditions each test sets up: withholding capacity actually pauses the
//! send, releasing it resumes the send intact, a paused stream does not block
//! another on the same connection, a reset stream leaves the connection serving,
//! and a dropped connection does not wedge the listener. They run on any core
//! count and need no throughput.
//!
//! What they do NOT do: reproduce the arena's 4096-connection `json-h2c` timeout,
//! or rule out concurrency, task starvation, or shutdown defects at that scale.
//! That zero stays unexplained; these are protocol-correctness tests, not a load
//! reproduction.
//!
//! One API limitation, noted rather than faked: `h2`'s client couples stream and
//! connection capacity in `release_capacity`, so a test cannot release only the
//! stream window and hold the connection window back. The connection-window case
//! below therefore checks completeness under a small connection window, not the
//! stream-vs-connection recovery distinction.

use crate::tests::free_port;
use crate::{hyper4k_respond, hyper4k_server_start, hyper4k_server_stop, Hyper4kRequest};
use bytes::Bytes;
use http::Request;
use std::ffi::{c_void, CString};
use std::time::Duration;
use tokio::net::TcpStream;

const BODY_LEN: usize = 256 * 1024;
const STREAM_WINDOW: u32 = 16 * 1024;

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

extern "C" fn big_handler(_user_data: *mut c_void, request: *const Hyper4kRequest) {
    let responder = unsafe { (*request).responder };
    let body = pattern(BODY_LEN);
    unsafe {
        hyper4k_respond(responder, 200, std::ptr::null(), 0, body.as_ptr(), body.len());
    }
}

/// Stops the server when the test scope ends, so an assertion panic still tears
/// it down rather than leaking the listener into the next test.
struct RunningServer(*mut crate::Hyper4kServer);
unsafe impl Send for RunningServer {}
impl Drop for RunningServer {
    fn drop(&mut self) {
        unsafe { hyper4k_server_stop(self.0) };
    }
}

fn start(port: u16) -> RunningServer {
    let host = CString::new("127.0.0.1").unwrap();
    let server =
        unsafe { hyper4k_server_start(host.as_ptr(), port, big_handler, std::ptr::null_mut()) };
    assert!(!server.is_null(), "server must start");
    RunningServer(server)
}

async fn connect(
    port: u16,
    stream_window: u32,
    conn_window: u32,
) -> (h2::client::SendRequest<Bytes>, tokio::task::JoinHandle<()>) {
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("connect");
    let (send, conn) = h2::client::Builder::new()
        .initial_window_size(stream_window)
        .initial_connection_window_size(conn_window)
        .handshake::<_, Bytes>(tcp)
        .await
        .expect("h2 handshake");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    let send = send.ready().await.expect("send ready");
    (send, driver)
}

/// Reads frames, releasing capacity as it goes, to completion.
async fn drain(mut body: h2::RecvStream) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.expect("data frame");
        out.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .expect("release capacity");
    }
    out
}

/// Every test body runs entirely inside this deadline, connect included, so a
/// stall anywhere fails the test instead of hanging the run.
fn within<F: std::future::Future>(secs: u64, f: F) -> F::Output {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        tokio::time::timeout(Duration::from_secs(secs), f)
            .await
            .expect("test exceeded its deadline (a stall that should not happen)")
    })
}

#[test]
fn withholding_capacity_pauses_the_send_then_releasing_resumes_it_intact() {
    let port = free_port();
    let _server = start(port);
    let body = within(15, async {
        let (mut send, _driver) = connect(port, STREAM_WINDOW, 1024 * 1024).await;
        let (resp, _tx) = send
            .send_request(Request::get("/big").body(()).unwrap(), true)
            .expect("send request");
        let mut body = resp.await.expect("response head").into_body();

        // Read the initial window worth of DATA WITHOUT releasing capacity.
        let mut received = Vec::new();
        while received.len() < STREAM_WINDOW as usize {
            let chunk = body
                .data()
                .await
                .expect("first frames within the window")
                .expect("data frame");
            received.extend_from_slice(&chunk);
        }

        // The window is now spent and never returned: the next frame must NOT
        // arrive. If the server ignored flow control it would keep sending here.
        let stalled = tokio::time::timeout(Duration::from_millis(700), body.data()).await;
        assert!(
            stalled.is_err(),
            "server kept sending past a window it was never given back",
        );

        // Give the window back; the rest must now flow.
        body.flow_control()
            .release_capacity(received.len())
            .expect("release capacity");
        let rest = drain(body).await;
        received.extend_from_slice(&rest);
        received
    });
    assert_eq!(body.len(), BODY_LEN, "every byte must arrive after resume");
    assert_eq!(body, pattern(BODY_LEN), "content intact across the pause");
}

#[test]
fn a_paused_stream_does_not_block_another_on_the_same_connection() {
    let port = free_port();
    let _server = start(port);
    let b_body = within(15, async {
        // Connection window generous so the connection itself is never the limit;
        // each stream has its own small window.
        let (mut send, _driver) = connect(port, STREAM_WINDOW, 4 * 1024 * 1024).await;

        // Stream A: consume its window and then hold it paused (never release).
        let (ra, _ta) = send
            .send_request(Request::get("/a").body(()).unwrap(), true)
            .unwrap();
        let mut a_body = ra.await.expect("A head").into_body();
        let mut a_seen = 0usize;
        while a_seen < STREAM_WINDOW as usize {
            let chunk = a_body.data().await.expect("A frame").expect("A data");
            a_seen += chunk.len();
        }
        // A is now parked with no capacity. Keep a_body alive (do not drop) so the
        // stream stays open and paused for the rest of the test.

        // Stream B on the same connection must complete fully while A stays paused.
        let (rb, _tb) = send
            .send_request(Request::get("/b").body(()).unwrap(), true)
            .unwrap();
        let b = drain(rb.await.expect("B head").into_body()).await;
        // Hold a_body across the await above.
        let _keep_a_paused = &a_body;
        b
    });
    assert_eq!(b_body.len(), BODY_LEN, "B must finish while A is paused");
    assert_eq!(b_body, pattern(BODY_LEN));
}

#[test]
fn resetting_one_stream_leaves_the_connection_serving() {
    let port = free_port();
    let _server = start(port);
    let after = within(15, async {
        let (mut send, _driver) = connect(port, STREAM_WINDOW, 4 * 1024 * 1024).await;

        // Start stream A, read one frame, then cancel just that stream (RST_STREAM).
        let (ra, mut a_tx) = send
            .send_request(Request::get("/a").body(()).unwrap(), true)
            .unwrap();
        let mut a_body = ra.await.expect("A head").into_body();
        let _ = a_body.data().await;
        a_tx.send_reset(h2::Reason::CANCEL);
        drop(a_body);

        // A fresh stream on the SAME connection must still complete.
        let (rb, _tb) = send
            .send_request(Request::get("/b").body(()).unwrap(), true)
            .unwrap();
        drain(rb.await.expect("B head").into_body()).await
    });
    assert_eq!(after.len(), BODY_LEN, "the connection serves after a stream reset");
}

#[test]
fn dropping_the_connection_mid_body_lets_the_server_serve_a_new_one() {
    let port = free_port();
    let _server = start(port);
    let served = within(15, async {
        // First connection: start a response, read one frame, then disconnect by
        // dropping the client and awaiting its driver so the TCP close completes.
        {
            let (mut send, driver) = connect(port, STREAM_WINDOW, 32 * 1024).await;
            let (resp, _tx) = send
                .send_request(Request::get("/drop").body(()).unwrap(), true)
                .unwrap();
            let mut body = resp.await.expect("head").into_body();
            let first = body.data().await;
            assert!(first.is_some(), "expected at least one frame before dropping");
            first.unwrap().expect("first frame ok");
            drop(body);
            drop(send);
            // Abort the driver task to drop the h2 Connection and close the TCP —
            // an explicit disconnect, not a detached JoinHandle. `await` after
            // `abort` returns promptly (Cancelled). The server sees a real close.
            driver.abort();
            let _ = driver.await;
        }

        // A brand-new connection must be served promptly.
        let (mut send, _driver) = connect(port, 64 * 1024, 1024 * 1024).await;
        let (resp, _tx) = send
            .send_request(Request::get("/after").body(()).unwrap(), true)
            .unwrap();
        drain(resp.await.expect("head").into_body()).await
    });
    assert_eq!(served.len(), BODY_LEN, "listener keeps serving after a disconnect");
}

#[test]
fn a_response_completes_under_a_small_connection_window() {
    let port = free_port();
    let _server = start(port);
    let body = within(15, async {
        // Stream window ample, connection window small: the connection budget is
        // the binding limit. Completeness proves connection-level windowing works;
        // see the module note on why stream-vs-connection recovery is not split.
        let (mut send, _driver) = connect(port, 1024 * 1024, 16 * 1024).await;
        let (resp, _tx) = send
            .send_request(Request::get("/big").body(()).unwrap(), true)
            .unwrap();
        drain(resp.await.expect("head").into_body()).await
    });
    assert_eq!(body.len(), BODY_LEN);
    assert_eq!(body, pattern(BODY_LEN));
}
