use super::*;
use std::ffi::CString;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

extern "C" fn reply(_: *mut c_void, req: *const Hyper4kRequest) {
    unsafe {
        hyper4k_respond(
            (*req).responder,
            200,
            std::ptr::null(),
            0,
            b"ok".as_ptr(),
            2,
        );
    }
}

struct Fixture {
    runtime: *mut Hyper4kRuntime,
    servers: Vec<*mut Hyper4kServer>,
}

impl Fixture {
    fn new() -> Self {
        let runtime = unsafe { hyper4k_runtime_new(2) };
        assert!(!runtime.is_null());
        Self {
            runtime,
            servers: Vec::new(),
        }
    }

    fn start(&mut self, port: u16) -> *mut Hyper4kServer {
        self.start_with(port, reply, std::ptr::null_mut())
    }

    fn start_with(
        &mut self,
        port: u16,
        callback: Hyper4kRequestCallback,
        data: *mut c_void,
    ) -> *mut Hyper4kServer {
        let host = CString::new("127.0.0.1").unwrap();
        let server =
            unsafe { hyper4k_server_start_on(self.runtime, host.as_ptr(), port, callback, data) };
        if !server.is_null() {
            self.servers.push(server);
        }
        server
    }

    fn stop(&mut self, server: *mut Hyper4kServer) {
        self.servers.retain(|&s| s != server);
        unsafe { hyper4k_server_stop(server) };
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        unsafe {
            for server in self.servers.drain(..) {
                hyper4k_server_stop(server);
            }
            hyper4k_runtime_shutdown(self.runtime);
        }
    }
}

fn connect(port: u16) -> BufReader<TcpStream> {
    let sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    BufReader::new(sock)
}

fn round_trip(client: &mut BufReader<TcpStream>) {
    client
        .get_mut()
        .write_all(b"GET / HTTP/1.1\r\nHost: local\r\n\r\n")
        .unwrap();
    let mut line = String::new();
    client.read_line(&mut line).unwrap();
    assert!(line.starts_with("HTTP/1.1 200 "), "{line}");
    loop {
        line.clear();
        assert_ne!(client.read_line(&mut line).unwrap(), 0);
        if line == "\r\n" {
            break;
        }
    }
    let mut body = [0; 2];
    client.read_exact(&mut body).unwrap();
    assert_eq!(&body, b"ok");
}

#[test]
fn stopping_one_listener_closes_its_keepalives_but_not_its_sibling() {
    let mut fixture = Fixture::new();
    let first_port = tests::free_port();
    let first = fixture.start(first_port);
    assert!(!first.is_null());
    let second_port = tests::free_port();
    let second = fixture.start(second_port);
    assert!(!second.is_null());
    let mut a = connect(first_port);
    let mut b = connect(second_port);
    round_trip(&mut a);
    round_trip(&mut b);
    fixture.stop(first);
    // stop is a callback-lifetime barrier, not merely an accept-loop signal.
    let mut byte = [0];
    match a.read(&mut byte) {
        Ok(0) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        other => panic!("stopped listener still owns a live connection: {other:?}"),
    }
    round_trip(&mut b);
    let rebound = fixture.start(first_port);
    assert!(!rebound.is_null(), "stop must release the listening socket");
    round_trip(&mut connect(first_port));
}

#[test]
fn failed_bind_does_not_shutdown_a_shared_runtime() {
    let mut fixture = Fixture::new();
    let port = tests::free_port();
    assert!(!fixture.start(port).is_null());
    assert!(fixture.start(port).is_null());
    round_trip(&mut connect(port));
}

#[test]
fn stop_waits_for_an_executing_callback_before_releasing_its_user_data() {
    stop_waits_for_callback(false);
}

#[test]
fn stop_also_joins_http2_stream_callbacks_spawned_outside_the_connection_task() {
    stop_waits_for_callback(true);
}

fn stop_waits_for_callback(http2: bool) {
    use std::sync::{mpsc, Mutex};
    struct Gate {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    extern "C" fn gated(data: *mut c_void, request: *const Hyper4kRequest) {
        let gate = unsafe { &*(data as *const Gate) };
        let _ = gate.entered.send(());
        if let Ok(receiver) = gate.release.lock() {
            let _ = receiver.recv_timeout(Duration::from_secs(5));
        }
        reply(std::ptr::null_mut(), request);
    }

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let gate = Box::new(Gate {
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let mut fixture = Fixture::new();
    let port = tests::free_port();
    let server = fixture.start_with(port, gated, (&*gate as *const Gate).cast_mut().cast());
    assert!(!server.is_null());
    let mut client = connect(port);
    if http2 {
        let socket = client.get_mut();
        socket
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .unwrap();
        socket.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).unwrap();
        // HPACK indexed GET, http, /; literal :authority=local. Stream 1,
        // END_HEADERS | END_STREAM. No DATA or flow-control credit is needed.
        let block = [0x82, 0x86, 0x84, 0x01, 5, b'l', b'o', b'c', b'a', b'l'];
        socket
            .write_all(&[0, 0, block.len() as u8, 1, 5, 0, 0, 0, 1])
            .unwrap();
        socket.write_all(&block).unwrap();
    } else {
        client
            .get_mut()
            .write_all(b"GET / HTTP/1.1\r\nHost: local\r\n\r\n")
            .unwrap();
    }
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    // Transfer sole ownership to the stop thread; retain the runtime and user_data.
    fixture.servers.retain(|&s| s != server);
    let server_address = server as usize;
    let (done_tx, done_rx) = mpsc::channel();
    let stopper = std::thread::spawn(move || {
        unsafe { hyper4k_server_stop(server_address as *mut Hyper4kServer) };
        let _ = done_tx.send(());
    });
    let returned_early = done_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    release_tx.send(()).unwrap();
    if !returned_early {
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    stopper.join().unwrap();
    assert!(
        !returned_early,
        "stop returned while user_data was still in use"
    );
}
