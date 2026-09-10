// Contract tests for the application-shared runtime: listeners borrow one runtime,
// stop independently, and a bind failure on one port does not disturb the others.

use std::ffi::c_void;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::raw::c_char;
use std::time::Duration;

use hyper4k::{
    hyper4k_respond, hyper4k_runtime_new, hyper4k_runtime_shutdown, hyper4k_server_start_on,
    hyper4k_server_stop, Hyper4kRequest,
};

// Minimal callback: answer every request with 200 "ok" synchronously.
extern "C" fn respond_ok(_user_data: *mut c_void, req: *const Hyper4kRequest) {
    let responder = unsafe { (*req).responder };
    let body = b"ok";
    unsafe {
        hyper4k_respond(
            responder,
            200,
            std::ptr::null(),
            0,
            body.as_ptr(),
            body.len(),
        );
    }
}

fn get(port: u16) -> Option<String> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).ok()?;
    Some(buf)
}

fn wait_up(port: u16) -> bool {
    for _ in 0..50 {
        if get(port).is_some() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn listeners_share_one_runtime_and_close_independently() {
    let rt = unsafe { hyper4k_runtime_new(2) };
    assert!(!rt.is_null());

    let a = unsafe {
        hyper4k_server_start_on(rt, c"127.0.0.1".as_ptr() as *const c_char, 18131, respond_ok, std::ptr::null_mut())
    };
    let b = unsafe {
        hyper4k_server_start_on(rt, c"127.0.0.1".as_ptr() as *const c_char, 18132, respond_ok, std::ptr::null_mut())
    };
    assert!(!a.is_null() && !b.is_null(), "both listeners start on the shared runtime");
    assert!(wait_up(18131) && wait_up(18132), "both answer");

    // Stopping A cancels only A's accept loop; B keeps serving on the same runtime.
    unsafe { hyper4k_server_stop(a) };
    std::thread::sleep(Duration::from_millis(200));
    assert!(get(18131).is_none(), "A stopped");
    assert!(get(18132).is_some(), "B unaffected by A's stop");

    unsafe { hyper4k_server_stop(b) };
    unsafe { hyper4k_runtime_shutdown(rt) };
}

#[test]
fn bind_failure_is_locally_contained() {
    let rt = unsafe { hyper4k_runtime_new(2) };
    assert!(!rt.is_null());

    let a = unsafe {
        hyper4k_server_start_on(rt, c"127.0.0.1".as_ptr() as *const c_char, 18133, respond_ok, std::ptr::null_mut())
    };
    assert!(!a.is_null());
    assert!(wait_up(18133));

    // Binding the same port again fails -> null, without touching the runtime or A.
    let dup = unsafe {
        hyper4k_server_start_on(rt, c"127.0.0.1".as_ptr() as *const c_char, 18133, respond_ok, std::ptr::null_mut())
    };
    assert!(dup.is_null(), "duplicate bind returns null");
    assert!(get(18133).is_some(), "the original listener still serves after a failed bind");

    unsafe { hyper4k_server_stop(a) };
    unsafe { hyper4k_runtime_shutdown(rt) };
}

#[test]
fn two_runtimes_are_independent() {
    let rt1 = unsafe { hyper4k_runtime_new(1) };
    let rt2 = unsafe { hyper4k_runtime_new(1) };
    assert!(!rt1.is_null() && !rt2.is_null());
    let s1 = unsafe {
        hyper4k_server_start_on(rt1, c"127.0.0.1".as_ptr() as *const c_char, 18134, respond_ok, std::ptr::null_mut())
    };
    assert!(!s1.is_null() && wait_up(18134));
    // Shutting down rt2 (which has no listeners) must not affect rt1's listener.
    unsafe { hyper4k_runtime_shutdown(rt2) };
    std::thread::sleep(Duration::from_millis(150));
    assert!(get(18134).is_some(), "rt1 listener survives rt2 shutdown");
    unsafe { hyper4k_server_stop(s1) };
    unsafe { hyper4k_runtime_shutdown(rt1) };
}
