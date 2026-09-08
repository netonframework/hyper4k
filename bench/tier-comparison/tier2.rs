// Tier 2: hyper4k engine + C ABI, native callback, NO Kotlin/Native runtime.
// Isolates engine + ABI + responder from the Kotlin layer (GC, coroutines, FFI copy).
use hyper4k::{hyper4k_respond, hyper4k_server_start, Hyper4kRequest};
use std::ffi::{c_void, CString};
use std::slice;

extern "C" fn on_request(_user_data: *mut c_void, req: *const Hyper4kRequest) {
    let r = unsafe { &*req };
    let q = unsafe { slice::from_raw_parts(r.query.ptr, r.query.len) };
    let query = std::str::from_utf8(q).unwrap_or("");
    let mut a = 0i64;
    let mut b = 0i64;
    for pair in query.split('&') {
        let mut it = pair.splitn(2, '=');
        match (it.next(), it.next()) {
            (Some("a"), Some(v)) => a = v.parse().unwrap_or(0),
            (Some("b"), Some(v)) => b = v.parse().unwrap_or(0),
            _ => {}
        }
    }
    let body = (a + b).to_string();
    unsafe {
        hyper4k_respond(r.responder, 200, std::ptr::null(), 0, body.as_ptr(), body.len());
    }
}

fn main() {
    let host = CString::new("0.0.0.0").unwrap();
    let server = unsafe { hyper4k_server_start(host.as_ptr(), 19091, on_request, std::ptr::null_mut()) };
    assert!(!server.is_null());
    // Park forever.
    loop { std::thread::sleep(std::time::Duration::from_secs(3600)); }
}
