//! Ignored by default: these need outbound network.
//! Run with `cargo test --test public_https -- --ignored`.
//!
//! Linking a staticlib proves nothing about loading the platform trust store at
//! runtime, and no offline test can: there is no publicly-signed peer to talk
//! to. That is why these two live here rather than in the unit tests.

use hyper4k::abi::*;
use hyper4k::client::handle::*;
use hyper4k::client::*;
use hyper4k::Hyper4kSlice;
use std::ffi::c_void;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Cap {
    status: AtomicU32,
    done: Mutex<Option<i32>>,
}

extern "C" fn on_headers(
    ud: *mut c_void,
    _id: u64,
    status: u16,
    _v: u8,
    _h: *const Hyper4kHeader,
    _n: usize,
) -> Hyper4kHeadersAction {
    unsafe { &*(ud as *const Cap) }
        .status
        .store(status as u32, Ordering::SeqCst);
    HYPER4K_HEADERS_CONTINUE
}

extern "C" fn on_chunk(_ud: *mut c_void, _id: u64, _p: *const u8, _l: usize) -> Hyper4kChunkAction {
    HYPER4K_CHUNK_CONTINUE
}

extern "C" fn on_done(ud: *mut c_void, _id: u64, e: *const Hyper4kError) {
    let cap = unsafe { &*(ud as *const Cap) };
    *cap.done.lock().unwrap() = Some(if e.is_null() {
        -999
    } else {
        unsafe { (*e).kind }
    });
}

fn run(url: &str, ca: Option<&str>) -> Arc<Cap> {
    let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
    };
    if let Some(pem) = ca {
        o.custom_ca_pem = pem.as_ptr();
        o.custom_ca_pem_len = pem.len();
    }
    let mut client = std::ptr::null_mut();
    assert_eq!(
        unsafe { hyper4k_client_new(&o, &mut client) },
        HYPER4K_STATUS_OK
    );

    let cap = Arc::new(Cap::default());
    let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_request_init(&mut r, std::mem::size_of::<Hyper4kClientRequest>() as u32)
    };
    let m = b"GET";
    r.method = Hyper4kSlice {
        ptr: m.as_ptr(),
        len: m.len(),
    };
    r.url = Hyper4kSlice {
        ptr: url.as_ptr(),
        len: url.len(),
    };
    let mut id = 0u64;
    assert_eq!(
        unsafe {
            hyper4k_client_send(
                client,
                &r,
                Some(on_headers),
                Some(on_chunk),
                Some(on_done),
                Arc::as_ptr(&cap) as *mut c_void,
                &mut id,
            )
        },
        HYPER4K_STATUS_OK
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    while cap.done.lock().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    unsafe { hyper4k_client_free(client) };
    cap
}

#[test]
#[ignore]
fn system_roots_validate_a_public_host() {
    let cap = run("https://example.com/", None);
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    assert_eq!(cap.status.load(Ordering::SeqCst), 200);
}

#[test]
#[ignore]
fn a_custom_ca_without_replace_keeps_the_system_roots() {
    // The offline "append" test only proves the private CA is trusted. It
    // cannot show the system roots survived, because it has no publicly-signed
    // peer. This is that half.
    // Generated here rather than committed: a checked-in certificate is a
    // future expiry failure with no owner.
    let key = rcgen::KeyPair::generate().unwrap();
    let mut p = rcgen::CertificateParams::new(vec!["hyper4k-throwaway".to_string()]).unwrap();
    p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = p.self_signed(&key).unwrap().pem();

    let cap = run("https://example.com/", Some(&ca));
    assert_eq!(
        *cap.done.lock().unwrap(),
        Some(-999),
        "adding a custom CA silently replaced the system trust store"
    );
}
