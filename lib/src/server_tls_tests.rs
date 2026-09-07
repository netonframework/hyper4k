//! Server-side TLS: a real handshake, ALPN, and the failure modes that must be
//! reported rather than swallowed.
//!
//! Certificates are generated per test with `rcgen`, so nothing here depends on
//! a file in the repository or on the machine's trust store.

use crate::tests::{free_port, test_handler};
use crate::*;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use std::ffi::CString;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

/// Writes a self-signed cert/key pair for `localhost` and returns their paths.
fn write_cert(dir: &std::path::Path) -> (String, String) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "localhost");
    params.distinguished_name = dn;
    let cert = params.self_signed(&key).unwrap();

    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    std::fs::File::create(&cert_path)
        .unwrap()
        .write_all(cert.pem().as_bytes())
        .unwrap();
    std::fs::File::create(&key_path)
        .unwrap()
        .write_all(key.serialize_pem().as_bytes())
        .unwrap();
    (
        cert_path.to_str().unwrap().to_owned(),
        key_path.to_str().unwrap().to_owned(),
    )
}

fn tempdir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("hyper4k-tls-{}", std::process::id()));
    let d = d.join(format!("{:?}", std::time::Instant::now()).replace(['{', '}', ' ', ':'], ""));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Connects with rustls, trusting only the certificate we just generated, and
/// returns the ALPN protocol the handshake settled on.
fn handshake_alpn(port: u16, cert_pem_path: &str, offer: &[&str]) -> Option<Vec<u8>> {
    let mut roots = RootCertStore::empty();
    let file = std::fs::File::open(cert_pem_path).unwrap();
    let mut reader = std::io::BufReader::new(file);
    for cert in rustls_pemfile::certs(&mut reader) {
        roots.add(cert.unwrap()).unwrap();
    }
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = offer.iter().map(|p| p.as_bytes().to_vec()).collect();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        let server_name = ServerName::try_from("localhost").unwrap();
        let tls = tokio::time::timeout(
            Duration::from_secs(5),
            connector.connect(server_name, tcp),
        )
        .await
        .expect("handshake did not time out")
        .expect("handshake succeeds against the cert we trust");
        tls.get_ref().1.alpn_protocol().map(|p| p.to_vec())
    })
}

#[test]
fn serves_a_request_over_a_real_tls_handshake() {
    let dir = tempdir();
    let (cert, key) = write_cert(&dir);
    let port = free_port();
    let host = CString::new("127.0.0.1").unwrap();
    let c = CString::new(cert.clone()).unwrap();
    let k = CString::new(key).unwrap();
    let alpn = CString::new("http/1.1").unwrap();

    let server = unsafe {
        hyper4k_server_start_tls(
            host.as_ptr(),
            port,
            c.as_ptr(),
            k.as_ptr(),
            alpn.as_ptr(),
            test_handler,
            std::ptr::null_mut(),
        )
    };
    assert!(!server.is_null(), "TLS server must start");

    let negotiated = handshake_alpn(port, &cert, &["http/1.1"]);
    assert_eq!(
        negotiated.as_deref(),
        Some(&b"http/1.1"[..]),
        "ALPN must settle on the protocol the server advertises"
    );

    unsafe { hyper4k_server_stop(server) };
}

#[test]
fn alpn_prefers_h2_when_both_are_offered() {
    let dir = tempdir();
    let (cert, key) = write_cert(&dir);
    let port = free_port();
    let host = CString::new("127.0.0.1").unwrap();
    let c = CString::new(cert.clone()).unwrap();
    let k = CString::new(key).unwrap();
    // Preference order is the server's, most preferred first.
    let alpn = CString::new("h2,http/1.1").unwrap();

    let server = unsafe {
        hyper4k_server_start_tls(
            host.as_ptr(), port, c.as_ptr(), k.as_ptr(), alpn.as_ptr(),
            test_handler, std::ptr::null_mut(),
        )
    };
    assert!(!server.is_null());

    assert_eq!(
        handshake_alpn(port, &cert, &["h2", "http/1.1"]).as_deref(),
        Some(&b"h2"[..]),
        "a client offering both must get the server's first choice"
    );

    unsafe { hyper4k_server_stop(server) };
}

#[test]
fn a_client_that_only_speaks_http1_still_gets_http1() {
    let dir = tempdir();
    let (cert, key) = write_cert(&dir);
    let port = free_port();
    let host = CString::new("127.0.0.1").unwrap();
    let c = CString::new(cert.clone()).unwrap();
    let k = CString::new(key).unwrap();
    let alpn = CString::new("h2,http/1.1").unwrap();

    let server = unsafe {
        hyper4k_server_start_tls(
            host.as_ptr(), port, c.as_ptr(), k.as_ptr(), alpn.as_ptr(),
            test_handler, std::ptr::null_mut(),
        )
    };
    assert!(!server.is_null());

    assert_eq!(
        handshake_alpn(port, &cert, &["http/1.1"]).as_deref(),
        Some(&b"http/1.1"[..]),
        "the server must fall back to what the client can actually speak"
    );

    unsafe { hyper4k_server_stop(server) };
}

#[test]
fn a_missing_or_unreadable_certificate_is_reported_as_null() {
    let port = free_port();
    let host = CString::new("127.0.0.1").unwrap();
    let missing = CString::new("/nonexistent/server.crt").unwrap();
    let key = CString::new("/nonexistent/server.key").unwrap();
    let alpn = CString::new("http/1.1").unwrap();

    let server = unsafe {
        hyper4k_server_start_tls(
            host.as_ptr(), port, missing.as_ptr(), key.as_ptr(), alpn.as_ptr(),
            test_handler, std::ptr::null_mut(),
        )
    };
    assert!(
        server.is_null(),
        "a server that cannot load its certificate must not pretend to start"
    );
}

#[test]
fn an_empty_alpn_list_advertises_nothing() {
    let dir = tempdir();
    let (cert, key) = write_cert(&dir);
    let port = free_port();
    let host = CString::new("127.0.0.1").unwrap();
    let c = CString::new(cert.clone()).unwrap();
    let k = CString::new(key).unwrap();
    let alpn = CString::new("").unwrap();

    let server = unsafe {
        hyper4k_server_start_tls(
            host.as_ptr(), port, c.as_ptr(), k.as_ptr(), alpn.as_ptr(),
            test_handler, std::ptr::null_mut(),
        )
    };
    assert!(!server.is_null());
    assert_eq!(
        handshake_alpn(port, &cert, &[]),
        None,
        "with nothing advertised the handshake settles on no protocol"
    );
    unsafe { hyper4k_server_stop(server) };
}
