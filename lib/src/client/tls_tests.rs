//! TLS tests.
//!
//! Every certificate is generated per test with `rcgen`, so nothing in the repo
//! expires and the "expired certificate" case can be produced deliberately.
//! No test here touches the network: the system-root check needs a publicly
//! signed peer and lives in the ignored public test instead.

use super::handle::*;
use super::handle_tests::{new_client_with_ca, send_and_wait, Capture};
use super::tls::{build_tls_config, TlsOptions};
use super::*;
use crate::abi::*;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

// --- fixtures --------------------------------------------------------------

pub struct TlsFixture {
    pub ca_pem: String,
    pub leaf_pem: String,
    pub key_pem: String,
}

fn params_for(name: &str) -> CertificateParams {
    let mut p = CertificateParams::new(vec![name.to_string()]).unwrap();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, name);
    p.distinguished_name = dn;
    p
}

impl TlsFixture {
    pub fn for_name(name: &str) -> Self {
        Self::build(name, false)
    }
    pub fn valid() -> Self {
        Self::build("localhost", false)
    }
    pub fn expired() -> Self {
        Self::build("localhost", true)
    }

    fn build(name: &str, expired: bool) -> Self {
        // Each fixture gets a distinct CA name. Reusing one DN made the "wrong
        // CA" case report BadSignature (same name, different key) instead of
        // UnknownIssuer, which is a different scenario from the one intended.
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = params_for(&format!("hyper4k-test-ca-{n}"));
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();

        let leaf_key = KeyPair::generate().unwrap();
        let mut leaf_params = params_for(name);
        if expired {
            // Deliberately in the past so validation must reject it.
            let past = time::OffsetDateTime::now_utc() - time::Duration::days(30);
            leaf_params.not_before = past - time::Duration::days(10);
            leaf_params.not_after = past;
        }
        let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();

        TlsFixture {
            ca_pem: ca.pem(),
            leaf_pem: leaf.pem(),
            key_pem: leaf_key.serialize_pem(),
        }
    }
}

pub struct TlsPeer {
    pub addr: SocketAddr,
    pub ca_pem: String,
    accepts: Arc<AtomicU32>,
}

impl TlsPeer {
    pub fn accept_count(&self) -> u32 {
        self.accepts.load(Ordering::SeqCst)
    }
    pub fn url(&self, path: &str) -> String {
        format!("https://localhost:{}{}", self.addr.port(), path)
    }
}

/// TLS peer advertising exactly the given ALPN protocols.
async fn spawn_tls_server(fx: TlsFixture, alpn: &[&str]) -> TlsPeer {
    use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

    let certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(fx.leaf_pem.as_bytes())
            .collect::<Result<_, _>>()
            .unwrap();
    let key = PrivateKeyDer::from_pem_slice(fx.key_pem.as_bytes()).unwrap();

    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    cfg.alpn_protocols = alpn.iter().map(|p| p.as_bytes().to_vec()).collect();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));

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
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(sock).await else {
                    return;
                };
                let is_h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2");
                let io = hyper_util::rt::TokioIo::new(tls);
                let svc = hyper::service::service_fn(|_r| async {
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(
                        http_body_util::Full::new(bytes::Bytes::from_static(b"tls-ok")),
                    ))
                });
                if is_h2 {
                    let _ = hyper::server::conn::http2::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, svc)
                    .await;
                } else {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await;
                }
            });
        }
    });

    TlsPeer {
        addr,
        ca_pem: fx.ca_pem,
        accepts,
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

// --- tests -----------------------------------------------------------------

#[test]
fn valid_chain_and_hostname_succeeds_over_alpn_h2() {
    // The positive counterpart. Every "must fail" case below only means
    // something because this one passes through the same code path.
    let r = rt();
    let peer = r.block_on(spawn_tls_server(TlsFixture::valid(), &["h2"]));
    let client = new_client_with_ca(&peer.ca_pem, HYPER4K_CLIENT_HTTP2_REQUIRED);
    let cap = send_and_wait(client, &peer.url("/x"));
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    assert_eq!(cap.status.load(Ordering::SeqCst), 200);
    assert_eq!(cap.version.load(Ordering::SeqCst), 2, "expected HTTP/2");
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn wrong_ca_fails_with_tls_ca() {
    let r = rt();
    let peer = r.block_on(spawn_tls_server(TlsFixture::valid(), &["h2", "http/1.1"]));
    let unrelated = TlsFixture::valid(); // a different CA entirely
    let client = new_client_with_ca(&unrelated.ca_pem, HYPER4K_CLIENT_CA_REPLACE_SYSTEM);
    let cap = send_and_wait(client, &peer.url("/x"));
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TLS_CA));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn wrong_hostname_fails_with_tls_hostname() {
    let r = rt();
    let peer = r.block_on(spawn_tls_server(
        TlsFixture::for_name("example.invalid"),
        &["h2", "http/1.1"],
    ));
    let client = new_client_with_ca(&peer.ca_pem, HYPER4K_CLIENT_CA_REPLACE_SYSTEM);
    let cap = send_and_wait(client, &peer.url("/x"));
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TLS_HOSTNAME));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn expired_certificate_fails_with_tls_expired() {
    let r = rt();
    let peer = r.block_on(spawn_tls_server(TlsFixture::expired(), &["h2", "http/1.1"]));
    let client = new_client_with_ca(&peer.ca_pem, HYPER4K_CLIENT_CA_REPLACE_SYSTEM);
    let cap = send_and_wait(client, &peer.url("/x"));
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TLS_EXPIRED));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn http2_required_fails_when_peer_offers_only_http11() {
    let r = rt();
    let peer = r.block_on(spawn_tls_server(TlsFixture::valid(), &["http/1.1"]));
    let client = new_client_with_ca(
        &peer.ca_pem,
        HYPER4K_CLIENT_CA_REPLACE_SYSTEM | HYPER4K_CLIENT_HTTP2_REQUIRED,
    );
    let cap = send_and_wait(client, &peer.url("/x"));
    assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_ALPN_NO_H2));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn without_http2_required_the_same_peer_succeeds_over_http11() {
    // Proves the previous test failed on policy, not on a broken handshake.
    let r = rt();
    let peer = r.block_on(spawn_tls_server(TlsFixture::valid(), &["http/1.1"]));
    let client = new_client_with_ca(&peer.ca_pem, HYPER4K_CLIENT_CA_REPLACE_SYSTEM);
    let cap = send_and_wait(client, &peer.url("/x"));
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    assert_eq!(cap.version.load(Ordering::SeqCst), 1);
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn custom_ca_is_trusted_without_replace() {
    // Only proves the private CA is added. That the *system* roots survived
    // cannot be shown offline — the ignored public test covers that half.
    let r = rt();
    let peer = r.block_on(spawn_tls_server(TlsFixture::valid(), &["h2"]));
    let client = new_client_with_ca(&peer.ca_pem, 0);
    let cap = send_and_wait(client, &peer.url("/x"));
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn ca_replace_system_drops_the_other_roots() {
    // Two independent local CAs. With REPLACE, only the configured one is
    // trusted. Deliberately no public host: unit tests must not need the network.
    let r = rt();
    let mine = r.block_on(spawn_tls_server(TlsFixture::valid(), &["h2"]));
    let other = r.block_on(spawn_tls_server(TlsFixture::valid(), &["h2"]));
    let client = new_client_with_ca(&mine.ca_pem, HYPER4K_CLIENT_CA_REPLACE_SYSTEM);

    let ok = send_and_wait(client, &mine.url("/x"));
    assert_eq!(*ok.done.lock().unwrap(), Some(-999));
    let bad = send_and_wait(client, &other.url("/x"));
    assert_eq!(*bad.done.lock().unwrap(), Some(HYPER4K_ERR_TLS_CA));
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn two_concurrent_h2_requests_share_one_real_tls_connection() {
    // The pool's fake connector proves the bookkeeping. Only this proves ALPN,
    // handshake and real multiplexing close the loop.
    let r = rt();
    let peer = r.block_on(spawn_tls_server(TlsFixture::valid(), &["h2"]));
    let client = new_client_with_ca(&peer.ca_pem, HYPER4K_CLIENT_CA_REPLACE_SYSTEM);

    let a = send_and_wait(client, &peer.url("/a"));
    let b = send_and_wait(client, &peer.url("/b"));
    assert_eq!(*a.done.lock().unwrap(), Some(-999));
    assert_eq!(*b.done.lock().unwrap(), Some(-999));
    assert_eq!(
        peer.accept_count(),
        1,
        "two h2 requests opened two TLS connections"
    );
    unsafe { hyper4k_client_free(client) };
}

#[test]
fn unknown_flag_bits_are_rejected() {
    let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
    unsafe {
        hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
    };
    o.flags = 1 << 40;
    let mut c = std::ptr::null_mut();
    assert_eq!(
        unsafe { hyper4k_client_new(&o, &mut c) },
        HYPER4K_STATUS_UNKNOWN_FLAGS
    );
}

#[test]
fn a_malformed_ca_bundle_is_rejected_at_config_time() {
    // Failing here rather than on the first request keeps a configuration
    // mistake from looking like a network problem.
    let opts = TlsOptions {
        custom_ca_pem: Some(b"not a certificate".to_vec()),
        replace_system_roots: true,
        require_h2: false,
        connect_timeout: Some(Duration::from_secs(1)),
    };
    assert_eq!(build_tls_config(&opts).unwrap_err(), HYPER4K_ERR_TLS_CA);
}
