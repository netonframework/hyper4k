//! TLS: root store construction, ALPN, and failure classification.
//!
//! rustls types never leave this module — the C ABI exposes none of them, so
//! upgrading rustls cannot change the klib API.

use super::pool::{ConnectFuture, Connected, Connector, PoolKey, Sender};
use crate::abi::*;
use bytes::Bytes;
use http_body_util::Full;
use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

pub(crate) struct TlsOptions {
    pub custom_ca_pem: Option<Vec<u8>>,
    pub replace_system_roots: bool,
    pub require_h2: bool,
    pub connect_timeout: Option<Duration>,
}

/// Build the client TLS configuration.
///
/// The custom CA is **added to** the platform roots by default; it replaces
/// them only when asked. The two are very different in practice (a private CA
/// alongside the public web, versus a closed trust domain), so the default is
/// not left to guesswork.
pub(crate) fn build_tls_config(opts: &TlsOptions) -> Result<ClientConfig, Hyper4kErrorKind> {
    let mut roots = RootCertStore::empty();

    if !opts.replace_system_roots {
        let loaded = rustls_native_certs::load_native_certs();
        for cert in loaded.certs {
            let _ = roots.add(cert);
        }
        if roots.is_empty() {
            // Nothing to trust means every handshake would fail with a
            // confusing CA error; say so at construction instead.
            return Err(HYPER4K_ERR_TLS_OTHER);
        }
    }

    if let Some(pem) = &opts.custom_ca_pem {
        let mut rd = std::io::BufReader::new(pem.as_slice());
        let mut added = 0usize;
        for item in rustls_pemfile::certs(&mut rd) {
            let cert = item.map_err(|_| HYPER4K_ERR_TLS_CA)?;
            roots.add(cert).map_err(|_| HYPER4K_ERR_TLS_CA)?;
            added += 1;
        }
        if added == 0 {
            return Err(HYPER4K_ERR_TLS_CA);
        }
    }

    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    // Offering only h2 makes a peer that cannot do h2 fail the handshake, which
    // is exactly what HTTP2_REQUIRED means: fail, never silently downgrade.
    config.alpn_protocols = if opts.require_h2 {
        vec![b"h2".to_vec()]
    } else {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    };
    Ok(config)
}

/// Map a handshake failure onto a stable, actionable category.
///
/// "Cannot connect" and "the certificate is wrong" are different operational
/// problems; a single opaque code would make both unactionable.
pub(crate) fn classify(err: &(dyn std::error::Error + 'static)) -> Hyper4kErrorKind {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    let mut text = String::new();
    while let Some(e) = cur {
        text.push_str(&e.to_string());
        text.push(';');
        if let Some(rustls_err) = e.downcast_ref::<rustls::Error>() {
            return classify_rustls(rustls_err);
        }
        cur = e.source();
    }
    classify_text(&text)
}

fn classify_rustls(e: &rustls::Error) -> Hyper4kErrorKind {
    use rustls::CertificateError as CE;
    match e {
        // UnknownIssuer: no root matched. BadSignature: a root matched by name
        // but not by key — a same-DN CA from a different trust domain. Both are
        // "this chain does not validate against our roots".
        rustls::Error::InvalidCertificate(CE::UnknownIssuer)
        | rustls::Error::InvalidCertificate(CE::BadSignature) => HYPER4K_ERR_TLS_CA,
        rustls::Error::InvalidCertificate(CE::NotValidForName)
        | rustls::Error::InvalidCertificate(CE::NotValidForNameContext { .. }) => {
            HYPER4K_ERR_TLS_HOSTNAME
        }
        rustls::Error::InvalidCertificate(CE::Expired)
        | rustls::Error::InvalidCertificate(CE::ExpiredContext { .. })
        | rustls::Error::InvalidCertificate(CE::NotValidYet)
        | rustls::Error::InvalidCertificate(CE::NotValidYetContext { .. }) => {
            HYPER4K_ERR_TLS_EXPIRED
        }
        rustls::Error::NoApplicationProtocol => HYPER4K_ERR_ALPN_NO_H2,
        _ => HYPER4K_ERR_TLS_OTHER,
    }
}

/// Fallback when the rustls error is only reachable as text (it is wrapped in
/// an `io::Error` on some paths).
fn classify_text(t: &str) -> Hyper4kErrorKind {
    let t = t.to_ascii_lowercase();
    if t.contains("unknownissuer")
        || t.contains("unknown issuer")
        || t.contains("badsignature")
        || t.contains("bad signature")
    {
        HYPER4K_ERR_TLS_CA
    } else if t.contains("notvalidforname") || t.contains("not valid for name") {
        HYPER4K_ERR_TLS_HOSTNAME
    } else if t.contains("expired") || t.contains("notvalidyet") {
        HYPER4K_ERR_TLS_EXPIRED
    } else if t.contains("no application protocol") || t.contains("noapplicationprotocol") {
        HYPER4K_ERR_ALPN_NO_H2
    } else if t.contains("certificate") || t.contains("handshake") || t.contains("tls") {
        HYPER4K_ERR_TLS_OTHER
    } else {
        HYPER4K_ERR_CONNECT
    }
}

pub(crate) struct TlsClientConnector {
    config: Arc<ClientConfig>,
    require_h2: bool,
    connect_timeout: Option<Duration>,
}

impl TlsClientConnector {
    pub(crate) fn new(opts: &TlsOptions) -> Result<Self, Hyper4kErrorKind> {
        Ok(TlsClientConnector {
            config: Arc::new(build_tls_config(opts)?),
            require_h2: opts.require_h2,
            connect_timeout: opts.connect_timeout,
        })
    }
}

impl Connector for TlsClientConnector {
    fn connect(&self, key: &PoolKey) -> ConnectFuture {
        let addr = format!("{}:{}", key.host, key.port);
        let host = key.host.clone();
        let config = self.config.clone();
        let require_h2 = self.require_h2;
        let timeout = self.connect_timeout;

        Box::pin(async move {
            let Ok(server_name) = ServerName::try_from(host.clone()) else {
                return Err(HYPER4K_ERR_TLS_HOSTNAME);
            };
            let tcp = {
                let fut = TcpStream::connect(&addr);
                match timeout {
                    Some(d) => tokio::time::timeout(d, fut)
                        .await
                        .map_err(|_| HYPER4K_ERR_TIMEOUT)?
                        .map_err(|_| HYPER4K_ERR_CONNECT)?,
                    None => fut.await.map_err(|_| HYPER4K_ERR_CONNECT)?,
                }
            };

            let tls = TlsConnector::from(config)
                .connect(server_name, tcp)
                .await
                .map_err(|e| classify(&e))?;

            // ALPN decides the protocol; the URL never does.
            let negotiated_h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2");
            if require_h2 && !negotiated_h2 {
                return Err(HYPER4K_ERR_ALPN_NO_H2);
            }

            let io = TokioIo::new(tls);
            if negotiated_h2 {
                let (sender, conn) =
                    http2::handshake::<_, _, Full<Bytes>>(TokioExecutor::new(), io)
                        .await
                        .map_err(|_| HYPER4K_ERR_PROTOCOL)?;
                let driver = tokio::spawn(async move {
                    let _ = conn.await;
                });
                Ok(Connected {
                    sender: Sender::H2(sender),
                    driver,
                })
            } else {
                let (sender, conn) = http1::handshake::<_, Full<Bytes>>(io)
                    .await
                    .map_err(|_| HYPER4K_ERR_PROTOCOL)?;
                let driver = tokio::spawn(async move {
                    let _ = conn.await;
                });
                Ok(Connected {
                    sender: Sender::H1(sender),
                    driver,
                })
            }
        })
    }
}
