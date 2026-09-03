//! HTTP proxy support (ABI v4.1).
//!
//! Only forward proxies speaking plain HTTP are supported: `http://host:port`.
//! Plaintext targets are sent through the proxy in absolute-form; TLS targets
//! get a CONNECT tunnel and then the usual handshake over it, so certificate
//! validation and ALPN are exactly what they would be without the proxy.
//! No proxy credentials in 4.1: a URL with userinfo is refused at construction
//! rather than silently sent unauthenticated.

use crate::abi::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProxyTarget {
    pub host: String,
    pub port: u16,
}

impl ProxyTarget {
    pub(crate) fn parse(raw: &str) -> Option<ProxyTarget> {
        let uri: hyper::Uri = raw.parse().ok()?;
        if uri.scheme_str() != Some("http") {
            return None;
        }
        let authority = uri.authority()?;
        if authority.as_str().contains('@') {
            return None;
        }
        if !matches!(uri.path(), "" | "/") || uri.query().is_some() {
            return None;
        }
        Some(ProxyTarget {
            host: uri.host()?.to_string(),
            port: uri.port_u16().unwrap_or(80),
        })
    }

    pub(crate) fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

pub(crate) async fn dial(addr: &str, timeout: Option<Duration>) -> Result<TcpStream, Hyper4kErrorKind> {
    let connect = TcpStream::connect(addr);
    match timeout {
        Some(d) => tokio::time::timeout(d, connect)
            .await
            .map_err(|_| HYPER4K_ERR_TIMEOUT)?
            .map_err(|_| HYPER4K_ERR_CONNECT),
        None => connect.await.map_err(|_| HYPER4K_ERR_CONNECT),
    }
}

/// Opens a CONNECT tunnel to `host:port` through `proxy`. The returned stream
/// carries the raw bytes of the target; TLS runs on top of it.
pub(crate) async fn tunnel(
    proxy: &ProxyTarget,
    host: &str,
    port: u16,
    timeout: Option<Duration>,
) -> Result<TcpStream, Hyper4kErrorKind> {
    let mut stream = dial(&proxy.addr(), timeout).await?;
    let request = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
    let exchange = async {
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|_| HYPER4K_ERR_CONNECT)?;
        let mut head = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let n = stream.read(&mut buf).await.map_err(|_| HYPER4K_ERR_CONNECT)?;
            if n == 0 {
                return Err(HYPER4K_ERR_CONNECT);
            }
            head.extend_from_slice(&buf[..n]);
            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            if head.len() > 8 * 1024 {
                return Err(HYPER4K_ERR_PROTOCOL);
            }
        }
        // "HTTP/1.1 200 Connection established": only the status matters.
        let status = std::str::from_utf8(&head)
            .ok()
            .and_then(|s| s.lines().next())
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .ok_or(HYPER4K_ERR_PROTOCOL)?;
        if !(200..300).contains(&status) {
            return Err(HYPER4K_ERR_PROTOCOL);
        }
        Ok(())
    };
    match timeout {
        Some(d) => tokio::time::timeout(d, exchange)
            .await
            .map_err(|_| HYPER4K_ERR_TIMEOUT)??,
        None => exchange.await?,
    }
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::ProxyTarget;

    #[test]
    fn parses_host_and_default_port() {
        let p = ProxyTarget::parse("http://proxy.local").unwrap();
        assert_eq!((p.host.as_str(), p.port), ("proxy.local", 80));
        let p = ProxyTarget::parse("http://10.0.0.1:3128/").unwrap();
        assert_eq!((p.host.as_str(), p.port), ("10.0.0.1", 3128));
    }

    #[test]
    fn refuses_what_it_cannot_honour() {
        // https proxies, credentials and paths are not part of 4.1; accepting
        // them would mean silently doing something other than what was asked.
        assert!(ProxyTarget::parse("https://proxy:443").is_none());
        assert!(ProxyTarget::parse("http://user:pw@proxy:3128").is_none());
        assert!(ProxyTarget::parse("http://proxy:3128/path").is_none());
        assert!(ProxyTarget::parse("socks5://proxy:1080").is_none());
        assert!(ProxyTarget::parse("proxy:3128").is_none());
    }
}
