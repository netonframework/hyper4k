//! The plaintext (`http://`) connector.
//!
//! **Always HTTP/1.1.** ABI v4 ships no h2c client, so an `http://` URL must
//! never negotiate HTTP/2 — doing so would smuggle in a capability the spec
//! deliberately excludes, with no ALPN and no way for the caller to opt out.

use super::pool::{ConnectFuture, Connected, Connector, PoolKey, Sender};
use crate::abi::*;
use bytes::Bytes;
use http_body_util::Full;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use std::time::Duration;
use tokio::net::TcpStream;

pub(crate) struct PlaintextConnector {
    pub connect_timeout: Option<Duration>,
}

impl Connector for PlaintextConnector {
    fn connect(&self, key: &PoolKey) -> ConnectFuture {
        let addr = format!("{}:{}", key.host, key.port);
        let timeout = self.connect_timeout;
        Box::pin(async move {
            let connect = TcpStream::connect(&addr);
            let stream = match timeout {
                Some(d) => tokio::time::timeout(d, connect)
                    .await
                    .map_err(|_| HYPER4K_ERR_TIMEOUT)?
                    .map_err(|_| HYPER4K_ERR_CONNECT)?,
                None => connect.await.map_err(|_| HYPER4K_ERR_CONNECT)?,
            };
            let io = TokioIo::new(stream);
            let (sender, conn) = http1::handshake::<_, Full<Bytes>>(io)
                .await
                .map_err(|_| HYPER4K_ERR_CONNECT)?;
            let driver = tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(Connected {
                sender: Sender::H1(sender),
                driver,
            })
        })
    }
}
