// Tier 1: pure hyper, no ABI, no Kotlin. The floor of engine + network on this box.
// Same shape as /baseline11: returns the sum of query params a+b as plain text.
use bytes::Bytes;
use http_body_util::Full;
use hyper::{body::Incoming, service::service_fn, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use std::convert::Infallible;
use tokio::net::TcpListener;

async fn handle(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let q = req.uri().query().unwrap_or("");
    let mut a = 0i64;
    let mut b = 0i64;
    for pair in q.split('&') {
        let mut it = pair.splitn(2, '=');
        match (it.next(), it.next()) {
            (Some("a"), Some(v)) => a = v.parse().unwrap_or(0),
            (Some("b"), Some(v)) => b = v.parse().unwrap_or(0),
            _ => {}
        }
    }
    Ok(Response::new(Full::new(Bytes::from((a + b).to_string()))))
}

#[tokio::main]
async fn main() {
    let addr = "0.0.0.0:19090";
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let _ = Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service_fn(handle))
                .await;
        });
    }
}
