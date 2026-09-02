//! Connection pool over hyper's low-level handshakes.
//!
//! Not `hyper_util::client::legacy::Client`: that type hides `try_send_request`,
//! which is the only supported way to learn a request was provably never sent
//! (spec §四), and it retries some cancelled requests on its own — which the
//! frozen retry rules forbid. Owning the pool also lets us account for paused
//! streams per connection and size the connection window at handshake time.

use crate::abi::*;
use bytes::Bytes;
use dashmap::DashMap;
use http_body_util::Full;
use hyper::client::conn::{http1, http2};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub(crate) type H1Sender = http1::SendRequest<Full<Bytes>>;
pub(crate) type H2Sender = http2::SendRequest<Full<Bytes>>;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct PoolKey {
    pub scheme: String,
    pub host: String,
    pub port: u16,
}

/// No TLS fingerprint in the key: a pool belongs to exactly one client, and one
/// client has exactly one trust configuration, so there is nothing to partition.
impl PoolKey {
    pub(crate) fn new(scheme: &str, host: &str, port: u16) -> Self {
        PoolKey {
            scheme: scheme.to_ascii_lowercase(),
            host: host.to_ascii_lowercase(),
            port,
        }
    }
}

pub(crate) enum Sender {
    H1(H1Sender),
    H2(H2Sender),
}

impl Sender {
    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Sender::H1(s) => s.is_closed(),
            Sender::H2(s) => s.is_closed(),
        }
    }
}

enum Slot {
    /// Exclusive: one in-flight request at a time. The sender lives here while idle.
    H1(Mutex<Option<H1Sender>>),
    /// Multiplexed: the sender is cloneable, so leases share it.
    H2(H2Sender),
}

pub struct ConnEntry {
    pub(crate) id: u64,
    slot: Slot,
    active: AtomicU32,
    paused: AtomicU32,
    dead: AtomicBool,
    driver: Mutex<Option<JoinHandle<()>>>,
}

impl ConnEntry {
    pub(crate) fn active_count(&self) -> u32 {
        self.active.load(Ordering::SeqCst)
    }
    pub(crate) fn paused_count(&self) -> u32 {
        self.paused.load(Ordering::SeqCst)
    }
    fn is_h2(&self) -> bool {
        matches!(self.slot, Slot::H2(_))
    }
    fn closed(&self) -> bool {
        if self.dead.load(Ordering::SeqCst) {
            return true;
        }
        match &self.slot {
            Slot::H2(s) => s.is_closed(),
            Slot::H1(m) => m
                .lock()
                .unwrap()
                .as_ref()
                .map(|s| s.is_closed())
                .unwrap_or(false),
        }
    }
}

/// Returns its capacity on drop.
///
/// There is deliberately no `Pool::release`: cancel, timeout, connection error,
/// retry switch and panic all unwind through `Drop`, so no path can leak a slot.
/// A pool that leaks capacity eventually believes every connection is full.
pub(crate) struct Lease {
    pub(crate) sender: Option<Sender>,
    pub(crate) conn_id: u64,
    pub(crate) entry: Arc<ConnEntry>,
}

impl Lease {
    pub(crate) fn sender_mut(&mut self) -> &mut Sender {
        self.sender.as_mut().expect("lease used after drop")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        match self.sender.take() {
            Some(Sender::H1(s)) => {
                // Hand the exclusive sender back unless the peer is gone.
                if s.is_closed() {
                    self.entry.dead.store(true, Ordering::SeqCst);
                } else if let Slot::H1(m) = &self.entry.slot {
                    *m.lock().unwrap() = Some(s);
                }
            }
            Some(Sender::H2(s)) => {
                if s.is_closed() {
                    self.entry.dead.store(true, Ordering::SeqCst);
                }
            }
            None => {}
        }
        self.entry.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Holds one unit of the per-connection paused-stream budget.
pub struct PauseGuard {
    entry: Arc<ConnEntry>,
}

impl PauseGuard {
    pub fn new(entry: Arc<ConnEntry>) -> Self {
        entry.paused.fetch_add(1, Ordering::SeqCst);
        PauseGuard { entry }
    }
}

impl Drop for PauseGuard {
    fn drop(&mut self) {
        self.entry.paused.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) struct Connected {
    pub sender: Sender,
    pub driver: JoinHandle<()>,
}

pub(crate) type ConnectFuture =
    Pin<Box<dyn Future<Output = Result<Connected, Hyper4kErrorKind>> + Send>>;

pub(crate) trait Connector: Send + Sync + 'static {
    fn connect(&self, key: &PoolKey) -> ConnectFuture;
}

/// Per-authority connection cap. Enough for a busy h1 caller without letting a
/// burst open an unbounded number of sockets.
pub(crate) const DEFAULT_MAX_CONNS_PER_KEY: u32 = 8;

/// Paused streams allowed on one h2 connection.
///
/// The reservation invariant: the connection window must stay above
/// (this cap x max per-stream occupancy) + the reserve the active streams need,
/// or parked streams would starve the live ones sharing that connection.
pub(crate) const DEFAULT_PAUSED_CAP: u32 = 4;

pub(crate) struct Pool {
    connector: Arc<dyn Connector>,
    conns: DashMap<PoolKey, Vec<Arc<ConnEntry>>>,
    /// Serialises dials per key. For h2 this is what turns a burst into one
    /// connection instead of N.
    dial_locks: DashMap<PoolKey, Arc<tokio::sync::Mutex<()>>>,
    next_id: AtomicU64,
    max_conns_per_key: u32,
    paused_cap: u32,
    closed: AtomicBool,
    shutting_down: AtomicBool,
    shutdown_complete: AtomicBool,
    shutdown_done: tokio::sync::Notify,
}

impl Pool {
    pub(crate) fn new(connector: Arc<dyn Connector>) -> Self {
        Pool {
            connector,
            conns: DashMap::new(),
            dial_locks: DashMap::new(),
            next_id: AtomicU64::new(1),
            max_conns_per_key: DEFAULT_MAX_CONNS_PER_KEY,
            // Reservation invariant: connection window must exceed
            // (paused cap x max stream occupancy) + the active reserve, so
            // paused streams can never starve the live ones on that connection.
            paused_cap: DEFAULT_PAUSED_CAP,
            closed: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            shutdown_done: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn with_max_connections_per_key(mut self, n: u32) -> Self {
        self.max_conns_per_key = n;
        self
    }

    pub(crate) fn with_paused_cap(mut self, n: u32) -> Self {
        self.paused_cap = n;
        self
    }

    fn eligible(&self, entry: &Arc<ConnEntry>) -> bool {
        if entry.closed() {
            return false;
        }
        if entry.paused_count() >= self.paused_cap {
            return false;
        }
        match &entry.slot {
            // Multiplexed: room for another stream.
            Slot::H2(_) => true,
            // Exclusive: only when nobody holds it.
            Slot::H1(m) => m.lock().unwrap().is_some(),
        }
    }

    fn take_lease(&self, entry: &Arc<ConnEntry>) -> Option<Lease> {
        let sender = match &entry.slot {
            Slot::H2(s) => Sender::H2(s.clone()),
            Slot::H1(m) => Sender::H1(m.lock().unwrap().take()?),
        };
        entry.active.fetch_add(1, Ordering::SeqCst);
        Some(Lease {
            sender: Some(sender),
            conn_id: entry.id,
            entry: entry.clone(),
        })
    }

    fn reuse(&self, key: &PoolKey) -> Option<Lease> {
        let list = self.conns.get(key)?;
        for entry in list.iter() {
            if self.eligible(entry) {
                if let Some(lease) = self.take_lease(entry) {
                    return Some(lease);
                }
            }
        }
        None
    }

    pub(crate) async fn acquire(&self, key: &PoolKey) -> Result<Lease, Hyper4kErrorKind> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(HYPER4K_ERR_CANCELLED);
        }
        if let Some(lease) = self.reuse(key) {
            return Ok(lease);
        }

        let lock = self
            .dial_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _dial = lock.lock().await;

        // Someone may have dialled while we waited — that is the whole point.
        if let Some(lease) = self.reuse(key) {
            return Ok(lease);
        }

        let live = self
            .conns
            .get(key)
            .map(|l| l.iter().filter(|e| !e.closed()).count())
            .unwrap_or(0);
        if live as u32 >= self.max_conns_per_key {
            // Deliberate throttling, not an allocator failure.
            return Err(HYPER4K_ERR_CONNECT);
        }

        let connected = self.connector.connect(key).await?;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let slot = match connected.sender {
            Sender::H1(s) => Slot::H1(Mutex::new(Some(s))),
            Sender::H2(s) => Slot::H2(s),
        };
        let entry = Arc::new(ConnEntry {
            id,
            slot,
            active: AtomicU32::new(0),
            paused: AtomicU32::new(0),
            dead: AtomicBool::new(false),
            driver: Mutex::new(Some(connected.driver)),
        });
        self.conns
            .entry(key.clone())
            .or_default()
            .push(entry.clone());
        self.take_lease(&entry).ok_or(HYPER4K_ERR_CONNECT)
    }

    /// Aborts and joins every connection driver.
    ///
    /// A driver that outlives shutdown would keep `hyper4k_client_free` blocked
    /// forever, so this must be exhaustive.
    pub(crate) async fn shutdown(&self) {
        // Single-flight: close() kicks one off and free() runs another, so
        // without this both would walk and clear the same map concurrently.
        // Whoever loses simply waits for the winner to finish.
        if self.shutting_down.swap(true, Ordering::SeqCst) {
            // Notify has no memory of past signals, so re-check the flag with
            // the waiter already registered. Awaiting first would hang forever
            // whenever the winner finished before we got here.
            loop {
                if self.shutdown_complete.load(Ordering::SeqCst) {
                    return;
                }
                let waiter = self.shutdown_done.notified();
                if self.shutdown_complete.load(Ordering::SeqCst) {
                    return;
                }
                waiter.await;
            }
        }
        self.closed.store(true, Ordering::SeqCst);
        let mut handles = Vec::new();
        for mut list in self.conns.iter_mut() {
            for entry in list.value_mut().iter() {
                entry.dead.store(true, Ordering::SeqCst);
                if let Some(h) = entry.driver.lock().unwrap().take() {
                    handles.push(h);
                }
            }
        }
        self.conns.clear();
        for h in handles {
            h.abort();
            let _ = h.await;
        }
        // Flag first, then wake: a waiter that registered after this point
        // still sees the flag and returns without awaiting.
        self.shutdown_complete.store(true, Ordering::SeqCst);
        self.shutdown_done.notify_waiters();
    }

    // --- test observability -------------------------------------------------

    pub(crate) fn total_paused(&self) -> u32 {
        self.conns
            .iter()
            .map(|l| l.value().iter().map(|e| e.paused_count()).sum::<u32>())
            .sum()
    }

    pub(crate) fn connection_count(&self, key: &PoolKey) -> usize {
        self.conns.get(key).map(|l| l.len()).unwrap_or(0)
    }

    pub(crate) fn active_count(&self, key: &PoolKey) -> u32 {
        self.conns
            .get(key)
            .map(|l| l.iter().map(|e| e.active_count()).sum())
            .unwrap_or(0)
    }

    pub(crate) fn eligible_connections(&self, key: &PoolKey) -> Vec<u64> {
        self.conns
            .get(key)
            .map(|l| {
                l.iter()
                    .filter(|e| self.eligible(e))
                    .map(|e| e.id)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn is_h2(&self, key: &PoolKey) -> bool {
        self.conns
            .get(key)
            .map(|l| l.iter().any(|e| e.is_h2()))
            .unwrap_or(false)
    }
}
