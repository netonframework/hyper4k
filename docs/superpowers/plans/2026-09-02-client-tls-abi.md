# hyper4k Client TLS ABI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give hyper4k an outbound HTTP client over a stable C ABI, with TLS, ALPN, connection pooling, streaming with backpressure, cancellation and RFC-9113-safe retry.

**Architecture:** The client lives entirely in Rust on Hyper's stable Rust API plus rustls. Kotlin sees a small, coarse-grained C ABI: create/close/free a client, send/cancel a request, and three callbacks (headers, chunk, done). No rustls, hyper or CryptoProvider type ever crosses the boundary. Per-request bounded queues on a dedicated bridge executor carry events out; backpressure is expressed by the chunk callback's return value, not by queue capacity.

**Connection layer:** `hyper::client::conn::{http1,http2}::handshake` with a pool we
own — **not** `hyper_util::client::legacy::Client`. Two reasons: the legacy client
does not expose `try_send_request`, which is the only supported way to learn that a
request was provably never sent (spec §四); and it retries some cancelled requests on
its own, which would violate the frozen retry rules. Owning the pool also gives us
per-connection paused-stream accounting and window sizing at handshake time.

**Tech Stack:** Rust 2021, hyper 1.11, hyper-util 0.1, tokio 1.53, rustls 0.23 with `aws-lc-rs`, tokio-rustls 0.26. Tests use `rcgen` for throwaway CAs and `hyper` client for the peer side.

**Spec:** `docs/ABI_V4_CLIENT_TLS.md` (DESIGN FROZEN, 2026-09-02)

## Scope

This plan implements the **Rust ABI only** — everything the spec froze. The Kotlin
`Hyper4kHttpClient` and its wiring into Neton's `HttpClient` interface are a
**separate plan**: the spec deliberately does not define that Kotlin API surface,
and bundling it here would produce a plan whose second half argues from a document
that does not exist. The one Kotlin-side item included is acceptance #43's width
assertion, because it guards the ABI itself.

## Global Constraints

- ABI version `HYPER4K_ABI_VERSION = (4 << 16) | 0`. Values of all cross-ABI
  constants are frozen; new members may only be appended.
- **No `typedef enum` for any cross-ABI type.** Use `typedef int32_t` + `#define`;
  Rust side uses `i32` or `#[repr(i32)]`.
- Exactly one crypto provider compiled in: rustls with `default-features = false`,
  feature `aws-lc-rs`.
- `crate-type` stays `["staticlib", "cdylib"]`; cross-target builds use
  `cargo rustc --crate-type staticlib` (already wired in `build.gradle.kts`).
- Platform matrix is **four targets**: `aarch64-apple-darwin`,
  `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`.
  Windows is out.
- `panic = "abort"` in release — no panic may cross the FFI boundary.
- Every "must fail" test needs a positive counterpart proving the same code path
  succeeds under a correct configuration.

---

## File Structure

`lib/src/lib.rs` is already 1196 lines and owns the whole server ABI. The client
goes in its own module tree rather than growing that file further.

- Create `lib/src/abi.rs` — `Hyper4kStatus`, `Hyper4kSlice`, `Hyper4kHeader`,
  version and capability entry points. Shared by server and client.
- Create `lib/src/client/mod.rs` — `Hyper4kClient` handle, options/request structs,
  the six exported client functions.
- Create `lib/src/client/pool.rs` — connection pool over low-level handshakes.
- Create `lib/src/client/bridge.rs` — per-request bounded queue, bridge executor,
  pause permit, generation counter, terminal gate.
- Create `lib/src/client/tls.rs` — rustls config construction, root store, ALPN.
- Create `lib/src/client/retry.rs` — `response_committed`, the provably-unsent
  signal from `try_send_request`, idempotency and retry budget.
- Modify `lib/src/lib.rs` — add `mod abi; mod client;`, re-export nothing else.
- Modify `lib/include/hyper4k.h` — client declarations.
- Modify `lib/Cargo.toml` — rustls, tokio-rustls, `rcgen` dev-dependency.

---

## Task 0: Spike — what safety signal does hyper actually give us?

**Throwaway.** The output is an answer recorded in the plan, not code we keep.
Tasks 4 and 7 both depend on it, so it runs first and blocks them.

**Question:** Can `SendRequest::try_send_request` + `TrySendError::take_message`
carry the whole retry boundary in spec §四, over both h2 and h1, without parsing
GOAWAY frames?

Source reading already says yes — hyper documents `take_message` as returning the
request only when it "was never fully sent". But source contracts are not runtime
behaviour, and the whole retry state machine rests on this one signal.

**Files:** `lib/tests/spike_retry_signal.rs` (deleted at the end of the task)

- [ ] **Step 1: Write the probe**

```rust
//! Throwaway spike. Delete after recording the findings in the plan.
//! Run: cargo test --test spike_retry_signal -- --nocapture

#[tokio::test]
async fn goaway_refused_stream_returns_the_request() {
    // Server: accept the connection, send SETTINGS, then GOAWAY(last_stream_id=0)
    // so any stream we open is provably unprocessed.
    let addr = spawn_h2_goaway_immediately().await;
    let (mut sender, conn) = h2_handshake(addr).await;
    tokio::spawn(conn);
    let err = sender
        .try_send_request(post_request("/x"))
        .await
        .expect_err("peer refuses everything");
    let mut err = err;
    assert!(err.take_message().is_some(),
            "GOAWAY-excluded request must come back for safe replay");
}

#[tokio::test]
async fn request_already_on_the_wire_is_not_returned() {
    // Server reads the full request, then drops the connection without responding.
    let addr = spawn_h2_read_then_drop().await;
    let (mut sender, conn) = h2_handshake(addr).await;
    tokio::spawn(conn);
    let mut err = sender
        .try_send_request(post_request("/x"))
        .await
        .expect_err("connection died");
    assert!(err.take_message().is_none(),
            "a serialized request must NOT look replayable");
}

#[tokio::test]
async fn http1_stale_pooled_connection_follows_the_same_rule() {
    // h1 keep-alive connection closed by the peer between requests.
    let addr = spawn_h1_close_after_first().await;
    let (mut sender, conn) = h1_handshake(addr).await;
    tokio::spawn(conn);
    let _ = sender.send_request(get_request("/first")).await;
    let mut err = sender
        .try_send_request(post_request("/second"))
        .await
        .expect_err("peer closed the keep-alive connection");
    assert!(err.take_message().is_some(),
            "a request never written to a dead connection must come back");
}

#[tokio::test]
async fn headers_received_then_connection_dies_is_not_replayable() {
    // The committed case: response started, then the peer vanishes.
    let addr = spawn_h2_headers_then_die().await;
    let (mut sender, conn) = h2_handshake(addr).await;
    tokio::spawn(conn);
    let res = sender.try_send_request(get_request("/x")).await;
    // Either we get a response whose body then errors, or a send error with no
    // message. Both must be distinguishable from "never sent".
    match res {
        Ok(resp) => {
            let err = collect_body(resp).await.expect_err("body must fail");
            eprintln!("committed-then-died surfaced as body error: {err}");
        }
        Err(mut e) => assert!(e.take_message().is_none()),
    }
}
```

- [ ] **Step 2: Run it**

Run: `cd lib && cargo test --test spike_retry_signal -- --nocapture`
Expected: all four pass. If `http1_stale_pooled_connection_follows_the_same_rule`
fails, h1 needs its own staleness check in the pool and Task 6 grows a branch.

- [ ] **Step 3: Record the findings here**

Write the outcome into this plan under Task 7 as a table, recording **the exact
hyper version tested** (`cargo tree -p hyper | head -1`) and the result of each of
the four probes:

| probe | expected | observed | hyper version |
|---|---|---|---|
| GOAWAY-excluded stream | `Some` | | |
| already serialised | `None` | | |
| h1 stale pooled connection | `Some` | | |
| committed then died | not replayable | | |

"Source docs say so" is not a finding; only observed behaviour is. If any row
disagrees, stop and re-plan Task 7 before writing production code. Task 7's
permanent black-box tests then take over this guarantee — the spike is deleted, so
nothing but those tests keeps it honest.

- [ ] **Step 4: Delete the spike and commit the findings**

```bash
git rm lib/tests/spike_retry_signal.rs
git add docs/superpowers/plans/2026-09-02-client-tls-abi.md
git commit -m "Record what the hyper retry signal guarantees"
```

---

## Task 1: ABI foundations

**Files:**
- Create: `lib/src/abi.rs`
- Modify: `lib/src/lib.rs:1-30` (add `mod abi;`)
- Modify: `lib/include/hyper4k.h`
- Test: `lib/src/abi.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: nothing.
- Produces: `pub type Hyper4kStatus = i32;` and its constants
  (`HYPER4K_OK = 0`, `HYPER4K_STATUS_ABI_MISMATCH = -1`, `_STRUCT_SIZE = -2`,
  `_UNKNOWN_FLAGS = -3`, `_INVALID_ARG = -4`, `_UNSUPPORTED = -5`,
  `_CLIENT_CLOSED = -6`, `_OOM = -7`, `_RESOURCE_EXHAUSTED = -8`,
  `_NOT_FOUND = -20`, `_ALREADY_DONE = -21`, `_NOT_PAUSED = -22`);
  `pub extern "C" fn hyper4k_abi_version() -> u32`;
  `hyper4k_version() -> *const c_char`;
  `hyper4k_server_capabilities() -> u64`; `hyper4k_client_capabilities() -> u64`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_version_is_four_zero() {
        assert_eq!(hyper4k_abi_version(), (4u32 << 16) | 0u32);
    }

    #[test]
    fn status_values_are_frozen() {
        // These numbers are published across languages. Changing one is a
        // breaking change, so pin them literally rather than by expression.
        assert_eq!(HYPER4K_OK, 0);
        assert_eq!(HYPER4K_STATUS_ABI_MISMATCH, -1);
        assert_eq!(HYPER4K_STATUS_STRUCT_SIZE, -2);
        assert_eq!(HYPER4K_STATUS_UNKNOWN_FLAGS, -3);
        assert_eq!(HYPER4K_STATUS_INVALID_ARG, -4);
        assert_eq!(HYPER4K_STATUS_UNSUPPORTED, -5);
        assert_eq!(HYPER4K_STATUS_CLIENT_CLOSED, -6);
        assert_eq!(HYPER4K_STATUS_OOM, -7);
        assert_eq!(HYPER4K_STATUS_RESOURCE_EXHAUSTED, -8);
        assert_eq!(HYPER4K_STATUS_NOT_FOUND, -20);
        assert_eq!(HYPER4K_STATUS_ALREADY_DONE, -21);
        assert_eq!(HYPER4K_STATUS_NOT_PAUSED, -22);
    }

    #[test]
    fn cross_abi_types_are_four_bytes() {
        assert_eq!(std::mem::size_of::<Hyper4kStatus>(), 4);
        assert_eq!(std::mem::size_of::<Hyper4kErrorKind>(), 4);
        assert_eq!(std::mem::size_of::<Hyper4kHeadersAction>(), 4);
        assert_eq!(std::mem::size_of::<Hyper4kChunkAction>(), 4);
    }

    #[test]
    fn client_capabilities_report_only_shipped_features() {
        // Nothing is implemented yet, so the client advertises nothing.
        assert_eq!(hyper4k_client_capabilities(), 0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lib && cargo test --lib abi::`
Expected: FAIL — `cannot find function hyper4k_abi_version`

- [ ] **Step 3: Write minimal implementation**

```rust
//! Shared C ABI surface: status codes, slices, version and capability queries.

use std::ffi::c_char;

pub type Hyper4kStatus = i32;

pub const HYPER4K_OK: Hyper4kStatus = 0;
pub const HYPER4K_STATUS_ABI_MISMATCH: Hyper4kStatus = -1;
pub const HYPER4K_STATUS_STRUCT_SIZE: Hyper4kStatus = -2;
pub const HYPER4K_STATUS_UNKNOWN_FLAGS: Hyper4kStatus = -3;
pub const HYPER4K_STATUS_INVALID_ARG: Hyper4kStatus = -4;
pub const HYPER4K_STATUS_UNSUPPORTED: Hyper4kStatus = -5;
pub const HYPER4K_STATUS_CLIENT_CLOSED: Hyper4kStatus = -6;
pub const HYPER4K_STATUS_OOM: Hyper4kStatus = -7;
pub const HYPER4K_STATUS_RESOURCE_EXHAUSTED: Hyper4kStatus = -8;
pub const HYPER4K_STATUS_NOT_FOUND: Hyper4kStatus = -20;
pub const HYPER4K_STATUS_ALREADY_DONE: Hyper4kStatus = -21;
pub const HYPER4K_STATUS_NOT_PAUSED: Hyper4kStatus = -22;

pub type Hyper4kErrorKind = i32;
pub const HYPER4K_ERR_NONE: Hyper4kErrorKind = 0;
pub const HYPER4K_ERR_DNS: Hyper4kErrorKind = 1;
pub const HYPER4K_ERR_CONNECT: Hyper4kErrorKind = 2;
pub const HYPER4K_ERR_TLS_CA: Hyper4kErrorKind = 3;
pub const HYPER4K_ERR_TLS_HOSTNAME: Hyper4kErrorKind = 4;
pub const HYPER4K_ERR_TLS_EXPIRED: Hyper4kErrorKind = 5;
pub const HYPER4K_ERR_TLS_OTHER: Hyper4kErrorKind = 6;
pub const HYPER4K_ERR_ALPN_NO_H2: Hyper4kErrorKind = 7;
pub const HYPER4K_ERR_PROTOCOL: Hyper4kErrorKind = 8;
pub const HYPER4K_ERR_TIMEOUT: Hyper4kErrorKind = 9;
pub const HYPER4K_ERR_IDLE_TIMEOUT: Hyper4kErrorKind = 10;
pub const HYPER4K_ERR_CANCELLED: Hyper4kErrorKind = 11;
pub const HYPER4K_ERR_TRUNCATED: Hyper4kErrorKind = 12;
pub const HYPER4K_ERR_OUTCOME_UNKNOWN: Hyper4kErrorKind = 13;

pub type Hyper4kHeadersAction = i32;
pub const HYPER4K_HEADERS_CONTINUE: Hyper4kHeadersAction = 0;
pub const HYPER4K_HEADERS_CANCEL: Hyper4kHeadersAction = 2;

pub type Hyper4kChunkAction = i32;
pub const HYPER4K_CHUNK_CONTINUE: Hyper4kChunkAction = 0;
pub const HYPER4K_CHUNK_PAUSE: Hyper4kChunkAction = 1;
pub const HYPER4K_CHUNK_CANCEL: Hyper4kChunkAction = 2;

#[repr(C)]
pub struct Hyper4kHeader {
    pub name: crate::Hyper4kSlice,
    pub value: crate::Hyper4kSlice,
}

#[repr(C)]
pub struct Hyper4kError {
    pub kind: Hyper4kErrorKind,
    pub protocol_code: u32,
    pub message: crate::Hyper4kSlice,
}

const ABI_VERSION: u32 = (4 << 16) | 0;
const VERSION_CSTR: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();

#[no_mangle]
pub extern "C" fn hyper4k_abi_version() -> u32 {
    ABI_VERSION
}

#[no_mangle]
pub extern "C" fn hyper4k_version() -> *const c_char {
    VERSION_CSTR.as_ptr() as *const c_char
}

/// Server capability bits. Streaming and h2c shipped in ABI v3 with tests.
pub const HYPER4K_SERVER_CAP_HTTP1: u64 = 1 << 0;
pub const HYPER4K_SERVER_CAP_H2C: u64 = 1 << 1;
pub const HYPER4K_SERVER_CAP_STREAMING: u64 = 1 << 2;

#[no_mangle]
pub extern "C" fn hyper4k_server_capabilities() -> u64 {
    HYPER4K_SERVER_CAP_HTTP1 | HYPER4K_SERVER_CAP_H2C | HYPER4K_SERVER_CAP_STREAMING
}

pub const HYPER4K_CLIENT_CAP_HTTP1: u64 = 1 << 0;
pub const HYPER4K_CLIENT_CAP_HTTP2: u64 = 1 << 1;
pub const HYPER4K_CLIENT_CAP_TLS: u64 = 1 << 2;
pub const HYPER4K_CLIENT_CAP_CUSTOM_CA: u64 = 1 << 3;
pub const HYPER4K_CLIENT_CAP_CANCEL: u64 = 1 << 4;
pub const HYPER4K_CLIENT_CAP_STREAMING: u64 = 1 << 5;

/// A bit may only appear here once its feature is implemented and tested.
/// Tasks 2–5 each flip exactly one group of bits on, in their own commit.
#[no_mangle]
pub extern "C" fn hyper4k_client_capabilities() -> u64 {
    0
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd lib && cargo test --lib abi::`
Expected: PASS, 4 tests

- [ ] **Step 5: Mirror the declarations into the C header**

Add to `lib/include/hyper4k.h`, above the existing server section, the fixed-width
typedefs and `#define`s exactly as written in spec §2.1, plus:

```c
uint32_t    hyper4k_abi_version(void);
const char *hyper4k_version(void);
uint64_t    hyper4k_server_capabilities(void);
uint64_t    hyper4k_client_capabilities(void);
```

- [ ] **Step 6: Commit**

```bash
git add lib/src/abi.rs lib/src/lib.rs lib/include/hyper4k.h
git commit -m "Add the shared ABI status codes and version queries"
```

---

## Task 2: Options and request structs with version-safe init

**Files:**
- Create: `lib/src/client/mod.rs`
- Modify: `lib/src/lib.rs` (add `mod client;`)
- Modify: `lib/include/hyper4k.h`
- Test: inline in `lib/src/client/mod.rs`

**Interfaces:**
- Consumes: `Hyper4kStatus` and constants from Task 1.
- Produces: `#[repr(C)] Hyper4kClientOptions` and `Hyper4kClientRequest` with the
  exact field order in spec §2.2 and §2.6;
  `hyper4k_client_options_init(*mut Hyper4kClientOptions, u32) -> Hyper4kStatus`;
  `hyper4k_client_request_init(*mut Hyper4kClientRequest, u32) -> Hyper4kStatus`;
  `pub const OPTIONS_MIN_SIZE: u32` (offset past `flags`) and
  `pub const REQUEST_MIN_SIZE: u32` (offset past `url`);
  `pub const KNOWN_CLIENT_FLAGS: u64`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_init_sets_defaults_distinguishable_from_zero() {
        let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
        let st = unsafe {
            hyper4k_client_options_init(&mut o, std::mem::size_of::<Hyper4kClientOptions>() as u32)
        };
        assert_eq!(st, HYPER4K_OK);
        assert_eq!(o.abi_version, hyper4k_abi_version());
        // The whole point of the init function: a zeroed struct would mean
        // "no retries", which is a different intent from "use the default".
        assert_eq!(o.max_retries, 2);
    }

    #[test]
    fn request_init_marks_idle_timeout_as_inherit() {
        let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
        let st = unsafe {
            hyper4k_client_request_init(&mut r, std::mem::size_of::<Hyper4kClientRequest>() as u32)
        };
        assert_eq!(st, HYPER4K_OK);
        // 0 would mean "disabled", silently dropping the client default.
        assert_eq!(r.read_idle_timeout_ms, u64::MAX);
    }

    #[test]
    fn init_never_writes_past_the_caller_allocation() {
        // An old caller allocates ONLY the old prefix. Guard bytes sit
        // immediately after it, so a library that ignores struct_size and
        // writes the full modern struct will smash them.
        //
        // (The earlier version of this test embedded a full-size struct before
        // the guard, so a buggy implementation writing the whole struct still
        // left the guard intact — it could not fail.)
        // Allocate through the real layout so the pointer is properly aligned —
        // casting a Vec<u8> and dereferencing it would be UB regardless of what
        // the function under test does.
        const GUARD: usize = 64;
        let prefix = OPTIONS_MIN_SIZE as usize;
        let layout = std::alloc::Layout::from_size_align(
            prefix + GUARD, std::mem::align_of::<Hyper4kClientOptions>()).unwrap();
        let base = unsafe { std::alloc::alloc(layout) };
        assert!(!base.is_null());
        unsafe { std::ptr::write_bytes(base, 0xAB, prefix + GUARD) };

        let st = unsafe { hyper4k_client_options_init(base as *mut _, OPTIONS_MIN_SIZE) };
        assert_eq!(st, HYPER4K_OK);
        let tail = unsafe { std::slice::from_raw_parts(base.add(prefix), GUARD) };
        assert!(tail.iter().all(|&b| b == 0xAB),
                "init wrote past the caller's struct_size");
        // Read the two prefix fields without materialising the whole struct.
        let abi = unsafe { std::ptr::read_unaligned(base as *const u32) };
        let size = unsafe { std::ptr::read_unaligned(base.add(4) as *const u32) };
        assert_eq!(abi, hyper4k_abi_version());
        assert_eq!(size, OPTIONS_MIN_SIZE);
        unsafe { std::alloc::dealloc(base, layout) };
    }

    #[test]
    fn a_larger_caller_struct_keeps_its_tail_untouched() {
        // The other direction: a new caller against an older library. Fields the
        // library does not know about must keep the caller's preset values.
        // Same alignment discipline as the test above.
        let known = size_of::<Hyper4kClientOptions>();
        let big = known + 64;
        let layout = std::alloc::Layout::from_size_align(
            big, std::mem::align_of::<Hyper4kClientOptions>()).unwrap();
        let base = unsafe { std::alloc::alloc(layout) };
        assert!(!base.is_null());
        unsafe { std::ptr::write_bytes(base, 0xCD, big) };
        // Ask for more than this build knows: init must clamp to its own size.
        let st = unsafe { hyper4k_client_options_init(base as *mut _, big as u32) };
        assert_eq!(st, HYPER4K_OK);
        let tail = unsafe { std::slice::from_raw_parts(base.add(known), big - known) };
        assert!(tail.iter().all(|&b| b == 0xCD),
                "init touched fields beyond what this build defines");
        unsafe { std::alloc::dealloc(base, layout) };
    }

    #[test]
    fn struct_size_below_minimum_is_rejected() {
        let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
        let st = unsafe { hyper4k_client_options_init(&mut o, OPTIONS_MIN_SIZE - 1) };
        assert_eq!(st, HYPER4K_STATUS_STRUCT_SIZE);
    }

    #[test]
    fn null_pointer_is_rejected() {
        let st = unsafe { hyper4k_client_options_init(std::ptr::null_mut(), 64) };
        assert_eq!(st, HYPER4K_STATUS_INVALID_ARG);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lib && cargo test --lib client::`
Expected: FAIL — `Hyper4kClientOptions` not found

- [ ] **Step 3: Write minimal implementation**

```rust
use crate::abi::*;
use std::mem::{offset_of, size_of};

#[repr(C)]
pub struct Hyper4kClientOptions {
    pub abi_version: u32,
    pub struct_size: u32,
    pub flags: u64,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub read_idle_timeout_ms: u64,
    pub max_retries: u32,
    pub _reserved: u32,
    pub custom_ca_pem: *const u8,
    pub custom_ca_pem_len: usize,
}

#[repr(C)]
pub struct Hyper4kClientRequest {
    pub abi_version: u32,
    pub struct_size: u32,
    pub method: crate::Hyper4kSlice,
    pub url: crate::Hyper4kSlice,
    pub headers: *const Hyper4kHeader,
    pub header_count: usize,
    pub body_ptr: *const u8,
    pub body_len: usize,
    pub read_idle_timeout_ms: u64,
}

pub const HYPER4K_CLIENT_HTTP2_REQUIRED: u64 = 1 << 0;
pub const HYPER4K_CLIENT_CA_REPLACE_SYSTEM: u64 = 1 << 1;
pub const KNOWN_CLIENT_FLAGS: u64 =
    HYPER4K_CLIENT_HTTP2_REQUIRED | HYPER4K_CLIENT_CA_REPLACE_SYSTEM;

/// Everything through `flags` must be present, or the caller told us nothing.
pub const OPTIONS_MIN_SIZE: u32 =
    (offset_of!(Hyper4kClientOptions, flags) + size_of::<u64>()) as u32;
pub const REQUEST_MIN_SIZE: u32 =
    (offset_of!(Hyper4kClientRequest, url) + size_of::<crate::Hyper4kSlice>()) as u32;

/// Writes at most `min(struct_size, size_of::<T>())` bytes. A caller built
/// against an older, smaller struct must not be written past.
unsafe fn init_prefix<T>(ptr: *mut T, struct_size: u32, min_size: u32, defaults: T)
    -> Hyper4kStatus
{
    if ptr.is_null() {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    if struct_size < min_size {
        return HYPER4K_STATUS_STRUCT_SIZE;
    }
    let writable = (struct_size as usize).min(size_of::<T>());
    std::ptr::copy_nonoverlapping(
        &defaults as *const T as *const u8,
        ptr as *mut u8,
        writable,
    );
    std::mem::forget(defaults);
    HYPER4K_OK
}

#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_options_init(
    opts: *mut Hyper4kClientOptions,
    struct_size: u32,
) -> Hyper4kStatus {
    let defaults = Hyper4kClientOptions {
        abi_version: hyper4k_abi_version(),
        struct_size,
        flags: 0,
        connect_timeout_ms: 10_000,
        request_timeout_ms: 60_000,
        read_idle_timeout_ms: 60_000,
        max_retries: 2,
        _reserved: 0,
        custom_ca_pem: std::ptr::null(),
        custom_ca_pem_len: 0,
    };
    init_prefix(opts, struct_size, OPTIONS_MIN_SIZE, defaults)
}

#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_request_init(
    request: *mut Hyper4kClientRequest,
    struct_size: u32,
) -> Hyper4kStatus {
    let defaults = Hyper4kClientRequest {
        abi_version: hyper4k_abi_version(),
        struct_size,
        method: crate::Hyper4kSlice { ptr: std::ptr::null(), len: 0 },
        url: crate::Hyper4kSlice { ptr: std::ptr::null(), len: 0 },
        headers: std::ptr::null(),
        header_count: 0,
        body_ptr: std::ptr::null(),
        body_len: 0,
        read_idle_timeout_ms: u64::MAX, // inherit
    };
    init_prefix(request, struct_size, REQUEST_MIN_SIZE, defaults)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd lib && cargo test --lib client::`
Expected: PASS, 5 tests

- [ ] **Step 5: Mirror into the C header and commit**

```bash
git add lib/src/client/mod.rs lib/src/lib.rs lib/include/hyper4k.h
git commit -m "Add version-safe client options and request initialisers"
```

---

## Task 3: Connection pool (plaintext core)

**Files:**
- Create: `lib/src/client/pool.rs`
- Test: inline in `lib/src/client/pool.rs`

Switching off `legacy::Client` makes the pool the load-bearing component, so it
gets its own task, file and tests rather than one line inside the lifecycle task.

**Plaintext only in this task.** The real `ClientConfig` builder does not exist
until Task 5, so ALPN-driven sender selection and trust-based pool partitioning are
tested there, against the real thing. Verifying ALPN against a stub would prove
nothing about ALPN.

**Interfaces:**
- Consumes: `Hyper4kStatus` (Task 1). **No TLS dependency** — the pool takes a
  `Connector` trait object, and this task only supplies the plaintext h1 one. Task 5
  injects the TLS connector.
- Produces:
  `pub(crate) struct PoolKey { scheme: Scheme, host: String, port: u16 }`;
  `pub(crate) enum Sender { H1(http1::SendRequest<Full<Bytes>>), H2(http2::SendRequest<Full<Bytes>>) }`;
  `pub(crate) struct Pool` with
  `async fn acquire(&self, key: &PoolKey) -> Result<Lease, Hyper4kErrorKind>`
  and `async fn shutdown(&self)`;
  `pub(crate) struct Lease { sender: Sender, conn_id: u64, entry: Arc<ConnEntry> }`
  whose `Drop` returns the capacity;
  `pub(crate) struct PauseGuard { entry: Arc<ConnEntry> }`, likewise `Drop`-based.

> The key has **no TLS fingerprint**. The pool belongs to one client, and one
> client has exactly one trust configuration, so nothing to partition. (A global
> pool would need real partitioning, and a 64-bit hash would not be a sound
> isolation boundary anyway.)

Frozen model:

- **Key** is `(scheme, host, port)`. Nothing about trust configuration enters it:
  the pool belongs to one client, and one client has exactly one TLS policy.
- **h1 connections are exclusive**, one in-flight request each; **h2 connections
  multiplex** up to `current_max_send_streams()`.
- **ALPN decides the sender variant** after the handshake — the pool does not guess
  from the URL.
- **Each connection owns a driver task.** The pool holds its `JoinHandle`; the
  driver ends when the connection closes or `shutdown()` aborts it. `shutdown()`
  must join every driver, otherwise `hyper4k_client_free` can hang.
- **Capacity accounting is RAII, never manual.** `ConnEntry` holds
  `active: AtomicU32` and `paused: AtomicU32`. `Lease::drop` releases the h1
  exclusive slot or decrements `active`; `PauseGuard::drop` decrements `paused`
  exactly once. There is no public `release()` or `unmark_paused()` — a cancel,
  timeout, connection error, retry switch or panic all unwind through `Drop`, so
  no path can leak capacity. A pool that leaks capacity eventually believes every
  connection is full and dials forever.
- **Eviction:** a lease is discarded on drop if `is_closed()`, and an h2 connection
  stops accepting new streams once `paused` reaches the window-reservation cap from
  spec §2.5. Connection selection checks **both** `active` and `paused`.
- **Dial de-duplication:** concurrent `acquire` calls for the same key await one
  in-flight dial via a `DashMap<PoolKey, Shared<...>>`, so a burst of requests to
  one authority opens one connection, not N.

- [ ] **Step 1: Write the failing test**

Two groups. Real sockets are **plaintext h1 only** — production must never pick H2
from an `http://` URL, because spec §2.1 says v4 ships no h2c client. H2 multiplexing
and the capacity counters are exercised through an injected fake connector, so the
pool logic is covered without smuggling in a plaintext H2 handshake. Real H2 arrives
in Task 5 over TLS with ALPN.

```rust
#[cfg(test)]
mod pool_tests {
    use super::*;

    // ---- real sockets, plaintext, h1 only -------------------------------

    #[tokio::test]
    async fn plaintext_urls_always_yield_an_h1_sender() {
        // The guard against accidentally shipping an h2c client.
        let peer = spawn_h1_server().await;
        let pool = Pool::new(plaintext_only());
        let lease = pool.acquire(&key_for(&peer)).await.unwrap();
        assert!(matches!(lease.sender, Sender::H1(_)),
                "http:// must not negotiate H2 in v4");
    }

    #[tokio::test]
    async fn h1_connections_are_exclusive_while_held() {
        let peer = spawn_h1_server().await;
        let pool = Pool::new(plaintext_only());
        let key = key_for(&peer);
        let a = pool.acquire(&key).await.unwrap();
        let b = pool.acquire(&key).await.unwrap();
        assert_ne!(a.conn_id, b.conn_id, "h1 connections are exclusive");
    }

    #[tokio::test]
    async fn h1_connections_are_reused_after_release() {
        // Without this, "pool" would just mean "dial every time" for h1.
        let peer = spawn_h1_server().await;
        let pool = Pool::new(plaintext_only());
        let key = key_for(&peer);
        let a = pool.acquire(&key).await.unwrap();
        let id = a.conn_id;
        drop(a);                              // RAII returns the exclusive slot
        let b = pool.acquire(&key).await.unwrap();
        assert_eq!(b.conn_id, id, "released h1 connection was not reused");
    }

    #[tokio::test]
    async fn concurrent_acquires_dial_once() {
        let peer = spawn_h1_server_counting_accepts().await;
        let pool = Pool::new(plaintext_only());
        let key = key_for(&peer);
        let leases = futures::future::join_all(
            (0..16).map(|_| pool.acquire(&key))
        ).await;
        assert!(leases.iter().all(|l| l.is_ok()));
        assert_eq!(peer.accept_count(), 1, "connection storm: dialed more than once");
    }

    #[tokio::test]
    async fn a_closed_connection_is_evicted_when_the_lease_drops() {
        let peer = spawn_h1_server().await;
        let pool = Pool::new(plaintext_only());
        let key = key_for(&peer);
        let lease = pool.acquire(&key).await.unwrap();
        let old = lease.conn_id;
        peer.drop_all_connections();
        wait_until_async(|| lease.sender.is_closed()).await;
        drop(lease);                          // RAII: there is no pool.release()
        let fresh = pool.acquire(&key).await.unwrap();
        assert_ne!(fresh.conn_id, old, "a dead connection was handed out again");
    }

    #[tokio::test]
    async fn shutdown_joins_every_connection_driver() {
        let peer = spawn_h1_server().await;
        let pool = Pool::new(plaintext_only());
        let _lease = pool.acquire(&key_for(&peer)).await.unwrap();
        // If a driver task outlives shutdown, hyper4k_client_free would hang.
        tokio::time::timeout(Duration::from_secs(5), pool.shutdown())
            .await
            .expect("shutdown did not join its drivers");
    }

    // ---- fake connector: H2 bookkeeping without an h2c handshake ---------

    #[tokio::test]
    async fn h2_leases_to_one_authority_share_a_connection() {
        let pool = Pool::new(fake_h2_connector());
        let key = fake_key();
        let a = pool.acquire(&key).await.unwrap();
        let b = pool.acquire(&key).await.unwrap();
        assert_eq!(a.conn_id, b.conn_id, "h2 must multiplex, not redial");
    }

    #[tokio::test]
    async fn an_h2_connection_at_the_paused_cap_stops_taking_new_streams() {
        let pool = Pool::new(fake_h2_connector()).with_paused_cap(2);
        let key = fake_key();
        let a = pool.acquire(&key).await.unwrap();
        let _g1 = PauseGuard::new(a.entry.clone());
        let _g2 = PauseGuard::new(a.entry.clone());
        let c = pool.acquire(&key).await.unwrap();
        assert_ne!(c.conn_id, a.conn_id,
                   "new stream landed on a connection at its paused cap");
    }

    #[tokio::test]
    async fn dropping_a_pause_guard_restores_capacity() {
        let pool = Pool::new(fake_h2_connector()).with_paused_cap(1);
        let key = fake_key();
        let a = pool.acquire(&key).await.unwrap();
        {
            let _g = PauseGuard::new(a.entry.clone());
            let other = pool.acquire(&key).await.unwrap();
            assert_ne!(other.conn_id, a.conn_id);
        }
        // Assert the accounting, not which connection gets picked next —
        // choosing a different live connection is also legal.
        assert_eq!(a.entry.paused_count(), 0, "paused count leaked after drop");
        assert!(pool.eligible_connections(&key).contains(&a.conn_id),
                "the unpaused connection did not return to the eligible set");
        assert_eq!(pool.connection_count(&key), 2, "an extra connection was dialled");
    }

    #[tokio::test]
    async fn capacity_survives_cancel_timeout_and_connection_error() {
        // Every abnormal exit unwinds through Drop; none may leak a slot.
        let pool = Pool::new(fake_h2_connector());
        let key = fake_key();
        let before = pool.active_count(&key);
        for scenario in [Abort::Cancel, Abort::Timeout, Abort::ConnError] {
            let lease = pool.acquire(&key).await.unwrap();
            simulate_abort(scenario, lease).await;   // consumes the lease
        }
        assert_eq!(pool.active_count(&key), before,
                   "an abnormal exit leaked pool capacity");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lib && cargo test --lib client::pool_tests`
Expected: FAIL — `Pool` not found

- [ ] **Step 3: Implement the pool**

Follow the frozen model above. `acquire` looks up the key, reuses a live h2 lease
below its stream and paused caps, otherwise joins or starts a dial. A dial delegates to the injected `Connector`. The plaintext connector added here
does TCP connect then `http1::handshake` — **never** `http2::handshake`, because an
`http://` URL must not produce an H2 connection in v4. The TLS connector in Task 5
picks the handshake from the negotiated ALPN protocol. Either way the pool spawns
the connection driver and stores its `JoinHandle`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd lib && cargo test --lib client::pool_tests`
Expected: PASS, 10 tests

- [ ] **Step 5: Commit**

```bash
git add lib/src/client/pool.rs lib/src/client/mod.rs
git commit -m "Add the connection pool"
```

---

## Task 4: Client lifecycle and plaintext request path

**Files:**
- Modify: `lib/src/client/mod.rs`
- Create: `lib/src/client/bridge.rs`
- Test: inline in `lib/src/client/mod.rs`

**Interfaces:**
- Consumes: Task 2's structs and constants.
- Produces: opaque `Hyper4kClient`;
  `hyper4k_client_new(*const Hyper4kClientOptions, *mut *mut Hyper4kClient) -> Hyper4kStatus`;
  `hyper4k_client_close(*mut Hyper4kClient)`; `hyper4k_client_free(*mut Hyper4kClient)`;
  `hyper4k_client_send(...) -> Hyper4kStatus` with `out_request_id: *mut u64`;
  `hyper4k_client_cancel(*mut Hyper4kClient, u64) -> Hyper4kStatus`;
  callback typedefs `Hyper4kOnHeaders`, `Hyper4kOnChunk`, `Hyper4kOnDone` exactly as
  in spec §2.5.

- [ ] **Step 1: Write the failing test**

The peer is a real hyper server on a loopback port so the test exercises DNS-free
connect, real HTTP/1.1 framing, and the callback ordering contract.

```rust
#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Capture {
        status: AtomicU32,
        body: Mutex<Vec<u8>>,
        done: Mutex<Option<i32>>, // Hyper4kErrorKind, or -999 for "success"
    }

    extern "C" fn on_headers(ud: *mut c_void, _id: u64, status: u16, version: u8,
                             _h: *const Hyper4kHeader, _n: usize) -> Hyper4kHeadersAction {
        let cap = unsafe { &*(ud as *const Capture) };
        cap.status.store(status as u32 | ((version as u32) << 16), Ordering::SeqCst);
        HYPER4K_HEADERS_CONTINUE
    }

    extern "C" fn on_chunk(ud: *mut c_void, _id: u64, ptr: *const u8, len: usize)
        -> Hyper4kChunkAction {
        let cap = unsafe { &*(ud as *const Capture) };
        cap.body.lock().unwrap().extend_from_slice(unsafe {
            std::slice::from_raw_parts(ptr, len)
        });
        HYPER4K_CHUNK_CONTINUE
    }

    extern "C" fn on_done(ud: *mut c_void, _id: u64, error: *const Hyper4kError) {
        let cap = unsafe { &*(ud as *const Capture) };
        *cap.done.lock().unwrap() =
            Some(if error.is_null() { -999 } else { unsafe { (*error).kind } });
    }

    #[test]
    fn plaintext_get_delivers_headers_body_and_done_once() {
        let addr = spawn_echo_server();          // helper below, HTTP/1.1, 200 "pong"
        let cap = Arc::new(Capture::default());

        let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
        unsafe { hyper4k_client_options_init(&mut o, size_of::<Hyper4kClientOptions>() as u32) };
        let mut client = std::ptr::null_mut();
        assert_eq!(unsafe { hyper4k_client_new(&o, &mut client) }, HYPER4K_OK);

        let url = format!("http://{addr}/ping");
        let mut req: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
        unsafe { hyper4k_client_request_init(&mut req, size_of::<Hyper4kClientRequest>() as u32) };
        req.method = slice_of(b"GET");
        req.url = slice_of(url.as_bytes());

        let mut id = 0u64;
        let st = unsafe {
            hyper4k_client_send(client, &req, on_headers, on_chunk, on_done,
                                Arc::as_ptr(&cap) as *mut c_void, &mut id)
        };
        assert_eq!(st, HYPER4K_OK);
        assert_ne!(id, 0);

        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(-999), "expected success");
        assert_eq!(cap.status.load(Ordering::SeqCst) & 0xFFFF, 200);
        assert_eq!(&*cap.body.lock().unwrap(), b"pong");

        unsafe { hyper4k_client_close(client) };
        unsafe { hyper4k_client_free(client) };
    }

    #[test]
    fn send_after_close_is_rejected_synchronously_without_callbacks() {
        let addr = spawn_echo_server();
        let cap = Arc::new(Capture::default());
        let client = new_default_client();
        unsafe { hyper4k_client_close(client) };

        let url = format!("http://{addr}/ping");
        let req = get_request(&url);
        let mut id = 0u64;
        let st = unsafe {
            hyper4k_client_send(client, &req, on_headers, on_chunk, on_done,
                                Arc::as_ptr(&cap) as *mut c_void, &mut id)
        };
        // Either accepted with exactly one OnDone, or refused with no callback.
        // Never both, never neither.
        assert_eq!(st, HYPER4K_STATUS_CLIENT_CLOSED);
        assert!(cap.done.lock().unwrap().is_none());
        unsafe { hyper4k_client_free(client) };
    }

    #[test]
    fn close_drives_inflight_requests_to_exactly_one_done() {
        let addr = spawn_slow_server();   // holds the response open
        let cap = Arc::new(Capture::default());
        let client = new_default_client();
        let url = format!("http://{addr}/slow");
        let req = get_request(&url);
        let mut id = 0u64;
        unsafe {
            hyper4k_client_send(client, &req, on_headers, on_chunk, on_done,
                                Arc::as_ptr(&cap) as *mut c_void, &mut id)
        };
        unsafe { hyper4k_client_close(client) };
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_CANCELLED));
        unsafe { hyper4k_client_free(client) };   // must not hang
    }

    #[test]
    fn cancel_reports_three_distinct_states() {
        let addr = spawn_slow_server();
        let cap = Arc::new(Capture::default());
        let client = new_default_client();
        let url = format!("http://{addr}/slow");
        let req = get_request(&url);
        let mut id = 0u64;
        unsafe {
            hyper4k_client_send(client, &req, on_headers, on_chunk, on_done,
                                Arc::as_ptr(&cap) as *mut c_void, &mut id)
        };
        assert_eq!(unsafe { hyper4k_client_cancel(client, id) }, HYPER4K_OK);
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_CANCELLED));
        assert_eq!(unsafe { hyper4k_client_cancel(client, id) }, HYPER4K_STATUS_ALREADY_DONE);
        assert_eq!(unsafe { hyper4k_client_cancel(client, 999_999) }, HYPER4K_STATUS_NOT_FOUND);
        unsafe { hyper4k_client_close(client) };
        unsafe { hyper4k_client_free(client) };
    }

    #[test]
    fn null_on_chunk_discards_the_body_but_still_completes() {
        let addr = spawn_echo_server();
        let cap = Arc::new(Capture::default());
        let client = new_default_client();
        let url = format!("http://{addr}/ping");
        let req = get_request(&url);
        let mut id = 0u64;
        let st = unsafe {
            hyper4k_client_send(client, &req, on_headers, None, on_done,
                                Arc::as_ptr(&cap) as *mut c_void, &mut id)
        };
        assert_eq!(st, HYPER4K_OK);
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(-999));
        assert!(cap.body.lock().unwrap().is_empty());
        unsafe { hyper4k_client_close(client) };
        unsafe { hyper4k_client_free(client) };
    }

    #[test]
    fn http_scheme_with_http2_required_is_refused_at_submit() {
        let client = new_client_with_flags(HYPER4K_CLIENT_HTTP2_REQUIRED);
        let req = get_request("http://127.0.0.1:1/x");
        let mut id = 0u64;
        let cap = Arc::new(Capture::default());
        let st = unsafe {
            hyper4k_client_send(client, &req, on_headers, on_chunk, on_done,
                                Arc::as_ptr(&cap) as *mut c_void, &mut id)
        };
        assert_eq!(st, HYPER4K_STATUS_UNSUPPORTED);
        assert!(cap.done.lock().unwrap().is_none());
        unsafe { hyper4k_client_close(client) };
        unsafe { hyper4k_client_free(client) };
    }
}
```

Write the helpers `spawn_echo_server`, `spawn_slow_server`, `wait_until`,
`slice_of`, `get_request`, `new_default_client`, `new_client_with_flags` in the
same module. `spawn_echo_server` binds `TcpListener` on port 0, spawns a tokio
task serving `hyper_util::server::conn::auto::Builder` returning 200 `pong`, and
returns the bound `SocketAddr`. `wait_until` polls with a 5-second deadline and
panics on timeout so a hang fails rather than blocks CI.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lib && cargo test --lib client::lifecycle_tests`
Expected: FAIL — `hyper4k_client_new` not found

- [ ] **Step 3: Implement the client**

`Hyper4kClient` owns a `tokio::runtime::Runtime`, **our own connection pool** over
`hyper::client::conn::{http1,http2}::handshake` (see the Connection layer note in the
header — the legacy client is not usable here), a `DashMap<u64, RequestHandle>` and a
bridge executor.

`send` validates the request (`INVALID_ARG` for a NULL client/request/out pointer or
unparsable URL, `UNSUPPORTED` for `http://` + `HTTP2_REQUIRED`), copies method, URL,
headers and body into owned `Bytes`, writes `*out_request_id`, registers the handle,
then spawns the task.

**The callback-ordering contract is the narrow one, and the code must match it.**
Writing `*out_request_id` before the spawn does *not* prove "no callback before
`send` returns" — the spawned task can run on another worker while `send` is still
unwinding. What is actually guaranteed, and all that is promised:

- `*out_request_id` is written before the request can produce any event.
- No callback is invoked **re-entrantly on the calling thread** inside `send`.
- A callback may run on another thread concurrently with `send` returning.

Update spec §2.6's wording to this contract as part of this task; the absolute
phrasing there ("绝不触发任何回调") overstates what a callee can guarantee.

**Termination goes through one place, and the terminal gate comes first.**
Aborting a tokio task does **not** run its ordinary cleanup path, so "abort and let
the task emit OnDone" would silently drop the callback. But enqueueing `OnDone` and
*then* aborting also leaves a window: between those two points the network task can
still push headers or a chunk, and the caller sees a callback after `OnDone`.

**There are two termination modes, and conflating them loses good data.** A normal
success also goes through `settle_once`, and by then headers and chunks are already
sitting in the bridge queue waiting to be delivered. Discarding them unconditionally
would leave the caller with nothing but `OnDone(NULL)` — a silently empty response.

```rust
enum Settle {
    /// Normal completion: deliver what is already queued, then Done.
    DrainThenDone,
    /// Cancel / close / TRUNCATED: the queued events are stale, drop them.
    DiscardThenDone(Hyper4kErrorKind),
}
```

The control side keeps its **own `done_tx`**, separate from the event producer, so
that step 3 below can close the producer and step 5 can still enqueue `Done`.

`settle_once(mode)` runs in this order, and the order is the contract:

1. **Atomically flip the request state to `TERMINAL`** (`compare_exchange` on an
   `AtomicU8`). Losing the race means someone else is settling; return immediately.
2. Stop the network task from producing **new** events.
3. Close the event producer half of the request's queue.
4. **`DiscardThenDone` only:** drain and drop every queued non-`Done` event. This is
   what discards the queued-but-undelivered headers in spec §四.
   **`DrainThenDone` keeps them** — they are the response.
5. Enqueue `OnDone` through `done_tx` as the **last** event on that queue.
6. Abort the network task.
7. Remove the handle from the map **only after `OnDone` has returned**, so
   `user_data` stays alive for the whole callback.

`close`, `cancel` and every error path use `DiscardThenDone`; only the normal
end-of-body path uses `DrainThenDone`.

`close` walks the map calling `settle_once(CANCELLED)`; `cancel` and every error
path call the same function. `free` blocks on a `tokio::sync::Notify` until the
handle map is empty, then drops the runtime.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd lib && cargo test --lib client::lifecycle_tests`
Expected: PASS, 6 tests

- [ ] **Step 5: Turn on the capability bits this task earned**

In `abi.rs`, change `hyper4k_client_capabilities` to return
`HYPER4K_CLIENT_CAP_HTTP1 | HYPER4K_CLIENT_CAP_CANCEL` and update
`client_capabilities_report_only_shipped_features` to match.

`CAP_STREAMING` is **not** lit here: chunks are delivered, but backpressure — the
half that makes streaming safe — lands in Task 5. A bit means "implemented and
tested", not "partially works".

- [ ] **Step 6: Commit**

```bash
git add lib/src/client/ lib/src/abi.rs lib/include/hyper4k.h
git commit -m "Add the client lifecycle and the plaintext request path"
```

---

## Task 5: TLS, ALPN and error classification

**Files:**
- Create: `lib/src/client/tls.rs`
- Modify: `lib/src/client/mod.rs`, `lib/Cargo.toml`
- Test: inline in `lib/src/client/tls.rs`

**Interfaces:**
- Consumes: Task 3's client.
- Produces: `pub(crate) fn build_tls_config(custom_ca_pem: Option<&[u8]>, replace_system: bool, require_h2: bool) -> Result<rustls::ClientConfig, Hyper4kErrorKind>`;
  `pub(crate) fn classify(err: &dyn std::error::Error) -> Hyper4kErrorKind`.

- [ ] **Step 1: Add dependencies**

```toml
rustls = { version = "0.23", default-features = false, features = ["aws-lc-rs", "std", "tls12"] }
tokio-rustls = { version = "0.26", default-features = false, features = ["aws-lc-rs", "tls12"] }
hyper-rustls = { version = "0.27", default-features = false, features = ["aws-lc-rs", "http1", "http2", "native-tokio"] }
rustls-pemfile = "2"

[dev-dependencies]
rcgen = "0.13"
```

`rcgen` generates a throwaway CA and leaf per test, so nothing expires in the repo
and the "expired certificate" case can be produced deliberately.

- [ ] **Step 2: Write the failing test**

```rust
#[cfg(test)]
mod tls_tests {
    use super::*;

    // Positive counterpart first: every "must fail" case below is only
    // meaningful because this one passes through the same code path.
    #[test]
    fn valid_chain_and_hostname_succeeds_over_alpn_h2() {
        let peer = spawn_tls_server(TlsFixture::valid());   // ALPN offers h2
        let client = new_client_with_ca(peer.ca_pem(), HYPER4K_CLIENT_HTTP2_REQUIRED);
        let cap = send_and_wait(client, &format!("https://localhost:{}/x", peer.port));
        assert_eq!(*cap.done.lock().unwrap(), Some(-999));
        assert_eq!(cap.status.load(Ordering::SeqCst) >> 16, 2, "expected HTTP/2");
    }

    #[test]
    fn wrong_ca_fails_with_tls_ca() {
        let peer = spawn_tls_server(TlsFixture::valid());
        let other = TlsFixture::valid();                    // unrelated CA
        let client = new_client_with_ca(other.ca_pem(), 0);
        let cap = send_and_wait(client, &format!("https://localhost:{}/x", peer.port));
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TLS_CA));
    }

    #[test]
    fn wrong_hostname_fails_with_tls_hostname() {
        let peer = spawn_tls_server(TlsFixture::for_name("example.invalid"));
        let client = new_client_with_ca(peer.ca_pem(), 0);
        let cap = send_and_wait(client, &format!("https://localhost:{}/x", peer.port));
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TLS_HOSTNAME));
    }

    #[test]
    fn expired_certificate_fails_with_tls_expired() {
        let peer = spawn_tls_server(TlsFixture::expired());
        let client = new_client_with_ca(peer.ca_pem(), 0);
        let cap = send_and_wait(client, &format!("https://localhost:{}/x", peer.port));
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TLS_EXPIRED));
    }

    #[test]
    fn http2_required_fails_when_peer_offers_only_http11() {
        let peer = spawn_tls_server(TlsFixture::valid().alpn(&["http/1.1"]));
        let client = new_client_with_ca(peer.ca_pem(), HYPER4K_CLIENT_HTTP2_REQUIRED);
        let cap = send_and_wait(client, &format!("https://localhost:{}/x", peer.port));
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_ALPN_NO_H2));
    }

    #[test]
    fn without_http2_required_the_same_peer_succeeds_over_http11() {
        // Proves the previous test failed on policy, not on a broken handshake.
        let peer = spawn_tls_server(TlsFixture::valid().alpn(&["http/1.1"]));
        let client = new_client_with_ca(peer.ca_pem(), 0);
        let cap = send_and_wait(client, &format!("https://localhost:{}/x", peer.port));
        assert_eq!(*cap.done.lock().unwrap(), Some(-999));
        assert_eq!(cap.status.load(Ordering::SeqCst) >> 16, 1);
    }

    #[test]
    fn custom_ca_appends_to_system_roots_by_default() {
        // A private CA is trusted, and a public host still validates.
        let peer = spawn_tls_server(TlsFixture::valid());
        let client = new_client_with_ca(peer.ca_pem(), 0);
        let cap = send_and_wait(client, &format!("https://localhost:{}/x", peer.port));
        assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    }

    #[test]
    fn ca_replace_system_drops_the_other_roots() {
        // Two independent local CAs. With REPLACE, only the configured one is
        // trusted. Deliberately no public host here: unit tests must not depend
        // on the network — that check lives in Task 7's ignored test.
        let mine = spawn_tls_server(TlsFixture::valid());
        let other = spawn_tls_server(TlsFixture::valid());
        let client = new_client_with_ca(mine.ca_pem(),
                                        HYPER4K_CLIENT_CA_REPLACE_SYSTEM);
        let ok = send_and_wait(client, &format!("https://localhost:{}/x", mine.port));
        assert_eq!(*ok.done.lock().unwrap(), Some(-999));
        let bad = send_and_wait(client, &format!("https://localhost:{}/x", other.port));
        assert_eq!(*bad.done.lock().unwrap(), Some(HYPER4K_ERR_TLS_CA));
    }

    #[tokio::test]
    async fn alpn_result_selects_the_sender_variant() {
        // Moved here from the pool task: this needs the real ClientConfig, which
        // only exists from this task onwards. Asserting ALPN against a stub would
        // have proved nothing about ALPN.
        let h2_peer = spawn_tls_server_alpn(&["h2"]).await;
        let h1_peer = spawn_tls_server_alpn(&["http/1.1"]).await;
        let pool = Pool::new(tls_from(build_tls_config(None, false, false).unwrap()));
        assert!(matches!(pool.acquire(&key_for(&h2_peer)).await.unwrap().sender,
                         Sender::H2(_)));
        assert!(matches!(pool.acquire(&key_for(&h1_peer)).await.unwrap().sender,
                         Sender::H1(_)));
    }

    #[test]
    fn unknown_flag_bits_are_rejected() {
        let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
        unsafe { hyper4k_client_options_init(&mut o, size_of::<Hyper4kClientOptions>() as u32) };
        o.flags = 1 << 40;
        let mut c = std::ptr::null_mut();
        assert_eq!(unsafe { hyper4k_client_new(&o, &mut c) }, HYPER4K_STATUS_UNKNOWN_FLAGS);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd lib && cargo test --lib client::tls_tests`
Expected: FAIL — `build_tls_config` not found

- [ ] **Step 4: Implement TLS**

`build_tls_config` loads platform roots via `rustls-native-certs`; when
`custom_ca_pem` is present it parses with `rustls-pemfile` and either adds to that
store or starts from an empty store when `CA_REPLACE_SYSTEM` is set. ALPN is
`["h2", "http/1.1"]`, or `["h2"]` alone when `HTTP2_REQUIRED` — an ALPN mismatch
then surfaces as a handshake failure that `classify` maps to
`HYPER4K_ERR_ALPN_NO_H2`. `classify` walks the error source chain and matches
`rustls::Error::InvalidCertificate(CertificateError::{UnknownIssuer, NotValidForName, Expired})`
onto `TLS_CA` / `TLS_HOSTNAME` / `TLS_EXPIRED`, falling back to `TLS_OTHER`.
Wire the connector into the client built in Task 3.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd lib && cargo test --lib client::tls_tests`
Expected: PASS, 10 tests

- [ ] **Step 6: Turn on the earned capability bits and commit**

`hyper4k_client_capabilities` adds `CAP_HTTP2 | CAP_TLS | CAP_CUSTOM_CA`.

```bash
git add lib/src/client/tls.rs lib/src/client/mod.rs lib/src/abi.rs lib/Cargo.toml lib/Cargo.lock
git commit -m "Add TLS with ALPN and classified handshake failures"
```

---

## Task 6: Backpressure

**Files:**
- Modify: `lib/src/client/bridge.rs`, `lib/src/client/mod.rs`
- Test: inline in `lib/src/client/bridge.rs`

**Interfaces:**
- Consumes: Task 3's bridge scaffolding.
- Produces: `hyper4k_client_resume(*mut Hyper4kClient, u64) -> Hyper4kStatus`;
  `pub(crate) struct PausePermit` with `fn take(&self) -> bool`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod backpressure_tests {
    use super::*;

    #[test]
    fn pause_stops_delivery_until_resume_and_never_repeats_a_chunk() {
        let peer = spawn_chunked_server(&[b"aaa", b"bbb", b"ccc"]);
        let seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        // on_chunk returns PAUSE for the first chunk only.
        let client = send_with_pausing_chunk_cb(peer.addr(), seen.clone());

        wait_until(|| seen.lock().unwrap().len() == 1);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(seen.lock().unwrap().len(), 1, "delivery continued while paused");

        assert_eq!(unsafe { hyper4k_client_resume(client.ptr, client.id) }, HYPER4K_OK);
        wait_until(|| seen.lock().unwrap().len() == 3);
        let got = seen.lock().unwrap().clone();
        assert_eq!(got, vec![b"aaa".to_vec(), b"bbb".to_vec(), b"ccc".to_vec()],
                   "resume must not replay the paused chunk");
    }

    #[test]
    fn resume_arriving_before_pause_lands_is_not_lost() {
        // The callback calls resume() on itself and *then* returns PAUSE.
        // Without a permit this deadlocks; the permit must consume the pause.
        let peer = spawn_chunked_server(&[b"a", b"b"]);
        let seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let client = send_with_self_resuming_chunk_cb(peer.addr(), seen.clone());
        wait_until(|| seen.lock().unwrap().len() == 2);   // fails by 5s timeout if lost
    }

    #[test]
    fn a_permit_does_not_leak_into_a_later_pause() {
        // Callback resumes during chunk 1 but returns CONTINUE, then pauses on
        // chunk 2. The stale permit must not release that second pause.
        let peer = spawn_chunked_server(&[b"a", b"b", b"c"]);
        let seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let client = send_with_leaky_permit_cb(peer.addr(), seen.clone());
        wait_until(|| seen.lock().unwrap().len() == 2);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(seen.lock().unwrap().len(), 2, "stale permit released a later pause");
    }

    #[test]
    fn resume_on_a_running_request_that_is_not_paused_reports_not_paused() {
        let peer = spawn_slow_server();
        let c = send_simple(peer.addr());
        assert_eq!(unsafe { hyper4k_client_resume(c.ptr, c.id) }, HYPER4K_STATUS_NOT_PAUSED);
    }

    #[test]
    fn chunk_cancel_terminates_with_cancelled() {
        let peer = spawn_chunked_server(&[b"a", b"b"]);
        let cap = send_with_cancelling_chunk_cb(peer.addr());
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_CANCELLED));
    }

    #[test]
    fn close_releases_paused_requests_so_free_returns() {
        let peer = spawn_chunked_server(&[b"a", b"b"]);
        let (client, cap) = send_and_pause_forever(peer.addr());
        unsafe { hyper4k_client_close(client) };
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_CANCELLED));
        unsafe { hyper4k_client_free(client) };   // must return, not hang
    }

    #[test]
    fn a_paused_stream_does_not_stall_a_sibling_on_the_same_connection() {
        // Two h2 streams on one connection; pause one indefinitely.
        let peer = spawn_tls_h2_server_with_two_paths();
        let (client, slow_id, fast) = start_paused_and_active_pair(&peer);
        let _ = slow_id;
        wait_until(|| fast.done.lock().unwrap().is_some());
        assert_eq!(*fast.done.lock().unwrap(), Some(-999));
    }

    #[test]
    fn exceeding_the_client_memory_cap_reports_resource_exhausted() {
        let peer = spawn_chunked_server_large();
        let client = new_client_with_memory_cap(64 * 1024);
        pause_requests_until_cap_reached(client, &peer);
        let st = send_one_more(client, &peer);
        assert_eq!(st, HYPER4K_STATUS_RESOURCE_EXHAUSTED);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lib && cargo test --lib client::backpressure_tests`
Expected: FAIL — `hyper4k_client_resume` not found

- [ ] **Step 3: Implement backpressure**

Each request owns a `PausePermit { armed: AtomicBool, generation: AtomicU64 }`.
`resume` sets `armed` for the **current** generation and returns `HYPER4K_OK` when
the request is paused or a chunk callback is running; `HYPER4K_STATUS_NOT_PAUSED`
otherwise. After a chunk callback returns, the bridge increments the generation:
on `PAUSE` it first tries `permit.take()` and only parks if that fails; on
`CONTINUE`/`CANCEL` it clears the permit so it cannot release a later pause. While
parked the task stops polling the body, which lets the h2 stream window close.
Track queued bytes per client against a cap; `send` returns
`HYPER4K_STATUS_RESOURCE_EXHAUSTED` past it. Cap paused streams per connection at
`connection_window / max_stream_occupancy` minus the active reserve, and route new
requests to a fresh connection once that cap is hit.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd lib && cargo test --lib client::backpressure_tests`
Expected: PASS, 8 tests

- [ ] **Step 5: Commit**

```bash
git add lib/src/client/bridge.rs lib/src/client/mod.rs lib/include/hyper4k.h
git commit -m "Express backpressure through the chunk callback"
```

---

## Task 7: Retry and timeouts

**Files:**
- Create: `lib/src/client/retry.rs`
- Modify: `lib/src/client/mod.rs`
- Test: inline in `lib/src/client/retry.rs`

**Interfaces:**
- Consumes: Tasks 3–5.
- Produces: `pub(crate) struct Attempt { generation: u64, response_committed: AtomicBool }`;
  `pub(crate) fn may_retry(committed: bool, provably_unsent: bool, method_idempotent: bool, budget_left: u32) -> bool`.

> The earlier signature took `goaway: Option<u32>` and `stream_id: u32`. Neither has
> a data source: hyper does not surface GOAWAY frames or stream ids, and Task 0
> established we do not need them. `provably_unsent` comes from
> `TrySendError::take_message().is_some()`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod retry_tests {
    use super::*;

    #[test]
    fn stream_above_last_stream_id_is_retried_transparently() {
        let peer = spawn_h2_server_that_goaways_at(1);
        let cap = send_post(&peer, "/x");        // lands on stream 3
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(-999));
        assert_eq!(peer.request_count(), 2, "expected one transparent retry");
        assert_eq!(cap.observed_request_ids(), 1, "request_id must not change");
    }

    #[test]
    fn inflight_post_below_last_stream_id_is_not_retried() {
        let peer = spawn_h2_server_that_accepts_then_drops();
        let cap = send_post(&peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_OUTCOME_UNKNOWN));
        assert_eq!(peer.request_count(), 1, "a POST that may have run was replayed");
    }

    #[test]
    fn inflight_get_below_last_stream_id_is_retried() {
        let peer = spawn_h2_server_that_accepts_then_drops_once();
        let cap = send_get(&peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    }

    #[test]
    fn streams_below_last_stream_id_still_complete_after_goaway() {
        // The correction from review round four: GOAWAY is not a verdict.
        let peer = spawn_h2_server_that_goaways_then_finishes_inflight();
        let cap = send_get(&peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(-999),
                   "an in-flight stream was failed on GOAWAY instead of completing");
    }

    #[test]
    fn a_committed_response_is_never_replayed() {
        // Headers reach the bridge queue, then the connection dies.
        let peer = spawn_h2_server_that_sends_headers_then_dies();
        let cap = send_get(&peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TRUNCATED));
        // Spec §四: the queued headers are discarded, not delivered. Only OnDone
        // reaches the caller. (An earlier draft asserted 1 here, contradicting
        // the spec it was implementing.)
        assert_eq!(cap.headers_calls(), 0,
                   "queued-but-undelivered headers must be dropped, not flushed");
        assert_eq!(peer.request_count(), 1, "a committed response was replayed");
    }

    #[test]
    fn refused_stream_is_retried_within_the_budget_then_reported() {
        let peer = spawn_h2_server_that_always_refuses();
        let client = new_client_with_max_retries(2);
        let cap = send_get_on(client, &peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(peer.request_count(), 3, "expected 1 attempt + 2 retries");
        assert_ne!(*cap.done.lock().unwrap(), Some(-999));
    }

    #[test]
    fn request_timeout_covers_all_retries() {
        let peer = spawn_h2_server_that_always_refuses();
        let client = new_client_with(200 /*request_timeout_ms*/, 10 /*max_retries*/);
        let started = Instant::now();
        let cap = send_get_on(client, &peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert!(started.elapsed() < Duration::from_millis(1000),
                "timeout was reset per attempt");
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_TIMEOUT));
    }

    #[test]
    fn read_idle_timeout_resets_on_every_chunk() {
        // Chunks every 50ms with a 150ms idle limit must not time out.
        let peer = spawn_chunked_server_with_gap(Duration::from_millis(50), 6);
        let client = new_client_with_idle(150);
        let cap = send_get_on(client, &peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    }

    #[test]
    fn read_idle_timeout_fires_on_a_stalled_stream() {
        let peer = spawn_chunked_server_that_stalls_after_first_chunk();
        let client = new_client_with_idle(150);
        let cap = send_get_on(client, &peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(HYPER4K_ERR_IDLE_TIMEOUT));
    }

    #[test]
    fn zero_timeouts_disable_rather_than_expire_immediately() {
        let peer = spawn_chunked_server_with_gap(Duration::from_millis(300), 2);
        let client = new_client_with(0 /*request*/, 0 /*idle via options*/);
        let cap = send_get_on(client, &peer, "/x");
        wait_until(|| cap.done.lock().unwrap().is_some());
        assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lib && cargo test --lib client::retry_tests`
Expected: FAIL — `Attempt` not found

- [ ] **Step 3: Implement the retry state machine**

`Attempt` carries a monotonically increasing generation and an irreversible
`response_committed` flag set **before the first response event is pushed onto the
bridge queue**, not after a callback returns. Bridge events carry their generation
and are dropped if it no longer matches.

`may_retry` is a pure function of four inputs — there is no GOAWAY frame, no
`last_stream_id` and no stream id anywhere in this module:

| `committed` | `provably_unsent` | method | budget | outcome |
|---|---|---|---|---|
| true | — | — | — | no retry, `OnDone(TRUNCATED)` |
| false | true | any | > 0 | **retry** (safe by protocol, method irrelevant) |
| false | true | any | 0 | no retry, report the transport error |
| false | false | idempotent | > 0 | retry |
| false | false | idempotent | 0 | no retry, report the transport error |
| false | false | non-idempotent | — | no retry, `OnDone(OUTCOME_UNKNOWN)` |

`provably_unsent` is `TrySendError::take_message().is_some()`. Idempotent methods
are GET, HEAD, PUT, DELETE, OPTIONS, TRACE.

**A retry rebuilds the request; it never reuses the old one.** `hyper::Request` is
consumed by `try_send_request`, and on the `take_message() == None` path it is not
handed back at all, so there is nothing left to resend. Each request therefore keeps
an immutable `RequestTemplate { method: Method, uri: Uri, headers: HeaderMap, body: Bytes }`
captured at `send`, and every attempt constructs a fresh `Request<Full<Bytes>>` from
it. `Bytes` clones are a refcount bump, so retrying costs no body copy.

Retries reuse the same `request_id`. The overall deadline is computed once at
`send` and shared across attempts; the idle deadline is rearmed on every delivered
chunk.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd lib && cargo test --lib client::retry_tests`
Expected: PASS, 10 tests

- [ ] **Step 5: Commit**

```bash
git add lib/src/client/retry.rs lib/src/client/mod.rs
git commit -m "Add the RFC 9113 retry state machine"
```

---

## Task 8: Platform matrix and real-world verification

**Files:**
- Modify: `build.gradle.kts:20,33` (drop the mingw triple and target)
- Modify: `README.md:76`, `README.zh-Hans.md:71` (drop the mingw build line)
- Modify: `../neton/neton-http-hyper4k/build.gradle.kts:15,47,52`
- Create: `lib/tests/public_https.rs`

- [ ] **Step 1: Write the runtime trust-store test**

`staticlib` linking proves nothing about loading platform roots at runtime, so this
one runs against a real public host and is the only test that needs the network.

```rust
//! Ignored by default: needs outbound network. Run with
//! `cargo test --test public_https -- --ignored`.

#[test]
#[ignore]
fn system_roots_validate_a_public_host() {
    let client = new_default_client();          // no custom CA
    let cap = send_and_wait(client, "https://example.com/");
    assert_eq!(*cap.done.lock().unwrap(), Some(-999));
    assert_eq!(cap.status.load(Ordering::SeqCst) & 0xFFFF, 200);
}

#[test]
#[ignore]
fn a_custom_ca_without_replace_keeps_the_system_roots() {
    // The unit test for "append" only proves the private CA works. It cannot
    // prove the system roots survived, because it has no publicly-signed peer.
    let client = new_client_with_ca(throwaway_ca_pem(), 0 /* no REPLACE */);
    let cap = send_and_wait(client, "https://example.com/");
    assert_eq!(*cap.done.lock().unwrap(), Some(-999),
               "adding a custom CA silently replaced the system trust store");
}
```

- [ ] **Step 2: Run it on both host families**

Run on macOS: `cd lib && cargo test --test public_https -- --ignored`
Run on Linux: same command in the Linux CI job.
Expected: PASS on both.

- [ ] **Step 3: Add the Kotlin-side width assertion**

Acceptance #43 wants the check on *both* sides; only the Rust half exists so far.
Add to `src/nativeTest/kotlin/hyper4k/AbiWidthTest.kt` in this repo:

`assertEquals(4, Int.SIZE_BYTES)` would pass no matter what the header says, so the
real check is a **compile-time** one: assign each ABI constant into an explicitly
typed `Int`. If a type reverts to a bare C `enum`, cinterop maps it to something
else and this stops compiling.

```kotlin
import kotlinx.cinterop.ExperimentalForeignApi
import hyper4k.*
import kotlin.test.Test
import kotlin.test.assertEquals

@OptIn(ExperimentalForeignApi::class)
class AbiWidthTest {
    @Test
    fun abiScalarsMapToKotlinInt() {
        // Each of these fails to COMPILE if cinterop stops mapping the type to Int.
        val status: Int = HYPER4K_OK
        val err: Int = HYPER4K_ERR_OUTCOME_UNKNOWN
        val headersAction: Int = HYPER4K_HEADERS_CANCEL
        val chunkAction: Int = HYPER4K_CHUNK_PAUSE

        assertEquals(0, status)
        assertEquals(13, err)
        assertEquals(2, headersAction)
        assertEquals(1, chunkAction)
        assertEquals((4 shl 16) or 0, hyper4k_abi_version().toInt())
    }
}
```

Also add to `lib/include/hyper4k.h`, so a C consumer fails at compile time too:

```c
_Static_assert(sizeof(Hyper4kStatus)        == 4, "Hyper4kStatus must be 32-bit");
_Static_assert(sizeof(Hyper4kErrorKind)     == 4, "Hyper4kErrorKind must be 32-bit");
_Static_assert(sizeof(Hyper4kHeadersAction) == 4, "Hyper4kHeadersAction must be 32-bit");
_Static_assert(sizeof(Hyper4kChunkAction)   == 4, "Hyper4kChunkAction must be 32-bit");
```

Run: `./gradlew macosArm64Test`

- [ ] **Step 4: Drop mingwX64 from this repo's matrix**

In `build.gradle.kts` remove `"mingwX64" to "x86_64-pc-windows-gnu"` from
`rustTriples` and the `mingwX64()` target. Remove the `x86_64-pc-windows-gnu` line
from both READMEs. Claiming a platform nobody builds or tests is worse than not
claiming it.

- [ ] **Step 5: Drop mingwX64 from the neton repo — separate commit, separate repo**

`../neton` is a **different git repository**; its files cannot go into a hyper4k
commit. Do this as its own change and verify it independently:

```bash
cd ../neton
# remove mingwX64() and the two mingwX64* dependsOn lines
$EDITOR neton-http-hyper4k/build.gradle.kts
./gradlew :neton-http-hyper4k:macosArm64Test
git add neton-http-hyper4k/build.gradle.kts
git commit -m "Drop the Windows target from the hyper4k adapter"
cd ../hyper4k
```

- [ ] **Step 6: Verify the four-platform matrix**

```bash
cd lib
for t in aarch64-apple-darwin x86_64-apple-darwin \
         x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu; do
  cargo rustc --release --target "$t" --crate-type staticlib --locked || exit 1
done
cd .. && ./gradlew macosArm64Test
```

Expected: four static libraries build; `macosArm64Test` passes.

- [ ] **Step 7: Update the design doc status and commit**

Change the header of `docs/ABI_V4_CLIENT_TLS.md` to
`DESIGN FROZEN / IMPLEMENTED`, and tick the capability note in §2.1.

```bash
git add build.gradle.kts README.md README.zh-Hans.md \
        lib/tests/public_https.rs src/nativeTest/kotlin/hyper4k/AbiWidthTest.kt \
        docs/ABI_V4_CLIENT_TLS.md
git commit -m "Ship the client on the four supported platforms"
```

---

## Self-Review

**Task 0 gates Tasks 4 and 7.** If the spike shows h1 and h2 disagree on the
provably-unsent signal, Task 7 needs a per-protocol branch and must be re-planned
before any production code is written. Do not skip ahead.

**Spec coverage.** §二 ABI basics → Tasks 1–2. §2.3 lifecycle → Task 4.
§2.5 backpressure and the resource caps → Tasks 3 and 6. §三 TLS → Task 5.
§四 retry → Tasks 0 and 7. §五 performance (header descriptor array, single body
copy, LTO) → structural, enforced by Task 4's `Hyper4kHeader` signature and the
existing release profile; the three-layer benchmark baseline is **not** in this
plan — it is follow-on work once the client runs, and is called out here rather
than silently dropped. §六 platform matrix → Task 8.

Acceptance items map as: 1–5, 11, 14 → Task 5; 6, 9, 16–17, 23–25, 27, 33, 35–37,
41 → Task 6; 7, 18–19, 28, 31 → Task 7; 8, 10, 12–13, 20, 26, 32 → Task 4;
15, 29–30, 34, 39 → Task 2; 21 → Task 8; 22 → Task 8; 38, 40, 43 → Tasks 1–2 plus
Task 8's Kotlin and C static assertions; 42 → Task 2.

**Placeholders.** None: every step names exact files, exact commands and the
expected pass/fail string.

**Type consistency.** `Hyper4kStatus`, `Hyper4kErrorKind`, `Hyper4kHeadersAction`,
`Hyper4kChunkAction` are declared in Task 1 and used unchanged in Tasks 2–6.
`Hyper4kClientOptions` and `Hyper4kClientRequest` are declared once in Task 2 and
consumed by name thereafter. `hyper4k_client_resume` appears only in Task 6, where
it is defined.
