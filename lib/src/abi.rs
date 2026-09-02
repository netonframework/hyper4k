//! Shared C ABI surface: status codes, error kinds, callback actions, version
//! and capability queries.
//!
//! Every type here crosses Rust, C and Kotlin/Native, so all of them are fixed
//! width. A C `enum` has implementation-defined width; freezing the values
//! without freezing the representation would not be freezing anything.

use crate::Hyper4kSlice;
use std::ffi::c_char;

// ---------------------------------------------------------------------------
// Synchronous status codes
// ---------------------------------------------------------------------------

pub type Hyper4kStatus = i32;

/// Success for the v4 client status channel.
///
/// NOT named `HYPER4K_OK`: ABI v3 already publishes `HYPER4K_OK = 1` as
/// `hyper4k_respond`'s "delivered". Two different values behind one name would
/// be a silent, type-unchecked trap for C callers.
pub const HYPER4K_STATUS_OK: Hyper4kStatus = 0;

/// `abi_version` in a caller-supplied struct is not compatible with this build.
pub const HYPER4K_STATUS_ABI_MISMATCH: Hyper4kStatus = -1;
/// `struct_size` is below the minimum this ABI accepts.
pub const HYPER4K_STATUS_STRUCT_SIZE: Hyper4kStatus = -2;
/// `flags` contains a bit this build does not know. Never ignored silently: a
/// dropped flag can be the one that was carrying a security decision.
pub const HYPER4K_STATUS_UNKNOWN_FLAGS: Hyper4kStatus = -3;
/// NULL where a pointer is required, or an unparsable URL / method / header.
pub const HYPER4K_STATUS_INVALID_ARG: Hyper4kStatus = -4;
/// A combination this version does not implement, e.g. `http://` with
/// `HTTP2_REQUIRED` (v4 ships no h2c client).
pub const HYPER4K_STATUS_UNSUPPORTED: Hyper4kStatus = -5;
pub const HYPER4K_STATUS_CLIENT_CLOSED: Hyper4kStatus = -6;
/// A real allocation failure.
pub const HYPER4K_STATUS_OOM: Hyper4kStatus = -7;
/// Deliberate throttling to stay under a configured cap. Distinct from `OOM` so
/// operators are not sent hunting for a memory leak that is not there.
pub const HYPER4K_STATUS_RESOURCE_EXHAUSTED: Hyper4kStatus = -8;

pub const HYPER4K_STATUS_NOT_FOUND: Hyper4kStatus = -20;
pub const HYPER4K_STATUS_ALREADY_DONE: Hyper4kStatus = -21;
pub const HYPER4K_STATUS_NOT_PAUSED: Hyper4kStatus = -22;

// ---------------------------------------------------------------------------
// Asynchronous error kinds (delivered through OnDone)
// ---------------------------------------------------------------------------

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
/// Includes cancellation caused by `hyper4k_client_close`.
pub const HYPER4K_ERR_CANCELLED: Hyper4kErrorKind = 11;
/// The response had started when the connection failed. The request was
/// certainly processed; only the response is incomplete.
pub const HYPER4K_ERR_TRUNCATED: Hyper4kErrorKind = 12;
/// It cannot be proven whether the peer processed the request. The only input
/// the caller may use to decide about replaying a non-idempotent request.
pub const HYPER4K_ERR_OUTCOME_UNKNOWN: Hyper4kErrorKind = 13;

// ---------------------------------------------------------------------------
// Callback actions
// ---------------------------------------------------------------------------

// Headers and chunks have separate action types on purpose: there is no
// "pause before the next chunk" at the headers stage, and a shared enum would
// leave an undefined OnHeaders+PAUSE combination.

pub type Hyper4kHeadersAction = i32;
pub const HYPER4K_HEADERS_CONTINUE: Hyper4kHeadersAction = 0;
pub const HYPER4K_HEADERS_CANCEL: Hyper4kHeadersAction = 2;

pub type Hyper4kChunkAction = i32;
pub const HYPER4K_CHUNK_CONTINUE: Hyper4kChunkAction = 0;
pub const HYPER4K_CHUNK_PAUSE: Hyper4kChunkAction = 1;
pub const HYPER4K_CHUNK_CANCEL: Hyper4kChunkAction = 2;

// ---------------------------------------------------------------------------
// Shared record types
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct Hyper4kHeader {
    pub name: Hyper4kSlice,
    pub value: Hyper4kSlice,
}

#[repr(C)]
pub struct Hyper4kError {
    /// One of the `HYPER4K_ERR_*` constants. The stable part.
    pub kind: Hyper4kErrorKind,
    /// Protocol-level code where one exists (e.g. an HTTP/2 error code), else 0.
    pub protocol_code: u32,
    /// Borrowed diagnostic text. For logs only — never branch on it.
    pub message: Hyper4kSlice,
}

// ---------------------------------------------------------------------------
// Version and capabilities
// ---------------------------------------------------------------------------

/// `(major << 16) | minor`. A major change means incompatible.
const ABI_VERSION: u32 = (4 << 16) | 0;

const VERSION_CSTR: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();

#[no_mangle]
pub extern "C" fn hyper4k_abi_version() -> u32 {
    ABI_VERSION
}

/// NUL-terminated crate version. Static storage; the caller must not free it.
#[no_mangle]
pub extern "C" fn hyper4k_version() -> *const c_char {
    VERSION_CSTR.as_ptr() as *const c_char
}

pub const HYPER4K_SERVER_CAP_HTTP1: u64 = 1 << 0;
pub const HYPER4K_SERVER_CAP_H2C: u64 = 1 << 1;
pub const HYPER4K_SERVER_CAP_STREAMING: u64 = 1 << 2;

/// Server capabilities. These three shipped in ABI v3 with tests behind them.
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
// No h2c bit: v4 ships no cleartext HTTP/2 client.

/// Client capabilities.
///
/// A bit appears here only once its feature is implemented **and tested** — not
/// when it partially works. Each task in the implementation plan lights its own
/// bits in its own commit.
#[no_mangle]
pub extern "C" fn hyper4k_client_capabilities() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_version_is_four_zero() {
        assert_eq!(hyper4k_abi_version(), (4u32 << 16) | 0u32);
    }

    #[test]
    fn status_values_are_frozen() {
        // These numbers are published across languages. Pin them literally, not
        // by expression: a literal is what a Kotlin or C consumer will hold.
        assert_eq!(HYPER4K_STATUS_OK, 0);
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
    fn error_kind_values_are_frozen() {
        assert_eq!(HYPER4K_ERR_NONE, 0);
        assert_eq!(HYPER4K_ERR_DNS, 1);
        assert_eq!(HYPER4K_ERR_CONNECT, 2);
        assert_eq!(HYPER4K_ERR_TLS_CA, 3);
        assert_eq!(HYPER4K_ERR_TLS_HOSTNAME, 4);
        assert_eq!(HYPER4K_ERR_TLS_EXPIRED, 5);
        assert_eq!(HYPER4K_ERR_TLS_OTHER, 6);
        assert_eq!(HYPER4K_ERR_ALPN_NO_H2, 7);
        assert_eq!(HYPER4K_ERR_PROTOCOL, 8);
        assert_eq!(HYPER4K_ERR_TIMEOUT, 9);
        assert_eq!(HYPER4K_ERR_IDLE_TIMEOUT, 10);
        assert_eq!(HYPER4K_ERR_CANCELLED, 11);
        assert_eq!(HYPER4K_ERR_TRUNCATED, 12);
        assert_eq!(HYPER4K_ERR_OUTCOME_UNKNOWN, 13);
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
        // Nothing client-side is implemented yet, so it advertises nothing.
        assert_eq!(hyper4k_client_capabilities(), 0);
    }

    #[test]
    fn server_capabilities_match_what_abi_v3_shipped() {
        let caps = hyper4k_server_capabilities();
        assert_ne!(caps & HYPER4K_SERVER_CAP_H2C, 0);
        assert_ne!(caps & HYPER4K_SERVER_CAP_STREAMING, 0);
    }

    #[test]
    fn version_string_is_nul_terminated() {
        let ptr = hyper4k_version();
        let s = unsafe { std::ffi::CStr::from_ptr(ptr) };
        assert_eq!(s.to_str().unwrap(), env!("CARGO_PKG_VERSION"));
    }
}
