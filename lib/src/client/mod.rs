//! Outbound HTTP client (ABI v4).
//!
//! Kotlin sees only this module's exported functions. Nothing from rustls,
//! hyper or the crypto provider crosses the boundary.

pub mod bridge;
pub mod handle;
pub(crate) mod plaintext;
pub(crate) mod pool;
pub(crate) mod retry;
pub(crate) mod tls;

#[cfg(test)]
mod backpressure_tests;
#[cfg(test)]
pub(crate) mod handle_tests;
#[cfg(test)]
mod pool_tests;
#[cfg(test)]
mod retry_tests;
#[cfg(test)]
mod tls_tests;

pub use handle::Hyper4kClient;

use crate::abi::*;
use crate::Hyper4kSlice;
use std::mem::size_of;

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

/// Fail rather than fall back when ALPN does not yield `h2`. Silent downgrade
/// would let a misconfiguration hide indefinitely.
pub const HYPER4K_CLIENT_HTTP2_REQUIRED: u64 = 1 << 0;
/// Replace the platform trust roots with `custom_ca_pem` instead of adding to
/// them. Replacing the root set is not certificate pinning; v4 has no pinning.
pub const HYPER4K_CLIENT_CA_REPLACE_SYSTEM: u64 = 1 << 1;

pub const KNOWN_CLIENT_FLAGS: u64 =
    HYPER4K_CLIENT_HTTP2_REQUIRED | HYPER4K_CLIENT_CA_REPLACE_SYSTEM;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct Hyper4kClientOptions {
    pub abi_version: u32,
    pub struct_size: u32,
    pub flags: u64,
    /// 0 disables the connect timeout. Not "use the default", not "expire
    /// immediately" — both readings exist in the wild, so this one is pinned.
    pub connect_timeout_ms: u64,
    /// 0 disables the overall timeout. SSE-style streams need that.
    pub request_timeout_ms: u64,
    /// Per-request default for the inter-chunk idle limit. 0 disables it.
    pub read_idle_timeout_ms: u64,
    /// *Additional* attempts: 0 means try once, 2 means at most three tries.
    pub max_retries: u32,
    pub _reserved: u32,
    /// NULL uses the platform roots only.
    pub custom_ca_pem: *const u8,
    pub custom_ca_pem_len: usize,
}

#[repr(C)]
pub struct Hyper4kClientRequest {
    pub abi_version: u32,
    pub struct_size: u32,
    pub method: Hyper4kSlice,
    pub url: Hyper4kSlice,
    pub headers: *const Hyper4kHeader,
    pub header_count: usize,
    pub body_ptr: *const u8,
    pub body_len: usize,
    /// `u64::MAX` inherits the client default, 0 disables, any other value
    /// overrides. Inherit is *not* 0: a zeroed struct would then silently
    /// disable the idle timeout the client was configured with.
    pub read_idle_timeout_ms: u64,
}

/// Fields through `flags` must be present; a caller that supplies less has told
/// us nothing at all, and "everything defaults" would turn their mistake into a
/// silent configuration.
pub const OPTIONS_MIN_SIZE: u32 = 24; // abi_version + struct_size + flags + connect_timeout
/// Fields through `url`.
pub const REQUEST_MIN_SIZE: u32 = 8 + 2 * size_of::<Hyper4kSlice>() as u32;

/// Writes at most `min(struct_size, size_of::<T>())` bytes.
///
/// Taking the caller's allocation size is not optional: a caller built against
/// a smaller v4.0 struct that loads a v4.1 library would otherwise be written
/// past the end of its own allocation.
unsafe fn init_prefix<T>(
    ptr: *mut T,
    struct_size: u32,
    min_size: u32,
    defaults: T,
) -> Hyper4kStatus {
    if ptr.is_null() {
        return HYPER4K_STATUS_INVALID_ARG;
    }
    if struct_size < min_size {
        return HYPER4K_STATUS_STRUCT_SIZE;
    }
    let writable = (struct_size as usize).min(size_of::<T>());
    let src = &defaults as *const T as *const u8;
    std::ptr::copy_nonoverlapping(src, ptr as *mut u8, writable);
    // `defaults` was moved bytewise; running its destructor would be a
    // double-free if T ever gains one.
    std::mem::forget(defaults);
    HYPER4K_STATUS_OK
}

/// Fill `opts` with this build's defaults.
///
/// # Safety
/// `opts` must point to at least `struct_size` writable, correctly aligned bytes.
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

/// Fill `request` with this build's defaults.
///
/// # Safety
/// `request` must point to at least `struct_size` writable, correctly aligned bytes.
#[no_mangle]
pub unsafe extern "C" fn hyper4k_client_request_init(
    request: *mut Hyper4kClientRequest,
    struct_size: u32,
) -> Hyper4kStatus {
    let defaults = Hyper4kClientRequest {
        abi_version: hyper4k_abi_version(),
        struct_size,
        method: Hyper4kSlice {
            ptr: std::ptr::null(),
            len: 0,
        },
        url: Hyper4kSlice {
            ptr: std::ptr::null(),
            len: 0,
        },
        headers: std::ptr::null(),
        header_count: 0,
        body_ptr: std::ptr::null(),
        body_len: 0,
        read_idle_timeout_ms: u64::MAX,
    };
    init_prefix(request, struct_size, REQUEST_MIN_SIZE, defaults)
}

/// Shared validation for a caller-supplied versioned struct.
pub(crate) fn validate_header(abi_version: u32, struct_size: u32, min_size: u32) -> Hyper4kStatus {
    if abi_version >> 16 != hyper4k_abi_version() >> 16 {
        return HYPER4K_STATUS_ABI_MISMATCH;
    }
    if struct_size < min_size {
        return HYPER4K_STATUS_STRUCT_SIZE;
    }
    HYPER4K_STATUS_OK
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Allocate through the real layout so the pointer is properly aligned.
    /// Casting a `Vec<u8>` and dereferencing it would be UB no matter how the
    /// function under test behaves, which would make the test meaningless.
    struct Probe {
        base: *mut u8,
        layout: std::alloc::Layout,
        size: usize,
    }

    impl Probe {
        fn new(prefix: usize, guard: usize, align: usize, fill: u8) -> Self {
            let layout = std::alloc::Layout::from_size_align(prefix + guard, align).unwrap();
            let base = unsafe { std::alloc::alloc(layout) };
            assert!(!base.is_null());
            unsafe { std::ptr::write_bytes(base, fill, prefix + guard) };
            Probe {
                base,
                layout,
                size: prefix + guard,
            }
        }
        fn tail(&self, from: usize) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.base.add(from), self.size - from) }
        }
        fn u32_at(&self, off: usize) -> u32 {
            unsafe { std::ptr::read_unaligned(self.base.add(off) as *const u32) }
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            unsafe { std::alloc::dealloc(self.base, self.layout) };
        }
    }

    #[test]
    fn options_init_sets_defaults_distinguishable_from_zero() {
        let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
        let st = unsafe {
            hyper4k_client_options_init(&mut o, size_of::<Hyper4kClientOptions>() as u32)
        };
        assert_eq!(st, HYPER4K_STATUS_OK);
        assert_eq!(o.abi_version, hyper4k_abi_version());
        // The whole point of the init function: a zeroed struct means "no
        // retries", which is a different intent from "use the default".
        assert_eq!(o.max_retries, 2);
        assert_eq!(o.request_timeout_ms, 60_000);
    }

    #[test]
    fn request_init_marks_idle_timeout_as_inherit() {
        let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
        let st = unsafe {
            hyper4k_client_request_init(&mut r, size_of::<Hyper4kClientRequest>() as u32)
        };
        assert_eq!(st, HYPER4K_STATUS_OK);
        // 0 would mean "disabled", silently dropping the client default.
        assert_eq!(r.read_idle_timeout_ms, u64::MAX);
    }

    #[test]
    fn init_never_writes_past_the_caller_allocation() {
        // An old caller allocates ONLY the old prefix; guard bytes sit
        // immediately after it. A library that ignores struct_size and writes
        // the whole modern struct smashes them.
        let prefix = OPTIONS_MIN_SIZE as usize;
        let p = Probe::new(prefix, 64, align_of::<Hyper4kClientOptions>(), 0xAB);

        let st = unsafe {
            hyper4k_client_options_init(p.base as *mut Hyper4kClientOptions, OPTIONS_MIN_SIZE)
        };
        assert_eq!(st, HYPER4K_STATUS_OK);
        assert!(
            p.tail(prefix).iter().all(|&b| b == 0xAB),
            "init wrote past the caller's struct_size"
        );
        assert_eq!(p.u32_at(0), hyper4k_abi_version());
        assert_eq!(p.u32_at(4), OPTIONS_MIN_SIZE);
    }

    #[test]
    fn a_larger_caller_struct_keeps_its_tail_untouched() {
        // The other direction: a newer caller against this build. Fields this
        // build does not know must keep the caller's preset bytes.
        let known = size_of::<Hyper4kClientOptions>();
        let p = Probe::new(known, 64, align_of::<Hyper4kClientOptions>(), 0xCD);
        let st = unsafe {
            hyper4k_client_options_init(p.base as *mut Hyper4kClientOptions, (known + 64) as u32)
        };
        assert_eq!(st, HYPER4K_STATUS_OK);
        assert!(
            p.tail(known).iter().all(|&b| b == 0xCD),
            "init touched fields beyond what this build defines"
        );
    }

    #[test]
    fn struct_size_below_minimum_is_rejected() {
        let mut o: Hyper4kClientOptions = unsafe { std::mem::zeroed() };
        let st = unsafe { hyper4k_client_options_init(&mut o, OPTIONS_MIN_SIZE - 1) };
        assert_eq!(st, HYPER4K_STATUS_STRUCT_SIZE);

        let mut r: Hyper4kClientRequest = unsafe { std::mem::zeroed() };
        let st = unsafe { hyper4k_client_request_init(&mut r, REQUEST_MIN_SIZE - 1) };
        assert_eq!(st, HYPER4K_STATUS_STRUCT_SIZE);
    }

    #[test]
    fn null_pointer_is_rejected() {
        let st = unsafe { hyper4k_client_options_init(std::ptr::null_mut(), 64) };
        assert_eq!(st, HYPER4K_STATUS_INVALID_ARG);
    }

    #[test]
    fn a_mismatched_major_abi_version_is_rejected() {
        let st = validate_header(3 << 16, 1024, OPTIONS_MIN_SIZE);
        assert_eq!(st, HYPER4K_STATUS_ABI_MISMATCH);
        let st = validate_header(hyper4k_abi_version(), 1024, OPTIONS_MIN_SIZE);
        assert_eq!(st, HYPER4K_STATUS_OK);
    }

    #[test]
    fn minimum_sizes_cover_the_documented_prefix() {
        // OPTIONS_MIN_SIZE must reach through `flags`; REQUEST_MIN_SIZE through `url`.
        assert!(OPTIONS_MIN_SIZE as usize >= 4 + 4 + 8);
        assert!(REQUEST_MIN_SIZE as usize >= 4 + 4 + 2 * size_of::<Hyper4kSlice>());
        assert!(OPTIONS_MIN_SIZE as usize <= size_of::<Hyper4kClientOptions>());
        assert!(REQUEST_MIN_SIZE as usize <= size_of::<Hyper4kClientRequest>());
    }
}
