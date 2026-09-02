//! Retry policy.
//!
//! There is no GOAWAY frame, no `last_stream_id` and no stream id anywhere in
//! this module. Hyper's h2 layer already computes the only thing we need — was
//! this request provably never written — and hands it back through
//! `TrySendError::take_message`. Re-deriving that from raw frames would be more
//! code guarding a weaker guarantee.

use hyper::Method;

/// Methods a connection failure may replay without changing server state.
pub(crate) fn is_idempotent(m: &Method) -> bool {
    matches!(
        *m,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE | Method::OPTIONS | Method::TRACE
    )
}

/// Decide whether another attempt is allowed.
///
/// | committed | provably_unsent | method        | budget | outcome        |
/// |-----------|-----------------|---------------|--------|----------------|
/// | true      | —               | —             | —      | no (TRUNCATED) |
/// | false     | true            | any           | > 0    | **retry**      |
/// | false     | true            | any           | 0      | no             |
/// | false     | false           | idempotent    | > 0    | retry          |
/// | false     | false           | idempotent    | 0      | no             |
/// | false     | false           | non-idempotent| —      | no (UNKNOWN)   |
pub(crate) fn may_retry(
    committed: bool,
    provably_unsent: bool,
    method_idempotent: bool,
    budget_left: u32,
) -> bool {
    if committed {
        // The caller has already seen part of the response. Replaying now would
        // deliver a second set of headers, which is corruption, not a retry.
        return false;
    }
    if budget_left == 0 {
        return false;
    }
    // `provably_unsent` beats idempotency: the protocol guarantees the peer
    // never saw it, so even a POST is safe.
    provably_unsent || method_idempotent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_committed_response_is_never_replayed() {
        assert!(!may_retry(true, true, true, 5));
        assert!(!may_retry(true, false, false, 5));
    }

    #[test]
    fn provably_unsent_beats_idempotency() {
        // A POST that never reached the wire is safe to replay.
        assert!(may_retry(false, true, false, 1));
    }

    #[test]
    fn an_unproven_non_idempotent_request_is_not_replayed() {
        assert!(!may_retry(false, false, false, 5));
    }

    #[test]
    fn an_unproven_idempotent_request_is_replayed() {
        assert!(may_retry(false, false, true, 1));
    }

    #[test]
    fn an_exhausted_budget_stops_every_case() {
        assert!(!may_retry(false, true, true, 0));
        assert!(!may_retry(false, false, true, 0));
    }

    #[test]
    fn idempotent_set_matches_the_spec() {
        for m in [
            Method::GET,
            Method::HEAD,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
            Method::TRACE,
        ] {
            assert!(is_idempotent(&m), "{m} should be idempotent");
        }
        for m in [Method::POST, Method::PATCH] {
            assert!(!is_idempotent(&m), "{m} must not be idempotent");
        }
    }
}
