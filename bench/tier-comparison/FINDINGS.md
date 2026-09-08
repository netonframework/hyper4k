# Per-request cost by tier — clean-box measurement

Measured on a **dedicated** Fedora 44 box (2 cores, 1.6 GB + swap, nothing else
running), so throughput and per-request CPU are trustworthy — unlike the earlier
shared box. Config matches production (`[logging] level = WARN`, no access log).

## Method

- Per-request CPU = our process's utime+stime delta over a 15s `wrk -t1 -c128`
  window ÷ requests. Warm-up first. One tier at a time, each measured on its own
  clean process. Cross-checked against the earlier shared box (tier1/2 agreed).
- All tiers compute the same sum a+b and return it as text.

## Results (per-request CPU)

| Tier | what | per-req CPU | added by this layer |
|------|------|-------------|---------------------|
| 1 | pure hyper (Rust) | 7.7 µs | — |
| 2 | hyper4k C ABI + native Rust callback | 9.7 µs | +2.0 µs (C ABI + responder) |
| 2.5B | Kotlin `Hyper4kServer` handler, no Neton dispatcher | 17.6 µs | +7.9 µs (FFI copies, coroutine, GC) |
| 3 | full Neton dispatcher | 33.8 µs | +16.2 µs (routing, request build, envelope, security, observability) |

## What this establishes

- **The engine is cheap.** hyper4k over pure hyper is +2 µs; the C ABI /
  responder / oneshot / DashMap is not the cost.
- **Crossing into Kotlin/Native is +8 µs.** FFI copies of method/path/query/body,
  the coroutine dispatch, and GC.
- **The Neton dispatcher is the single biggest layer: +16 µs.** Routing lookup,
  BufferedHttpRequest/ArgsView construction, the response-envelope path, the
  security pipeline, and observability. It is ~half of the full per-request cost
  and is entirely in `neton-http` — no FFI or lifetime hazards. This is the
  highest-value target for a single-variable optimization.

## Lessons paid for here

- A default (no application.conf) run logs `http.access` to stdout per request and
  collapsed tier3 to ~630 rps. That is a config artifact, not a Neton bottleneck;
  production runs WARN. Always match the production log level when measuring.
- CPU-pinning (taskset) on a 4-core box starved the GC'd tier and distorted it;
  a dedicated box measured cleanly without pinning.

## Not claimed

- 2 cores, not the arena's 64; absolute numbers differ there, the per-layer split
  is the transferable finding.
- The +16 µs dispatcher cost is not yet broken into routing vs envelope vs
  request-build; that is the next measurement before optimizing.
