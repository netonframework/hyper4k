# Per-request cost by tier — clean-box, GC-aligned

Dedicated Fedora 44 box (2 cores, 1.6 GB + swap, nothing else running). Both
Kotlin tiers use the SAME 256 MB GC floor and WARN logging, so their delta is not
a GC-policy or logging artifact. `run.sh` in this directory is the exact harness;
each run saves per-tier per-round wrk output, binary hashes, config, request
errors, swap pages and RSS under /tmp/tierbench/<ts>/.

## Results (per-request process CPU: our PID's utime+stime ÷ requests, wrk -t1 -c128)

| Tier | what | per-req CPU | delta to previous |
|------|------|-------------|-------------------|
| 1 | pure hyper (Rust) | ~7.7 µs | — |
| 2 | hyper4k C ABI + native Rust callback | ~9.7 µs | +2.0 µs |
| 2.5B | Kotlin `Hyper4kServer` handler, no Neton dispatcher (256 MB GC) | ~17.7 µs | +8.0 µs |
| 3 | full Neton dispatcher (256 MB GC, WARN) | ~34–35 µs | +16–17 µs |

Stable across runs; tier2.5B measured the same (17.6–17.7 µs) at a 10 MB and a
256 MB GC floor, so the floor is not what separates it from tier3.

## What this supports (and what it does not)

- **hyper4k over pure hyper is +2 µs — ~26% of tier1, not "free".** Low priority
  to optimize, but not proven negligible.
- **Crossing into Kotlin/Native is +8 µs** (FFI copies, coroutine, GC).
- **The full Neton path is +16–17 µs over the raw Kotlin server.** This is the
  *difference between two tiers*, not the dispatcher's isolated self-time: it
  still bundles request conversion, response envelope, routing, security checks,
  GC and scheduling. It is the strongest candidate for where to look next, not a
  finished attribution.

## Explicitly not claimed

- No "architecture floor". No claim the +16 µs is safe to change without care —
  request-object reuse, coroutine context and response state can introduce
  concurrency/lifetime bugs even though the code is Kotlin.
- 2 cores, not the arena's 64. Contention and GC scalability can shift these
  ratios there; only the layered ordering is expected to transfer.

## Next (do not shortcut to the microbench)

1. Sample tier3's real HTTP path (perf cpu-clock) to split the +16 µs into
   request-build / routing / envelope / GC before changing anything.
2. Only then pick one hotspot; use the dispatch microbench to iterate the small
   change; confirm the gain on the full HTTP A/B, not the microbench.
