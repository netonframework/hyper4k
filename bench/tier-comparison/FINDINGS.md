# Per-request cost by tier — clean-box, GC-aligned

Dedicated Fedora 44 box (2 cores, 1.6 GB + swap, nothing else running). Both
Kotlin tiers use the SAME 256 MB GC floor and WARN logging, so their delta is not
a GC-policy or logging artifact. The numbers below were taken MANUALLY (one tier at a time, commands in the git
history of this file); `run.sh` reproduces the same procedure but has not yet
been shown to regenerate these exact numbers. Two close manual runs are a signal,
not full validation. `run.sh` is the harness;
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

## First single-variable result (confirmed, small)

Hotspot from sampling: `method.toHttpMethod()` ran `uppercase()` + enum
`valueOf` per request (`uppercaseCodePoint` ~0.85% of tier3 samples). Change:
match upper-case methods verbatim, fall back for odd casing (neton commit
691e4fd).

Clean-box A/B, WARN, interleaved 4 rounds (before = tier3, after = tier3b, only
this change differs), per-request process CPU:

| | per-req CPU (4 rounds) | rps |
|---|---|---|
| before | 34.0 / 33.8 / 33.8 / 34.1 µs | ~39.9k |
| after  | 33.5 / 33.5 / 33.1 / 33.6 µs | ~40.8k |

Every "after" round is below every "before" round (max after 33.6 < min before
33.8) — a clean, reproducible ~1.2% (≈0.4 µs/req) improvement, matching the
hotspot's ~0.85% sampled share. Small, but it confirms the pipeline: sample →
one change → end-to-end gain that matches the predicted size. This is the
opposite of the earlier microbench gains that did not translate.

## Biggest remaining hotspot (not yet changed)

HashMap operations are ~4.3% of tier3 and near-absent in tier2.5B — the
dispatcher building per-request maps, dominated by query-parameter parsing into
a HashMap<String,List<String>>. It is load-bearing (handlers read the params),
so it needs a lighter parse rather than removal. Next target, with the same
sample → change → A/B discipline.

## Second single-variable result (query-param scan) — confirmed, bigger

Hotspot: dispatcher HashMap ops ~4.3% of tier3 (near-absent in tier2.5B) — the
per-request query map (LinkedHashMap + a MutableList per key + decoded strings),
built eagerly for ArgsView even when the handler reads a couple of params by name.

Change (neton 6421660): `queryParam(name)` and `args.first/all` scan the raw
query string; the full map stays lazy for iteration. Cross-checked vs the map
parse across shapes.

Clean-box A/B (Fedora, WARN, 4 interleaved rounds), on top of the method fix:

| | per-req CPU (4 rounds) | rps |
|---|---|---|
| method only | 33.4 / 33.5 / 33.1 / 33.2 µs | ~41.0k |
| + query scan | 31.1 / 31.4 / 31.4 / 31.8 µs | ~42.6k |

Every "scan" round below every "method" round: a clean ~5.7% (≈1.9 µs/req)
improvement, again matching the hotspot's sampled share (~4.3% > the method
fix's ~0.85%, and this win is ~5x larger). Cumulative over both changes:
tier3 ~34 → ~31.4 µs, ~7.6%.

Note on "keep a resident map": a single shared mutable map cannot hold
per-request-varying params under concurrency without racing. Scanning (or
per-request build) is the safe way to the same zero-shared-state goal; scanning
also avoids the allocation entirely for keyed reads.
