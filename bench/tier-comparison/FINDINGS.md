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

## json-h2c: reproduced *slower*, ruled OUT Nagle — but NOT root-caused

The arena's json-h2c is far slower than baseline-h2c (and json-h2c/4096 records 0:
3730% CPU, zero completed — busy, not deadlocked). Reproduced on the clean box
with a small h2c client (N conns x M streams):

| load | baseline-h2c | json-h2c |
|---|---|---|
| 1x1 (no queuing) | 91 µs, 10.8k rps | 145 µs, 6.8k rps |
| 16x32 | 12.7 ms, 40.5k rps | 55.7 ms, 9.3k rps |

At 1x1 there is no ~40-200 ms floor, so **delayed-ACK/Nagle is not the cause and
TCP_NODELAY would not fix json-h2c** (hypothesis tested and rejected before
shipping). json-h2c is simply CPU-heavier per request (JSON serialization +
larger body + more DATA frames): ~1.6x at 1x1, widening under load as the extra
cost queues. What this does and does not show:
- Rules out delayed-ACK/Nagle as the cause (no ms-scale floor at 1x1), so
  TCP_NODELAY would not fix json-h2c. Tested and rejected before shipping.
- Shows json-h2c is heavier per request end-to-end and queues under load. It does
  NOT prove per-request CPU is the *sole* cause, and it did NOT reproduce a zero:
  16x32 still did ~9.3k rps. The arena's 0 was a 45s timeout with missing stats,
  not "0 completed in 5s".
- `json-h2c/4096` stays UNRESOLVED. That the object pool / cheaper serialization
  would fix it is a hypothesis to test on the arena, not a conclusion.

## Context-object pool A/B: no measurable benefit at 2 cores (honest negative)

The request-context pool (neton `feature/request-context-pool`, gated by
`http.contextPoolSize`) was A/B'd on the clean 2-core Fedora box with ONE binary
(`examples:bench`, `NETON_CTX_POOL` toggles it), WARN logging, `/json`,
`wrk -t2 -c128 -d15s`, four interleaved rounds, per-request process CPU from
/proc/PID/stat:

| | per-req CPU (4 rounds) | rps | RSS |
|---|---|---|---|
| pool off (control) | 34.11 / 34.27 / 34.10 / 34.10 µs | 37.4–37.7k | ~21 MB |
| pool on (2048) | 34.59 / 34.21 / 34.43 / 34.26 µs | 36.8–37.8k | ~21 MB |

CPU, throughput and memory are all within noise; pool-on is if anything a hair
slower. The three per-request objects it pools (context + view + memory
response) are simply not a material cost at this scale: K/N reclaims short-lived
allocations cheaply and the SpinLock + lease-token bookkeeping roughly cancels
whatever it saves. This matches the sampling story — the real hotspots were
method parsing and the query map (both already fixed); context allocation never
showed up.

What this does and does not conclude:
- On a 2-core box the pool is a wash. Correctness is fully proven
  (ContextPoolTest + 144-test suite), and it is OFF by default (contextPoolSize=0),
  so it costs nothing shipped.
- It does NOT test the 64-core arena, where GC scalability/contention and
  cross-thread allocation churn differ. That the pool helps there is a HYPOTHESIS,
  testable only by setting `http.contextPoolSize` on the arena entry and running
  /benchmark — not a conclusion. On current evidence, do not claim a benefit.

## Context-pool A/B, valid this time (/work exercises all three objects)

The first A/B used `/json`, whose handler returns a Map and never touches
request or response — so the view and memory buffer were never created and the
pool had nothing to reuse (GPT caught this). Redone on `/work`, a handler that
reads `request.queryParam` (builds the view), reads `request.headers` (builds the
header map) and writes via `context.response` (builds the memory buffer). Smoke
confirmed it returns `id=42 ua=18`, i.e. the view is live. Same box, WARN,
`wrk -t2 -c128 -d15s`, 4 interleaved rounds:

| | per-req CPU (4 rounds) | rps | RSS |
|---|---|---|---|
| pool off | 39.57 / 39.13 / 38.85 / 38.80 µs | 33.2–34.0k | ~21 MB |
| pool on (2048) | 39.22 / 38.99 / 38.98 / 39.25 µs | 34.0–34.5k | ~21 MB |

Per-request CPU is flat (~39 µs both; off's 38.80 min is below on's 38.98). rps
is ~1.3% higher with the pool but the ranges overlap (off 34.0k ≈ on 34.1k), so
it is inside run-to-run noise, not a clean win like the method/query-scan changes
were. RSS identical. Conclusion stands and is now on a valid measurement: at 2
cores the context pool gives no meaningful benefit — the per-request cost is the
work (header map, query scan, envelope, write), not the allocation of these three
small short-lived objects, which K/N already reclaims cheaply.

Not tested: 64-core arena GC scalability (hypothesis only). What this run does NOT
claim: that "off = zero cost vs pre-change" — the plain path was itself
restructured; proving byte-equivalence would need a pre-change baseline, not a
toggle of the same binary.

## JSON string encoding: bulk-append fast path — confirmed win

Sampling /json (perf cpu-clock, clean 2-core box) showed no single dominant
hotspot, but a JSON-serialization cluster: StringBuilder.ensureCapacity ~0.75% +
appendJsonString ~0.54%. Cause: `appendJsonString` appended char-by-char, each
append re-checking capacity, even when nothing needed escaping.

Change (neton `perf/json-string-fastpath`): scan once for the first char needing
an escape; if none, append the whole string in one bulk copy. Byte-identical
(fastEnvelopeIsByteIdenticalToKotlinx covers all escape chars + 中文).

Clean-box A/B, two binaries differing ONLY in this path (a temp compile switch),
pool off, 4 interleaved rounds:

- Tiny `/json` (~58 B, two short strings): after ~37.2k vs before ~36.6k rps —
  ~1% and inside noise; strings are a small fraction of that response.
- `/jsonbig` (3786 B, 25 items x 5 string/num fields): before 240/239/240/238 us
  @ ~6.9k rps; after 201/200/198/199 us @ ~8.2k rps. Every after round beats
  every before round (no overlap): **-17% per-request CPU (~40 us/req), +18% rps.**

So the win scales with string content — negligible on trivial bodies, decisive on
the realistic JSON payloads the arena's json profiles use. This is a clean
sample -> one change -> non-overlapping A/B result, like the method/query-scan
wins and unlike the context pool.
