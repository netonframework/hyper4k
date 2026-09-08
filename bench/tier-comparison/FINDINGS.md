# Three-tier per-request cost, measured

Reproducible on one Linux box (interleaved, same load, same box). Corrects
earlier macOS-sample guesses, which were not trustworthy.

## Method

- Box: Rocky Linux 9, 4 cores (a small VM — NOT the arena's 64 cores).
- Load: `wrk -t4 -c256 -d15s` on `/baseline11?a=3&b=4` (1-byte-ish sum response).
- Per-request CPU = process CPU time delta (`/proc/PID/stat` utime+stime) over
  the window ÷ requests completed. Interleaved rounds; see `run.sh`.
- Real sampling: `perf record -e cpu-clock` (software event; hardware `cycles` is
  unavailable on this VM). ~1.1M samples. `perf report -e cycles` yields none.

## Tiers

| Tier | what | rps | per-request CPU |
|------|------|-----|-----------------|
| 1 | pure hyper (Rust), no ABI, no Kotlin | ~200–252k | 7.5–9.6 µs |
| 2 | hyper4k engine + C ABI + native Rust callback (no Kotlin runtime) | ~215–219k | 8.9–9.0 µs |
| 3 | full Neton (Kotlin/Native: dispatcher, FFI copies, coroutine, GC) | ~102–109k | 24–25 µs |

## What it shows

- **Tier1 ≈ Tier2.** The hyper4k C ABI, responder, oneshot and DashMap
  registration add essentially nothing over pure hyper. The engine is not the
  cost.
- **Tier2 → Tier3 is +~16 µs/request**, entirely the Kotlin/Native layer: the
  per-request FFI copies (method/path/query/body), the coroutine dispatch, GC,
  and the neton dispatcher/routing. This is ~2/3 of the full per-request cost and
  is where optimization has room.

## Not claimed

- This is a 4-core VM, not the 64-core arena; absolute numbers and the
  network-stack share differ there. The *relative* engine-vs-Kotlin split is the
  transferable finding.
- The 16 µs is not yet split among FFI copy / coroutine / GC / dispatcher. That
  needs a Tier 2.5 (minimal Kotlin callback) before any single-variable fix.
- No "architecture floor" conclusion. Earlier claims of one are withdrawn.
