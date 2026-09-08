# Per-request cost by tier — what is defensible, and what is not

Measured on one small Linux box. Read the limits before quoting any number.

## Hard limits on this measurement

- **The box is shared.** It also runs privchat-server, a geario ntex server, and
  redis. Heavy load tests on 4 cores contend with those, so **throughput and any
  GC-sensitive tier are contaminated**. Do not trust rps here.
- **4 cores, not the arena's 64.** Absolute costs and the network-stack share
  differ there. Only the *relative* per-tier deltas are a localization clue.
- **Hardware `cycles` PMU is unavailable** (VM); `perf -e cpu-clock` works
  (~1.1M samples) and showed the kernel network stack, softirq, plus this VM's
  nftables/SELinux dominating — i.e. app cost is a minority *on this box*.

## Defensible: per-request *process* CPU (utime+stime of our own PID)

This metric charges only our process's CPU, so co-tenancy inflates it less than
throughput. Stable across interleaved rounds:

| Tier | what | per-request CPU |
|------|------|-----------------|
| 1 | pure hyper (Rust) | ~7 µs |
| 2 | hyper4k C ABI + native Rust callback | ~9 µs |
| 2.5B | Kotlin `Hyper4kServer` handler (FFI copy + coroutine + GC), no Neton dispatcher | ~15 µs |

- **Tier1 → Tier2: +~2 µs.** The C ABI + responder + oneshot/DashMap has a small
  but real cost. (Earlier "zero cost" is withdrawn.)
- **Tier2 → Tier2.5B: +~6 µs.** Crossing into Kotlin/Native per request — the FFI
  copies of method/path/query/body, the coroutine dispatch, and GC — roughly
  doubles per-request CPU over the raw ABI. This is real and in our code.

## NOT established

- **Tier3 (full Neton dispatcher) is not reliably measured here.** On this shared
  box the GC'd full path collapsed under contention (wildly variable, ~100+ µs);
  those numbers are contamination, not the dispatcher's true cost. The
  dispatcher's added cost over Tier2.5B is unknown until measured on a clean,
  isolated box or on the arena.
- No "architecture floor" claim. No split of the Kotlin cost into exact
  FFI/coroutine/GC shares — GC appears at several tiers and is not isolated.

## Next

1. Get a clean, isolated box (or a quiet window) and measure Tier3 comparably.
2. Only then pick one hotspot inside the Kotlin path for a single-variable,
   before/after experiment.
