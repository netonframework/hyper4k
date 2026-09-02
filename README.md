# hyper4k

High-performance **Tokio + Hyper** HTTP engine, exposed through a borrowed-slice C ABI as the
native foundation of the Neton (Kotlin/Native) web framework.

[中文文档](README.zh-Hans.md)

> Design rule: hyper4k does **protocol and transport only** (accept / parse / body / write-back /
> connection lifecycle). Routing, middleware and handlers all stay on the Neton (Kotlin) side.
> Hyper never touches the route table.

## Layering

```
hyper4k (Rust crate, lib/)   Tokio runtime + Hyper protocol engine + C ABI
   ↓ cinterop
hyper4k (Kotlin/Native)      Reusable wrapper (Hyper4kServer / Request / Response)
   ↓ dependency
neton-http-hyper4k           Neton adapter, wired into RouteMatcher / RequestEngine
```

hyper4k is itself a Kotlin/Native project, with the Rust engine embedded as the `lib/` subdirectory:

```
hyper4k/
  README.md
  build.gradle.kts            Kotlin/Native module (cinterop wrapper)
  src/                        commonMain / nativeMain / nativeInterop
  lib/                        Rust crate: Cargo.toml, cbindgen.toml, src/lib.rs, include/hyper4k.h
```

- `lib/Cargo.toml` / `lib/src/lib.rs` — the Rust crate
- `lib/include/hyper4k.h` — the C ABI (contract and threading notes live here)
- `build.gradle.kts` / `src/` — the Kotlin/Native wrapper (reusable on its own, no Neton dependency)

## C ABI at a glance

```c
Hyper4kServer* hyper4k_server_start(host, port, on_request, user_data);   // NULL when the bind fails
int32_t        hyper4k_respond(responder, status, headers_ptr,len, body_ptr,len);  // 1 on success
void           hyper4k_server_stop(server);

/* Streaming responses: begin, then write chunks, then finish. */
int32_t        hyper4k_response_begin(responder, status, headers_ptr, len);
int32_t        hyper4k_response_write(responder, chunk_ptr, len);
int32_t        hyper4k_response_finish(responder);
```

Threading model (push): `on_request` is called on a Tokio worker thread and should return quickly;
`hyper4k_respond` completes the request afterwards, possibly from another thread. The Kotlin wrapper
copies a request snapshot inside the callback and hands it to a managed coroutine that runs the
`suspend` handler. The response handle is a single-use numeric token, so a late call after the
request was cancelled or already answered fails safely instead of touching freed memory. The crate
is built with `panic = "abort"` so a panic never crosses the FFI boundary.

Concurrency is bounded by default: past the limit the server answers 503 immediately, and a request
over its deadline gets 504. Shutdown stops accepting new work, waits for in-flight handlers up to the
grace deadline, then shuts Tokio down. There is no synchronous-handoff switch.

## Platforms

Four targets are built and tested: macosArm64, macosX64, linuxX64, linuxArm64.
**Windows is not supported.** Claiming a platform nobody builds or tests is worse
than not claiming it.

## Building the Rust crate

Needs a local Rust toolchain. Produce `libhyper4k.a` per target from `lib/`:

```bash
cd lib

# Host
cargo build --release

# Explicit target (Kotlin/Native target -> Rust triple)
rustup target add aarch64-apple-darwin
cargo build --release --target aarch64-apple-darwin      # macosArm64
cargo build --release --target x86_64-apple-darwin       # macosX64
cargo build --release --target x86_64-unknown-linux-gnu  # linuxX64
cargo build --release --target aarch64-unknown-linux-gnu # linuxArm64
```

For cross-compiling, [`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild) is the easiest
route: `cargo zigbuild --release --target <triple>`. Artifacts land in
`lib/target/<triple>/release/libhyper4k.a`.

Release builds must resolve dependencies from the committed lockfile:

```bash
cargo build --release --locked
```

Rust checks:

```bash
cd lib
cargo fmt --all -- --check
cargo test --all-targets --locked
cargo clippy --all-targets -- -D warnings
```

## Building the Kotlin/Native wrapper

The root `build.gradle.kts` already covers it: four Kotlin/Native targets, per-target cinterop that
injects the matching `libhyper4k.a` path, the system libraries each platform needs to link, and
convenience `cargoBuild<Target>` tasks.

Host verification:

```bash
./gradlew macosArm64Test
```

Other platforms use their own `<target>Test` task. Gradle builds the Rust static library for the
same target first.

## Using it from Neton

`neton-http-hyper4k` is a module of the Neton repository and is Neton's default server engine: the
bare `http { }` DSL resolves to `Hyper4kHttpAdapter` as soon as an application depends on that
module. Naming the adapter explicitly is equivalent and works the same way:

```kotlin
import neton.http.hyper4k.Hyper4kHttpAdapter

Neton.run(args) {
    http(::Hyper4kHttpAdapter) { port = 8080 }
}
```

`hyper4k` itself does not depend on Neton; `neton-http-hyper4k` is what supplies routing, security,
parameter binding and the response envelope. Engine choice is compile-time application code — there
is no `http.engine` setting and no runtime provider registry — so an application that never depends
on the adapter never links the Rust/FFI artifacts.

## Benchmarking

Run both sides under identical conditions:

```bash
# Load tools
#   wrk:        https://github.com/wg/wrk
#   bombardier: go install github.com/codesenberg/bombardier@latest

# Small JSON (where Rust gains the most)
wrk -t4 -c128 -d30s http://127.0.0.1:8080/health
bombardier -c 128 -d 30s http://127.0.0.1:8080/health
```

Watch RPS, p50/p99 latency, and connection setup cost. Measure the **pure transport** gap with an
echo/health route first, then add a business handler for the end-to-end gap. For I/O-bound, long
streaming workloads such as an AI gateway the bottleneck is upstream latency and the Rust foundation
gains the least — do not use that shape to decide whether a migration is worth it.

## Roadmap

- [x] v1: single request with aggregated body (method/path/headers/body + write-back)
- [x] Hyper4kHttpAdapter JSON / form / security / CORS request pipeline
- [x] Async handoff: the callback copies the request and returns; a Kotlin/Native coroutine runs the suspend handler
- [x] Bounded concurrency, request timeouts and graceful shutdown
- [ ] Multipart upload support
- [x] Streaming body / SSE relay (guarded by a real-socket chunking test; a buffered implementation fails the build)
- [x] HTTP/2 prior knowledge (h2c): real client handshake, request dispatch, concurrent streams on one connection
- [x] Client TLS: HTTPS, SNI, certificate validation, ALPN, connection pool,
      streaming with backpressure, cancellation and RFC 9113-safe retry
- [ ] Server-side TLS (still terminated upstream by nginx / Envoy / HAProxy)
- [ ] Optional `hyper4k-tower`: Tower ecosystem integration (timeout / trace / load-shed)
