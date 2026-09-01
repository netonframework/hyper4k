# hyper4k

高性能 **Tokio + Hyper** HTTP 引擎，通过借用切片 C ABI 暴露，作为 Neton（Kotlin/Native）Web 框架的原生底座。

> 设计原则：hyper4k 只做**协议与传输**（accept / parse / body / 写回 / 连接生命周期）。
> 路由、中间件、handler 全部留在 Neton（Kotlin）侧。Hyper 永远不碰路由表。

## 分层

```
hyper4k (Rust crate, lib/)   Tokio runtime + Hyper 协议引擎 + C ABI
   ↓ cinterop
hyper4k (Kotlin/Native 项目)   可复用的封装（Hyper4kServer / Request / Response）
   ↓ 依赖
neton-http-hyper4k             可选 Neton Adapter，接进 RouteMatcher / RequestEngine
```

hyper4k 本身是一个 Kotlin/Native 项目，Rust 引擎作为 `lib/` 子目录内嵌：

```
hyper4k/
  README.md
  build.gradle.kts            Kotlin/Native 模块（cinterop 封装）
  src/                        commonMain / nativeMain / nativeInterop
  lib/                        Rust crate：Cargo.toml, cbindgen.toml, src/lib.rs, include/hyper4k.h
```

- `lib/Cargo.toml` / `lib/src/lib.rs` —— Rust crate
- `lib/include/hyper4k.h` —— C ABI（契约与线程模型注释都在这里）
- `build.gradle.kts` / `src/` —— Kotlin/Native 封装（独立可复用，不依赖 Neton）

## C ABI 速览

```c
Hyper4kServer* hyper4k_server_start(host, port, on_request, user_data);  // 绑定失败返回 NULL
int32_t        hyper4k_respond(responder, status, headers_ptr,len, body_ptr,len); // 成功返回 1
void           hyper4k_server_stop(server);
```

线程模型（push）：`on_request` 在 Tokio worker 线程上被调用，语义上应尽快返回，之后
（可在另一线程）调用 `hyper4k_respond` 完成该请求。Kotlin 封装会在回调内复制请求快照，
然后交给受管协程执行 `suspend` handler。响应句柄是单次数字 token；请求已取消或已经响应时，
迟到调用安全返回 0。crate 以 `panic = "abort"` 编译，保证 panic 不跨 FFI。

默认并发是有界的：达到上限立即返回 503，单请求超过 deadline 返回 504。停止时先停止接收
新任务、等待在途 handler 到达 grace deadline，再关闭 Tokio。不存在同步 handoff 开关。

## 构建 Rust crate

需要本机 Rust 工具链。在 `lib/` 目录下逐 target 产出 `libhyper4k.a`：

```bash
cd lib

# 本机
cargo build --release

# 指定 target（K/Native target -> Rust triple）
rustup target add aarch64-apple-darwin
cargo build --release --target aarch64-apple-darwin     # macosArm64
cargo build --release --target x86_64-apple-darwin      # macosX64
cargo build --release --target x86_64-unknown-linux-gnu # linuxX64
cargo build --release --target aarch64-unknown-linux-gnu# linuxArm64
cargo build --release --target x86_64-pc-windows-gnu    # mingwX64
```

跨平台交叉编译推荐 [`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild)：
`cargo zigbuild --release --target <triple>`。产物在 `lib/target/<triple>/release/libhyper4k.a`。

运行 Rust 验证：

```bash
cd lib
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

## 构建 Kotlin/Native 封装

根目录的 `build.gradle.kts` 已配置：5 个 K/Native target、按 target 注入 `libhyper4k.a`
路径的 cinterop、各平台链接所需系统库，以及便捷的 `cargoBuild<Target>` 任务。

本机 Kotlin/Native 验证：

```bash
./gradlew macosArm64Test
```

其他平台使用对应的 `<target>Test` 任务。Gradle 会先构建相同 target 的 Rust 静态库。

## 接入 Neton

Neton 默认只包含 Ktor Adapter。独立的 `neton-http-hyper4k` 仓库负责 Neton 集成，
应用引入后把 Adapter 构造器直接传给 HTTP Component：

```kotlin
import neton.http.hyper4k.Hyper4kHttpAdapter

Neton.run(args) {
    http(::Hyper4kHttpAdapter) { port = 8080 }
}
```

`hyper4k` 本身不依赖 Neton；`neton-http-hyper4k` 才负责 routing、security、parameter
binding 和 response envelope。Adapter 选择属于编译期应用代码，不使用 `http.engine`
配置或运行时 Provider 注册；未引入 Hyper Adapter 的应用不会链接 Rust/FFI 产物。

## 压测对比

启动两套各跑一遍，同条件对比：

```bash
# 安装压测工具
#   wrk:   https://github.com/wg/wrk
#   bombardier: go install github.com/codesenberg/bombardier@latest

# 小 JSON（Rust 收益最大的场景）
wrk -t4 -c128 -d30s http://127.0.0.1:8080/health
bombardier -c 128 -d 30s http://127.0.0.1:8080/health
```

关注：RPS、p50/p99 延迟、连接建立开销。建议先用 echo/health 路由量出**纯传输层**差距，
再加业务 handler 量端到端差距。AI gateway 这类 I/O 密集、长流式场景，瓶颈在上游延迟，
Rust 底座收益最小——别用它来判断是否值得迁移。

## 路线图

- [x] v1：单请求聚合 body 打通（method/path/headers/body + 写回）
- [x] Hyper4kHttpAdapter JSON / form / security / CORS request pipeline
- [x] 异步 handoff：回调复制请求后立即返回，Kotlin/Native 协程处理 suspend handler
- [x] 有界并发、请求超时与优雅停止
- [ ] Multipart upload support
- [x] 流式 body / SSE relay（真 socket 分块测试把关，缓冲实现会让构建失败）
- [x] HTTP/2 prior-knowledge（h2c）：真实 client handshake + 请求派发 + 同连接并发 stream
- [ ] HTTP/2 over TLS（ALPN 协商，随 TLS 能力一并）
- [ ] 可选 `hyper4k-tower`：接入 Tower 生态（timeout / trace / load-shed）
```
