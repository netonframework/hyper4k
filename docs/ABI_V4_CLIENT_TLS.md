# hyper4k ABI v4：Client 与 TLS

> 状态：**设计待评审**，未开始实现。
>
> 目标：给 Neton 的出站 `HttpClient` 一个 Rust 底座，支持 HTTPS、SNI、证书校验与
> ALPN；解锁 APNs（强制 HTTP/2 over TLS）与公网 HTTPS 调用。当前 Neton 的
> client 只有 Ktor 一种实现，`neton-http-hyper4k` 里零 client 代码。
>
> 前置结论（已定，不再讨论）：
>
> - **只做 Client TLS。** Server 维持 h1/h2c，TLS 由 nginx / Envoy / HAProxy 等
>   入口终止。不设计 Server TLS ABI —— 为一个尚不存在的需求提前冻结接口，比
>   将来另开一版更贵。
> - **不使用 Hyper 官方 C API。** 它标注 `unstable (no semver)`，只有 Client
>   connection 级原语、没有 Server API，且会把 socket 读写、executor poll、task
>   轮询、header 迭代全部变成跨 FFI 往返，同时把 Hyper 生命周期暴露给 Kotlin。
>   连接池、DNS、TLS、重连仍要我们自己封。hyper4k 用 Hyper 的**稳定 Rust API**，
>   自己冻结一层粗粒度 C ABI，才是兼容性与性能的最优解。
> - **不实现客户端证书认证。** APNs 走 token authentication（JWT）。mTLS 是另一
>   件事，有需求时再加。
> - **不提供正式的 `insecureSkipVerify` 配置。** 跳过校验的能力只允许存在于测试
>   夹具，不进公开 ABI。

---

## 一、分层与边界

```text
Kotlin/Native (neton-http-hyper4k)
    ↓ 少量、粗粒度、稳定的 C ABI
hyper4k (Rust)
    ↓ Hyper 稳定 Rust API
hyper + hyper-util + tokio + rustls
```

Rust 侧独占：DNS 解析、SNI、ALPN 协商、证书校验、连接池、HTTP/2 多路复用与
HPACK、GOAWAY 后重连、超时与取消。

Kotlin 侧只看到六件事：创建 client、关闭 client、发起请求、取消请求、headers
到达、body chunk 到达、请求完成或失败。**不暴露任何 rustls / hyper /
CryptoProvider 类型**，Hyper 升级不影响 klib API。

---

## 二、ABI 基础（必须先于 TLS 落地）

`Hyper4kClientOptions` 是本项目第一个带版本的配置结构体。在它发出去之前，版本与
能力查询必须先在，否则以后加字段就只能靠猜。

```c
uint32_t    hyper4k_abi_version(void);
const char *hyper4k_version(void);

/* 拆成两个，而不是一个含糊的总 bitmask：server 与 client 的能力集合不同，
   合在一起会逼调用方去猜某一位对哪一侧有意义。 */
uint64_t hyper4k_server_capabilities(void);
uint64_t hyper4k_client_capabilities(void);
```

**只定义已经实现且有测试的位**，不预留猜测性能力。位一旦发出去含义就冻结。

配置结构体一律 `abi_version` + `struct_size` 前置，新增字段追加在尾部：

```c
typedef struct {
    uint32_t       abi_version;
    uint32_t       struct_size;
    uint64_t       flags;              /* HYPER4K_CLIENT_HTTP2_REQUIRED 等 */
    uint64_t       connect_timeout_ms;
    uint64_t       request_timeout_ms;
    const uint8_t *custom_ca_pem;      /* NULL = 只用系统根证书 */
    size_t         custom_ca_pem_len;
} Hyper4kClientOptions;
```

### 2.1 Client 调用面

粗粒度、低频。一次请求跨 FFI 的次数与 body chunk 数同阶，与 header 数无关。

```c
typedef struct { const uint8_t *ptr; size_t len; } Hyper4kSlice;
typedef struct { Hyper4kSlice name; Hyper4kSlice value; } Hyper4kHeader;

/* 生命周期：应用建一个 client，内部持有 tokio runtime 与连接池。 */
Hyper4kClient *hyper4k_client_new(const Hyper4kClientOptions *opts);
void           hyper4k_client_free(Hyper4kClient *client);

/* 响应事件回调。同一 request_id 的顺序保证：headers → chunk* → done。
   切片在回调内借用，返回后即失效；需要留存必须复制。 */
typedef void (*Hyper4kOnHeaders)(void *ud, uint64_t request_id, uint16_t status,
                                 const Hyper4kHeader *headers, size_t header_count);
typedef void (*Hyper4kOnChunk)(void *ud, uint64_t request_id,
                               const uint8_t *ptr, size_t len);
typedef void (*Hyper4kOnDone)(void *ud, uint64_t request_id, int32_t error_code);

/* 返回 request_id（0 表示提交失败）。body 在调用内复制一次。 */
uint64_t hyper4k_client_send(Hyper4kClient *client,
                             const Hyper4kSlice *method, const Hyper4kSlice *url,
                             const Hyper4kHeader *headers, size_t header_count,
                             const uint8_t *body_ptr, size_t body_len,
                             Hyper4kOnHeaders on_headers, Hyper4kOnChunk on_chunk,
                             Hyper4kOnDone on_done, void *user_data);

/* 幂等；请求已完成时安全返回。 */
int32_t hyper4k_client_cancel(Hyper4kClient *client, uint64_t request_id);
```

`error_code` 复用现有返回码体系，并新增 TLS / ALPN / GOAWAY 相关码。**「结果未知」
必须是一个独立错误码**（见 §四），不能和普通传输失败混在一起 —— 调用方要靠它决定
能不能重试。

---

## 三、TLS

- `rustls 0.23` + `tokio-rustls`，`default-features = false` 显式启用**单个**
  crypto provider，避免两个后端同时进二进制。
- Provider 选 `aws-lc-rs`（rustls 默认，为 FIPS 与后量子留空间）。四平台实测见
  §六。**Provider 是构建实现细节，不进 Neton 配置。**
- 默认加载系统根证书；`custom_ca_pem` 仅供测试与私有服务。
- TLS 1.2 与 1.3 都支持。
- ALPN 通告 `h2` 与 `http/1.1`。`HYPER4K_CLIENT_HTTP2_REQUIRED` 置位时，ALPN 没
  协商到 `h2` 直接失败，**禁止静默降级** —— 降级会让配置错误长期隐藏，而 APNs
  明确要求 HTTP/2 与 TLS 1.2 以上。

---

## 四、GOAWAY 与重试（RFC 9113）

**不对所有在途请求自动重试。** 边界由协议给定，不由我们的偏好给定：

| 情形 | 协议保证 | 处理 |
|---|---|---|
| stream ID > GOAWAY 的 `last_stream_id` | 未被处理 | 自动在新连接重试 |
| `REFUSED_STREAM` | 未被处理 | 自动重试 |
| stream ID ≤ `last_stream_id` | **可能已产生副作用** | 不自动重试；返回明确的「结果未知」错误 |
| GOAWAY 之后的新请求 | —— | 直接走新连接 |

APNs 的 POST 同样适用。只有传输层能证明请求未被处理时才自动重试；其余交给上层按
`apns-id` 与业务策略决定。为了「可靠推送」而盲目重试会制造重复通知。

---

## 五、性能约束

- **Client** header 从第一天就用切片描述符数组（`Hyper4kHeader[]`），不引入文本
  拼接。
- Server 侧现有的 `"Name: Value\n"` 文本编码有同样的问题，但改它是对**已发布
  server ABI 的破坏性变更**，不属于本轮。留作 ABI v5 的独立项，届时与 server 的
  其他变更一起发一版，不要为了顺手而混进 client 这一版。
- 请求回调用借用切片；异步 handoff 时只复制一次。
- TLS、HPACK、连接池、响应解析全部留在 Rust。
- body chunk 合理批量，避免小块高频跨 FFI。
- release 保持 LTO + 单 codegen unit + strip（现状已如此）。
- 建立三层基线：纯 Hyper、hyper4k C ABI、Neton 完整链路，分别记吞吐、p99、RSS、
  每请求分配。没有基线就无法判断损耗出在哪一层。

---

## 六、平台矩阵

Windows 不在当前发布矩阵内。**因此 `mingwX64` 要从公开平台矩阵中移除** —— 一边
声称支持一边不测试，比明确不支持更糟。涉及 `hyper4k/build.gradle.kts`、两份
README，以及 `neton/neton-http-hyper4k/build.gradle.kts`。

crypto provider 交叉编译实测（2026-09-02，staticlib）：

| target | `ring` | `aws-lc-rs` |
|---|---|---|
| aarch64-apple-darwin | OK | OK |
| x86_64-apple-darwin | OK | OK |
| x86_64-unknown-linux-gnu | OK | OK |
| aarch64-unknown-linux-gnu | OK | OK |

`aws-lc-rs` 的非 FIPS 构建**不需要 cmake 二进制** —— 已在 PATH 移除 cmake 后重新
编译验证。日志里的 `Compiling cmake v0.1.58` 只是那个 crate 被编译。

---

## 七、验收

Rust 侧自动化测试，全部用本地生成的 CA 与证书：

1. 真实 TLS + ALPN `h2` 的请求／响应往返
2. 错误 CA 必须失败
3. 错误 hostname 必须失败
4. 过期证书必须失败
5. 对只支持 h1 的服务器，`HTTP_2_REQUIRED` 必须失败且不降级
6. 同一 TLS 连接上完成多个并发 HTTP/2 stream
7. 服务端发 GOAWAY 后：`last_stream_id` 之外的请求自动在新连接恢复；
   `last_stream_id` 之内的在途 POST **不**自动重试，返回结果未知
8. 四个平台编译通过；macOS 与 Linux 做真实运行测试

APNs 真凭据 smoke 作为**可选外部测试**，不进普通单测。

每条「必须失败」的用例都要做反向验证：先确认它在正确配置下通过，再确认错误配置
下确实失败。否则一个永远返回错误的实现也能让这些用例全绿。
