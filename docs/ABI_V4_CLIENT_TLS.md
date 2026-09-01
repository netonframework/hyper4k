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

Kotlin 侧只看到这些：创建 client、关闭 client、释放 client、发起请求、取消请求，
以及三类回调（headers 到达、body chunk 到达、请求终结）。**不暴露任何 rustls /
hyper / CryptoProvider 类型**，Hyper 升级不影响 klib API。

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

### 2.1 类型与常量

```c
typedef struct Hyper4kClient Hyper4kClient;   /* opaque */

typedef struct { const uint8_t *ptr; size_t len; } Hyper4kSlice;
typedef struct { Hyper4kSlice name; Hyper4kSlice value; } Hyper4kHeader;

/* ABI 版本：(major << 16) | minor。major 变化即不兼容。 */
#define HYPER4K_ABI_VERSION  ((4u << 16) | 0u)

/* client capability bits —— 只列已实现且有测试的。 */
#define HYPER4K_CLIENT_CAP_TLS           (1ull << 0)
#define HYPER4K_CLIENT_CAP_HTTP2         (1ull << 1)
#define HYPER4K_CLIENT_CAP_CUSTOM_CA     (1ull << 2)
#define HYPER4K_CLIENT_CAP_CANCEL        (1ull << 3)

/* client flags */
#define HYPER4K_CLIENT_HTTP2_REQUIRED    (1ull << 0)
#define HYPER4K_CLIENT_CA_REPLACE_SYSTEM (1ull << 1)  /* 缺省是"追加" */
```

`custom_ca_pem` **默认追加到系统根证书之上**；置 `CA_REPLACE_SYSTEM` 才替换。
两种语义差别很大（私有 CA 场景要追加，固定 pinning 场景要替换），不能靠猜。

`hyper4k_client_new()` 在调用内**复制 options 指向的全部数据**，返回后调用方的
缓冲即可释放。兼容规则：`struct_size` 小于本版本已知大小时，缺失字段取默认值；
大于时忽略尾部多余字节；**`flags` 里出现未知位一律拒绝**（静默忽略未知开关会让
安全相关的 flag 失效而无人察觉）。

### 2.2 生命周期：close 与 free 分离

单一 `free` 无法同时满足"不阻塞"和"不 use-after-free"。拆成两步：

```c
Hyper4kClient *hyper4k_client_new(const Hyper4kClientOptions *opts,
                                  int32_t *out_error);

/* 幂等、非阻塞。停止接收新请求，取消在途请求。
   每个已接受的请求仍会收到恰好一次 OnDone。 */
void    hyper4k_client_close(Hyper4kClient *client);

/* 阻塞直到所有请求到达终态且不再产生任何回调，然后释放。
   MUST NOT 从回调线程调用 —— 那会自己等自己，必然死锁。
   调用方保证：free 返回后 user_data 才可以被回收。 */
void    hyper4k_client_free(Hyper4kClient *client);
```

Kotlin 侧的 `close()` 映射为 `close` + `free`，在非回调线程上执行。

### 2.3 回调线程与背压

回调**不在 Tokio I/O worker 上执行**。Rust 内部为每个 client 维护一个专用的
bridge executor 与**有界**队列；I/O worker 只把事件投递进队列。

- 队列满时，Rust 侧对该 stream **施加 HTTP/2 流控背压**（停止读取窗口），
  而不是无界缓存，也不是丢数据，更不是阻塞 I/O worker。
- 慢消费者只拖慢自己那条 stream，不影响同连接的其他 stream。

顺序与并发保证：

- 同一 `request_id`：`OnHeaders` → `OnChunk`\* → `OnDone`，**严格串行**。
- 不同 `request_id`：回调**可以并发**，Kotlin 侧回调实现必须是线程安全的。
- `OnDone` 对每个**已被接受**的请求**恰好一次**；其后不再有该 id 的任何回调。
- 回调**不得让异常跨越 C ABI**。Kotlin wrapper 必须在边界捕获，转成对该请求的
  取消 —— 异常穿过 FFI 是未定义行为，而 crate 是 `panic = "abort"`。

### 2.4 发起与取消

```c
typedef struct {
    uint32_t             abi_version;
    uint32_t             struct_size;
    Hyper4kSlice         method;
    Hyper4kSlice         url;
    const Hyper4kHeader *headers;
    size_t               header_count;
    const uint8_t       *body_ptr;
    size_t               body_len;
    uint64_t             read_idle_timeout_ms;  /* 0 = 用 client 缺省 */
} Hyper4kClientRequest;

/* 返回错误码；成功时通过 out 参数给出 id。
   **在本函数返回之前绝不触发任何回调** —— 否则 Kotlin 还没登记 id 就先收到事件。 */
int32_t hyper4k_client_send(Hyper4kClient *client,
                            const Hyper4kClientRequest *request,
                            Hyper4kOnHeaders on_headers,
                            Hyper4kOnChunk on_chunk,
                            Hyper4kOnDone on_done,
                            void *user_data,
                            uint64_t *out_request_id);

/* 返回 ACCEPTED / ALREADY_COMPLETED / NOT_FOUND。
   取消胜出时，该请求仍会收到一次 OnDone(CANCELLED)。 */
int32_t hyper4k_client_cancel(Hyper4kClient *client, uint64_t request_id);
```

用返回值表达提交失败的具体原因（URL 非法、header 非法、client 已关闭……），
比"返回 0"能给运维的信息多得多。

**超时分两级**：`request_timeout_ms` 是整个请求的上限，`read_idle_timeout_ms`
是**块间空闲**上限。SSE 这类长流响应不适用总时长上限，必须靠空闲超时兜底。

### 2.5 错误描述符

`OnDone` 只给一个整数无法运维 —— "为什么连不上"和"证书哪里不对"是两个问题。

```c
typedef enum {
    HYPER4K_ERR_NONE = 0,
    HYPER4K_ERR_DNS,              HYPER4K_ERR_CONNECT,
    HYPER4K_ERR_TLS_CA,           HYPER4K_ERR_TLS_HOSTNAME,
    HYPER4K_ERR_TLS_EXPIRED,      HYPER4K_ERR_TLS_OTHER,
    HYPER4K_ERR_ALPN_NO_H2,       HYPER4K_ERR_PROTOCOL,
    HYPER4K_ERR_TIMEOUT,          HYPER4K_ERR_IDLE_TIMEOUT,
    HYPER4K_ERR_CANCELLED,        HYPER4K_ERR_CLIENT_CLOSED,
    HYPER4K_ERR_OUTCOME_UNKNOWN,  /* 见 §四，重试判定的唯一依据 */
} Hyper4kErrorKind;

typedef struct {
    int32_t      kind;        /* Hyper4kErrorKind，稳定分类 */
    uint32_t     protocol_code; /* 如 HTTP/2 错误码；无则 0 */
    Hyper4kSlice message;     /* 借用的诊断文本，仅供日志，不得用于分支判断 */
} Hyper4kError;

typedef void (*Hyper4kOnDone)(void *ud, uint64_t request_id,
                              const Hyper4kError *error);  /* NULL = 成功 */
```

**HTTP 4xx/5xx 是正常响应,不是错误**：走 `OnHeaders` + `OnDone(NULL)`。只有传输
层与协议层失败才进 error。把 404 当异常会逼调用方用错误码做业务分支。

**Trailers 本版不支持**，收到即忽略；需要时作为独立 ABI 项另加。

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

## 四、GOAWAY、连接中断与重试（RFC 9113）

**收到 GOAWAY 不是终态。** 这是我初稿写错的地方：服务端发 GOAWAY 后**仍会继续
完成** `last_stream_id` 之内的 stream，此时立刻判定"结果未知"会把大量正常完成的
请求误报为失败。正确的状态机分两步 —— 先按 GOAWAY 分类，再看连接实际怎么结束。

**第一步，收到 GOAWAY 时：**

| 情形 | 协议保证 | 处理 |
|---|---|---|
| stream ID > `last_stream_id` | 确定未被处理 | 自动在新连接重试 |
| 收到 `REFUSED_STREAM` | 确定未被处理 | 自动重试 |
| stream ID ≤ `last_stream_id` | 可能正在处理 | **继续等待**，不做判定 |
| GOAWAY 之后的新请求 | —— | 直接走新连接 |

**第二步，连接随后中断时**，对仍未收到完整响应的在途请求：

| 请求方法 | 处理 |
|---|---|
| 幂等（GET / HEAD / PUT / DELETE / OPTIONS / TRACE） | 可自动重试 |
| 非幂等（POST / PATCH） | **不自动重试**，`OnDone(OUTCOME_UNKNOWN)` |

**没有 GOAWAY 的连接中断走同一张表。** 传输层无法证明请求未被处理时，语义就是
"结果未知"，与是否先收到 GOAWAY 无关。

**自动重试必须有次数上限**（缺省 2 次）。连续 `REFUSED_STREAM` 或反复 GOAWAY 的
服务端会让无上限的重试变成活锁，把一次故障放大成持续压测。

APNs 的 POST 同样适用。只有传输层能证明请求未被处理时才自动重试；其余情况交给
上层按 `apns-id` 与业务策略决定。为了"可靠推送"而盲目重试会制造重复通知。

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
7. GOAWAY 状态机：
   - `last_stream_id` 之外的请求自动在新连接恢复
   - `last_stream_id` 之内的 stream 在 GOAWAY 后**仍正常完成**（不得误判为失败）
   - 连接随后中断时，在途 GET 自动重试、在途 POST 返回 `OUTCOME_UNKNOWN`
   - 无 GOAWAY 的连接中断走同一判定
   - 重试次数上限生效：持续 `REFUSED_STREAM` 不会活锁
8. 生命周期：`close` 后每个已接受请求恰好一次 `OnDone`；`free` 返回后无任何回调
9. 背压：慢消费者只拖慢自身 stream，不阻塞 I/O worker，不无界缓存
10. `send()` 返回前不触发任何回调
11. 错误分类可区分 DNS / 连接 / CA / hostname / 过期 / ALPN / 超时 / 取消
12. HTTP 4xx/5xx 走成功路径（`OnDone(NULL)`），不进 error
13. `cancel` 三态正确，且取消胜出后仍收到一次 `OnDone(CANCELLED)`
14. `custom_ca_pem` 追加与替换两种模式行为符合定义
15. 未知 `flags` 位被拒绝；`struct_size` 大于/小于当前版本的兼容规则生效
16. 四个平台编译通过；macOS 与 Linux 做真实运行测试

APNs 真凭据 smoke 作为**可选外部测试**，不进普通单测。

每条「必须失败」的用例都要做反向验证：先确认它在正确配置下通过，再确认错误配置
下确实失败。否则一个永远返回错误的实现也能让这些用例全绿。
