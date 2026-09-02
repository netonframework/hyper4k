# hyper4k ABI v4：Client 与 TLS

> 状态：
> ```
> CLIENT TLS CORE:    IMPLEMENTED
> PUBLIC ABI:         IMPLEMENTED
> RESOURCE CONTRACT:  INCOMPLETE
> RELEASE READINESS:  BLOCKED
> ```
> C 头文件已声明完整 client API，并有一条穿过它跑 new → send → callbacks →
> close → free 的 Kotlin 契约测试。`free()` 是确定性等待（bridge 计数归零），
> 没有超时。
>
> 仍阻塞的是资源模型：client 级内存 / 请求上限未接（`RESOURCE_EXHAUSTED` 在生产
> 路径从未返回过）、每请求一个阻塞 worker、HTTP/2 连接窗口预留没有落到 builder
> 的 `initial_connection_window_size`。
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

**一个位只有在对应能力实现完成、测试通过之后，才允许由 `capabilities` 返回。**
不预留猜测性能力；位一旦发出去含义就冻结。本文档冻结的是位的含义，不是"这些能力
已经存在" —— 实现尚未开始。

所有配置结构体一律 `abi_version` + `struct_size` 前置，新增字段只能追加在尾部。
规范定义见 §2.2（options）与 §2.6（request）—— **全文各只有一份**，不得在别处
重复给出结构体定义。

### 2.1 类型、状态码与常量

```c
typedef struct Hyper4kClient Hyper4kClient;   /* opaque */

typedef struct { const uint8_t *ptr; size_t len; } Hyper4kSlice;
typedef struct { Hyper4kSlice name; Hyper4kSlice value; } Hyper4kHeader;

/* ABI 版本：(major << 16) | minor。major 变化即不兼容。 */
#define HYPER4K_ABI_VERSION  ((4u << 16) | 0u)
```

**同步返回码统一为 `Hyper4kStatus`，数值冻结**。`new` / `send` / `cancel` /
`resume` 全部返回它。数值一旦发布不得改动，新增只能追加。

**所有跨 ABI 的枚举一律用固定宽度 `int32_t`，不用 C `enum`** —— `enum` 的底层
宽度由编译器决定，跨 Rust / C / Kotlin/Native 三侧不能依赖默认布局。Rust 侧对应
`#[repr(i32)]` 或直接用 `i32`。冻结数值而不冻结表示，等于没冻结。

```c
typedef int32_t Hyper4kStatus;

#define HYPER4K_STATUS_OK                 ((Hyper4kStatus)  0)

/* 提交期参数与状态 */
#define HYPER4K_STATUS_ABI_MISMATCH       ((Hyper4kStatus) -1)  /* abi_version 不兼容 */
#define HYPER4K_STATUS_STRUCT_SIZE        ((Hyper4kStatus) -2)  /* struct_size 小于最小合法值 */
#define HYPER4K_STATUS_UNKNOWN_FLAGS      ((Hyper4kStatus) -3)  /* flags 含未知位 */
#define HYPER4K_STATUS_INVALID_ARG        ((Hyper4kStatus) -4)  /* NULL 指针、非法 URL / method / header */
#define HYPER4K_STATUS_UNSUPPORTED        ((Hyper4kStatus) -5)  /* 如 http:// + HTTP2_REQUIRED */
#define HYPER4K_STATUS_CLIENT_CLOSED      ((Hyper4kStatus) -6)
#define HYPER4K_STATUS_OOM                ((Hyper4kStatus) -7)  /* 真实分配失败 */
#define HYPER4K_STATUS_RESOURCE_EXHAUSTED ((Hyper4kStatus) -8)  /* 主动限流，见 §2.5 */

/* cancel / resume 专用 */
#define HYPER4K_STATUS_NOT_FOUND          ((Hyper4kStatus)-20)  /* 无此 request_id */
#define HYPER4K_STATUS_ALREADY_DONE       ((Hyper4kStatus)-21)  /* 已到终态 */
#define HYPER4K_STATUS_NOT_PAUSED         ((Hyper4kStatus)-22)  /* resume 一个未暂停的请求 */
```

`OOM` 与 `RESOURCE_EXHAUSTED` 不同：前者是分配器真的失败（`try_reserve` 之类），
后者是我们**主动**拒绝以守住内存上限。把主动限流报成 OOM 会误导运维去查内存泄漏。

> 成功码叫 `HYPER4K_STATUS_OK` 而不是 `HYPER4K_OK`：ABI v3 已经发布了
> `HYPER4K_OK = 1`（`hyper4k_respond` 的"已交付"）。同一个名字两个值，对 C 调用方
> 是无类型检查的静默陷阱 —— 实现 Task 1 时编译器报了宏重定义才发现。

`Hyper4kStatus` 是**同步**通道，`Hyper4kErrorKind`（§2.4）是**异步**通道。二者不
重叠：提交期失败只出现在返回值里，不会再触发 `OnDone`；反之亦然。所以上一版那个
既当提交错误又当 `OnDone` 错误的 `BAD_REQUEST` 已删除，提交期非法输入统一走
`HYPER4K_STATUS_INVALID_ARG`。

```c
/* client capability bits —— 只列 v4 已实现且有测试的。 */
#define HYPER4K_CLIENT_CAP_HTTP1         (1ull << 0)
#define HYPER4K_CLIENT_CAP_HTTP2         (1ull << 1)
#define HYPER4K_CLIENT_CAP_TLS           (1ull << 2)
#define HYPER4K_CLIENT_CAP_CUSTOM_CA     (1ull << 3)
#define HYPER4K_CLIENT_CAP_CANCEL        (1ull << 4)
#define HYPER4K_CLIENT_CAP_STREAMING     (1ull << 5)  /* 响应体分块 + 背压 */
/* v4 不提供 h2c prior knowledge（明文 HTTP/2），故无对应位。 */

/* client flags */
#define HYPER4K_CLIENT_HTTP2_REQUIRED    (1ull << 0)
#define HYPER4K_CLIENT_CA_REPLACE_SYSTEM (1ull << 1)  /* 缺省是"追加" */
```

`CA_REPLACE_SYSTEM` **替换的是信任根集合**，不是 certificate pinning —— pinning
锁定的是具体证书或公钥，是另一回事，本版不提供。

### 2.2 Options

```c
typedef struct {
    uint32_t       abi_version;
    uint32_t       struct_size;
    uint64_t       flags;
    uint64_t       connect_timeout_ms;
    uint64_t       request_timeout_ms;       /* 0 = 不限总时长 */
    uint64_t       read_idle_timeout_ms;     /* 0 = 不限块间空闲 */
    uint32_t       max_retries;              /* 额外重试次数，0 = 不重试 */
    uint32_t       _reserved;
    const uint8_t *custom_ca_pem;            /* NULL = 只用系统根证书 */
    size_t         custom_ca_pem_len;
} Hyper4kClientOptions;

/* 填入本版本默认值（含 max_retries = 2）。
   没有它就无法区分"调用方要 0 次重试"和"调用方零初始化了结构体"。

   **必须传入调用方实际分配的大小**：旧调用方按 v4.0 的较小结构体分配，运行时却
   加载了追加过字段的 v4.1 库时，只有指针的版本会按新结构体大小写入而越界。
   实现只写 min(struct_size, 本库已知大小)，并把结构体里的 struct_size 设为传入值。 */
Hyper4kStatus hyper4k_client_options_init(Hyper4kClientOptions *opts,
                                          uint32_t struct_size);
```

`connect_timeout_ms = 0` 表示**禁用**连接超时（一直等到 OS 放弃），与
`request_timeout_ms = 0` 同义。**不是**"用默认值"，也**不是**"立即超时" —— 这三种
理解都有人持有，所以必须写死。想要默认值就用 `hyper4k_client_options_init()`。

`request_timeout_ms = 0` 表示**禁用**总超时。SSE 这类长流必须能禁用它。

`max_retries` 是**额外**重试次数：`0` = 只尝试一次，`2` = 最多三次尝试。

**`struct_size` 有最小合法值**（到 `flags` 为止的前缀）。小于它一律拒绝 —— 否则
一个空结构体也能靠"缺失字段取默认"整体通过，把调用方的错误变成静默的默认行为。
大于当前版本已知大小时忽略尾部多余字节。**`flags` 含未知位一律拒绝。**

`hyper4k_client_new()` 在调用内复制 options 指向的全部数据，返回后调用方缓冲即可
释放。

### 2.3 生命周期与线程模型

```c
Hyper4kStatus hyper4k_client_new(const Hyper4kClientOptions *opts,
                                 Hyper4kClient **out_client);

/* 幂等、非阻塞。 */
void hyper4k_client_close(Hyper4kClient *client);

/* 阻塞直到所有请求到达终态且不再产生任何回调，然后释放。 */
void hyper4k_client_free(Hyper4kClient *client);
```

**`close()` 必须强制解除所有 PAUSED 请求**：丢弃待发送 chunk、取消该请求、发出
`OnDone(CANCELLED)`。关闭过程**不得依赖 Kotlin 再调用 `resume()`** —— 否则一个
被暂停又不再消费的请求会让 `free()` 永久阻塞。

`send()` 与 `close()` 竞争的边界冻结为二选一，不存在中间态：要么同步返回
`HYPER4K_STATUS_CLIENT_CLOSED`（请求从未被接受，不会有任何回调），要么被接受并
保证恰好一次 `OnDone`。

线程契约：

- `send` / `cancel` / `resume` / `close` 可从**任意线程**并发调用，**包括回调
  线程**。这四个都是可重入、非阻塞的。
- `free` 需要**独占所有权**，不得与其他 API 并发；**MUST NOT 从回调线程调用**。
- `free(NULL)` 与重复 `free` **不承诺安全**。Kotlin wrapper 负责保证只调用一次。
- 每个请求的 `user_data` 在**该请求的 `OnDone` 返回后**即可释放。
- 回调必须快速返回、不得阻塞、**不得调用 `free`**。

> 上一版禁止从回调线程调用 `cancel`，同时又要求 Kotlin 把回调里的异常转成取消 ——
> 这两条无法同时成立。现在 `cancel` 明确是回调线程安全的，矛盾消除；而回调返回值
> （§2.5）提供了不依赖 `cancel` 的第二条退出路径。

### 2.4 错误描述符

`OnDone` 只给一个整数无法运维 —— "为什么连不上"和"证书哪里不对"是两个问题。
定义在回调之前，回调签名才能引用它。

```c
/* 数值全部显式冻结，不依赖 C enum 自动递增 —— 中间插入一个成员就会
   平移后面所有值，而这些数字已经跨语言发出去了。 */
typedef int32_t Hyper4kErrorKind;

#define HYPER4K_ERR_NONE             ((Hyper4kErrorKind)  0)
#define HYPER4K_ERR_DNS              ((Hyper4kErrorKind)  1)
#define HYPER4K_ERR_CONNECT          ((Hyper4kErrorKind)  2)
#define HYPER4K_ERR_TLS_CA           ((Hyper4kErrorKind)  3)
#define HYPER4K_ERR_TLS_HOSTNAME     ((Hyper4kErrorKind)  4)
#define HYPER4K_ERR_TLS_EXPIRED      ((Hyper4kErrorKind)  5)
#define HYPER4K_ERR_TLS_OTHER        ((Hyper4kErrorKind)  6)
#define HYPER4K_ERR_ALPN_NO_H2       ((Hyper4kErrorKind)  7)
#define HYPER4K_ERR_PROTOCOL         ((Hyper4kErrorKind)  8)
#define HYPER4K_ERR_TIMEOUT          ((Hyper4kErrorKind)  9)
#define HYPER4K_ERR_IDLE_TIMEOUT     ((Hyper4kErrorKind) 10)
#define HYPER4K_ERR_CANCELLED        ((Hyper4kErrorKind) 11)  /* 含 close() 导致的取消 */
#define HYPER4K_ERR_TRUNCATED        ((Hyper4kErrorKind) 12)  /* 响应已开始但未完整收到，见 §四 */
#define HYPER4K_ERR_OUTCOME_UNKNOWN  ((Hyper4kErrorKind) 13)  /* 见 §四，重试判定的唯一依据 */
```

上一版的 `HYPER4K_ERR_CLIENT_CLOSED` 已删除：提交期关闭走同步
`HYPER4K_STATUS_CLIENT_CLOSED`，已接受请求被 `close()` 终止时走
`HYPER4K_ERR_CANCELLED`，它没有任何产生路径。

```c

typedef struct {
    int32_t      kind;          /* Hyper4kErrorKind，稳定分类 */
    uint32_t     protocol_code; /* 如 HTTP/2 错误码；无则 0 */
    Hyper4kSlice message;       /* 借用的诊断文本，仅供日志，不得用于分支判断 */
} Hyper4kError;
```

**HTTP 4xx/5xx 是正常响应,不是错误**：走 `OnHeaders` + `OnDone(NULL)`。只有传输
层与协议层失败才进 error。

**Trailers 本版不支持**，收到即忽略。

### 2.5 回调

三种回调集中定义在 `send` 之前，`send` 才能引用它们。

```c
/* 借用期：所有切片仅在回调体内有效，返回后即失效；需要留存必须复制。
   线程：同一 request_id 严格串行；不同 request_id 可并发，
        Kotlin 侧回调实现必须线程安全。
   异常：不得跨越 C ABI —— crate 是 panic = "abort"，穿过去就是 UB。
        Kotlin wrapper 在边界捕获后，返回 CANCEL 即可终止该请求。 */

/* 两种动作类型分开：headers 阶段没有"暂停下一块"的语义，
   合成一个枚举会留下一个未定义的 OnHeaders+PAUSE 组合。 */
typedef int32_t Hyper4kHeadersAction;
#define HYPER4K_HEADERS_CONTINUE ((Hyper4kHeadersAction) 0)
#define HYPER4K_HEADERS_CANCEL   ((Hyper4kHeadersAction) 2)

typedef int32_t Hyper4kChunkAction;
#define HYPER4K_CHUNK_CONTINUE   ((Hyper4kChunkAction) 0)
#define HYPER4K_CHUNK_PAUSE      ((Hyper4kChunkAction) 1)
#define HYPER4K_CHUNK_CANCEL     ((Hyper4kChunkAction) 2)

/* version: 1 = HTTP/1.1, 2 = HTTP/2。AUTO 协商下调用方靠它观测实际结果。 */
typedef Hyper4kHeadersAction (*Hyper4kOnHeaders)(void *ud, uint64_t request_id,
                                                 uint16_t status, uint8_t version,
                                                 const Hyper4kHeader *headers,
                                                 size_t header_count);

typedef Hyper4kChunkAction (*Hyper4kOnChunk)(void *ud, uint64_t request_id,
                                             const uint8_t *ptr, size_t len);

typedef void (*Hyper4kOnDone)(void *ud, uint64_t request_id,
                              const Hyper4kError *error);  /* NULL = 成功 */

/* 恢复被 PAUSE 的响应体。幂等、回调线程安全、非阻塞。 */
Hyper4kStatus hyper4k_client_resume(Hyper4kClient *client, uint64_t request_id);
```

**背压靠回调返回值,不靠队列容量。** `void` 返回的回调无法把"消费不过来"告诉
Rust —— 阻塞它会占住 bridge worker，立即返回又会在 Kotlin 侧堆出无界缓存。

`PAUSE` 的语义**固定为「当前 chunk 已消费，暂停下一块」**。恢复后**不得重发当前
chunk** —— 否则调用方必须自己去重，那等于把背压的复杂度推给了每一个使用者。

**`resume()` 早于 PAUSE 落地不得丢失唤醒，但 permit 只属于当前 chunk。** `PAUSE`
从回调返回到 Rust 真正挂起之间有一个窗口，Kotlin 可能已经消费完并调用
`resume()`。丢失唤醒会让 stream 永久卡死，且只在高吞吐下偶发；而 permit 若被留到
以后，又会错误解除未来某次暂停。两种失败都必须在设计层排除，语义冻结为：

| `resume()` 调用时机 | 返回 | permit 处理 |
|---|---|---|
| 当前 chunk 回调仍在执行中 | `HYPER4K_STATUS_OK` | 记下 permit，**只属于本次 chunk** |
| 该回调随后返回 `PAUSE` | —— | **立即消费 permit**，不真正挂起 |
| 该回调随后返回 `CONTINUE` / `CANCEL` | —— | **丢弃 permit**，绝不影响以后任何 chunk |
| 请求确实处于暂停态 | `HYPER4K_STATUS_OK` | 解除挂起 |
| 既无回调执行中，也未暂停 | `HYPER4K_STATUS_NOT_PAUSED` | —— |

**每个 request 一条独立的有界队列**，不是每个 client 一条。共用一条时慢 stream 会
堵住同连接的其他 stream。

但"每请求一条有界队列"**还不足以**保证隔离，必须同时约束三件事：

- **client 级总内存上限。** 每请求队列各自有界，N 个请求叠加仍可无界增长。超过
  上限时拒绝新请求（`HYPER4K_STATUS_RESOURCE_EXHAUSTED` —— 主动限流，不是 `OOM`），
  而不是继续吃内存。
- **bridge executor 公平调度。** 就绪的请求轮转投递，不能让一个高吞吐 stream 饿死
  其他 stream。
- **HTTP/2 connection window 预留不变式。** 大量 PAUSED stream 会把**连接级**窗口
  耗尽，届时连未暂停的 stream 也读不动 —— 这是 stream 级流控挡不住的。仅仅把
  "后续新请求"切到新连接**不够**：已经在这条连接上的活跃流照样被拖死。必须维持

  > connection window ≥ (每连接允许的 PAUSED stream 上限 × 单流最大占用)
  >                     + 活跃流的保留容量

  实现据此限制每连接的 PAUSED stream 数；达到上限时新请求才改用新连接。

`OnDone` 对每个**已被接受**的请求**恰好一次**；其后不再有该 id 的任何回调。

### 2.6 发起与取消

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
    /* UINT64_MAX = 继承 client 缺省；0 = 显式禁用；正数 = 覆盖。
       上一版用 0 表示继承，导致 SSE 无法显式关掉空闲超时。 */
    uint64_t             read_idle_timeout_ms;
} Hyper4kClientRequest;

/* 必须提供：零初始化的 request 会让 read_idle_timeout_ms = 0，
   在不知情的情况下把"继承 client 缺省"变成"禁用空闲超时"。
   至少设置 abi_version、struct_size、read_idle_timeout_ms = UINT64_MAX。
   struct_size 语义同 hyper4k_client_options_init。 */
Hyper4kStatus hyper4k_client_request_init(Hyper4kClientRequest *request,
                                          uint32_t struct_size);
```

request 同样有**最小合法 `struct_size`**（到 `url` 为止），小于即
`HYPER4K_STATUS_STRUCT_SIZE`。

**NULL 规则**（违反一律 `HYPER4K_STATUS_INVALID_ARG`）：

| 参数 | 可否为 NULL |
|---|---|
| `client` / `request` / `out_request_id` | 否 |
| `on_headers` / `on_done` | 否 |
| `on_chunk` | **可以** —— 表示丢弃响应体，仅要状态与 headers |
| `user_data` | 可以，原样回传 |
| `headers`（`header_count = 0` 时） | 可以 |
| `body_ptr`（`body_len = 0` 时） | 可以 |

```c

/* 已复制 method / url / headers / body 全部输入切片 —— 返回后调用方缓冲即可释放。

   回调时序契约（callee 能真正保证的范围）：
     1. *out_request_id 在该请求可能产生任何事件之前写好；
     2. 回调**不会在调用 send() 的线程上同步重入**；
     3. 回调**可以**在另一线程上与 send() 的返回边界并发发生。
   早先写的"返回前绝不触发任何回调"做不到：spawn 出去的任务可能在 send() 尚未
   返回时就在别的 worker 上跑起来，而这种绝对时序无法由被调方单方面证明。 */
Hyper4kStatus hyper4k_client_send(Hyper4kClient *client,
                                  const Hyper4kClientRequest *request,
                                  Hyper4kOnHeaders on_headers,
                                  Hyper4kOnChunk on_chunk,
                                  Hyper4kOnDone on_done,
                                  void *user_data,
                                  uint64_t *out_request_id);

/* 幂等、回调线程安全、非阻塞。
   取消胜出时，该请求仍会收到一次 OnDone(CANCELLED)。 */
Hyper4kStatus hyper4k_client_cancel(Hyper4kClient *client, uint64_t request_id);
```

**URL scheme 与 `HTTP2_REQUIRED` 的组合**：

| scheme | `HTTP2_REQUIRED` | 行为 |
|---|---|---|
| `https://` | 否 | ALPN 通告 `h2` + `http/1.1`，按协商结果 |
| `https://` | 是 | ALPN 未得到 `h2` 即失败，不降级 |
| `http://` | 否 | HTTP/1.1 |
| `http://` | 是 | 提交即失败（`HYPER4K_STATUS_UNSUPPORTED`） |

**超时分两级**：`request_timeout_ms` 是整个请求（含全部重试）的上限，**不因重试
而重置**；`read_idle_timeout_ms` 是块间空闲上限，**每收到一个 body chunk 就重置**。

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

**自动重试只允许发生在响应尚未提交给调用方之前。**

判据**不是**"回调是否已执行" —— headers 可能已经进入 bridge 队列但还没进 Kotlin，
在那个窗口里重试，旧连接排队中的回调随后仍会送达，调用方就会收到两份 headers。

冻结为：

- 每个请求持有一个**不可逆**的 `response_committed` 标志，在**第一份响应数据进入
  bridge 队列之前**置位（不是在回调返回之后）。
- 每次尝试带一个 **generation**。重试时 generation 递增，**旧 generation 的排队
  回调一律作废**，不再投递给 Kotlin。
- 透明重试期间 **`request_id` 保持不变** —— 对调用方而言始终是同一个请求。

| `response_committed` | 连接中断时的处理 |
|---|---|
| 否 | 按上面两张表判定：可重试则透明重试，否则 `OUTCOME_UNKNOWN` |
| 是 | **一律不重试**，返回 `HYPER4K_ERR_TRUNCATED` |

`TRUNCATED` 与 `OUTCOME_UNKNOWN` 是两件事：前者请求确定已被处理、只是响应没收完；
后者连是否被处理都不知道。调用方对这两种情况的决策完全不同。

**自动重试次数上限**由 `Hyper4kClientOptions.max_retries` 给出（额外次数，缺省
2，即最多三次尝试）。连续 `REFUSED_STREAM` 或反复 GOAWAY 的服务端会让无上限的
重试变成活锁，把一次故障放大成持续压测。

`request_timeout_ms` **覆盖全部重试**，不因每次重试而重置 —— 否则"总超时"就不是
上限，一个反复失败的请求能挂到 `max_retries × timeout`。

> **非规范性实现注记（不是 ABI 契约的一部分）**
>
> 当前 Hyper 1.11 参考实现使用低层 `SendRequest::try_send_request()` 与
> `TrySendError::take_message()` 判断请求是否**确定未发送**，**不自行解析 GOAWAY
> 的 `last_stream_id`** —— hyper 的 h2 层已经算好了这个边界。
>
> - `Some(request)`：请求未完成序列化到连接，可证明未发送，任何方法都可透明重试。
> - `None`：**不等于请求已执行**，只表示无法证明未发送。此时按本节规则处理。
> - `response_committed = true`：无论方法与错误类型，一律禁止重试，返回 `TRUNCATED`。
>
> 记在这里是为了防止后来者照字面去实现帧解析；但语义由本节定义，不绑死在任何一个
> Hyper API 名称上。换实现只要满足同样的判定即可。

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
10. 回调时序契约：`*out_request_id` 先于任何事件写好；回调不在 `send()` 调用线程
    同步重入（构造一个在回调里断言"当前线程不是 send 线程"的用例）
11. 错误分类可区分 DNS / 连接 / CA / hostname / 过期 / ALPN / 超时 / 取消
12. HTTP 4xx/5xx 走成功路径（`OnDone(NULL)`），不进 error
13. `cancel` 三态正确，且取消胜出后仍收到一次 `OnDone(CANCELLED)`
14. `custom_ca_pem` 追加与替换两种模式行为符合定义
15. 未知 `flags` 位被拒绝；`struct_size` 大于/小于当前版本的兼容规则生效
16. 背压闭环：`OnChunk` 返回 `PAUSE` 后 Rust 停止 poll 该 body，对端 HTTP/2 流控
    窗口收敛；`resume` 后继续。慢 stream 不影响同连接其他 stream
17. `OnChunk` 返回 `CANCEL` 等价于取消，仍收到一次 `OnDone(CANCELLED)`
18. 响应已可见后连接中断：**不重放**，返回 `TRUNCATED`（与 `OUTCOME_UNKNOWN`
    可区分）
19. `request_timeout_ms` 覆盖全部重试、不随重试重置；`read_idle_timeout_ms` 每收
    到一个 chunk 重置；两者为 0 时的语义符合定义
20. `http://` + `HTTP2_REQUIRED` 提交即失败
21. 四个平台编译通过
22. **系统根证书在 macOS 与 Linux 上各做一次真实公网 HTTPS 运行测试** —— staticlib
    编译通过只证明链接得上，不证明运行时能正确加载平台信任根
23. `PAUSE` 后 `resume` 不重发当前 chunk
24. **丢失唤醒**：`resume()` 在 PAUSE 落地之前到达时不得丢失，stream 必须继续；
    高并发下反复 pause/resume 不出现永久卡死
25. `close()` 能解除 PAUSED 请求并使其收到 `OnDone(CANCELLED)`；被暂停且不再消费
    的请求**不会让 `free()` 阻塞**
26. `send()` 与 `close()` 并发：结果只能是同步 `CLIENT_CLOSED` 或恰好一次
    `OnDone`，不存在既不返回错误也无回调的中间态
27. 从**回调线程**调用 `cancel` / `resume` / `close` 不死锁
28. 重试边界的两个场景必须分别验证，结论相反：
    - **尚无任何响应数据入队时断线** → 透明重试，`request_id` 不变，旧 generation
      的传输事件不得泄漏给 Kotlin
    - **headers 已入队但尚未投递时断线** → **不重试**；那份 headers 也不投递；
      最终只有一次 `OnDone(TRUNCATED)`
29. `Hyper4kStatus` 各值与文档一致；`struct_size` 小于最小合法值被拒绝
30. `hyper4k_client_options_init()` 填出的默认值可区分于零初始化
31. `read_idle_timeout_ms` 三种取值（继承 / 禁用 / 覆盖）行为符合定义
32. `OnHeaders` 报告的 `version` 与实际协商结果一致
33. permit 归属：提前 `resume()` 在回调返回 `CONTINUE` / `CANCEL` 后被丢弃，
    **不影响以后任何一次 pause**；返回 `PAUSE` 时被立即消费
34. `hyper4k_client_request_init()` 填出的默认可区分于零初始化；request 的
    最小 `struct_size` 被强制
35. `on_chunk = NULL` 时正常收到 headers 与 `OnDone`，响应体被丢弃
36. 大量 PAUSED stream 不会耗尽连接级窗口而拖死同连接的活跃 stream
37. client 级总内存上限生效：超限时新请求得到 `HYPER4K_STATUS_RESOURCE_EXHAUSTED`，
    进程不 OOM
38. `Hyper4kErrorKind` 各值与文档一致（跨语言常量比对，不靠自动递增）
39. **跨版本结构体兼容双向验证**：
    - 旧的小结构体调用新库：`*_init(ptr, sizeof(old_struct))` 只写前缀，不越界
    - 新的大结构体调用旧库：尾部字段保持调用方预置值，旧库不误读
40. `HYPER4K_STATUS_RESOURCE_EXHAUSTED` 与 `HYPER4K_STATUS_OOM` 分别可触发且不混淆
41. 连接窗口预留不变式成立：把每连接 PAUSED stream 加到上限后，**同连接上的活跃
    stream 仍能正常读取**（仅验证"新请求走新连接"不足以证明这一点）
42. `connect_timeout_ms = 0` 行为是禁用而非默认值或立即超时
43. 所有跨 ABI 类型宽度为 4 字节：`static_assert(sizeof(Hyper4kStatus) == 4)` 等，
    Rust 与 Kotlin 侧各断言一次

APNs 真凭据 smoke 作为**可选外部测试**，不进普通单测。

每条「必须失败」的用例都要做反向验证：先确认它在正确配置下通过，再确认错误配置
下确实失败。否则一个永远返回错误的实现也能让这些用例全绿。
