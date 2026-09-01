# hyper4k ABI v3：流式响应与 h2c

> 状态：**Draft**，待评审后实现。
>
> 目标：补齐 `STREAMING_RESPONSE` 与 `HTTP_2` 两项引擎能力
> （Neton [HTTP 引擎能力规范](../../neton-docs/docs/zh-hans/spec/http-engine-capabilities.md)
> §3 现状矩阵里 hyper4k 仅有 `ASYNC_HANDOFF`）。
>
> 前置结论（已定，不再讨论）：
>
> - **只做 h2c，不做 TLS/ALPN。** RPC 走内网明文，证书由 nginx 前置代理终止。
>   引擎自己持证书是另一件事，不与 h2 支持绑定。
> - **HTTP/2 不是流式的前置条件**，反之亦然。两项独立发布、独立回滚。

---

## 一、为什么必须动 C ABI

现有 ABI 只有一次性应答：

```c
/* 每个 responder 必须且只能调用一次。 */
int32_t hyper4k_respond(Hyper4kResponder responder, uint16_t status,
                        const uint8_t *headers_ptr, size_t headers_len,
                        const uint8_t *body_ptr, size_t body_len);
```

`BufferedHttpDispatcher.dispatch(request, liveResponse: HttpResponse?)` 的流式钩子
**已经在共享层就位**——Ktor 传 `KtorLiveResponse`，hyper4k 传 `null`。
所以 Kotlin 侧要做的只是实现一个 live response；但它需要一个「先发头、再分多次发体、最后收尾」
的下行通道，现有 ABI 给不了。**这是本设计存在的唯一理由**：不是重构，是补一条通道。

---

## 二、ABI v3 新增函数

保持 v2 全部语义不变，新增三个函数。三者与 `hyper4k_respond` **互斥**：
一个 responder 要么走一次性应答，要么走流式，不能混用。

```c
/*
 * 开始一个流式响应：立即发出状态行与响应头，body 随后分块写出。
 *
 * 调用后 responder 进入流式状态，hyper4k_respond 对它失效（返回
 * HYPER4K_ERR_WRONG_STATE）。必须以 hyper4k_response_finish 收尾，
 * 否则连接会在 drop 时被判为异常终止。
 *
 * headers 编码同 v2：每行 "Name: Value\n"。
 * 不要自己设置 Content-Length —— 流式响应由引擎按协议选择
 * chunked(HTTP/1.1) 或 DATA 帧(HTTP/2)。
 */
int32_t hyper4k_response_begin(Hyper4kResponder responder,
                               uint16_t status,
                               const uint8_t *headers_ptr, size_t headers_len);

/*
 * 写出一个 body 块。数据在本调用内被拷贝，返回后调用方缓冲即可释放。
 *
 * **背压**：客户端读得慢时本函数会阻塞调用线程直到下游可写。
 * Kotlin 侧 MUST 在能安全阻塞的调度器上调用它（见 §四），
 * 绝不可在 Tokio 回调线程上调用 —— 那会把 v2 好不容易解决的
 * 「阻塞引擎 worker」问题以更隐蔽的形式带回来。
 *
 * 返回 HYPER4K_ERR_CLIENT_GONE 表示客户端已断开：调用方应停止产生数据
 * 并调用 finish 收尾，**不是**错误路径，SSE 客户端关页面就是这个码。
 */
int32_t hyper4k_response_write(Hyper4kResponder responder,
                               const uint8_t *chunk_ptr, size_t chunk_len);

/*
 * 结束流式响应并释放 responder。之后该 responder 失效。
 *
 * 幂等：重复调用返回 HYPER4K_ERR_WRONG_STATE 而不是 UB。
 */
int32_t hyper4k_response_finish(Hyper4kResponder responder);
```

错误码补充：

```c
#define HYPER4K_ERR_WRONG_STATE   (-4)  /* responder 状态不允许该操作 */
#define HYPER4K_ERR_CLIENT_GONE   (-5)  /* 客户端已断开，停止写入并 finish */
```

---

## 三、Rust 侧实现要点

### 3.1 responder 状态机

`Responder` 内部从「持有一个 oneshot::Sender」扩展为：

```
Idle ──hyper4k_respond──────────────► Done
  │
  └───hyper4k_response_begin──► Streaming ──write*──► Streaming
                                     │
                                     └──finish──► Done
```

状态转换 MUST 由 token 校验守护。**沿用 v2 已有的单次数字 token 模型**，
不要把 Rust 裸指针跨 FFI 交给 Kotlin —— 迟到的写入（超时后、停止后、
客户端断开后）必须安全失败，而不是访问已释放内存。

### 3.2 body 通道

`hyper::body::Body` 换成由 `mpsc::channel` 驱动的流。`hyper4k_response_write`
往 channel 发送并**阻塞等待容量**——这就是背压的来源，也是 §二里
「MUST 在可阻塞调度器上调用」的原因。

channel 容量取小值（建议 2~4 个块）：容量大只是把内存堆在 Rust 侧，
并不能让慢客户端变快，反而掩盖背压。

### 3.3 h2c

accept 循环：

```rust
// 现状（lib/src/lib.rs 约 227 行）
hyper::server::conn::http1::Builder::new()

// 改为
hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
```

`auto::Builder` 按连接首部 preface 自动识别 h1 / h2c，**单端口同时服务两种协议**，
客户端不需要预先知道服务端说什么。

`hyper` 依赖已带 `features = ["http1", "http2", "server"]`，只需补 `hyper-util`
的 `server-auto` feature。

> 不做 ALPN：那需要引擎持有证书。TLS 由 nginx 终止（已定）。

---

## 四、Kotlin 侧（neton-http-hyper4k）

### 4.1 live response

实现 `Hyper4kLiveResponse : HttpResponse`，把 `HttpBodyWriter.writeChunk`
映射到 `hyper4k_response_write`，并在 dispatch 结束时 `finish`。
然后 `dispatch` 从传 `null` 改为传它：

```kotlin
dispatcher.dispatch(request.toBuffered(), liveResponse = Hyper4kLiveResponse(responder))
```

### 4.2 调度器约束（关键）

`hyper4k_response_write` 会阻塞。当前 handler 跑在
`scope.launch(start = CoroutineStart.UNDISPATCHED)` 上——**UNDISPATCHED 意味着
不挂起的 handler 直接在引擎线程内联执行**，此时调用 write 就会阻塞 Tokio worker。

所以：**一旦 handler 进入流式（调用了 begin），后续 write MUST 切到可阻塞的调度器**。
UNDISPATCHED 的快路径优化只对「不流式、不挂起」的 handler 成立，
流式路径必须显式跳出来。

> 这条是本设计里最容易写错、且错了之后只在高并发流式场景才暴露的地方。
> 一致性测试要专门覆盖：并发 N 个 SSE 连接时，非流式请求的延迟不应劣化。

### 4.3 能力声明

**先让一致性测试通过，再加声明**（能力规范 §6）：

```kotlin
override val capabilities = setOf(
    HttpCapability.ASYNC_HANDOFF,
    HttpCapability.STREAMING_RESPONSE,   // 4.1 + 4.2 完成且测试通过后才加
    HttpCapability.HTTP_2,               // 3.3 完成且 h2c 测试通过后才加
)
```

---

## 五、privchat-application 切引擎

顺序不可颠倒：

1. `settings.gradle.kts` 加 `includeBuild("../../Neton/hyper4k")` 与
   `includeBuild("../../Neton/neton-http-hyper4k")`
2. 启动处 `.http(::Hyper4kHttpAdapter)`
3. **切换前必须确认**：application 用到的能力都已被 hyper4k 声明。
   启动期校验会自己挡住（这正是能力模型的用途），但提前对一遍能省一次失败启动
4. 回归：privchat-godot-demo 的 8 条 e2e 全绿

> 阻塞项：`neton-http-hyper4k` 当前 composite build 的 klib 解析是坏的
> （`Could not find .../neton-http/build/classes/kotlin/macosArm64/main/klib/neton-http`），
> 与本设计无关，但不修则这一层动不了。

---

## 六、验收

1. 一致性套件在 Ktor CIO 与 hyper4k 上跑同一份断言，流式用例两边都真跑（不是 skipped）
2. SSE：客户端在收到第 1 个事件时，服务端**尚未**发出最后一个事件
   （这是「真流式 vs 缓冲」的唯一可靠判据）
3. 客户端中途断开 → `HYPER4K_ERR_CLIENT_GONE`，服务端不泄漏 responder
4. h2c：`curl --http2-prior-knowledge` 完成一次完整请求-响应
5. 并发 100 条 SSE 时，普通请求 p99 延迟不劣化（验证 §4.2 没写错）
6. `hyper4k_respond` 与流式三函数混用 → `HYPER4K_ERR_WRONG_STATE`，不 UB
