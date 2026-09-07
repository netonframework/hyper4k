# Neton beta10：请求热路径优化记录

这批改动尚未发布，也尚未在 HttpArena 的 Linux 64 核环境验收。减少分配是代码层面的事实，不等于已经恢复 beta7 吞吐；不得据此填写性能提升百分比。

## 当前候选改动

| 改动 | 消除的开销 | 保留的行为 |
| --- | --- | --- |
| Claude 已有的 ASCII 响应头精确计长 | 非空 ASCII 响应头块末尾的第二次数组分配及整块复制 | 多值头、空值、非 ASCII 回退 |
| ASCII 请求头名直接比较 String | 未物化头 map 时，每次查询的临时头名 ByteArray | 大小写不敏感、首值、缺失返回 null、非 ASCII 快照兼容 |
| Rust 借用已解析的 method/path/query | 三次 to_owned 调用；通常是 method/path 两次堆分配，有非空 query 时再省一次 | C ABI、Kotlin 自有快照、同步和异步响应、请求数据生命周期 |

未改动 Tokio 调度、GC 配置、安全管线、超时、响应注册表或同步槽位协议。当前改动可以独立回退，便于归因。

## 本地验证

在 hyper4k/lib 下运行 `cargo test --locked --lib`；在 hyper4k 下运行 `./gradlew macosArm64Test`。

在 neton 下运行 `./gradlew -Phyper4k.local=true :neton-http-hyper4k:macosArm64Test`，明确使用本地引擎。若默认依赖仍为已发布的 hyper4k 0.6.0，不加该开关就不会包含这批优化。正式发布必须先发布包含改动的引擎新版本，再更新 Neton 依赖并验证发布产物，不能仅凭本地源码替换成功发布 beta10。

回归覆盖请求头查找、编码长度、真实 HTTP/1.1 POST/GET 字段传递及异步快照响应；现有套件继续覆盖 HTTP/2、TLS、流式响应和断连。

本轮 macOS arm64 验证结果：Rust 119 项、hyper4k Kotlin/Native 39 项、Neton 引擎适配联调 32 项均通过。最终联调使用 `--rerun-tasks` 重建，避免读取旧的互操作产物。`git diff --check` 通过；全库 `cargo fmt --check` 仍报告既有格式差异，本轮没有批量重排无关文件。

## Linux 发布前验收

1. 固定编译器、依赖锁、构建配置、CPU 亲和性、worker 数、GC 配置及压测客户端。保存二进制哈希和完整配置。
2. beta7、未修改 beta9、当前候选在同机交替重复；每个正式进程预热后采集同一个稳定时间窗。先测 baseline 长连接，再覆盖 latency-1m、limited-conn、pipelined、baseline-h2c。
3. 记录成功请求数、错误/超时、进程 CPU 时间增量、延迟分布、RSS 和 GC 数据。CPU/请求使用窗口内服务端进程 CPU 秒数增量除以完成请求数，包含该进程的 GC 与其他线程。固定速率负载同时报告实际完成率。
4. 报告逐轮结果及配对差异。恢复 beta7 吞吐及 CPU 成本、且其他关键负载和 p99 不退步后，才把候选作为性能修复发布；不以单轮名次或 macOS 测试通过代替验收。
5. 如果长连接退步仍在，下一步在该 Linux 环境对比 GC/分配、线程负载与 CPU 热点，再隔离 Neton 改动与 hyper4k 版本。不要同时重构 IO、GC 和同步响应注册机制。
