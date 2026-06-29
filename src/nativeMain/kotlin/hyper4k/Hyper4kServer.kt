package hyper4k

import hyper4k.cinterop.*
import hyper4k.cinterop.Hyper4kRequest as CHyper4kRequest
import kotlinx.cinterop.*

/**
 * hyper4k 的高性能 Kotlin/Native 封装。
 *
 * 设计目标：可被任意 Kotlin/Native 项目复用，不依赖 Neton。
 * [HyperHttpAdapter]（在 neton-http 里）只是它的一个消费者。
 *
 * v1 处理模型为**同步**：[start] 的 handler 在 Tokio worker 线程上被直接调用，
 * 返回即写回响应。适合 CPU 型 / echo 型 handler 与压测基准。
 * 若 handler 内部要做阻塞/挂起 I/O，应放大 Tokio worker 数，或等待后续的
 * 异步 handoff 版本（回调入队 Kotlin 协程，完成后再 respond）。
 *
 * 线程：回调发生在 Rust/Tokio 线程上。依赖 Kotlin/Native 新内存模型
 * （Kotlin 1.7.20+，2.x 默认）允许从外部线程进入 Kotlin 运行时。
 */
@OptIn(ExperimentalForeignApi::class)
class Hyper4kServer(
    private val host: String = "0.0.0.0",
    private val port: Int,
) {
    private var server: CPointer<cnames.structs.Hyper4kServer>? = null
    private var handlerRef: StableRef<Hyper4kHandler>? = null

    val isRunning: Boolean get() = server != null

    /** 启动并阻塞地完成绑定。绑定失败（端口占用等）抛 [IllegalStateException]。 */
    fun start(handler: Hyper4kHandler) {
        check(server == null) { "hyper4k server already started" }
        val ref = StableRef.create(handler)
        handlerRef = ref
        val s = hyper4k_server_start(
            host = host,
            port = port.toUShort(),
            on_request = staticCFunction(::onRequest),
            user_data = ref.asCPointer(),
        )
        if (s == null) {
            ref.dispose()
            handlerRef = null
            error("hyper4k_server_start failed for $host:$port (port in use or bind error)")
        }
        server = s
    }

    /** 优雅停止并释放底层资源。 */
    fun stop() {
        server?.let { hyper4k_server_stop(it) }
        server = null
        handlerRef?.dispose()
        handlerRef = null
    }
}

/**
 * C 回调入口。staticCFunction 不能捕获状态，handler 通过 user_data(StableRef) 取回。
 * 请求数据从 Rust 借用切片复制到 Kotlin；响应使用 pinned 缓冲传入 Rust。
 */
@OptIn(ExperimentalForeignApi::class)
private fun onRequest(userData: COpaquePointer?, reqPtr: CPointer<CHyper4kRequest>?) {
    if (userData == null || reqPtr == null) return
    val handler = userData.asStableRef<Hyper4kHandler>().get()
    val c = reqPtr.pointed

    val req = Hyper4kRequest(
        method = c.method.copyToString(),
        path = c.path.copyToString(),
        query = c.query.copyToString(),
        rawHeaders = c.headers.copyToString(),
        body = c.body.copyToByteArray(),
    )

    val resp: Hyper4kResponse = try {
        handler(req)
    } catch (t: Throwable) {
        Hyper4kResponse.text(500, "hyper4k handler error: ${t.message}")
    }

    val headerBytes =
        if (resp.headers.isEmpty()) ByteArray(0)
        else resp.headers.entries
            .joinToString("\n") { "${it.key}: ${it.value}" }
            .encodeToByteArray()

    // pinned -> 把响应缓冲的地址直接交给 Rust，Rust 拷贝一次后即返回。
    headerBytes.usePinned { hp ->
        resp.body.usePinned { bp ->
            hyper4k_respond(
                responder = c.responder,
                status = resp.status.toUShort(),
                headers_ptr = if (headerBytes.isEmpty()) null else hp.addressOf(0).reinterpret(),
                headers_len = headerBytes.size.convert(),
                body_ptr = if (resp.body.isEmpty()) null else bp.addressOf(0).reinterpret(),
                body_len = resp.body.size.convert(),
            )
        }
    }
}

// --- Hyper4kSlice -> Kotlin 拷贝辅助 ---

@OptIn(ExperimentalForeignApi::class)
private fun Hyper4kSlice.copyToByteArray(): ByteArray {
    val n = this.len.toInt()
    val p = this.ptr
    if (n == 0 || p == null) return ByteArray(0)
    return p.readBytes(n)
}

@OptIn(ExperimentalForeignApi::class)
private fun Hyper4kSlice.copyToString(): String = copyToByteArray().decodeToString()
