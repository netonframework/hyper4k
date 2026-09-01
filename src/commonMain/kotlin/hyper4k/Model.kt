package hyper4k

/**
 * 一次进站请求的高层视图。
 *
 * 注意：这些字段在构造时已从 Rust 的借用切片**拷贝**进 Kotlin 堆，
 * 因此可安全地跨线程、跨协程持有（异步处理也没问题）。
 */
class Hyper4kRequest(
    val method: String,
    val path: String,
    val query: String,
    /** 原始头块字节，"Name: Value\n" 串联。惰性解析见 [headers]。 */
    val rawHeaderBytes: ByteArray,
    val body: ByteArray,
) {
    /** 兼容旧构造：文本头块会自动转成字节存储。 */
    constructor(
        method: String,
        path: String,
        query: String,
        rawHeaders: String,
        body: ByteArray,
    ) : this(method, path, query, rawHeaders.encodeToByteArray(), body)

    /** 原始头块文本（从字节惰性解码）。 */
    val rawHeaders: String by lazy(LazyThreadSafetyMode.NONE) { rawHeaderBytes.decodeToString() }

    /** 解析后的请求头（大小写按原样保留；查找见 [header]）。 */
    val headers: Map<String, List<String>> by lazy(LazyThreadSafetyMode.NONE) {
        if (rawHeaderBytes.isEmpty()) emptyMap() else buildMap<String, MutableList<String>> {
            // 直接从字节解码，避免先造 String 再 split 的二次分配。
            for (line in rawHeaderBytes.decodeToString().split('\n')) {
                if (line.isEmpty()) continue
                val i = line.indexOf(':')
                if (i <= 0) continue
                val name = line.substring(0, i).trim()
                val key = keys.firstOrNull { it.equals(name, ignoreCase = true) } ?: name
                getOrPut(key) { mutableListOf() }.add(line.substring(i + 1).trim())
            }
        }.mapValues { it.value.toList() }
    }

    /** 大小写不敏感取头。 */
    fun header(name: String): String? =
        headers.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value?.firstOrNull()
}

/**
 * 一次响应。[headers] 会被编码回 "Name: Value\n" 文本块。
 *
 * [streamed] 为 true 表示响应体已经由 [Hyper4kResponseChannel] 直接写给引擎了，
 * 引擎不再写出这个对象——它只是一个「我已经自己应答完了」的回执。
 */
class Hyper4kResponse(
    val status: Int,
    val headers: Map<String, List<String>> = emptyMap(),
    val body: ByteArray = EMPTY,
    val streamed: Boolean = false,
) {
    companion object {
        private val EMPTY = ByteArray(0)

        /** handler 走完流式路径后的回执，见 [Hyper4kResponseChannel]。 */
        fun streamed(status: Int = 200) = Hyper4kResponse(status, streamed = true)

        fun text(status: Int = 200, body: String, contentType: String = "text/plain; charset=utf-8") =
            Hyper4kResponse(status, mapOf("Content-Type" to listOf(contentType)), body.encodeToByteArray())

        fun json(status: Int = 200, body: String) =
            Hyper4kResponse(status, mapOf("Content-Type" to listOf("application/json")), body.encodeToByteArray())

        fun bytes(status: Int = 200, body: ByteArray, contentType: String = "application/octet-stream") =
            Hyper4kResponse(status, mapOf("Content-Type" to listOf(contentType)), body)
    }
}

/**
 * 流式响应的下行通道：先发头，再分块发体，最后收尾（ABI v3）。
 *
 * 与「返回一个 [Hyper4kResponse]」互斥：一旦调用了 [begin]，handler 必须自己
 * 用 [finish] 收尾，并返回 [Hyper4kResponse.streamed] 作为回执。
 *
 * [write] 带背压——客户端读得慢时它会等。实现保证这个等待不会发生在引擎线程上。
 */
interface Hyper4kResponseChannel {
    /** 是否已经进入流式（调用过 [begin]）。 */
    val isStreaming: Boolean

    /** 已写出的 body 字节数。 */
    val bytesWritten: Long

    /**
     * 立即发出状态行与响应头，body 随后由 [write] 供给。
     *
     * 不要自己设置 Content-Length：长度由引擎按协议表达
     * （HTTP/1.1 chunked 或 HTTP/2 DATA 帧）。
     */
    suspend fun begin(status: Int, headers: Map<String, List<String>> = emptyMap())

    /**
     * 写出一块 body。返回 false 表示**客户端已断开**：停止产生数据并 [finish] 收尾。
     * 这是正常路径（SSE 客户端关页面就走这里），不是错误。
     */
    suspend fun write(chunk: ByteArray): Boolean

    /** 收尾并释放。幂等：重复调用无副作用。 */
    suspend fun finish()
}

/** 异步请求处理器。Hyper4k 会在受管 Kotlin 协程中执行它。 */
typealias Hyper4kHandler = suspend (Hyper4kRequest) -> Hyper4kResponse

/**
 * 可流式的请求处理器。
 *
 * 两种收尾方式二选一：返回一个普通 [Hyper4kResponse]（引擎负责写出），
 * 或者用 [channel] 自己流式写出、然后返回 [Hyper4kResponse.streamed]。
 */
typealias Hyper4kStreamingHandler =
    suspend (request: Hyper4kRequest, channel: Hyper4kResponseChannel) -> Hyper4kResponse
