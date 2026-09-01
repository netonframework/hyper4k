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
 * [streamed] is true when the body already went to the engine through a
 * [Hyper4kResponseChannel]; the engine will not write this object out again.
 */
class Hyper4kResponse(
    val status: Int,
    val headers: Map<String, List<String>> = emptyMap(),
    val body: ByteArray = EMPTY,
    val streamed: Boolean = false,
) {
    companion object {
        private val EMPTY = ByteArray(0)

        /** Receipt for a handler that answered through a [Hyper4kResponseChannel]. */
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
 * Downstream channel for a streaming response: headers first, then body chunks,
 * then close (ABI v3).
 *
 * Mutually exclusive with returning a [Hyper4kResponse]: once [begin] is called
 * the handler closes the stream itself with [finish] and returns
 * [Hyper4kResponse.streamed] as its receipt.
 *
 * [write] carries backpressure and waits while the client reads slowly.
 * Implementations keep that wait off the engine threads.
 */
interface Hyper4kResponseChannel {
    /** Whether the response has entered streaming, that is [begin] was called. */
    val isStreaming: Boolean

    /** Body bytes written so far. */
    val bytesWritten: Long

    /**
     * Sends the status line and headers now; the body follows through [write].
     *
     * Do not set Content-Length: the engine expresses the length per protocol,
     * as HTTP/1.1 chunked or HTTP/2 DATA frames.
     */
    suspend fun begin(status: Int, headers: Map<String, List<String>> = emptyMap())

    /**
     * Writes one body chunk. false means the client is gone: stop producing data
     * and close with [finish]. That is a normal path, not an error.
     */
    suspend fun write(chunk: ByteArray): Boolean

    /** Closes and releases the response. Idempotent. */
    suspend fun finish()
}

/** 异步请求处理器。Hyper4k 会在受管 Kotlin 协程中执行它。 */
typealias Hyper4kHandler = suspend (Hyper4kRequest) -> Hyper4kResponse

/**
 * A request handler that may stream its response body.
 *
 * Two ways to finish, pick one: return an ordinary [Hyper4kResponse] and let the
 * engine write it, or stream through [channel] and return
 * [Hyper4kResponse.streamed].
 */
typealias Hyper4kStreamingHandler =
    suspend (request: Hyper4kRequest, channel: Hyper4kResponseChannel) -> Hyper4kResponse
