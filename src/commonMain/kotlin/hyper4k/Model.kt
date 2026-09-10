package hyper4k

/**
 * 一次进站请求的高层视图。
 *
 * 注意：这些字段在构造时已从 Rust 的借用切片**拷贝**进 Kotlin 堆，
 * 因此可安全地跨线程、跨协程持有（异步处理也没问题）。
 */
class Hyper4kRequest internal constructor(
    val method: String,
    val path: String,
    val query: String,
    // 借用的头块原生指针地址（0=已是私有字节）与其长度。见下方惰性物化说明。
    private val headerAddr: Long,
    private var ownedHeaderBytes: ByteArray?,
    private val borrowedLen: Int,
    val body: ByteArray,
) {
    /** 常规构造：调用方拥有的头块字节，直接持有（非借用）。 */
    constructor(
        method: String,
        path: String,
        query: String,
        rawHeaderBytes: ByteArray,
        body: ByteArray,
    ) : this(method, path, query, 0L, rawHeaderBytes, rawHeaderBytes.size, body)

    /** 兼容旧构造：文本头块会自动转成字节存储。 */
    constructor(
        method: String,
        path: String,
        query: String,
        rawHeaders: String,
        body: ByteArray,
    ) : this(method, path, query, rawHeaders.encodeToByteArray(), body)

    // 头块可能来自 Rust 的借用切片（[ownedHeaderBytes]=null）。引擎保证该借用在整个
    // handler 执行期间有效（同步/异步/客户端断连皆然，见 lib.rs REQUEST_BACKINGS），
    // 故真正读头时才拷入私有存储；绝大多数跑分路由不读头，于是零拷贝、零扫描。
    private fun materialized(): ByteArray {
        var b = ownedHeaderBytes
        if (b == null) {
            b = copyBorrowedHeaders(headerAddr, borrowedLen)
            ownedHeaderBytes = b
        }
        return b
    }

    /** 原始头块字节。触发物化。 */
    val rawHeaderBytes: ByteArray get() = materialized()

    /** 原始头块文本。触发物化。 */
    val rawHeaders: String get() = materialized().decodeToString()

    private var headersOrNull: Map<String, List<String>>? = null

    /** 解析后的请求头（大小写按原样保留；查找见 [header]）。 */
    val headers: Map<String, List<String>>
        get() = headersOrNull ?: parseHeaders().also { headersOrNull = it }

    private fun parseHeaders(): Map<String, List<String>> = run {
        if (materialized().isEmpty()) emptyMap() else buildMap<String, MutableList<String>> {
            // 直接从字节解码，避免先造 String 再 split 的二次分配。
            for (line in materialized().decodeToString().split('\n')) {
                if (line.isEmpty()) continue
                val i = line.indexOf(':')
                if (i <= 0) continue
                val name = line.substring(0, i).trim()
                val key = keys.firstOrNull { it.equals(name, ignoreCase = true) } ?: name
                getOrPut(key) { mutableListOf() }.add(line.substring(i + 1).trim())
            }
        }.mapValues { it.value.toList() }
    }

    /**
     * 大小写不敏感取头，不解析整块。
     *
     * 走 [headers] 会为一次查找建出整张 map：一个 HashMap，外加每个头两个 String
     * 和一个 List。分发链在入口就要读一个 `X-Request-Id`，于是每个请求都付了这笔
     * 钱，而绝大多数请求根本不看其余的头。这里直接在原始字节上扫。
     */
    fun header(name: String): String? {
        headersOrNull?.let { parsed ->
            return parsed.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value?.firstOrNull()
        }
        // HTTP field names are ASCII. Compare against the String directly:
        // even a missing X-Request-Id must not allocate a temporary ByteArray.
        // Keep the legacy byte comparison for callers supplying non-ASCII
        // names through the public snapshot constructor.
        val encoded = if (name.any { it.code > 0x7f }) name.encodeToByteArray() else null
        val raw = materialized()
        var lineStart = 0
        while (lineStart < raw.size) {
            var lineEnd = lineStart
            while (lineEnd < raw.size && raw[lineEnd] != NEWLINE) lineEnd++
            val colon = indexOfColon(raw, lineStart, lineEnd)
            if (colon > lineStart && matchesIgnoreCase(raw, lineStart, colon, name, encoded)) {
                var vs = colon + 1
                while (vs < lineEnd && (raw[vs] == SPACE || raw[vs] == TAB)) vs++
                var ve = lineEnd
                while (ve > vs && (raw[ve - 1] == SPACE || raw[ve - 1] == TAB || raw[ve - 1] == CR)) ve--
                return raw.decodeToString(vs, ve)
            }
            lineStart = lineEnd + 1
        }
        return null
    }

    private companion object {
        const val NEWLINE: Byte = 0x0A
        const val CR: Byte = 0x0D
        const val COLON: Byte = 0x3A
        const val SPACE: Byte = 0x20
        const val TAB: Byte = 0x09

        fun indexOfColon(raw: ByteArray, from: Int, to: Int): Int {
            var i = from
            while (i < to) { if (raw[i] == COLON) return i; i++ }
            return -1
        }

        /** ASCII 大小写不敏感比较；头名按 RFC 只能是 ASCII。 */
        fun matchesIgnoreCase(raw: ByteArray, from: Int, to: Int, name: String, encoded: ByteArray?): Boolean {
            val length = encoded?.size ?: name.length
            if (to - from != length) return false
            var i = 0
            while (i < length) {
                val a = lower(raw[from + i])
                val b = lower(encoded?.get(i) ?: name[i].code.toByte())
                if (a != b) return false
                i++
            }
            return true
        }

        fun lower(b: Byte): Byte =
            if (b >= 'A'.code.toByte() && b <= 'Z'.code.toByte()) (b + 32).toByte() else b
    }
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

/**
 * TLS for a listener.
 *
 * [alpnProtocols] is the server's preference order, most preferred first. The
 * handshake picks the first entry the client also offers, so `["h2", "http/1.1"]`
 * serves HTTP/2 to clients that speak it and HTTP/1.1 to the rest. An empty list
 * advertises no ALPN at all, which leaves HTTP/1.1.
 */
class Hyper4kTls(
    val certificatePath: String,
    val privateKeyPath: String,
    val alpnProtocols: List<String> = listOf("http/1.1"),
)

/**
 * Copies [len] bytes from the borrowed native header pointer [addr] into an owned
 * ByteArray. Called only when a handler actually reads a header, so the common
 * zero-read path never touches the header bytes. The engine keeps the pointer
 * valid for the whole handler (see lib.rs REQUEST_BACKINGS).
 */
internal expect fun copyBorrowedHeaders(addr: Long, len: Int): ByteArray
