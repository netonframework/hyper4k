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
    /** 原始头块，"Name: Value\n" 串联。惰性解析见 [headers]。 */
    val rawHeaders: String,
    val body: ByteArray,
) {
    /** 解析后的请求头（大小写按原样保留；查找见 [header]）。 */
    val headers: Map<String, String> by lazy(LazyThreadSafetyMode.NONE) {
        if (rawHeaders.isEmpty()) emptyMap()
        else buildMap {
            for (line in rawHeaders.split('\n')) {
                if (line.isEmpty()) continue
                val i = line.indexOf(':')
                if (i <= 0) continue
                put(line.substring(0, i).trim(), line.substring(i + 1).trim())
            }
        }
    }

    /** 大小写不敏感取头。 */
    fun header(name: String): String? =
        headers.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value
}

/** 一次响应。[headers] 会被编码回 "Name: Value\n" 文本块。 */
class Hyper4kResponse(
    val status: Int,
    val headers: Map<String, String> = emptyMap(),
    val body: ByteArray = EMPTY,
) {
    companion object {
        private val EMPTY = ByteArray(0)

        fun text(status: Int = 200, body: String, contentType: String = "text/plain; charset=utf-8") =
            Hyper4kResponse(status, mapOf("Content-Type" to contentType), body.encodeToByteArray())

        fun json(status: Int = 200, body: String) =
            Hyper4kResponse(status, mapOf("Content-Type" to "application/json"), body.encodeToByteArray())

        fun bytes(status: Int = 200, body: ByteArray, contentType: String = "application/octet-stream") =
            Hyper4kResponse(status, mapOf("Content-Type" to contentType), body)
    }
}

/** 同步请求处理器。在 Tokio worker 线程上被调用——必须返回一个响应。 */
typealias Hyper4kHandler = (Hyper4kRequest) -> Hyper4kResponse
