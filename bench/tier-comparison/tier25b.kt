import hyper4k.Hyper4kServer
import hyper4k.Hyper4kResponse
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay
import platform.posix.getenv
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString

// Tier 2.5B: through Hyper4kServer's request conversion + coroutine dispatch,
// but NOT the Neton dispatcher. Same sum as the other tiers.
@OptIn(ExperimentalForeignApi::class)
fun main() {
    val port = getenv("HYPER4K_SUM_PORT")?.toKString()?.toIntOrNull() ?: 19092
    val server = Hyper4kServer(host = "0.0.0.0", port = port, requestTimeoutMillis = 0)
    runBlocking {
        server.start { req ->
            var a = 0L; var b = 0L
            for (pair in req.query.split('&')) {
                val i = pair.indexOf('=')
                if (i > 0) {
                    val k = pair.substring(0, i); val v = pair.substring(i + 1)
                    when (k) { "a" -> a = v.toLongOrNull() ?: 0; "b" -> b = v.toLongOrNull() ?: 0 }
                }
            }
            Hyper4kResponse.text(200, (a + b).toString())
        }
        while (true) delay(3600_000)  // start() returns; keep the process alive
    }
}
