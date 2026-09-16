@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package hyper4k

import hyper4k.cinterop.Hyper4kSlice
import kotlinx.cinterop.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.time.TimeSource

class RequestMethodLookupTest {
    private fun decode(method: String): String = memScoped {
        val bytes = method.encodeToByteArray()
        // No NUL terminator: matching must stay inside the ABI slice.
        val data = allocArray<UByteVar>(bytes.size.coerceAtLeast(1))
        for (i in bytes.indices) data[i] = bytes[i].toUByte()
        val slice = alloc<Hyper4kSlice>()
        slice.ptr = data
        slice.len = bytes.size.convert()
        val result = slice.copyHttpMethod()
        for (i in bytes.indices) data[i] = 0u
        result
    }

    @Test
    fun standardMethodsReuseImmutableStrings() {
        for (method in listOf("GET", "PUT", "POST", "HEAD", "PATCH", "TRACE", "DELETE", "OPTIONS", "CONNECT")) {
            assertSame(method, decode(method))
        }
    }

    @Test
    fun extensionMethodsPreserveCaseAndOwnTheirMemory() {
        for (method in listOf("get", "Get", "PROPFIND", "GE", "GETX", "POSX", "OPTIONX", "")) {
            assertEquals(method, decode(method))
        }
    }

    @Test
    fun compareMethodDecoding() = memScoped {
        if (platform.posix.getenv("HYPER4K_METHOD_BENCH")?.toKString() != "1") return@memScoped
        val bytes = "POST".encodeToByteArray()
        val data = allocArray<UByteVar>(bytes.size)
        for (i in bytes.indices) data[i] = bytes[i].toUByte()
        val slice = alloc<Hyper4kSlice>()
        slice.ptr = data
        slice.len = bytes.size.convert()
        val iterations = 500_000
        fun measure(optimized: Boolean): Long {
            var checksum = 0L
            val start = TimeSource.Monotonic.markNow()
            repeat(iterations) {
                val method = if (optimized) slice.copyHttpMethod() else data.readBytes(bytes.size).decodeToString()
                checksum += method.length + method[0].code
            }
            val elapsed = start.elapsedNow().inWholeNanoseconds
            assertEquals(iterations * 84L, checksum)
            return elapsed / iterations
        }
        measure(false)
        measure(true)
        repeat(6) { round ->
            for (optimized in if (round % 2 == 0) listOf(false, true) else listOf(true, false)) {
                println("method-decode round=$round optimized=$optimized ns/op=${measure(optimized)}")
            }
        }
    }
}
