@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package hyper4k

import kotlinx.cinterop.*
import kotlinx.coroutines.*
import kotlin.test.*
import kotlin.time.TimeSource

class RequestStringDecoderTest {
    private fun decode(decoder: RequestStringDecoder, bytes: ByteArray): String = memScoped {
        val source = allocArray<UByteVar>(bytes.size.coerceAtLeast(1))
        for (i in bytes.indices) source[i] = bytes[i].toUByte()
        val result = decoder.decode(source, bytes.size)
        for (i in bytes.indices) source[i] = 0u
        result
    }

    @Test
    fun matchesUtf8DecoderIncludingMalformedAndEmbeddedNull() {
        val decoder = RequestStringDecoder()
        val cases = listOf(
            "", "/baseline11", "a=3&b=4", "127.0.0.1:8080", "[::1]:8080",
            "中文/é/😀", "a\u0000b", "x".repeat(4096), "y".repeat(4097),
        ).map { it.encodeToByteArray() } + listOf(
            byteArrayOf(0xc0.toByte(), 0xaf.toByte()),
            byteArrayOf(0xf0.toByte(), 0x9f.toByte()),
            byteArrayOf(0xff.toByte(), 0, 65),
        )
        for (bytes in cases) assertEquals(bytes.decodeToString(), decode(decoder, bytes))
    }

    @Test
    fun ownedStringsSurviveScratchReuseAndLargeRequestsDoNotGrowRetention() {
        val decoder = RequestStringDecoder()
        val first = decode(decoder, "first-request".encodeToByteArray())
        val second = decode(decoder, "different-value".encodeToByteArray())
        val capacity = decoder.retainedCapacity
        decode(decoder, ByteArray(64 * 1024) { 65 })
        assertEquals(capacity, decoder.retainedCapacity)
        assertEquals("first-request", first)
        assertEquals("different-value", second)
        decode(decoder, ByteArray(4096) { 66 })
        assertEquals(4096, decoder.retainedCapacity)
        assertEquals("x", decode(decoder, byteArrayOf(120)))
    }

    @Test
    fun validatesPointersAndLengths() {
        val decoder = RequestStringDecoder()
        assertEquals("", decoder.decode(null, 0))
        assertFailsWith<IllegalArgumentException> { decoder.decode(null, 1) }
        assertFailsWith<IllegalArgumentException> { decoder.decode(null, -1) }
    }

    @Test
    fun threadLocalSlicesStayOwnedAcrossSuspension() = runBlocking {
        (0 until 32).map { worker -> async(Dispatchers.Default) {
            repeat(100) { iteration ->
                val expected = "worker=$worker&iteration=$iteration&value=中文"
                val result = memScoped {
                    val bytes = expected.encodeToByteArray()
                    val source = allocArray<UByteVar>(bytes.size)
                    for (i in bytes.indices) source[i] = bytes[i].toUByte()
                    val slice = alloc<hyper4k.cinterop.Hyper4kSlice>()
                    slice.ptr = source
                    slice.len = bytes.size.convert()
                    slice.copyRequestString()
                }
                yield()
                assertEquals(expected, result)
            }
        } }.awaitAll()
        Unit
    }

    @Test
    fun compareScratchDecoding() = memScoped {
        if (platform.posix.getenv("HYPER4K_STRING_BENCH")?.toKString() != "1") return@memScoped
        for ((case, value) in listOf("/baseline11", "a=3&b=4", "127.0.0.1:54321", "中文/😀", "x".repeat(512)).withIndex()) {
            val bytes = value.encodeToByteArray()
            val source = allocArray<UByteVar>(bytes.size)
            for (i in bytes.indices) source[i] = bytes[i].toUByte()
            val decoder = RequestStringDecoder()
            fun measure(reuse: Boolean): Long {
                var checksum = 0L
                val start = TimeSource.Monotonic.markNow()
                repeat(100_000) {
                    val result = if (reuse) decoder.decode(source, bytes.size)
                        else source.readBytes(bytes.size).decodeToString()
                    checksum += result.length
                }
                assertEquals(100_000L * value.length, checksum)
                return start.elapsedNow().inWholeNanoseconds / 100_000
            }
            measure(false)
            measure(true)
            repeat(6) { round ->
                for (reuse in if (round % 2 == 0) listOf(false, true) else listOf(true, false)) {
                    println("string-decode case=$case bytes=${bytes.size} round=$round reuse=$reuse ns/op=${measure(reuse)}")
                }
            }
        }
    }
}
