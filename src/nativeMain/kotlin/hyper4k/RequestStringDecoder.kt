@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package hyper4k

import hyper4k.cinterop.Hyper4kSlice
import kotlinx.cinterop.*
import kotlin.native.concurrent.ThreadLocal
import platform.posix.memcpy

// Experimental: enable before process startup; never query the environment per request.
internal val requestStringScratchEnabled =
    platform.posix.getenv("HYPER4K_REQUEST_STRING_SCRATCH")?.toKString() == "1"

// Scratch never escapes this synchronous decoder and is never shared by workers.
@ThreadLocal
private val requestStringDecoder = RequestStringDecoder()

internal fun Hyper4kSlice.copyRequestString(): String {
    require(len <= Int.MAX_VALUE.toULong()) { "HTTP string exceeds Kotlin array capacity" }
    return requestStringDecoder.decode(ptr, len.toInt())
}

internal class RequestStringDecoder {
    private var scratch = ByteArray(0)
    internal val retainedCapacity: Int get() = scratch.size

    fun decode(source: CPointer<UByteVar>?, size: Int): String {
        require(size >= 0)
        if (size == 0) return ""
        requireNotNull(source) { "Non-empty HTTP string has a null pointer" }
        if (size > MAX_RETAINED_BYTES) return source.readBytes(size).decodeToString()
        if (scratch.size < size) {
            var capacity = 64
            while (capacity < size) capacity *= 2
            scratch = ByteArray(capacity)
        }
        scratch.usePinned { memcpy(it.addressOf(0), source, size.convert()) }
        // decodeToString creates an owned String; later calls may overwrite scratch.
        return scratch.decodeToString(endIndex = size)
    }

    companion object {
        const val MAX_RETAINED_BYTES = 4096
    }
}
