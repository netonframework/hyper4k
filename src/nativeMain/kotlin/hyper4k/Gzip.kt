package hyper4k

import hyper4k.cinterop.hyper4k_gzip
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned

/**
 * gzip-compress [src] using the engine's Rust encoder. Returns the compressed
 * bytes, or null if [src] is empty or compression fails (caller should then send
 * the body uncompressed). The output carries a full gzip header/trailer, so it is
 * a valid `Content-Encoding: gzip` payload.
 */
@OptIn(ExperimentalForeignApi::class)
public fun gzip(src: ByteArray): ByteArray? {
    if (src.isEmpty()) return null
    // gzip can be marginally larger than the input on incompressible data; this
    // bound comfortably covers the worst case, so a single call always suffices.
    val cap = src.size + src.size / 2 + 128
    val dst = ByteArray(cap)
    val n = src.usePinned { s ->
        dst.usePinned { d ->
            hyper4k_gzip(
                src_ptr = s.addressOf(0).reinterpret(),
                src_len = src.size.convert(),
                dst_ptr = d.addressOf(0).reinterpret(),
                dst_cap = cap.convert(),
            )
        }
    }
    if (n <= 0L) return null
    return dst.copyOf(n.toInt())
}
