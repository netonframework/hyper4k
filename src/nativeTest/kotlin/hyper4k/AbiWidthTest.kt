package hyper4k

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.cinterop.ExperimentalForeignApi
import hyper4k.cinterop.HYPER4K_CHUNK_PAUSE
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_HTTP2
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_STREAMING
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_PROXY
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_TLS
import hyper4k.cinterop.HYPER4K_ERR_OUTCOME_UNKNOWN
import hyper4k.cinterop.HYPER4K_HEADERS_CANCEL
import hyper4k.cinterop.HYPER4K_STATUS_OK
import hyper4k.cinterop.hyper4k_abi_version
import hyper4k.cinterop.hyper4k_client_capabilities

/**
 * Guards the ABI's *representation*, not just its values.
 *
 * `assertEquals(4, Int.SIZE_BYTES)` would pass no matter what the C header
 * says. The real check is the compile-time one below: each constant is assigned
 * into an explicitly typed `Int`, so if a type ever reverts from
 * `typedef int32_t` to a bare C `enum`, cinterop maps it to something else and
 * this file stops compiling.
 */
@OptIn(ExperimentalForeignApi::class)
class AbiWidthTest {

    @Test
    fun abiScalarsMapToKotlinInt() {
        val status: Int = HYPER4K_STATUS_OK
        val err: Int = HYPER4K_ERR_OUTCOME_UNKNOWN
        val headersAction: Int = HYPER4K_HEADERS_CANCEL
        val chunkAction: Int = HYPER4K_CHUNK_PAUSE

        assertEquals(0, status)
        assertEquals(13, err)
        assertEquals(2, headersAction)
        assertEquals(1, chunkAction)
    }

    @Test
    fun abiVersionIsFourOne() {
        assertEquals((4 shl 16) or 1, hyper4k_abi_version().toInt())
    }

    @Test
    fun clientCapabilitiesReportOnlyWhatShipped() {
        val caps = hyper4k_client_capabilities()
        // Implemented and tested in this ABI version.
        assertEquals(HYPER4K_CLIENT_CAP_TLS, caps and HYPER4K_CLIENT_CAP_TLS)
        assertEquals(HYPER4K_CLIENT_CAP_HTTP2, caps and HYPER4K_CLIENT_CAP_HTTP2)
        assertEquals(HYPER4K_CLIENT_CAP_STREAMING, caps and HYPER4K_CLIENT_CAP_STREAMING)
        assertEquals(HYPER4K_CLIENT_CAP_PROXY, caps and HYPER4K_CLIENT_CAP_PROXY)
    }
}
