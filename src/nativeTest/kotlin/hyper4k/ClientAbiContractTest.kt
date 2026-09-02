package hyper4k

import hyper4k.cinterop.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.cinterop.*

/**
 * Drives the whole client lifecycle through the public C header, the way a real
 * consumer would: new -> send -> callbacks -> close -> free.
 *
 * The width test next door only checks constants. Nothing there proves a caller
 * can actually reach the client API — the header did not even declare it until
 * this test forced the issue.
 */
/** File-level so the `staticCFunction` callbacks below can reach it. */
internal object Sink {
    var status: Int = -1
    var version: Int = -1
    var body: String = ""
    var doneCalls: Int = 0
    var errorKind: Int = Int.MIN_VALUE
    var eventsAfterDone: Int = 0

    fun reset() {
        status = -1; version = -1; body = ""; doneCalls = 0
        errorKind = Int.MIN_VALUE; eventsAfterDone = 0
    }
}

@OptIn(ExperimentalForeignApi::class)
class ClientAbiContractTest {

    // File-level so the static C callbacks below can reach it.
    internal object SinkHolder

    @Test
    fun aFullRequestRoundTripThroughThePublicAbi() {
        Sink.reset()
        // The peer is hyper4k's own server ABI. Client and server meeting over a
        // real socket is a stronger check than either side alone.
        val port = 18_923
        val server = Hyper4kServer(host = "127.0.0.1", port = port)
        kotlinx.coroutines.runBlocking {
            server.start { Hyper4kResponse.text(body = "hello-abi") }
        }

        memScoped {
            val opts = alloc<Hyper4kClientOptions>()
            val st = hyper4k_client_options_init(
                opts.ptr,
                sizeOf<Hyper4kClientOptions>().toUInt(),
            )
            assertEquals(HYPER4K_STATUS_OK, st)

            val clientRef = alloc<CPointerVar<cnames.structs.Hyper4kClient>>()
            assertEquals(HYPER4K_STATUS_OK, hyper4k_client_new(opts.ptr, clientRef.ptr))
            val client = clientRef.value!!

            val url = "http://127.0.0.1:$port/hello"
            val method = "GET"
            val req = alloc<Hyper4kClientRequest>()
            assertEquals(
                HYPER4K_STATUS_OK,
                hyper4k_client_request_init(req.ptr, sizeOf<Hyper4kClientRequest>().toUInt()),
            )
            method.encodeToByteArray().usePinned { m ->
                url.encodeToByteArray().usePinned { u ->
                    req.method.ptr = m.addressOf(0).reinterpret()
                    req.method.len = m.get().size.convert()
                    req.url.ptr = u.addressOf(0).reinterpret()
                    req.url.len = u.get().size.convert()

                    val idVar = alloc<ULongVar>()
                    val sent = hyper4k_client_send(
                        client, req.ptr,
                        staticCFunction(::onHeaders),
                        staticCFunction(::onChunk),
                        staticCFunction(::onDone),
                        null, idVar.ptr,
                    )
                    assertEquals(HYPER4K_STATUS_OK, sent)
                    assertTrue(idVar.value > 0uL, "out_request_id must be written")

                    waitUntil("done") { Sink.doneCalls > 0 }
                }
            }

            assertEquals(200, Sink.status)
            assertEquals(1, Sink.version, "plaintext must be HTTP/1.1")
            assertEquals("hello-abi", Sink.body)
            assertEquals(1, Sink.doneCalls, "OnDone must fire exactly once")
            assertEquals(0, Sink.eventsAfterDone, "an event arrived after OnDone")

            hyper4k_client_close(client)
            hyper4k_client_free(client)
        }
        kotlinx.coroutines.runBlocking { server.stop() }
    }

    @Test
    fun sendAfterCloseIsRefusedWithoutAnyCallback() {
        Sink.reset()
        memScoped {
            val opts = alloc<Hyper4kClientOptions>()
            hyper4k_client_options_init(opts.ptr, sizeOf<Hyper4kClientOptions>().toUInt())
            val clientRef = alloc<CPointerVar<cnames.structs.Hyper4kClient>>()
            hyper4k_client_new(opts.ptr, clientRef.ptr)
            val client = clientRef.value!!
            hyper4k_client_close(client)

            val req = alloc<Hyper4kClientRequest>()
            hyper4k_client_request_init(req.ptr, sizeOf<Hyper4kClientRequest>().toUInt())
            val url = "http://127.0.0.1:1/x"
            val method = "GET"
            method.encodeToByteArray().usePinned { m ->
                url.encodeToByteArray().usePinned { u ->
                    req.method.ptr = m.addressOf(0).reinterpret()
                    req.method.len = m.get().size.convert()
                    req.url.ptr = u.addressOf(0).reinterpret()
                    req.url.len = u.get().size.convert()
                    val idVar = alloc<ULongVar>()
                    val st = hyper4k_client_send(
                        client, req.ptr,
                        staticCFunction(::onHeaders),
                        staticCFunction(::onChunk),
                        staticCFunction(::onDone),
                        null, idVar.ptr,
                    )
                    assertEquals(HYPER4K_STATUS_CLIENT_CLOSED, st)
                }
            }
            assertEquals(0, Sink.doneCalls, "a refused request produced a callback")
            hyper4k_client_free(client)
        }
    }

    private fun waitUntil(what: String, f: () -> Boolean) {
        val start = kotlin.time.TimeSource.Monotonic.markNow()
        while (start.elapsedNow() < kotlin.time.Duration.parse("10s")) {
            if (f()) return
            platform.posix.usleep(5_000u)
        }
        throw AssertionError("timed out waiting for: $what")
    }

}

@OptIn(ExperimentalForeignApi::class)
private fun onHeaders(
    @Suppress("UNUSED_PARAMETER") ud: COpaquePointer?,
    @Suppress("UNUSED_PARAMETER") id: ULong,
    status: UShort,
    version: UByte,
    @Suppress("UNUSED_PARAMETER") headers: CPointer<Hyper4kHeader>?,
    @Suppress("UNUSED_PARAMETER") count: ULong,
): Hyper4kHeadersAction {
    Sink.let {
        if (it.doneCalls > 0) it.eventsAfterDone++
        it.status = status.toInt()
        it.version = version.toInt()
    }
    return HYPER4K_HEADERS_CONTINUE
}

@OptIn(ExperimentalForeignApi::class)
private fun onChunk(
    @Suppress("UNUSED_PARAMETER") ud: COpaquePointer?,
    @Suppress("UNUSED_PARAMETER") id: ULong,
    ptr: CPointer<UByteVar>?,
    len: ULong,
): Hyper4kChunkAction {
    Sink.let {
        if (it.doneCalls > 0) it.eventsAfterDone++
        if (ptr != null && len > 0uL) {
            val bytes = ByteArray(len.toInt()) { i -> ptr[i].toByte() }
            it.body += bytes.decodeToString()
        }
    }
    return HYPER4K_CHUNK_CONTINUE
}

@OptIn(ExperimentalForeignApi::class)
private fun onDone(
    @Suppress("UNUSED_PARAMETER") ud: COpaquePointer?,
    @Suppress("UNUSED_PARAMETER") id: ULong,
    error: CPointer<Hyper4kError>?,
) {
    Sink.let {
        it.doneCalls++
        it.errorKind = error?.pointed?.kind ?: -999
    }
}
