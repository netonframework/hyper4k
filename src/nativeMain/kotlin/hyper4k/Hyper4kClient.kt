package hyper4k

import hyper4k.cinterop.HYPER4K_CHUNK_CANCEL
import hyper4k.cinterop.HYPER4K_CHUNK_CONTINUE
import hyper4k.cinterop.HYPER4K_CHUNK_PAUSE
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_CANCEL
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_CUSTOM_CA
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_HTTP1
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_HTTP2
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_STREAMING
import hyper4k.cinterop.HYPER4K_CLIENT_CAP_TLS
import hyper4k.cinterop.HYPER4K_CLIENT_CA_REPLACE_SYSTEM
import hyper4k.cinterop.HYPER4K_CLIENT_HTTP2_REQUIRED
import hyper4k.cinterop.HYPER4K_HEADERS_CANCEL
import hyper4k.cinterop.HYPER4K_HEADERS_CONTINUE
import hyper4k.cinterop.HYPER4K_STATUS_ALREADY_DONE
import hyper4k.cinterop.HYPER4K_STATUS_CLIENT_CLOSED
import hyper4k.cinterop.HYPER4K_STATUS_INVALID_ARG
import hyper4k.cinterop.HYPER4K_STATUS_NOT_FOUND
import hyper4k.cinterop.HYPER4K_STATUS_NOT_PAUSED
import hyper4k.cinterop.HYPER4K_STATUS_OK
import hyper4k.cinterop.HYPER4K_STATUS_RESOURCE_EXHAUSTED
import hyper4k.cinterop.HYPER4K_STATUS_UNSUPPORTED
import hyper4k.cinterop.Hyper4kChunkAction
import hyper4k.cinterop.Hyper4kClientOptions as COptions
import hyper4k.cinterop.Hyper4kClientRequest as CRequest
import hyper4k.cinterop.Hyper4kError as CError
import hyper4k.cinterop.Hyper4kHeader as CHeader
import hyper4k.cinterop.Hyper4kHeadersAction
import hyper4k.cinterop.hyper4k_client_cancel
import hyper4k.cinterop.hyper4k_client_capabilities
import hyper4k.cinterop.hyper4k_client_close
import hyper4k.cinterop.hyper4k_client_free
import hyper4k.cinterop.hyper4k_client_new
import hyper4k.cinterop.hyper4k_client_options_init
import hyper4k.cinterop.hyper4k_client_request_init
import hyper4k.cinterop.hyper4k_client_resume
import hyper4k.cinterop.hyper4k_client_send
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.StableRef
import kotlinx.cinterop.UByteVar
import kotlinx.cinterop.ULongVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.asStableRef
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pin
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.staticCFunction
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.withContext
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.decrementAndFetch
import kotlin.concurrent.atomics.incrementAndFetch

/**
 * Outbound HTTP client over the ABI v4 C surface (`hyper4k/docs/ABI_V4_CLIENT_TLS.md`).
 *
 * This layer owns exactly the obligations the ABI leaves to the caller: one
 * `free`, callbacks that never throw or block, per-request event ordering,
 * PAUSE/resume backpressure, and the `send`/`close` race. It knows nothing
 * about any framework above it; that is what keeps hyper4k publishable on its
 * own.
 *
 * Lifecycle: construct → [send]* → [close]. `close` is suspend because `free`
 * blocks until the engine can no longer call back, and that wait belongs on a
 * dedicated thread, never on a coroutine dispatcher shared with other work.
 */
@OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)
class Hyper4kClient(options: Hyper4kClientOptions = Hyper4kClientOptions()) {

    private val handle: CPointer<cnames.structs.Hyper4kClient>
    private val lifecycle = AtomicInt(OPEN)

    init {
        handle = memScoped {
            val opts = alloc<COptions>()
            checkStatus(hyper4k_client_options_init(opts.ptr, sizeOf<COptions>().toUInt()), "options_init")
            var flags = 0uL
            if (options.requireHttp2) flags = flags or HYPER4K_CLIENT_HTTP2_REQUIRED
            if (options.replaceSystemCa) flags = flags or HYPER4K_CLIENT_CA_REPLACE_SYSTEM
            opts.flags = flags
            options.connectTimeoutMillis?.let { opts.connect_timeout_ms = it.toULong() }
            options.requestTimeoutMillis?.let { opts.request_timeout_ms = it.toULong() }
            options.readIdleTimeoutMillis?.let { opts.read_idle_timeout_ms = it.toULong() }
            options.maxRetries?.let { opts.max_retries = it.toUInt() }

            val out = alloc<CPointerVar<cnames.structs.Hyper4kClient>>()
            val pem = options.customCaPem
            val status = if (pem != null && pem.isNotEmpty()) {
                pem.usePinned { pinned ->
                    opts.custom_ca_pem = pinned.addressOf(0).reinterpret()
                    opts.custom_ca_pem_len = pem.size.convert()
                    hyper4k_client_new(opts.ptr, out.ptr)
                }
            } else {
                hyper4k_client_new(opts.ptr, out.ptr)
            }
            checkStatus(status, "client_new")
            out.value ?: error("hyper4k_client_new returned OK without a client")
        }
    }

    val isOpen: Boolean get() = lifecycle.load() == OPEN

    /**
     * Submits a request. Returns as soon as the engine has accepted it; the
     * response arrives through [Hyper4kResponseStream.next].
     *
     * [chunkHighWater] is how many undelivered body chunks may pile up before
     * the engine is told to pause. Consumers that read slowly bound memory with
     * it; consumers that read promptly never see it engage.
     */
    fun send(request: Hyper4kClientRequest, chunkHighWater: Int = DEFAULT_CHUNK_HIGH_WATER): Hyper4kResponseStream {
        require(chunkHighWater > 0) { "chunkHighWater must be positive" }
        if (lifecycle.load() != OPEN) throw Hyper4kClientClosedException()

        val state = RequestState(chunkHighWater)
        // Registered before send: the ABI allows a callback to run on another
        // thread before send() has returned to us.
        val ref = StableRef.create(state)
        val status = memScoped {
            val req = alloc<CRequest>()
            checkStatus(hyper4k_client_request_init(req.ptr, sizeOf<CRequest>().toUInt()), "request_init")
            request.readIdleTimeoutMillis?.let { req.read_idle_timeout_ms = it.toULong() }

            val method = request.method.encodeToByteArray()
            val url = request.url.encodeToByteArray()
            val names = request.headers.map { it.first.encodeToByteArray() }
            val values = request.headers.map { it.second.encodeToByteArray() }
            val headerArray = allocArray<CHeader>(request.headers.size)

            // Every input slice is borrowed only for the duration of send(): the
            // engine copies them before returning. Pin them all, call, unpin.
            withPinned(listOf(method, url) + names + values + listOf(request.body)) { pins ->
                var i = 0
                req.method.ptr = pins[i].addressOfOrNull(); req.method.len = method.size.convert(); i++
                req.url.ptr = pins[i].addressOfOrNull(); req.url.len = url.size.convert(); i++
                for (h in request.headers.indices) {
                    headerArray[h].name.ptr = pins[i].addressOfOrNull(); headerArray[h].name.len = names[h].size.convert(); i++
                }
                for (h in request.headers.indices) {
                    headerArray[h].value.ptr = pins[i].addressOfOrNull(); headerArray[h].value.len = values[h].size.convert(); i++
                }
                req.headers = if (request.headers.isEmpty()) null else headerArray
                req.header_count = request.headers.size.convert()
                req.body_ptr = pins[i].addressOfOrNull()
                req.body_len = request.body.size.convert()

                val out = alloc<ULongVar>()
                val st = hyper4k_client_send(
                    handle, req.ptr,
                    staticCFunction(::onHeaders),
                    staticCFunction(::onChunk),
                    staticCFunction(::onDone),
                    ref.asCPointer(),
                    out.ptr,
                )
                if (st == HYPER4K_STATUS_OK) state.requestId.store(out.value.toLong())
                st
            }
        }
        if (status != HYPER4K_STATUS_OK) {
            // Refused synchronously: the ABI promises no callback will ever fire,
            // so this is the only place the StableRef can be released.
            ref.dispose()
            when (status) {
                HYPER4K_STATUS_CLIENT_CLOSED -> throw Hyper4kClientClosedException()
                HYPER4K_STATUS_INVALID_ARG, HYPER4K_STATUS_UNSUPPORTED ->
                    throw IllegalArgumentException("hyper4k rejected the request (status $status): ${request.method} ${request.url}")
                HYPER4K_STATUS_RESOURCE_EXHAUSTED -> throw Hyper4kClientOverloadedException()
                else -> throw Hyper4kClientException("hyper4k_client_send failed with status $status")
            }
        }
        return Hyper4kResponseStream(handle, state)
    }

    /**
     * Stops accepting requests, cancels the in-flight ones, waits until the engine
     * can no longer call back, and frees it. Idempotent. Must not be called from
     * inside a callback (the ABI forbids `free` there); this class never runs
     * user code on callback threads, so that cannot happen by accident.
     */
    suspend fun close() {
        if (!lifecycle.compareAndSet(OPEN, CLOSING)) return
        hyper4k_client_close(handle)
        withContext(freePool) { hyper4k_client_free(handle) }
        lifecycle.store(FREED)
    }

    companion object {
        const val DEFAULT_CHUNK_HIGH_WATER: Int = 8

        private const val OPEN = 0
        private const val CLOSING = 1
        private const val FREED = 2

        /** Engine capability bits, reported by the loaded library itself. */
        fun engineCapabilities(): Hyper4kClientCapabilities {
            val bits = hyper4k_client_capabilities()
            return Hyper4kClientCapabilities(
                http1 = bits and HYPER4K_CLIENT_CAP_HTTP1 != 0uL,
                http2 = bits and HYPER4K_CLIENT_CAP_HTTP2 != 0uL,
                tls = bits and HYPER4K_CLIENT_CAP_TLS != 0uL,
                customCa = bits and HYPER4K_CLIENT_CAP_CUSTOM_CA != 0uL,
                cancel = bits and HYPER4K_CLIENT_CAP_CANCEL != 0uL,
                streaming = bits and HYPER4K_CLIENT_CAP_STREAMING != 0uL,
            )
        }

        /**
         * `free` blocks until every callback has drained. Two threads, not one:
         * a client being freed must never wait behind another client's free.
         */
        @OptIn(DelicateCoroutinesApi::class)
        private val freePool by lazy { newFixedThreadPoolContext(2, "hyper4k-client-free") }

        private fun checkStatus(status: Int, what: String) {
            if (status != HYPER4K_STATUS_OK) throw Hyper4kClientException("hyper4k_client_$what failed with status $status")
        }
    }
}

/** Client-level options. `null` keeps the engine's own default for that field. */
class Hyper4kClientOptions(
    val connectTimeoutMillis: Long? = null,
    /** Whole request including retries; 0 disables. SSE needs this disabled. */
    val requestTimeoutMillis: Long? = null,
    /** Gap between body chunks; re-armed per chunk; 0 disables. */
    val readIdleTimeoutMillis: Long? = null,
    /** Additional attempts after the first; 0 means try once. */
    val maxRetries: Int? = null,
    /** Fail instead of downgrading when ALPN does not yield h2. */
    val requireHttp2: Boolean = false,
    /** PEM bundle appended to (or, with [replaceSystemCa], replacing) the platform roots. */
    val customCaPem: ByteArray? = null,
    val replaceSystemCa: Boolean = false,
)

class Hyper4kClientRequest(
    val method: String,
    val url: String,
    val headers: List<Pair<String, String>> = emptyList(),
    val body: ByteArray = ByteArray(0),
    /** Overrides the client's read-idle timeout for this request; 0 disables it. */
    val readIdleTimeoutMillis: Long? = null,
)

data class Hyper4kClientCapabilities(
    val http1: Boolean,
    val http2: Boolean,
    val tls: Boolean,
    val customCa: Boolean,
    val cancel: Boolean,
    val streaming: Boolean,
)

/** One event of a response, in the order the ABI promises: Headers, Chunk*, Done. */
sealed interface Hyper4kClientEvent {
    class Headers(
        val status: Int,
        /** 1 = HTTP/1.1, 2 = HTTP/2: the negotiated protocol, observable per response. */
        val version: Int,
        val headers: List<Pair<String, String>>,
    ) : Hyper4kClientEvent

    class Chunk(val bytes: ByteArray) : Hyper4kClientEvent

    /** `error == null` is success. HTTP 4xx/5xx are successes here. */
    class Done(val error: Hyper4kClientError?) : Hyper4kClientEvent
}

class Hyper4kClientError(
    /** Stable category; branch on this. */
    val kind: Hyper4kErrorKind,
    /** The raw `HYPER4K_ERR_*` value, kept for kinds this build does not know yet. */
    val rawKind: Int,
    /** Protocol-level code (an HTTP/2 error code, for instance) or 0. */
    val protocolCode: UInt,
    /** For logs. The ABI says never to branch on it and it means that. */
    val message: String,
)

/**
 * `HYPER4K_ERR_*` as an enum. Values are the ABI's frozen numbers; consumers
 * outside this library do not see the cinterop package, so the mapping lives here.
 */
enum class Hyper4kErrorKind(val code: Int) {
    NONE(0),
    DNS(1),
    CONNECT(2),
    TLS_CA(3),
    TLS_HOSTNAME(4),
    TLS_EXPIRED(5),
    TLS_OTHER(6),
    ALPN_NO_H2(7),
    PROTOCOL(8),
    TIMEOUT(9),
    IDLE_TIMEOUT(10),
    CANCELLED(11),
    /** The response started but did not arrive whole. Chunks already delivered stand. */
    TRUNCATED(12),
    /**
     * The connection broke before the response was committed and the request was
     * not idempotent, so nothing can say whether the server acted on it. The one
     * honest answer; callers must not retry on their own.
     */
    OUTCOME_UNKNOWN(13),
    /** A kind newer than this build. Check [Hyper4kClientError.rawKind]. */
    UNKNOWN(-1);

    companion object {
        fun fromCode(code: Int): Hyper4kErrorKind = entries.firstOrNull { it.code == code } ?: UNKNOWN
    }
}

open class Hyper4kClientException(message: String) : RuntimeException(message)
class Hyper4kClientClosedException : Hyper4kClientException("hyper4k client is closed")
class Hyper4kClientOverloadedException : Hyper4kClientException("hyper4k client refused the request: in-flight limit reached")

/**
 * The consumer side of one request.
 *
 * Backpressure works in two halves. The callback thread counts undelivered
 * chunks and returns PAUSE once the high-water mark is reached; [next] counts
 * them back down and calls resume once the consumer has caught up. The engine
 * may register the pause slightly after the callback returned, so a resume
 * that lands in that gap is reported as NOT_PAUSED and retried.
 */
@OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)
class Hyper4kResponseStream internal constructor(
    private val client: CPointer<cnames.structs.Hyper4kClient>,
    private val state: RequestState,
) {
    val requestId: Long get() = state.requestId.load()

    /** Next event. After [Hyper4kClientEvent.Done] the stream is finished. */
    suspend fun next(): Hyper4kClientEvent {
        val event = state.events.receive()
        if (event is Hyper4kClientEvent.Chunk) {
            val remaining = state.queued.decrementAndFetch()
            if (state.paused.load() == 1 && remaining <= state.highWater / 2) resumeWhenParked()
        }
        return event
    }

    /** Idempotent, non-blocking. The engine still delivers one Done(CANCELLED). */
    fun cancel() {
        val id = state.requestId.load()
        if (id > 0) hyper4k_client_cancel(client, id.toULong())
    }

    private suspend fun resumeWhenParked() {
        val id = awaitRequestId()
        while (true) {
            when (hyper4k_client_resume(client, id.toULong())) {
                HYPER4K_STATUS_OK -> { state.paused.store(0); return }
                HYPER4K_STATUS_NOT_PAUSED -> delay(1)   // callback returned PAUSE, engine not parked yet
                HYPER4K_STATUS_ALREADY_DONE, HYPER4K_STATUS_NOT_FOUND -> { state.paused.store(0); return }
                else -> { state.paused.store(0); return }
            }
        }
    }

    private suspend fun awaitRequestId(): Long {
        while (true) {
            val id = state.requestId.load()
            if (id > 0) return id
            delay(1)
        }
    }
}

@OptIn(ExperimentalAtomicApi::class)
internal class RequestState(val highWater: Int) {
    val events = Channel<Hyper4kClientEvent>(Channel.UNLIMITED)
    val queued = AtomicInt(0)
    val paused = AtomicInt(0)
    val requestId = AtomicLong(0)
    val done = AtomicInt(0)
}

// ---------------------------------------------------------------------------
// C callbacks. Top-level so staticCFunction can take them. Each one only moves
// data into the request's channel; nothing here suspends, blocks, or throws.
// ---------------------------------------------------------------------------

@OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)
private fun onHeaders(
    ud: COpaquePointer?,
    @Suppress("UNUSED_PARAMETER") id: ULong,
    status: UShort,
    version: UByte,
    headers: CPointer<CHeader>?,
    count: ULong,
): Hyper4kHeadersAction {
    val state = ud?.asStableRef<RequestState>()?.get() ?: return HYPER4K_HEADERS_CANCEL
    return try {
        val list = ArrayList<Pair<String, String>>(count.toInt())
        if (headers != null) {
            for (i in 0 until count.toInt()) {
                val h = headers[i]
                list += h.name.ptr.readString(h.name.len) to h.value.ptr.readString(h.value.len)
            }
        }
        state.events.trySend(Hyper4kClientEvent.Headers(status.toInt(), version.toInt(), list))
        HYPER4K_HEADERS_CONTINUE
    } catch (_: Throwable) {
        HYPER4K_HEADERS_CANCEL
    }
}

@OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)
private fun onChunk(
    ud: COpaquePointer?,
    @Suppress("UNUSED_PARAMETER") id: ULong,
    ptr: CPointer<UByteVar>?,
    len: ULong,
): Hyper4kChunkAction {
    val state = ud?.asStableRef<RequestState>()?.get() ?: return HYPER4K_CHUNK_CANCEL
    return try {
        val bytes = if (ptr == null || len == 0uL) ByteArray(0) else ptr.readBytes(len.toInt())
        val queued = state.queued.incrementAndFetch()
        state.events.trySend(Hyper4kClientEvent.Chunk(bytes))
        if (queued >= state.highWater) {
            state.paused.store(1)
            HYPER4K_CHUNK_PAUSE
        } else {
            HYPER4K_CHUNK_CONTINUE
        }
    } catch (_: Throwable) {
        HYPER4K_CHUNK_CANCEL
    }
}

@OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)
private fun onDone(
    ud: COpaquePointer?,
    @Suppress("UNUSED_PARAMETER") id: ULong,
    error: CPointer<CError>?,
) {
    val ref = ud?.asStableRef<RequestState>() ?: return
    try {
        val state = ref.get()
        val mapped = error?.pointed?.let { e ->
            Hyper4kClientError(
                kind = Hyper4kErrorKind.fromCode(e.kind),
                rawKind = e.kind,
                protocolCode = e.protocol_code,
                message = e.message.ptr.readString(e.message.len),
            )
        }
        state.done.store(1)
        state.events.trySend(Hyper4kClientEvent.Done(mapped))
        state.events.close()
    } catch (_: Throwable) {
        // Nothing may cross the boundary. The channel close above is the last
        // thing the consumer needs; if even that failed there is nobody to tell.
    } finally {
        // The ABI releases user_data once OnDone returns; nothing follows it.
        ref.dispose()
    }
}

@OptIn(ExperimentalForeignApi::class)
private fun CPointer<UByteVar>?.readString(len: ULong): String =
    if (this == null || len == 0uL) "" else readBytes(len.toInt()).decodeToString()

/** Pins every array at once; empty arrays pin to a null address, which the ABI accepts with len 0. */
@OptIn(ExperimentalForeignApi::class)
private inline fun <T> withPinned(arrays: List<ByteArray>, block: (List<PinnedOrEmpty>) -> T): T {
    val pins = ArrayList<PinnedOrEmpty>(arrays.size)
    try {
        for (a in arrays) pins += PinnedOrEmpty(a)
        return block(pins)
    } finally {
        pins.forEach { it.unpin() }
    }
}

@OptIn(ExperimentalForeignApi::class)
private class PinnedOrEmpty(array: ByteArray) {
    private val pinned = if (array.isEmpty()) null else array.pin()
    fun addressOfOrNull(): CPointer<UByteVar>? = pinned?.addressOf(0)?.reinterpret()
    fun unpin() = pinned?.unpin()
}
