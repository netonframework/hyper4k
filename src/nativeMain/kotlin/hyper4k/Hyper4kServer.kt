package hyper4k

import hyper4k.cinterop.HYPER4K_ERR_CLIENT_GONE
import hyper4k.cinterop.HYPER4K_ERR_WOULD_BLOCK
import hyper4k.cinterop.HYPER4K_ERR_WRONG_STATE
import hyper4k.cinterop.HYPER4K_OK
import hyper4k.cinterop.Hyper4kRequest as CHyper4kRequest
import hyper4k.cinterop.hyper4k_respond
import hyper4k.cinterop.hyper4k_response_begin
import hyper4k.cinterop.hyper4k_response_finish
import hyper4k.cinterop.hyper4k_response_write
import hyper4k.cinterop.hyper4k_server_start
import hyper4k.cinterop.hyper4k_server_stop
import kotlinx.cinterop.*
import kotlinx.coroutines.withContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/**
 * Tokio + Hyper server with a suspend-native Kotlin/Native handoff.
 *
 * The Rust callback only snapshots request bytes and schedules work. Application handlers never
 * execute on Tokio workers. Concurrency is bounded so overload is rejected instead of building an
 * unbounded coroutine or memory backlog.
 */
@OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)
class Hyper4kServer(
    private val host: String = "0.0.0.0",
    private val port: Int,
    private val maxConcurrentRequests: Int = DEFAULT_MAX_CONCURRENT_REQUESTS,
    private val requestTimeoutMillis: Long = DEFAULT_REQUEST_TIMEOUT_MILLIS,
    private val shutdownGraceMillis: Long = DEFAULT_SHUTDOWN_GRACE_MILLIS,
    private val failureResponse: (status: Int, message: String) -> Hyper4kResponse = ::defaultFailureResponse,
) {
    private var server: CPointer<cnames.structs.Hyper4kServer>? = null
    private var stateRef: StableRef<AsyncRequestDispatcher>? = null

    init {
        require(maxConcurrentRequests > 0) { "maxConcurrentRequests must be positive" }
        require(requestTimeoutMillis > 0) { "requestTimeoutMillis must be positive" }
        require(shutdownGraceMillis >= 0) { "shutdownGraceMillis must not be negative" }
    }

    val isRunning: Boolean get() = server != null

    /** Starts the listener. Request handlers are always dispatched asynchronously. */
    fun start(handler: Hyper4kHandler) {
        start { request, _ -> handler(request) }
    }

    /**
     * Starts the listener with a handler that may stream its response body.
     *
     * A handler that never touches the channel behaves exactly like [start]'s.
     */
    fun start(handler: Hyper4kStreamingHandler) {
        check(server == null) { "hyper4k server already started" }
        val state = AsyncRequestDispatcher(
            handler,
            maxConcurrentRequests,
            requestTimeoutMillis,
            failureResponse,
            ::respond,
        )
        val ref = StableRef.create(state)
        stateRef = ref
        val startedServer = hyper4k_server_start(
            host = host,
            port = port.toUShort(),
            on_request = staticCFunction(::onRequest),
            user_data = ref.asCPointer(),
        )
        if (startedServer == null) {
            state.cancel()
            ref.dispose()
            stateRef = null
            error("hyper4k_server_start failed for $host:$port (port in use or bind error)")
        }
        server = startedServer
    }

    /** Stops admission, drains in-flight handlers up to the grace deadline, then closes Tokio. */
    suspend fun stop() {
        val runningServer = server ?: return
        val ref = stateRef
        val state = ref?.get()
        state?.stopAccepting()
        state?.awaitDrained(shutdownGraceMillis)

        server = null
        hyper4k_server_stop(runningServer)
        state?.cancelAndJoin(shutdownGraceMillis)
        ref?.dispose()
        stateRef = null
    }

    companion object {
        const val DEFAULT_MAX_CONCURRENT_REQUESTS: Int = 1024
        const val DEFAULT_REQUEST_TIMEOUT_MILLIS: Long = 30_000
        const val DEFAULT_SHUTDOWN_GRACE_MILLIS: Long = 5_000
    }
}

@OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)
internal class AsyncRequestDispatcher(
    private val handler: Hyper4kStreamingHandler,
    maxConcurrentRequests: Int,
    private val requestTimeoutMillis: Long,
    private val failureResponse: (status: Int, message: String) -> Hyper4kResponse = ::defaultFailureResponse,
    private val complete: (ULong, Hyper4kResponse) -> Boolean,
    private val newChannel: (ULong) -> Hyper4kResponseChannel = ::NativeResponseChannel,
) {
    private val accepting = AtomicBoolean(true)
    private val activeRequests = AtomicInt(0)
    private val slots = Semaphore(maxConcurrentRequests)
    private val rootJob = SupervisorJob()
    private val scope = CoroutineScope(rootJob + Dispatchers.Default)

    fun submit(requestPointer: CPointer<CHyper4kRequest>) {
        val source = requestPointer.pointed
        submit(
            request = Hyper4kRequest(
                method = source.method.copyToString(),
                path = source.path.copyToString(),
                query = source.query.copyToString(),
                // 头块直接存字节，省掉 String 中转；解析在 Hyper4kRequest 内惰性完成。
                rawHeaderBytes = source.headers.copyToByteArray(),
                body = source.body.copyToByteArray(),
            ),
            responder = source.responder,
        )
    }

    internal fun submit(request: Hyper4kRequest, responder: ULong) {
        if (!accepting.load()) {
            completeSafely(responder, failureResponse(503, "Service Unavailable"))
            return
        }
        if (!slots.tryAcquire()) {
            completeSafely(responder, failureResponse(503, "Service Unavailable"))
            return
        }
        activeRequests.fetchAndAdd(1)

        if (!accepting.load()) {
            finish(responder, failureResponse(503, "Service Unavailable"))
            return
        }

        // UNDISPATCHED: handlers that never suspend run inline on the engine worker,
        // skipping the cross-thread hop to Dispatchers.Default; suspending handlers
        // still resume on Default as before.
        scope.launch(start = CoroutineStart.UNDISPATCHED) {
            val channel = newChannel(responder)
            val response = try {
                if (requestTimeoutMillis > 0) {
                    lazyTimeout(requestTimeoutMillis) { handler(request, channel) }
                } else {
                    handler(request, channel)
                }
            } catch (_: TimeoutCancellationException) {
                failureResponse(504, "Gateway Timeout")
            } catch (_: CancellationException) {
                failureResponse(503, "Service Unavailable")
            } catch (_: Throwable) {
                failureResponse(500, "Internal Server Error")
            }
            finish(responder, response, channel)
        }
    }

    fun stopAccepting() {
        accepting.store(false)
    }

    suspend fun awaitDrained(graceMillis: Long) {
        val deadline = kotlin.time.Clock.System.now().toEpochMilliseconds() + graceMillis
        while (activeRequests.load() > 0 && kotlin.time.Clock.System.now().toEpochMilliseconds() < deadline) {
            delay(DRAIN_POLL_MILLIS)
        }
    }

    fun cancel() {
        rootJob.cancel()
    }

    suspend fun cancelAndJoin(graceMillis: Long = Hyper4kServer.DEFAULT_SHUTDOWN_GRACE_MILLIS) {
        rootJob.cancel()
        if (graceMillis > 0) {
            withTimeoutOrNull(graceMillis) { rootJob.join() }
        }
    }

    private suspend fun finish(
        responder: ULong,
        response: Hyper4kResponse,
        channel: Hyper4kResponseChannel?,
    ) {
        try {
            // Once a request is streaming its headers are already out, so nothing
            // the handler returns can be written, not even the 5xx fallbacks.
            // Closing the stream is the only correct move.
            if (channel != null && channel.isStreaming) {
                runCatching { channel.finish() }
            } else if (!response.streamed) {
                completeSafely(responder, response)
            }
        } finally {
            activeRequests.fetchAndAdd(-1)
            slots.release()
        }
    }

    private fun finish(responder: ULong, response: Hyper4kResponse) {
        try {
            completeSafely(responder, response)
        } finally {
            activeRequests.fetchAndAdd(-1)
            slots.release()
        }
    }

    private fun completeSafely(responder: ULong, response: Hyper4kResponse) {
        try {
            complete(responder, response)
        } catch (_: Throwable) {
            // Exceptions must never cross the Kotlin/C/Rust callback boundary.
        }
    }

    private companion object {
        const val DRAIN_POLL_MILLIS = 5L
    }
}

@OptIn(ExperimentalForeignApi::class)
private fun onRequest(userData: COpaquePointer?, requestPointer: CPointer<CHyper4kRequest>?) {
    if (requestPointer == null) return
    val responder = requestPointer.pointed.responder
    if (userData == null) {
        runCatching { respond(responder, internalErrorResponse()) }
        return
    }
    try {
        userData.asStableRef<AsyncRequestDispatcher>().get().submit(requestPointer)
    } catch (_: Throwable) {
        runCatching { respond(responder, internalErrorResponse()) }
    }
}

@OptIn(ExperimentalForeignApi::class)
private fun respond(responder: ULong, response: Hyper4kResponse): Boolean {
    val headerBytes = encodeHeaders(response.headers)
    return headerBytes.usePinned { headers ->
        response.body.usePinned { body ->
            hyper4k_respond(
                responder = responder,
                status = response.status.toUShort(),
                headers_ptr = if (headerBytes.isEmpty()) null else headers.addressOf(0).reinterpret(),
                headers_len = headerBytes.size.convert(),
                body_ptr = if (response.body.isEmpty()) null else body.addressOf(0).reinterpret(),
                body_len = response.body.size.convert(),
            ) != 0
        }
    }
}

private fun defaultFailureResponse(status: Int, message: String) = Hyper4kResponse.text(status, message)

// ---------------------------------------------------------------------------
// Streaming downstream channel (ABI v3)
// ---------------------------------------------------------------------------

/**
 * Dedicated pool for streaming writes.
 *
 * Not [Dispatchers.Default]: that pool is shared by every coroutine and sized to
 * the CPU count. `hyper4k_response_write` expresses backpressure by blocking its
 * thread, so a handful of slow clients would fill it and stall unrelated
 * requests. (`Dispatchers.IO` is still internal on Kotlin/Native.)
 *
 * Size it by the streams that are stuck at once, not the streams that are alive:
 * the channel has capacity, so a write returns immediately while the client
 * keeps up and holds no thread at all.
 */
@OptIn(DelicateCoroutinesApi::class)
private val streamWriteDispatcher by lazy { newFixedThreadPoolContext(32, "hyper4k-stream") }

/**
 * Maps [Hyper4kResponseChannel] onto the ABI v3 begin / write / finish calls.
 *
 * Every write hops to [streamWriteDispatcher] first. `hyper4k_response_write`
 * expresses backpressure by blocking its caller, and handlers start UNDISPATCHED,
 * so a handler that never suspends runs inline on a Tokio worker. Blocking there
 * spends an engine thread waiting on a slow client, which is the old
 * "blocked engine worker" problem in a quieter form.
 *
 * The engine backs this up: asked to block on an engine thread it returns
 * HYPER4K_ERR_WOULD_BLOCK instead. That code means the hop above failed, so it
 * is thrown rather than retried silently.
 */
@OptIn(ExperimentalForeignApi::class)
internal class NativeResponseChannel(private val responder: ULong) : Hyper4kResponseChannel {
    private var started = false
    private var finished = false
    private var written = 0L

    override val isStreaming: Boolean get() = started && !finished
    override val bytesWritten: Long get() = written

    override suspend fun begin(status: Int, headers: Map<String, List<String>>) {
        check(!started) { "hyper4k: response already begun" }
        val headerBytes = encodeHeaders(headers)
        // begin does not block, it only hands the head of the stream to the
        // connection task, so it stays put and can still hit the v2 sync fast path.
        val rc = headerBytes.usePinned { pinned ->
            hyper4k_response_begin(
                responder = responder,
                status = status.toUShort(),
                headers_ptr = if (headerBytes.isEmpty()) null else pinned.addressOf(0).reinterpret(),
                headers_len = headerBytes.size.convert(),
            )
        }
        check(rc == HYPER4K_OK) { "hyper4k_response_begin failed with $rc" }
        started = true
    }

    override suspend fun write(chunk: ByteArray): Boolean {
        check(started) { "hyper4k: write before begin" }
        check(!finished) { "hyper4k: write after finish" }
        if (chunk.isEmpty()) return true

        val rc = withContext(streamWriteDispatcher) {
            chunk.usePinned { pinned ->
                hyper4k_response_write(
                    responder = responder,
                    chunk_ptr = pinned.addressOf(0).reinterpret(),
                    chunk_len = chunk.size.convert(),
                )
            }
        }
        return when (rc) {
            HYPER4K_OK -> {
                written += chunk.size
                true
            }
            // The client closed its tab: stop producing data and close. Not an error.
            HYPER4K_ERR_CLIENT_GONE -> false
            HYPER4K_ERR_WOULD_BLOCK -> error(
                "hyper4k_response_write would block on an engine thread; " +
                    "streaming writes must run on a blocking-capable dispatcher",
            )
            else -> error("hyper4k_response_write failed with $rc")
        }
    }

    override suspend fun finish() {
        if (!started || finished) return
        finished = true
        val rc = hyper4k_response_finish(responder)
        check(rc == HYPER4K_OK || rc == HYPER4K_ERR_WRONG_STATE) {
            "hyper4k_response_finish failed with $rc"
        }
    }
}

private fun encodeHeaders(headers: Map<String, List<String>>): ByteArray =
    if (headers.isEmpty()) {
        ByteArray(0)
    } else {
        headers.entries
            .flatMap { (name, values) -> values.map { value -> "$name: $value" } }
            .joinToString("\n")
            .encodeToByteArray()
    }

/**
 * Runs [block] without arming a timer unless it actually suspends.
 *
 * withTimeout only observes cancellation at suspension points, so a handler that
 * never suspends gets no protection from it anyway; arming the timer eagerly on
 * every request just burns CPU on setup/teardown. The inline path stays
 * timer-free; the timer is armed only once the handler suspends.
 */
private suspend fun <R> lazyTimeout(timeoutMillis: Long, block: suspend () -> R): R = coroutineScope {
    var completedInline = false
    val worker = async(start = CoroutineStart.UNDISPATCHED) {
        val result = block()
        completedInline = true
        result
    }
    if (completedInline) worker.await() else withTimeout(timeoutMillis) { worker.await() }
}

private fun internalErrorResponse() = defaultFailureResponse(500, "Internal Server Error")

@OptIn(ExperimentalForeignApi::class)
private fun hyper4k.cinterop.Hyper4kSlice.copyToByteArray(): ByteArray {
    val size = len.toInt()
    val bytes = ptr
    if (size == 0 || bytes == null) return ByteArray(0)
    return bytes.readBytes(size)
}

@OptIn(ExperimentalForeignApi::class)
private fun hyper4k.cinterop.Hyper4kSlice.copyToString(): String = copyToByteArray().decodeToString()
