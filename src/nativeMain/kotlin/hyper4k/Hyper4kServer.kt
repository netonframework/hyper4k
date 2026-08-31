package hyper4k

import hyper4k.cinterop.Hyper4kRequest as CHyper4kRequest
import hyper4k.cinterop.hyper4k_respond
import hyper4k.cinterop.hyper4k_server_start
import hyper4k.cinterop.hyper4k_server_stop
import kotlinx.cinterop.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
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
    private val handler: Hyper4kHandler,
    maxConcurrentRequests: Int,
    private val requestTimeoutMillis: Long,
    private val failureResponse: (status: Int, message: String) -> Hyper4kResponse = ::defaultFailureResponse,
    private val complete: (ULong, Hyper4kResponse) -> Boolean,
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
            val response = try {
                if (requestTimeoutMillis > 0) {
                    lazyTimeout(requestTimeoutMillis) { handler(request) }
                } else {
                    handler(request)
                }
            } catch (_: TimeoutCancellationException) {
                failureResponse(504, "Gateway Timeout")
            } catch (_: CancellationException) {
                failureResponse(503, "Service Unavailable")
            } catch (_: Throwable) {
                failureResponse(500, "Internal Server Error")
            }
            finish(responder, response)
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
    val headerBytes = if (response.headers.isEmpty()) {
        ByteArray(0)
    } else {
        response.headers.entries
            .flatMap { (name, values) -> values.map { value -> "$name: $value" } }
            .joinToString("\n")
            .encodeToByteArray()
    }
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
