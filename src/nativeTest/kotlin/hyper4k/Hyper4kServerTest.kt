package hyper4k

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Hyper4kServerTest {
    @Test
    fun preservesRepeatedRequestHeaders() {
        val request = Hyper4kRequest("GET", "/", "", "X-Test: one\nx-test: two\n", ByteArray(0))

        assertEquals(listOf("one", "two"), request.headers["X-Test"])
        assertEquals("one", request.header("x-test"))
    }

    @Test
    fun startsAndStops() = runBlocking {
        val server = Hyper4kServer(host = "127.0.0.1", port = 0)

        server.start { Hyper4kResponse.text(body = "ok") }
        assertTrue(server.isRunning)

        server.stop()
        assertFalse(server.isRunning)
    }

    @Test
    fun suspendingHandlerCompletesAsynchronously() = runBlocking {
        val response = CompletableDeferred<Hyper4kResponse>()
        val dispatcher = AsyncRequestDispatcher(
            handler = { _, _ ->
                delay(20)
                Hyper4kResponse.text(body = "async")
            },
            maxConcurrentRequests = 1,
            requestTimeoutMillis = 1_000,
            complete = { _, value -> response.complete(value) },
        )

        dispatcher.submit(request(), 1uL)

        assertEquals("async", response.await().body.decodeToString())
        dispatcher.stopAccepting()
        dispatcher.awaitDrained(1_000)
        dispatcher.cancelAndJoin()
    }

    @Test
    fun rejectsOverloadInsteadOfGrowingAnUnboundedQueue() = runBlocking {
        val release = CompletableDeferred<Unit>()
        val responses = mutableMapOf<ULong, Hyper4kResponse>()
        val secondResponse = CompletableDeferred<Unit>()
        val dispatcher = AsyncRequestDispatcher(
            handler = { _, _ ->
                release.await()
                Hyper4kResponse.text(body = "first")
            },
            maxConcurrentRequests = 1,
            requestTimeoutMillis = 1_000,
            complete = { id, value ->
                responses[id] = value
                if (id == 2uL) secondResponse.complete(Unit)
                true
            },
        )

        dispatcher.submit(request(), 1uL)
        dispatcher.submit(request(), 2uL)
        secondResponse.await()

        assertEquals(503, responses.getValue(2uL).status)
        release.complete(Unit)
        dispatcher.stopAccepting()
        dispatcher.awaitDrained(1_000)
        dispatcher.cancelAndJoin()
    }

    @Test
    fun timesOutSlowHandler() = runBlocking {
        val response = CompletableDeferred<Hyper4kResponse>()
        val dispatcher = AsyncRequestDispatcher(
            handler = { _, _ ->
                delay(1_000)
                Hyper4kResponse.text(body = "late")
            },
            maxConcurrentRequests = 1,
            requestTimeoutMillis = 10,
            complete = { _, value -> response.complete(value) },
        )

        dispatcher.submit(request(), 1uL)

        assertEquals(504, response.await().status)
        dispatcher.stopAccepting()
        dispatcher.awaitDrained(1_000)
        dispatcher.cancelAndJoin()
    }

    @Test
    fun releasesConcurrencySlotWhenResponseWriterFails() = runBlocking {
        var invocation = 0
        val secondStarted = CompletableDeferred<Unit>()
        val firstCompletionAttempted = CompletableDeferred<Unit>()
        val dispatcher = AsyncRequestDispatcher(
            handler = { _, _ ->
                invocation += 1
                if (invocation == 2) secondStarted.complete(Unit)
                Hyper4kResponse.text(body = "ok")
            },
            maxConcurrentRequests = 1,
            requestTimeoutMillis = 1_000,
            complete = { id, _ ->
                if (id == 1uL) firstCompletionAttempted.complete(Unit)
                error("write failed")
            },
        )

        dispatcher.submit(request(), 1uL)
        firstCompletionAttempted.await()
        delay(5)
        dispatcher.submit(request(), 2uL)

        withTimeout(1_000) { secondStarted.await() }
        dispatcher.stopAccepting()
        dispatcher.awaitDrained(1_000)
        dispatcher.cancelAndJoin()
    }

    @Test
    fun streamingHandlerBypassesTheOneShotWriter() = runBlocking {
        val channel = RecordingChannel()
        val finished = CompletableDeferred<Unit>()
        var oneShotWrites = 0
        val dispatcher = AsyncRequestDispatcher(
            handler = { _, out ->
                out.begin(200, mapOf("Content-Type" to listOf("text/event-stream")))
                out.write("data: 1\n\n".encodeToByteArray())
                out.write("data: 2\n\n".encodeToByteArray())
                out.finish()
                finished.complete(Unit)
                Hyper4kResponse.streamed()
            },
            maxConcurrentRequests = 1,
            requestTimeoutMillis = 1_000,
            complete = { _, _ -> oneShotWrites += 1; true },
            newChannel = { channel },
        )

        dispatcher.submit(request(), 1uL)
        withTimeout(1_000) { finished.await() }

        // A streamed response must not be written out again, that sends headers twice.
        assertEquals(0, oneShotWrites)
        assertEquals(200, channel.status)
        assertEquals(listOf("data: 1\n\n", "data: 2\n\n"), channel.chunks)
        assertTrue(channel.isFinished)
        dispatcher.stopAccepting()
        dispatcher.awaitDrained(1_000)
        dispatcher.cancelAndJoin()
    }

    @Test
    fun finishesTheStreamWhenAStreamingHandlerFails() = runBlocking {
        val channel = RecordingChannel()
        var oneShotWrites = 0
        val dispatcher = AsyncRequestDispatcher(
            handler = { _, out ->
                out.begin(200)
                out.write("partial".encodeToByteArray())
                error("handler blew up halfway through")
            },
            maxConcurrentRequests = 1,
            requestTimeoutMillis = 1_000,
            complete = { _, _ -> oneShotWrites += 1; true },
            newChannel = { channel },
        )

        dispatcher.submit(request(), 1uL)
        dispatcher.stopAccepting()
        dispatcher.awaitDrained(1_000)

        // The headers are already out, so the 5xx fallback cannot be written.
        // Closing the stream is the only correct move.
        assertEquals(0, oneShotWrites)
        assertTrue(channel.isFinished)
        dispatcher.cancelAndJoin()
    }

    /** Fake channel that records calls, so dispatch logic is testable without a responder. */
    private class RecordingChannel : Hyper4kResponseChannel {
        var status: Int? = null
        val chunks = mutableListOf<String>()
        var isFinished = false
        private var begun = false

        override val isStreaming: Boolean get() = begun && !isFinished
        override var bytesWritten: Long = 0L
            private set

        override suspend fun begin(status: Int, headers: Map<String, List<String>>) {
            this.status = status
            begun = true
        }

        override suspend fun write(chunk: ByteArray): Boolean {
            chunks.add(chunk.decodeToString())
            bytesWritten += chunk.size
            return true
        }

        override suspend fun finish() {
            isFinished = true
        }
    }

    private fun request() = Hyper4kRequest(
        method = "GET",
        path = "/",
        query = "",
        rawHeaders = "",
        body = ByteArray(0),
    )
}
