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
            handler = {
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
            handler = {
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
            handler = {
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
            handler = {
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

    private fun request() = Hyper4kRequest(
        method = "GET",
        path = "/",
        query = "",
        rawHeaders = "",
        body = ByteArray(0),
    )
}
