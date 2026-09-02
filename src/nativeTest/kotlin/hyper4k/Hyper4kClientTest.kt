package hyper4k

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The Kotlin wrapper against hyper4k's own server, over a real socket.
 * ClientAbiContractTest proves the C surface; this proves the obligations the
 * wrapper took on: ordering, backpressure, cancellation, the close/send race.
 */
class Hyper4kClientTest {

    private var nextPort = 18_940

    private fun <T> withServer(handler: Hyper4kStreamingHandler, block: suspend (port: Int) -> T): T {
        val port = nextPort++
        val server = Hyper4kServer(host = "127.0.0.1", port = port)
        server.start(handler)
        return try {
            runBlocking { withTimeout(20_000) { block(port) } }
        } finally {
            runBlocking { server.stop() }
        }
    }

    private suspend fun Hyper4kResponseStream.drain(): Triple<Hyper4kClientEvent.Headers, ByteArray, Hyper4kClientEvent.Done> {
        val headers = assertIs<Hyper4kClientEvent.Headers>(next())
        var body = ByteArray(0)
        while (true) {
            when (val e = next()) {
                is Hyper4kClientEvent.Chunk -> body += e.bytes
                is Hyper4kClientEvent.Done -> return Triple(headers, body, e)
                is Hyper4kClientEvent.Headers -> throw AssertionError("headers twice")
            }
        }
    }

    @Test
    fun roundTripsARequestInOrder() = withServer({ request, _ ->
        Hyper4kResponse.text(body = "echo:${request.method}:${request.path}:${request.body.decodeToString()}")
    }) { port ->
        val client = Hyper4kClient()
        try {
            val stream = client.send(
                Hyper4kClientRequest(
                    method = "POST",
                    url = "http://127.0.0.1:$port/items",
                    headers = listOf("X-Test" to "1"),
                    body = "payload".encodeToByteArray(),
                ),
            )
            val (headers, body, done) = stream.drain()
            assertEquals(200, headers.status)
            assertEquals(1, headers.version, "plaintext is HTTP/1.1")
            assertEquals("echo:POST:/items:payload", body.decodeToString())
            assertNull(done.error)
            assertTrue(stream.requestId > 0)
        } finally {
            client.close()
        }
    }

    @Test
    fun backpressurePausesTheEngineWhenTheConsumerLags() = withServer({ _, channel ->
        channel.begin(200)
        repeat(64) { channel.write(ByteArray(1024) { it.toByte() }) }
        channel.finish()
        Hyper4kResponse.streamed()
    }) { port ->
        val client = Hyper4kClient()
        try {
            val stream = client.send(Hyper4kClientRequest("GET", "http://127.0.0.1:$port/big"), chunkHighWater = 4)
            assertIs<Hyper4kClientEvent.Headers>(stream.next())
            // Read nothing for a while: with a high-water of 4 the engine has to
            // park, and the stream must survive being parked and resumed.
            delay(300)
            var total = 0
            while (true) {
                when (val e = stream.next()) {
                    is Hyper4kClientEvent.Chunk -> total += e.bytes.size
                    is Hyper4kClientEvent.Done -> { assertNull(e.error); break }
                    else -> throw AssertionError("unexpected $e")
                }
            }
            assertEquals(64 * 1024, total)
        } finally {
            client.close()
        }
    }

    @Test
    fun cancelDeliversExactlyOneDoneWithCancelled() {
        val release = CompletableDeferred<Unit>()
        withServer({ _, channel ->
            channel.begin(200)
            channel.write("first".encodeToByteArray())
            release.await()
            channel.finish()
            Hyper4kResponse.streamed()
        }) { port ->
            val client = Hyper4kClient()
            try {
                val stream = client.send(Hyper4kClientRequest("GET", "http://127.0.0.1:$port/slow"))
                assertIs<Hyper4kClientEvent.Headers>(stream.next())
                assertIs<Hyper4kClientEvent.Chunk>(stream.next())
                stream.cancel()
                stream.cancel()
                val done = assertIs<Hyper4kClientEvent.Done>(stream.next())
                val error = assertNotNull(done.error)
                assertEquals(Hyper4kErrorKind.CANCELLED, error.kind)
            } finally {
                release.complete(Unit)
                client.close()
            }
        }
    }

    @Test
    fun connectionRefusedSurfacesAsAnErrorKind() = runBlocking {
        val client = Hyper4kClient(Hyper4kClientOptions(maxRetries = 0))
        try {
            val stream = client.send(Hyper4kClientRequest("GET", "http://127.0.0.1:1/nothing"))
            val done = assertIs<Hyper4kClientEvent.Done>(stream.next())
            assertEquals(Hyper4kErrorKind.CONNECT, assertNotNull(done.error).kind)
        } finally {
            client.close()
        }
    }

    @Test
    fun sendAfterCloseIsRefusedAndCloseIsIdempotent() = runBlocking<Unit> {
        val client = Hyper4kClient()
        client.close()
        client.close()
        assertFailsWith<Hyper4kClientClosedException> {
            client.send(Hyper4kClientRequest("GET", "http://127.0.0.1:1/x"))
        }
    }

    @Test
    fun closeWithAParkedRequestDoesNotHang() = withServer({ _, channel ->
        channel.begin(200)
        repeat(64) { channel.write(ByteArray(1024)) }
        channel.finish()
        Hyper4kResponse.streamed()
    }) { port ->
        val client = Hyper4kClient()
        val stream = client.send(Hyper4kClientRequest("GET", "http://127.0.0.1:$port/big"), chunkHighWater = 2)
        assertIs<Hyper4kClientEvent.Headers>(stream.next())
        delay(200) // engine is now parked on this request
        // The ABI promises close() releases parked requests itself; a hang here
        // is exactly the bug that promise exists to rule out.
        withTimeout(5_000) { client.close() }
    }

    @Test
    fun engineReportsItsCapabilities() {
        val caps = Hyper4kClient.engineCapabilities()
        assertTrue(caps.http1 && caps.http2 && caps.tls && caps.cancel && caps.streaming, caps.toString())
    }
}
