package hyper4k

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import platform.posix.AF_INET
import platform.posix.SOCK_STREAM
import platform.posix.SOL_SOCKET
import platform.posix.SO_RCVTIMEO
import platform.posix.close
import platform.posix.connect
import platform.posix.getsockname
import platform.posix.recv
import platform.posix.send
import platform.posix.setsockopt
import platform.posix.sockaddr
import platform.posix.sockaddr_in
import platform.posix.socket
import platform.posix.socklen_tVar
import platform.posix.timeval
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * End-to-end cover for the streaming path: Kotlin channel -> cinterop -> Rust ABI
 * -> a real socket. The fake-channel tests stop at the Kotlin boundary, so the
 * pointer and length marshalling in [NativeResponseChannel] only runs here.
 *
 * Apple targets only, since it talks BSD sockets directly. The protocol behaviour
 * itself is covered on every platform by the Rust tests in lib/src/lib.rs.
 */
@OptIn(ExperimentalForeignApi::class)
class Hyper4kStreamingSocketTest {

    @Test
    fun deliversTheFirstEventBeforeTheLastOneIsWritten() = runBlocking {
        val firstSent = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val port = freePort()

        val server = Hyper4kServer(host = "127.0.0.1", port = port)
        server.start { _, channel ->
            channel.begin(200, mapOf("Content-Type" to listOf("text/event-stream")))
            channel.write("data: event-1\n\n".encodeToByteArray())
            firstSent.complete(Unit)
            // The last event is held back until the test confirms it read the first,
            // so "streamed, not buffered" holds structurally rather than by timing.
            release.await()
            channel.write("data: event-2\n\n".encodeToByteArray())
            channel.finish()
            Hyper4kResponse.streamed()
        }

        val client = Socket(port)
        try {
            client.send("GET /events HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")

            val head = client.readUntil("data: event-1")
            assertTrue(head.startsWith("HTTP/1.1 200 OK"), head)
            assertFalse(head.contains("data: event-2"), "response was buffered, not streamed: $head")
            withTimeout(5_000) { firstSent.await() }

            release.complete(Unit)
            val rest = client.readUntil("data: event-2")
            assertTrue(rest.contains("data: event-2"), rest)
        } finally {
            client.close()
            server.stop()
        }
    }

    @Test
    fun reportsClientGoneAfterTheSocketIsClosed() = runBlocking {
        val gone = CompletableDeferred<Boolean>()
        val started = CompletableDeferred<Unit>()
        val closed = CompletableDeferred<Unit>()
        val port = freePort()

        val server = Hyper4kServer(host = "127.0.0.1", port = port)
        server.start { _, channel ->
            channel.begin(200, mapOf("Content-Type" to listOf("text/event-stream")))
            channel.write("data: first\n\n".encodeToByteArray())
            started.complete(Unit)
            closed.await()
            // Enough chunks to outrun the body channel's capacity, so the write has to
            // notice the receiver is gone instead of parking in the buffer.
            var accepted = true
            repeat(32) { if (accepted) accepted = channel.write("data: more\n\n".encodeToByteArray()) }
            gone.complete(!accepted)
            channel.finish()
            Hyper4kResponse.streamed()
        }

        val client = Socket(port)
        client.send("GET /events HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        client.readUntil("data: first")
        withTimeout(5_000) { started.await() }
        client.close()
        closed.complete(Unit)

        // A client closing its tab is a normal path, and the handler learns about it.
        assertEquals(true, withTimeout(5_000) { gone.await() })
        server.stop()
    }
}

/** 127.0.0.1 in network byte order, as laid out on the little-endian Apple targets. */
private const val LOOPBACK: UInt = 0x0100007Fu

@OptIn(ExperimentalForeignApi::class)
private fun freePort(): Int = memScoped {
    val fd = socket(AF_INET, SOCK_STREAM, 0)
    check(fd >= 0) { "socket() failed" }
    try {
        val addr = alloc<sockaddr_in>()
        addr.sin_family = AF_INET.convert()
        addr.sin_addr.s_addr = LOOPBACK
        addr.sin_port = 0u
        check(platform.posix.bind(fd, addr.ptr.reinterpret<sockaddr>(), sizeOf<sockaddr_in>().convert()) == 0) {
            "bind() failed"
        }
        val length = alloc<socklen_tVar>()
        length.value = sizeOf<sockaddr_in>().convert()
        check(getsockname(fd, addr.ptr.reinterpret<sockaddr>(), length.ptr) == 0) { "getsockname() failed" }
        val networkOrder = addr.sin_port.toInt()
        ((networkOrder and 0xFF) shl 8) or ((networkOrder shr 8) and 0xFF)
    } finally {
        close(fd)
    }
}

/** Minimal blocking client. Only what these two tests need. */
@OptIn(ExperimentalForeignApi::class)
private class Socket(port: Int) {
    private val fd = socket(AF_INET, SOCK_STREAM, 0)
    private var isClosed = false

    init {
        check(fd >= 0) { "socket() failed" }
        memScoped {
            val addr = alloc<sockaddr_in>()
            addr.sin_family = AF_INET.convert()
            addr.sin_addr.s_addr = LOOPBACK
            addr.sin_port = (((port and 0xFF) shl 8) or ((port shr 8) and 0xFF)).convert()
            check(connect(fd, addr.ptr.reinterpret<sockaddr>(), sizeOf<sockaddr_in>().convert()) == 0) {
                "connect() to 127.0.0.1:$port failed"
            }
            // Without a timeout a regression here would hang the test run instead of failing.
            val timeout = alloc<timeval>()
            timeout.tv_sec = 5
            timeout.tv_usec = 0
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, timeout.ptr, sizeOf<timeval>().convert())
        }
    }

    fun send(text: String) {
        val bytes = text.encodeToByteArray()
        bytes.usePinned { pinned ->
            var sent = 0
            while (sent < bytes.size) {
                val n = send(fd, pinned.addressOf(sent), (bytes.size - sent).convert(), 0).toInt()
                check(n > 0) { "send() failed" }
                sent += n
            }
        }
    }

    /** Reads until [marker] shows up, and returns everything read so far. */
    fun readUntil(marker: String): String {
        val buffer = ByteArray(4096)
        val seen = StringBuilder()
        while (!seen.contains(marker)) {
            val n = buffer.usePinned { pinned ->
                recv(fd, pinned.addressOf(0), buffer.size.convert(), 0).toInt()
            }
            check(n > 0) { "connection closed or timed out while waiting for \"$marker\", got: $seen" }
            seen.append(buffer.decodeToString(0, n))
        }
        return seen.toString()
    }

    fun close() {
        if (!isClosed) {
            isClosed = true
            close(fd)
        }
    }
}
