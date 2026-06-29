package hyper4k

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Hyper4kServerTest {
    @Test
    fun startsAndStops() {
        val server = Hyper4kServer(host = "127.0.0.1", port = 0)

        server.start { Hyper4kResponse.text(body = "ok") }
        assertTrue(server.isRunning)

        server.stop()
        assertFalse(server.isRunning)
    }
}
