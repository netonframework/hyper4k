package hyper4k

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * `header()` scans the raw block instead of parsing it, so it has to agree with
 * `headers` on every shape the parser accepts — otherwise a request would see a
 * different value depending on whether something else happened to touch the map
 * first.
 */
class RequestHeaderLookupTest {

    private fun req(raw: String) = Hyper4kRequest("GET", "/", "", raw, ByteArray(0))

    private fun bothAgree(raw: String, name: String) {
        val scanned = req(raw).header(name)
        val viaMap = req(raw).let { it.headers; it.header(name) }
        assertEquals(viaMap, scanned, "scan and map disagree on '$name' in <<$raw>>")
    }

    @Test
    fun findsAHeaderRegardlessOfCase() {
        val raw = "Host: example.com\nX-Request-Id: abc-123\nAccept: */*\n"
        assertEquals("abc-123", req(raw).header("x-request-id"))
        assertEquals("abc-123", req(raw).header("X-REQUEST-ID"))
        assertEquals("example.com", req(raw).header("host"))
        bothAgree(raw, "X-Request-Id")
    }

    @Test
    fun trimsSurroundingWhitespaceAndCarriageReturns() {
        val raw = "X-Trace:   spaced   \r\nTight:v\n"
        assertEquals("spaced", req(raw).header("X-Trace"))
        assertEquals("v", req(raw).header("Tight"))
        bothAgree(raw, "X-Trace")
    }

    @Test
    fun missingHeaderIsNull() {
        val raw = "Host: example.com\n"
        assertNull(req(raw).header("X-Request-Id"))
        assertNull(req("").header("Host"))
        bothAgree(raw, "X-Request-Id")
    }

    @Test
    fun firstValueWinsWhenAHeaderRepeats() {
        val raw = "Accept: a\nAccept: b\n"
        assertEquals("a", req(raw).header("Accept"))
        bothAgree(raw, "Accept")
    }

    @Test
    fun aValueMayBeEmptyAndAMalformedLineIsSkipped() {
        val raw = "Empty:\nnot-a-header\nAfter: yes\n"
        assertEquals("", req(raw).header("Empty"))
        assertEquals("yes", req(raw).header("After"))
        assertNull(req(raw).header("not-a-header"))
        bothAgree(raw, "After")
    }

    @Test
    fun aNameThatIsAPrefixOfAnotherDoesNotMatchIt() {
        val raw = "X-Request-Id-Extra: no\nX-Request-Id: yes\n"
        assertEquals("yes", req(raw).header("X-Request-Id"))
        bothAgree(raw, "X-Request-Id")
    }

    @Test
    fun aValueMayHoldNonAsciiBytes() {
        val raw = "X-Name: 中文值\n"
        assertEquals("中文值", req(raw).header("X-Name"))
        bothAgree(raw, "X-Name")
    }

    @Test
    fun snapshotConstructorPreservesNonAsciiNameLookup() {
        // Network field names are ASCII, but the public snapshot constructor
        // previously allowed arbitrary UTF-8 names. Keep that behavior.
        val request = req("X-名字: value\nX-Name: ascii\n")
        assertEquals("value", request.header("x-名字"))
        assertNull(request.header("x-名"))
        assertEquals("ascii", request.header("X-NAME"))
        assertEquals("value", request.headers["X-名字"]?.first())
        assertEquals("value", request.header("x-名字"))
    }
}
