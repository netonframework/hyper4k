package hyper4k

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The encoder has an ASCII fast path that copies char-by-char. Anything wider
 * would be truncated by that, so these pin both branches against the same
 * expected bytes.
 */
class EncodeHeadersTest {

    @Test
    fun commonContentTypesPreserveExactBytes() {
        for (type in listOf("text/plain; charset=utf-8", "application/json",
            "application/json; charset=utf-8", "text/html; charset=utf-8")) {
            assertEquals("Content-Type: $type", encode(mapOf("Content-Type" to listOf(type))))
        }
    }

    @Test
    fun commonTypeDoesNotHideOtherHeadersOrRepeatedValues() {
        assertEquals("Content-Type: application/json\nSet-Cookie: a=1\nSet-Cookie: b=2",
            encode(linkedMapOf("Content-Type" to listOf("application/json"),
                "Set-Cookie" to listOf("a=1", "b=2"))))
        assertEquals("Content-Type: application/json\nContent-Type: text/plain",
            encode(mapOf("Content-Type" to listOf("application/json", "text/plain"))))
        assertEquals("content-type: application/json",
            encode(mapOf("content-type" to listOf("application/json"))))
    }

    @Test
    fun changesToSourceHeadersDoNotChangePreviouslyEncodedBytes() {
        val values = mutableListOf("application/json")
        val headers = mutableMapOf("Content-Type" to values)
        val first = encodeHeadersForTest(headers)
        values[0] = "custom/type"
        assertEquals("Content-Type: application/json", first.decodeToString())
        assertEquals("Content-Type: custom/type", encode(headers))
        headers.clear()
        assertEquals("", encode(headers))
    }

    private fun encode(h: Map<String, List<String>>) =
        encodeHeadersForTest(h).decodeToString()

    @Test
    fun encodesOneHeaderPerLine() {
        assertEquals(
            "Content-Type: text/plain\nX-Trace: abc",
            encode(mapOf("Content-Type" to listOf("text/plain"), "X-Trace" to listOf("abc"))),
        )
    }

    @Test
    fun repeatedNamesBecomeSeparateLines() {
        assertEquals(
            "Set-Cookie: a=1\nSet-Cookie: b=2",
            encode(mapOf("Set-Cookie" to listOf("a=1", "b=2"))),
        )
    }

    @Test
    fun anEmptyMapEncodesToNothing() {
        assertEquals(0, encodeHeadersForTest(emptyMap()).size)
    }

    @Test
    fun anEmptyValueStillProducesItsLine() {
        assertEquals("X-Empty: ", encode(mapOf("X-Empty" to listOf(""))))
    }

    /** The case the ASCII fast path would corrupt. */
    @Test
    fun aNonAsciiValueSurvivesAsUtf8() {
        val encoded = encodeHeadersForTest(mapOf("X-Name" to listOf("中文值")))
        assertEquals("X-Name: 中文值", encoded.decodeToString())
        // Byte-for-byte, not just decodable: three bytes per character.
        assertEquals("X-Name: ".length + 9, encoded.size)
    }

    @Test
    fun mixedAsciiAndNonAsciiTakesTheCorrectPath() {
        assertEquals(
            "A: one\nB: 二",
            encode(mapOf("A" to listOf("one"), "B" to listOf("二"))),
        )
    }
}

class EncodeHeadersSizingTest {
    /** The buffer must be sized exactly: no trailing slack, no second copy. */
    @Test
    fun theEncodedBlockHasNoTrailingByte() {
        val one = encodeHeadersForTest(mapOf("A" to listOf("b")))
        assertEquals("A: b".length, one.size)
        val two = encodeHeadersForTest(mapOf("A" to listOf("b"), "C" to listOf("d")))
        assertEquals("A: b\nC: d".length, two.size)
        val repeated = encodeHeadersForTest(mapOf("S" to listOf("1", "2", "3")))
        assertEquals("S: 1\nS: 2\nS: 3".length, repeated.size)
    }

    @Test
    fun aNameWithOnlyEmptyValueListEncodesToNothing() {
        assertEquals(0, encodeHeadersForTest(mapOf("Empty" to emptyList())).size)
    }
}
