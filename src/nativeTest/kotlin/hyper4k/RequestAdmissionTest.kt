package hyper4k

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.test.Test
import kotlin.test.assertEquals

@OptIn(ExperimentalAtomicApi::class, DelicateCoroutinesApi::class)
class RequestAdmissionTest {
    @Test
    fun concurrentAdmissionIsBoundedAndEveryRequestCompletesOnce() = runBlocking {
        val workers = newFixedThreadPoolContext(8, "admission-test")
        try {
            repeat(10) {
                val release = CompletableDeferred<Unit>()
                val accepted = AtomicInt(0)
                val refused = AtomicInt(0)
                val completed = List(128) { AtomicInt(0) }
                val dispatcher = AsyncRequestDispatcher(
                    handler = { _, _ ->
                        accepted.fetchAndAdd(1)
                        release.await()
                        Hyper4kResponse.text(body = "ok")
                    },
                    maxConcurrentRequests = 7,
                    requestTimeoutMillis = 0,
                    complete = { id, response ->
                        completed[id.toInt()].fetchAndAdd(1)
                        if (response.status == 503) refused.fetchAndAdd(1)
                        true
                    },
                )
                try {
                    withContext(workers) {
                        coroutineScope {
                            repeat(128) { id ->
                                launch {
                                    dispatcher.submit(Hyper4kRequest("GET", "/", "", "", ByteArray(0)), id.toULong())
                                }
                            }
                        }
                    }
                    assertEquals(7, accepted.load())
                    assertEquals(121, refused.load())
                    release.complete(Unit)
                    dispatcher.awaitDrained(5_000)
                    completed.forEach { assertEquals(1, it.load()) }
                    dispatcher.stopAccepting()
                } finally {
                    release.complete(Unit)
                    dispatcher.cancelAndJoin()
                }
            }
        } finally {
            workers.close()
        }
    }
}
