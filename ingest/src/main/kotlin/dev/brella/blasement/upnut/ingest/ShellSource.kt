@file:Suppress("SuspendFunctionOnCoroutineScope")

package dev.brella.blasement.upnut.ingest

import dev.brella.blasement.upnut.common.UpNutEvent
import dev.brella.blasement.upnut.common.get
import dev.brella.blasement.upnut.common.getValue
import dev.brella.blasement.upnut.common.loopEvery
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.statement.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.intOrNull
import org.slf4j.Logger
import org.springframework.r2dbc.core.DatabaseClient
import java.time.Clock
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

public interface ShellSource {
    public suspend fun CoroutineScope.processNuts(mailbox: SendChannel<Pair<Long, List<UpNutEvent>>>)

    @OptIn(ExperimentalTime::class)
    public abstract class PaginatedUrl(
        val loopEvery: Duration,
        val limit: Int,
        val delayBetweenLoops: Duration,
        val totalLimit: Long,
        val logger: Logger,
        val sortingByHot: Boolean? = false
    ) : ShellSource {
        override suspend fun CoroutineScope.processNuts(mailbox: SendChannel<Pair<Long, List<UpNutEvent>>>) {
            val etags: MutableMap<Int, String> = HashMap()

            loopEvery(loopEvery, `while` = { isActive }) {
                try {
                    var start = 0

                    while (isActive && start < totalLimit) {
                        val now = now()
                        val response = retrievePage(start, limit, now)
                        val existingTag = etags[start]
                        val responseTag = response.headers["Etag"]
                        if (existingTag != null && existingTag == responseTag) {
                            start += limit
                            logger.trace("Hit ETag; continuing at {}", start)
                        } else {
                            val list = response.responseToPageList()
                                .trimIf(sortingByHot == true)

                            mailbox.send(Pair(now, list))

                            if (list.size < limit) break

                            responseTag?.let { etags[start] = it }
                            start += list.size

                            delay(delayBetweenLoops)
                        }
                    }
                } catch (th: Throwable) {
                    logger.error("Caught error when looping through {}: ", this, th)
                }
            }
        }


        abstract suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse
        protected abstract suspend fun HttpResponse.responseToPageList(): List<UpNutEvent>
    }


    @OptIn(ExperimentalTime::class)
    public class GlobalFeed(loopEvery: Duration, limit: Int, delayBetweenLoops: Duration, totalLimit: Long, val httpClient: HttpClient, logger: Logger, val sortBy: Int? = null, val category: Int? = null) : PaginatedUrl(
        loopEvery,
        limit,
        delayBetweenLoops,
        totalLimit,
        logger,
        sortBy == BLASEBALL_SORTING_HOT
    ) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getGlobalFeedAsResponse(limit = limit, sort = sortBy, category = category, start = start)

        override suspend fun HttpResponse.responseToPageList(): List<UpNutEvent> =
            receive()
    }

    @OptIn(ExperimentalTime::class)
    public class TeamFeed(
        val teamID: String,
        loopEvery: Duration,
        limit: Int,
        delayBetweenLoops: Duration,
        totalLimit: Long,
        val httpClient: HttpClient,
        logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null
    ) : PaginatedUrl(loopEvery, limit, delayBetweenLoops, totalLimit, logger, sortBy == BLASEBALL_SORTING_HOT) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getTeamFeedAsResponse(teamID, limit = limit, sort = sortBy, category = category, start = start)

        override suspend fun HttpResponse.responseToPageList(): List<UpNutEvent> =
            receive()
    }

    @OptIn(ExperimentalTime::class)
    public class PlayerFeed(
        val playerID: String,
        loopEvery: Duration,
        limit: Int,
        delayBetweenLoops: Duration,
        totalLimit: Long,
        val httpClient: HttpClient,
        logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null
    ) : PaginatedUrl(loopEvery, limit, delayBetweenLoops, totalLimit, logger, sortBy == BLASEBALL_SORTING_HOT) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getPlayerFeedAsResponse(playerID, limit = limit, sort = sortBy, category = category, start = start)

        override suspend fun HttpResponse.responseToPageList(): List<UpNutEvent> =
            receive()
    }

    @OptIn(ExperimentalTime::class)
    public class GameFeed(
        val gameID: String,
        loopEvery: Duration,
        limit: Int,
        delayBetweenLoops: Duration,
        totalLimit: Long,
        val httpClient: HttpClient,
        logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null
    ) : PaginatedUrl(loopEvery, limit, delayBetweenLoops, totalLimit, logger, sortBy == BLASEBALL_SORTING_HOT) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getGameFeedAsResponse(gameID, limit = limit, sort = sortBy, category = category, start = start)

        override suspend fun HttpResponse.responseToPageList(): List<UpNutEvent> =
            receive()
    }

    @OptIn(ExperimentalTime::class)
    public class StoryFeed(
        val storyID: String,
        loopEvery: Duration,
        limit: Int,
        delayBetweenLoops: Duration,
        totalLimit: Long,
        val httpClient: HttpClient,
        logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null
    ) : PaginatedUrl(loopEvery, limit, delayBetweenLoops, totalLimit, logger, sortBy == BLASEBALL_SORTING_HOT) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getStoryFeedAsResponse(storyID, limit = limit, sort = sortBy, category = category, start = start)

        override suspend fun HttpResponse.responseToPageList(): List<UpNutEvent> =
            receive()
    }

    @ExperimentalTime
    public class Librarian(
        val loopEvery: Duration,
        val limit: Int,
        val delayBetweenLoops: Duration,
        val totalLimit: Long,
        val httpClient: HttpClient,
        val databaseClient: DatabaseClient,
        val logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null
    ) : ShellSource {
        override suspend fun CoroutineScope.processNuts(mailbox: SendChannel<Pair<Long, List<UpNutEvent>>>) {
            val etags: MutableMap<Int, String> = HashMap()
            val shouldTrim = sortBy == BLASEBALL_SORTING_HOT || sortBy == BLASEBALL_SORTING_TOP

            val metadataAdjustment = actor<Pair<String, List<UpNutEvent>>> {
                receiveAsFlow()
                    .onEach { (storyID, list) ->
                        databaseClient.inConnectionAwait { connection ->
                            if (list.isEmpty()) return@inConnectionAwait

                            val statement =
                                connection.createStatement("INSERT INTO storytime (feed_id, story_id) VALUES ( \$1, \$2 ) ON CONFLICT DO NOTHING")

                            val storyUUID = UUID.fromString(storyID)

                            list.forEach { event ->
                                statement.bind("$1", event.id)
                                    .bind("$2", storyUUID)
                                    .add()
                            }

                            statement.awaitRowsUpdated()
                        }
                    }.launchIn(this)
                    .join()
            }

            loopEvery(loopEvery, `while` = { isActive }) {
                try {
                    val storyList = databaseClient.sql("SELECT id FROM library WHERE redacted = FALSE")
                        .map { row -> row.getValue<UUID>("id").toString() }
                        .all()
                        .collectList()
                        .await()

                    logger.info("Retrieving nuts and scales for {}", storyList)

                    storyList.forEach { story ->
                        var start = 0

                        while (isActive && start < totalLimit) {
                            val now = now()

                            val response = httpClient.getStoryFeedAsResponse(story, limit = limit, sort = sortBy, category = category, start = start)
                            val existingTag = etags[start]
                            val responseTag = response.headers["Etag"]
                            if (existingTag != null && existingTag == responseTag) {
                                start += limit
                                logger.trace("Hit ETag; continuing at {}", start)
                            } else {
                                val list = response.receive<List<UpNutEvent>>()
                                    .trimIf(shouldTrim)

                                mailbox.send(Pair(now, list))
                                metadataAdjustment.send(Pair(story, list))

                                if (list.size < limit) break

                                responseTag?.let { etags[start] = it }
                                start += list.size

                                delay(delayBetweenLoops)
                            }
                        }
                    }
                } catch (th: Throwable) {
                    logger.error("If someone screams in the library and there's no librarian, does it throw an error?", th)
                }
            }
        }
    }
}

public suspend inline fun ShellSource.processNuts(scope: CoroutineScope, sendTo: SendChannel<Pair<Long, List<UpNutEvent>>>) =
    scope.processNuts(sendTo)

inline fun nowInstant() = Instant.now(Clock.systemUTC())
inline fun now() = Instant.now(Clock.systemUTC()).toEpochMilli()

inline fun List<UpNutEvent>.trimIf(condition: Boolean): List<UpNutEvent> =
    if (condition) takeWhile { event ->
        (event.nuts.intOrNull ?: 0) > 0 ||
        (event.scales.intOrNull ?: 0) > 0
    } else this