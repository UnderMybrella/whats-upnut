@file:Suppress("SuspendFunctionOnCoroutineScope")

package dev.brella.blasement.upnut.ingest

import dev.brella.blasement.upnut.common.BlaseballSource
import dev.brella.blasement.upnut.common.UpNutEvent
import dev.brella.blasement.upnut.common.UpNutIngest
import dev.brella.blasement.upnut.common.get
import dev.brella.blasement.upnut.common.getStringOrNull
import dev.brella.blasement.upnut.common.getValue
import dev.brella.blasement.upnut.common.loopEvery
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.statement.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import org.slf4j.Logger
import org.springframework.r2dbc.core.DatabaseClient
import java.time.Clock
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

public interface ShellSource {
    public suspend fun CoroutineScope.processNuts(mailbox: SendChannel<UpNutIngest>)

    @OptIn(ExperimentalTime::class)
    public abstract class PaginatedUrl(
        val loopEvery: Duration,
        val limit: Int,
        val delayBetweenLoops: Duration,
        val totalLimit: Long,
        val logger: Logger,
        val source: BlaseballSource,
        val sortingByHot: Boolean? = false
    ) : ShellSource {
        override suspend fun CoroutineScope.processNuts(mailbox: SendChannel<UpNutIngest>) {
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

                            mailbox.send(UpNutIngest(now, source, list))

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
    public class GlobalFeed(
        loopEvery: Duration,
        limit: Int,
        delayBetweenLoops: Duration,
        totalLimit: Long,
        val httpClient: HttpClient,
        logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null,
        val type: Int? = null
    ) : PaginatedUrl(
        loopEvery,
        limit,
        delayBetweenLoops,
        totalLimit,
        logger,
        BlaseballSource.global(),
        sortBy == BLASEBALL_SORTING_HOT
    ) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getGlobalFeedAsResponse(limit = limit, sort = sortBy, category = category, type = type, start = start)

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
        val category: Int? = null,
        val type: Int? = null
    ) : PaginatedUrl(
        loopEvery,
        limit,
        delayBetweenLoops,
        totalLimit,
        logger,
        BlaseballSource.team(UUID.fromString(teamID)),
        sortBy == BLASEBALL_SORTING_HOT
    ) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getTeamFeedAsResponse(teamID, limit = limit, sort = sortBy, category = category, type = type, start = start)

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
        val category: Int? = null,
        val type: Int? = null
    ) : PaginatedUrl(
        loopEvery,
        limit,
        delayBetweenLoops,
        totalLimit,
        logger,
        BlaseballSource.player(UUID.fromString(playerID)),
        sortBy == BLASEBALL_SORTING_HOT
    ) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getPlayerFeedAsResponse(playerID, limit = limit, sort = sortBy, category = category, type = type, start = start)

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
        val category: Int? = null,
        val type: Int? = null
    ) : PaginatedUrl(
        loopEvery,
        limit,
        delayBetweenLoops,
        totalLimit,
        logger,
        BlaseballSource.game(UUID.fromString(gameID)),
        sortBy == BLASEBALL_SORTING_HOT
    ) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getGameFeedAsResponse(gameID, limit = limit, sort = sortBy, category = category, type = type, start = start)

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
        val category: Int? = null,
        val type: Int? = null
    ) : PaginatedUrl(
        loopEvery,
        limit,
        delayBetweenLoops,
        totalLimit,
        logger,
        BlaseballSource.story(UUID.fromString(storyID)),
        sortBy == BLASEBALL_SORTING_HOT
    ) {
        override suspend fun retrievePage(start: Int, limit: Int, now: Long): HttpResponse =
            httpClient.getStoryFeedAsResponse(storyID, limit = limit, sort = sortBy, category = category, type = type, start = start)

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
        val category: Int? = null,
        val type: Int? = null
    ) : ShellSource {
        override suspend fun CoroutineScope.processNuts(mailbox: SendChannel<UpNutIngest>) {
            val etags: MutableMap<String, String> = HashMap()

            loopEvery(loopEvery, `while` = { isActive }) {
                try {
                    val storyList = databaseClient.sql("SELECT id FROM library WHERE unredacted_since IS NOT NULL")
                        .map { row -> row.getValue<UUID>("id").toString() }
                        .all()
                        .collectList()
                        .await()

                    logger.debug("Retrieving nuts and scales for {}", storyList)

                    storyList.forEach { story ->
                        val source = BlaseballSource.story(UUID.fromString(story))

                        val now = now()

                        val response = httpClient.getStoryFeedAsResponse(story, category = category, type = type, limit = 2000)
                        val existingTag = etags[story]
                        val responseTag = response.headers["Etag"]
                        if (existingTag != null && existingTag == responseTag) {
                            logger.trace("Hit ETag for {}", story)

                            delay(delayBetweenLoops)
                        } else {
                            val list = response.receive<List<UpNutEvent>>()
                                .mapIndexed { index, upNutEvent ->
                                    upNutEvent withMetadata { put("_upnuts_index", index) }
                                }

                            mailbox.send(UpNutIngest(now, source, list))
                            responseTag?.let { etags[story] = it }

                            delay(delayBetweenLoops)
                        }
                    }
                } catch (th: Throwable) {
                    logger.error("If someone screams in the library and there's no librarian, does it throw an error?", th)
                }
            }
        }
    }

    @ExperimentalTime
    public class AllPlayers(
        val loopEvery: Duration,
        val limit: Int,
        val delayBetweenLoops: Duration,
        val delayBetweenPlayers: Duration,
        val totalLimit: Long,
        val httpClient: HttpClient,
        val logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null,
        val type: Int? = null
    ) : ShellSource {
        override suspend fun CoroutineScope.processNuts(mailbox: SendChannel<UpNutIngest>) {
            val etags: MutableMap<Int, String> = HashMap()
            val shouldTrim = sortBy == BLASEBALL_SORTING_HOT || sortBy == BLASEBALL_SORTING_TOP

            loopEvery(loopEvery, `while` = { isActive }) {
                try {
                    val allPlayers = httpClient.getAllPlayers()
                        .shuffled()

                    logger.debug("Slowly trawling through {}", allPlayers)

                    allPlayers.forEach { (playerID) ->
                        var start = 0

                        val source = BlaseballSource.player(UUID.fromString(playerID))

                        while (isActive && start < totalLimit) {
                            val now = now()

                            val response = httpClient.getPlayerFeedAsResponse(playerID, limit = limit, sort = sortBy, category = category, type = type, start = start)
                            val existingTag = etags[start]
                            val responseTag = response.headers["Etag"]
                            if (existingTag != null && existingTag == responseTag) {
                                start += limit
                                logger.trace("Hit ETag; continuing at {}", start)
                            } else {
                                val list = response.receive<List<UpNutEvent>>()
                                    .trimIf(shouldTrim)

                                mailbox.send(UpNutIngest(now, source, list))

                                if (list.size < limit) break

                                responseTag?.let { etags[start] = it }
                                start += list.size

                                delay(delayBetweenLoops)
                            }
                        }

                        delay(delayBetweenPlayers)
                    }
                } catch (th: Throwable) {
                    logger.error("A Rogue Umpire incinerated an invisible servant", th)
                }
            }
        }
    }

    @ExperimentalTime
    public class RunningGames(
        val loopEvery: Duration,
        val limit: Int,
        val delayBetweenLoops: Duration,
        val delayBetweenGames: Duration,
        val totalLimit: Long,
        val httpClient: HttpClient,
        val logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null,
        val type: Int? = null
    ) : ShellSource {
        override suspend fun CoroutineScope.processNuts(mailbox: SendChannel<UpNutIngest>) {
            val etags: MutableMap<Int, String> = HashMap()
            val shouldTrim = sortBy == BLASEBALL_SORTING_HOT || sortBy == BLASEBALL_SORTING_TOP

            val yesterdaysGames: MutableList<String> = ArrayList()

            loopEvery(loopEvery, `while` = { isActive }) {
                try {
                    val gamesForToday = httpClient.getTodaysGames()
                        .mapNotNull { it.getStringOrNull("id") }

                    val loopThroughToday = ArrayList(yesterdaysGames)
                    loopThroughToday.addAll(gamesForToday)

                    yesterdaysGames.clear()
                    yesterdaysGames.addAll(gamesForToday)

                    logger.debug("What's on the schedule today - {}", loopThroughToday)

                    loopThroughToday.forEach { gameID ->
                        var start = 0

                        val source = BlaseballSource.game(UUID.fromString(gameID))

                        while (isActive && start < totalLimit) {
                            val now = now()

                            val response = httpClient.getGameFeedAsResponse(gameID, limit = limit, sort = sortBy, category = category, type = type, start = start)
                            val existingTag = etags[start]
                            val responseTag = response.headers["Etag"]
                            if (existingTag != null && existingTag == responseTag) {
                                start += limit
                                logger.trace("Hit ETag; continuing at {}", start)
                            } else {
                                val list = response.receive<List<UpNutEvent>>()
                                    .trimIf(shouldTrim)

                                mailbox.send(UpNutIngest(now, source, list))

                                if (list.size < limit) break

                                responseTag?.let { etags[start] = it }
                                start += list.size

                                delay(delayBetweenLoops)
                            }
                        }

                        delay(delayBetweenGames)
                    }
                } catch (th: Throwable) {
                    logger.error("The weather turned; schedules rewritten in the darkness", th)
                }
            }
        }
    }

    @ExperimentalTime
    public class LiquidFriend(
        val loopEvery: Duration,
        val limit: Int,
        val delayBetweenLoops: Duration,
        val delayBetweenGames: Duration,
        val totalLimit: Long,
        val httpClient: HttpClient,
        val databaseClient: DatabaseClient,
        val logger: Logger,
        val sortBy: Int? = null,
        val category: Int? = null,
        val type: Int? = null
    ) : ShellSource {
        override suspend fun CoroutineScope.processNuts(mailbox: SendChannel<UpNutIngest>) {
            val etags: MutableMap<Int, String> = HashMap()
            val etagsContain: MutableMap<String, List<UUID>> = HashMap()

            loopEvery(loopEvery, `while` = { isActive }) {
                logger.debug("Retrieving faxes...")

                val faxes = databaseClient.sql("SELECT feed_id, source_id, source_type FROM detective_work")
                                .map { row -> Pair(row.getValue<UUID>("feed_id"), BlaseballSource(row.get<UUID>("source_id"), row.getValue("source_type"))) }
                                .all()
                                .collectList()
                                .awaitFirstOrNull()
                                ?.groupBy(Pair<UUID, BlaseballSource>::second, Pair<UUID, BlaseballSource>::first) ?: emptyMap()

                logger.debug("Pending faxes - {}", faxes)


                faxes.forEach { (source, investigate) ->
                    try {
                        val feedEvents: MutableList<UUID> = ArrayList(investigate)
                        var start = 0

                        when (source.sourceType) {
                            BlaseballSource.GLOBAL_FEED ->
                                while (isActive && start < totalLimit && feedEvents.isNotEmpty()) {
                                    val now = now()

                                    val response = httpClient.getGlobalFeedAsResponse(limit = limit, sort = sortBy, category = category, type = type, start = start)
                                    val existingTag = etags[start]
                                    val responseTag = response.headers["Etag"]
                                    if (existingTag != null && existingTag == responseTag) {
                                        start += limit
                                        etagsContain[responseTag]?.let(feedEvents::removeAll)
                                        logger.trace("Hit ETag; continuing at {}", start)
                                    } else {
                                        val list = response.receive<List<UpNutEvent>>()
                                        val listIDs = list.map(UpNutEvent::id)

                                        feedEvents.removeAll(listIDs)

                                        mailbox.send(UpNutIngest(now, source, list))

                                        if (list.size < limit) break

                                        responseTag?.let {
                                            etags[start] = it
                                            etagsContain[it] = listIDs
                                        }
                                        start += list.size

                                        delay(delayBetweenLoops)
                                    }
                                }

                            BlaseballSource.GAME_FEED -> {
                                val gameID = source.sourceID?.toString()
                                if (gameID == null) {
                                    logger.warn("Game feed has a null source; how am I meant to find out about {}", feedEvents)
                                    return@forEach
                                }

                                while (isActive && start < totalLimit && feedEvents.isNotEmpty()) {
                                    val now = now()

                                    val response = httpClient.getGameFeedAsResponse(gameID, limit = limit, sort = sortBy, category = category, type = type, start = start)
                                    val existingTag = etags[start]
                                    val responseTag = response.headers["Etag"]
                                    if (existingTag != null && existingTag == responseTag) {
                                        start += limit
                                        etagsContain[responseTag]?.let(feedEvents::removeAll)
                                        logger.trace("Hit ETag; continuing at {}", start)
                                    } else {
                                        val list = response.receive<List<UpNutEvent>>()
                                        val listIDs = list.map(UpNutEvent::id)

                                        feedEvents.removeAll(listIDs)

                                        mailbox.send(UpNutIngest(now, source, list))

                                        if (list.size < limit) break

                                        responseTag?.let {
                                            etags[start] = it
                                            etagsContain[it] = listIDs
                                        }
                                        start += list.size

                                        delay(delayBetweenLoops)
                                    }
                                }
                            }
                            BlaseballSource.PLAYER_FEED -> {
                                val playerID = source.sourceID?.toString()
                                if (playerID == null) {
                                    logger.warn("Player feed has a null source; how am I meant to find out about {}", feedEvents)
                                    return@forEach
                                }

                                while (isActive && start < totalLimit && feedEvents.isNotEmpty()) {
                                    val now = now()

                                    val response = httpClient.getPlayerFeedAsResponse(playerID, limit = limit, sort = sortBy, category = category, type = type, start = start)
                                    val existingTag = etags[start]
                                    val responseTag = response.headers["Etag"]
                                    if (existingTag != null && existingTag == responseTag) {
                                        start += limit
                                        etagsContain[responseTag]?.let(feedEvents::removeAll)
                                        logger.trace("Hit ETag; continuing at {}", start)
                                    } else {
                                        val list = response.receive<List<UpNutEvent>>()
                                        val listIDs = list.map(UpNutEvent::id)

                                        feedEvents.removeAll(listIDs)

                                        mailbox.send(UpNutIngest(now, source, list))

                                        if (list.size < limit) break

                                        responseTag?.let {
                                            etags[start] = it
                                            etagsContain[it] = listIDs
                                        }
                                        start += list.size

                                        delay(delayBetweenLoops)
                                    }
                                }
                            }
                            BlaseballSource.TEAM_FEED -> {
                                val teamID = source.sourceID?.toString()
                                if (teamID == null) {
                                    logger.warn("Player feed has a null source; how am I meant to find out about {}", feedEvents)
                                    return@forEach
                                }

                                while (isActive && start < totalLimit && feedEvents.isNotEmpty()) {
                                    val now = now()

                                    val response = httpClient.getTeamFeedAsResponse(teamID, limit = limit, sort = sortBy, category = category, type = type, start = start)
                                    val existingTag = etags[start]
                                    val responseTag = response.headers["Etag"]
                                    if (existingTag != null && existingTag == responseTag) {
                                        start += limit
                                        etagsContain[responseTag]?.let(feedEvents::removeAll)
                                        logger.trace("Hit ETag; continuing at {}", start)
                                    } else {
                                        val list = response.receive<List<UpNutEvent>>()
                                        val listIDs = list.map(UpNutEvent::id)

                                        feedEvents.removeAll(listIDs)

                                        mailbox.send(UpNutIngest(now, source, list))

                                        if (list.size < limit) break

                                        responseTag?.let {
                                            etags[start] = it
                                            etagsContain[it] = listIDs
                                        }
                                        start += list.size

                                        delay(delayBetweenLoops)
                                    }
                                }
                            }
                            BlaseballSource.STORY_CHAPTER -> {
                                val storyChapter = source.sourceID?.toString()
                                if (storyChapter == null) {
                                    logger.warn("Player feed has a null source; how am I meant to find out about {}", feedEvents)
                                    return@forEach
                                }

                                while (isActive && start < totalLimit && feedEvents.isNotEmpty()) {
                                    val now = now()

                                    val response = httpClient.getStoryFeedAsResponse(storyChapter, limit = limit, sort = sortBy, category = category, type = type, start = start)
                                    val existingTag = etags[start]
                                    val responseTag = response.headers["Etag"]
                                    if (existingTag != null && existingTag == responseTag) {
                                        start += limit
                                        etagsContain[responseTag]?.let(feedEvents::removeAll)
                                        logger.trace("Hit ETag; continuing at {}", start)
                                    } else {
                                        val list = response.receive<List<UpNutEvent>>()
                                        val listIDs = list.map(UpNutEvent::id)

                                        feedEvents.removeAll(listIDs)

                                        mailbox.send(UpNutIngest(now, source, list))

                                        if (list.size < limit) break

                                        responseTag?.let {
                                            etags[start] = it
                                            etagsContain[it] = listIDs
                                        }
                                        start += list.size

                                        delay(delayBetweenLoops)
                                    }
                                }
                            }
                        }
                    } catch (th: Throwable) {
                        logger.error("Unscrambled events; friends lost to shadows", th)
                    }
                    delay(delayBetweenGames)

                }
            }
        }
    }
}

public suspend inline fun ShellSource.processNuts(scope: CoroutineScope, sendTo: SendChannel<UpNutIngest>) =
    scope.processNuts(sendTo)

inline fun nowInstant() = Instant.now(Clock.systemUTC())
inline fun now() = Instant.now(Clock.systemUTC()).toEpochMilli()

inline fun List<UpNutEvent>.trimIf(condition: Boolean): List<UpNutEvent> =
    if (condition) takeWhile { event ->
        (event.nuts.intOrNull ?: 0) > 0 ||
        (event.scales.intOrNull ?: 0) > 0
    } else this