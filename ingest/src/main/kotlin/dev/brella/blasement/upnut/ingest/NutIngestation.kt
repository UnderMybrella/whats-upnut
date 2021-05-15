package dev.brella.blasement.upnut.ingest

import com.soywiz.klock.DateTime
import com.soywiz.klock.DateTimeTz
import dev.brella.blasement.upnut.common.*
import dev.brella.kornea.blaseball.BlaseballApi
import dev.brella.kornea.blaseball.endpoints.BlaseballDatabaseService
import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.kornea.errors.common.doOnFailure
import dev.brella.kornea.errors.common.doOnSuccess
import dev.brella.kornea.errors.common.doOnThrown
import dev.brella.kornea.errors.common.getOrNull
import dev.brella.ktornea.common.getAsResult
import dev.brella.ktornea.common.installGranularHttp
import dev.brella.ktornea.common.postAsResult
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.compression.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.content.*
import io.ktor.http.*
import io.ktor.util.*
import io.r2dbc.spi.Row
import kotlinx.coroutines.*
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.intOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.core.await
import java.io.File
import java.nio.ByteBuffer
import java.time.Clock
import java.time.Instant
import java.util.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.coroutines.CoroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

import org.springframework.r2dbc.core.bind as bindNullable
import io.r2dbc.postgresql.codec.Json as R2Json

class NutIngestation(val config: JsonObject, val nuts: UpNutClient) : CoroutineScope {
    companion object {
        val THE_GAME_BAND = UUID.fromString("7fcb63bc-11f2-40b9-b465-f1d458692a63")

        const val NUTS_THRESHOLD = 1_000
        const val SCALES_THRESHOLD = 1_000

        @JvmStatic
        fun main(args: Array<String>) {
            val baseConfig: JsonObject = File("ingest.json").takeIf(File::exists)?.readText()?.let(Json::decodeFromString) ?: JsonObject(emptyMap())
            val r2dbc = baseConfig.getJsonObjectOrNull("r2dbc") ?: File("r2dbc.json").takeIf(File::exists)?.readText()?.let(Json::decodeFromString) ?: JsonObject(emptyMap())
            val ingest = NutIngestation(baseConfig, UpNutClient(r2dbc))

            runBlocking { ingest.join() }
        }
    }

    override val coroutineContext: CoroutineContext = SupervisorJob() + Dispatchers.IO

    val initJob = launch {
        nuts.client.sql("CREATE TABLE IF NOT EXISTS upnuts (id BIGSERIAL PRIMARY KEY, nuts INT, scales INT, feed_id uuid NOT NULL, source uuid, provider uuid NOT NULL, time BIGINT NOT NULL)")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS game_nuts (id BIGSERIAL PRIMARY KEY, feed_id uuid NOT NULL, game_id uuid NOT NULL);")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS player_nuts (id BIGSERIAL PRIMARY KEY, feed_id uuid NOT NULL, player_id uuid NOT NULL);")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS team_nuts (id BIGSERIAL PRIMARY KEY, feed_id uuid NOT NULL, team_id uuid NOT NULL);")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS event_metadata (feed_id UUID NOT NULL PRIMARY KEY, created BIGINT NOT NULL, season INT NOT NULL, tournament INT NOT NULL, type INT NOT NULL, day INT NOT NULL, phase INT NOT NULL, category INT NOT NULL)")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS snow_crystals (snow_id BIGINT NOT NULL PRIMARY KEY, uuid UUID NOT NULL);")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS library (id UUID NOT NULL PRIMARY KEY, chapter_title_redacted VARCHAR(128), book_title VARCHAR(128) NOT NULL, chapter_title VARCHAR(128) NOT NULL, redacted BOOLEAN NOT NULL DEFAULT TRUE);")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS webhooks (id BIGSERIAL PRIMARY KEY, url VARCHAR(256) NOT NULL, subscribed_to BIGINT NOT NULL, secret_key bytea NOT NULL)")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS events (id BIGSERIAL PRIMARY KEY, send_to VARCHAR(256) NOT NULL, data json NOT NULL, created BIGINT NOT NULL, sent_at BIGINT)")
            .await()

        nuts.client.sql("CREATE INDEX IF NOT EXISTS snow_index_uuid ON snow_crystals (uuid);")
            .await()
    }

    val http = HttpClient(OkHttp) {
        installGranularHttp()

        install(ContentEncoding) {
            gzip()
            deflate()
            identity()
        }

        install(JsonFeature) {
            serializer = KotlinxSerializer(kotlinx.serialization.json.Json {
                ignoreUnknownKeys = true
            })
        }

        expectSuccess = false

        defaultRequest {
            userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0")
        }
    }
    val blaseballApi = BlaseballApi(http)

    suspend fun getGlobalFeed(category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: String? = null): KorneaResult<List<UpNutEvent>> =
        http.getAsResult("https://blaseball.com/database/feed/global") {
            if (category != null) parameter("category", category)
            if (type != null) parameter("type", type)
            if (limit != BlaseballDatabaseService.YES_BRELLA_I_WOULD_LIKE_UNLIMITED_EVENTS) parameter("limit", limit)
            if (sort != null) parameter("sort", sort)
            if (start != null) parameter("start", start)
        }

    suspend fun getTeamFeed(teamID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: String? = null): KorneaResult<List<UpNutEvent>> =
        http.getAsResult("https://blaseball.com/database/feed/team") {
            parameter("id", teamID)

            if (category != null) parameter("category", category)
            if (type != null) parameter("type", type)
            if (limit != BlaseballDatabaseService.YES_BRELLA_I_WOULD_LIKE_UNLIMITED_EVENTS) parameter("limit", limit)
            if (sort != null) parameter("sort", sort)
            if (start != null) parameter("start", start)
        }

    suspend fun getStoryFeed(chapterID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: String? = null): KorneaResult<List<UpNutEvent>> =
        http.getAsResult("https://blaseball.com/database/feed/story") {
            parameter("id", chapterID)

            if (category != null) parameter("category", category)
            if (type != null) parameter("type", type)
            if (limit != BlaseballDatabaseService.YES_BRELLA_I_WOULD_LIKE_UNLIMITED_EVENTS) parameter("limit", limit)
            if (sort != null) parameter("sort", sort)
            if (start != null) parameter("start", start)
        }

    val defaultScope = config.getJsonObjectOrNull("default")

    fun getIntInScope(scope: String, key: String, default: Int) =
        config.getJsonObjectOrNull(scope)?.getIntOrNull(key)
        ?: defaultScope?.getIntOrNull(key)
        ?: default

    fun getLongInScope(scope: String, key: String, default: Long) =
        config.getJsonObjectOrNull(scope)?.getLongOrNull(key)
        ?: defaultScope?.getLongOrNull(key)
        ?: default

    fun getIntInScope(scopes: Iterable<String>, key: String, default: Int) =
        scopes.firstOrNull(config::containsKey)?.let(config::getJsonObjectOrNull)?.getIntOrNull(key)
        ?: defaultScope?.getIntOrNull(key)
        ?: default

    fun getLongInScope(scopes: Iterable<String>, key: String, default: Long) =
        scopes.firstOrNull(config::containsKey)?.let(config::getJsonObjectOrNull)?.getLongOrNull(key)
        ?: defaultScope?.getLongOrNull(key)
        ?: default

    inline fun nowInstant() = Instant.now(Clock.systemUTC())
    inline fun now() = Instant.now(Clock.systemUTC()).toEpochMilli()

    @OptIn(ExperimentalTime::class)
    val globalByTopJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.GlobalByTop")

        val limit = getIntInScope("global_top", "limit", 100)
        val loopEvery = getIntInScope("global_top", "loop_duration_s", 60)
        val delay = getLongInScope("global_top", "delay_ms", 100)
        val delayOnFailure = getLongInScope("global_top", "delay_on_failure_ms", 100)
        val totalLimit = getLongInScope("global_top", "total_limit", Long.MAX_VALUE)

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
//            getFeed(limit = limit, start = lastID?.let(BLASEBALL_TIME_PATTERN::format))
            var start = 0
            val now = now()

            while (isActive && start < totalLimit) {
                val list = getGlobalFeed(limit = limit, sort = 2, start = start.toString())
                               .doOnThrown { error ->
                                   logger.debug("Exception occurred when retrieving Global Feed (By Top, start = {0}, limit = {1})", start, limit, error.exception)
                               }.doOnFailure { delay(delayOnFailure) }
                               .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 || event.scales.intOrNull ?: 0 > 0 } ?: break

                list.forEach { event -> processNuts(now, event) }

                if (list.size < limit) break
                start += list.size

                delay(delay)
            }
        }
    }

    @OptIn(ExperimentalTime::class)
    val globalByHotJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.GlobalByHot")

        val limit = getIntInScope("global_hot", "limit", 100)
        val loopEvery = getIntInScope("global_hot", "loop_duration_s", 60)
        val delayOnFailure = getLongInScope("global_hot", "delay_on_failure_ms", 100)

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
            val now = now()

            val list = getGlobalFeed(limit = limit, sort = 3)
                .doOnThrown { error -> logger.debug("Exception occurred when retrieving Global Feed (By Hot, limit = {0})", limit, error.exception) }
                .doOnFailure { delay(delayOnFailure) }
                .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 || event.scales.intOrNull ?: 0 > 0 }
                ?.reversed()

            list?.forEach { event -> processNuts(now, event) }
        }
    }

    @OptIn(ExperimentalTime::class)
    val teamsByTopJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.TeamsByTop")

        blaseballApi.getAllTeams()
            .getOrNull()
            ?.map { team ->
                launch {
                    val limit = getIntInScope(listOf(team.nickname, "teams_top"), "limit", 100)
                    val loopEvery = getIntInScope(listOf(team.nickname, "teams_top"), "loop_duration_s", 60)
                    val delay = getLongInScope(listOf(team.nickname, "teams_top"), "delay_ms", 100)
                    val delayOnFailure = getLongInScope(listOf(team.nickname, "teams_top"), "delay_on_failure_ms", 100)
                    val totalLimit = getLongInScope(listOf(team.nickname, "teams_top"), "total_limit", Long.MAX_VALUE)

                    loopEvery(loopEvery.seconds, `while` = { isActive }) {
                        var start = 0
                        val now = now()

                        while (isActive && start < totalLimit) {
                            val list = getTeamFeed(team.id.id, limit = limit, sort = 2, start = start.toString())
                                           .doOnThrown { error ->
                                               logger.debug("Exception occurred when retrieving Team Feed (By Top, start = {0}, limit = {1}, team = {2})", start, limit, team.fullName, error.exception)
                                           }.doOnFailure { delay(delayOnFailure) }
                                           .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 || event.scales.intOrNull ?: 0 > 0 } ?: break

                            list.forEach { event -> processNuts(now, event) }

                            if (list.size < limit) break
                            start += list.size

                            delay(delay)
                        }
                    }
                }
            }
            ?.joinAll()
    }

    @OptIn(ExperimentalTime::class)
    val teamsByHotJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.GlobalByHot")

        blaseballApi.getAllTeams()
            .getOrNull()
            ?.map { team ->
                launch {

                    val limit = getIntInScope(listOf(team.nickname, "team_hot"), "limit", 100)
                    val loopEvery = getIntInScope(listOf(team.nickname, "team_hot"), "loop_duration_s", 60)
                    val delayOnFailure = getLongInScope(listOf(team.nickname, "team_hot"), "delay_on_failure_ms", 100)

                    loopEvery(loopEvery.seconds, `while` = { isActive }) {
                        val now = now()

                        val list = getTeamFeed(team.id.id, limit = limit, sort = 3)
                            .doOnThrown { error -> logger.debug("Exception occurred when retrieving Team Feed (By Hot, limit = {0}, team = {1})", limit, team.fullName, error.exception) }
                            .doOnFailure { delay(delayOnFailure) }
                            .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 || event.scales.intOrNull ?: 0 > 0 }
                            ?.reversed()

                        list?.forEach { event -> processNuts(now, event) }
                    }
                }
            }
            ?.joinAll()
    }

    @OptIn(ExperimentalTime::class)
    val storyByOldestJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.StoryByOldest")

        val limit = getIntInScope("story_top", "limit", 100)
        val loopEvery = getIntInScope("story_top", "loop_duration_s", 60)
        val delay = getLongInScope("story_top", "delay_ms", 100)
        val delayOnFailure = getLongInScope("story_top", "delay_on_failure_ms", 100)
        val totalLimit = getLongInScope("story_top", "total_limit", Long.MAX_VALUE)

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
//            getFeed(limit = limit, start = lastID?.let(BLASEBALL_TIME_PATTERN::format))
            var start = 0
            val now = now()

            val storyList = nuts.client.sql("SELECT id, redacted FROM library").map { row ->
                (row["id"] as? UUID)?.let { uuid ->
                    if (row["redacted"] as? Boolean != false) return@map null
                    else uuid
                }
            }.all().collectList().awaitFirstOrNull()?.filterNotNull() ?: emptyList()

            logger.debug("Logging chapters: {}", storyList)

            storyList.map { chapterID ->
                launch {
                    while (isActive && start < totalLimit) {
                        val list = getStoryFeed(chapterID = chapterID.toString(), limit = limit, sort = 0, start = start.toString())
                                       .doOnThrown { error ->
                                           logger.debug(
                                               "Exception occurred when retrieving Story Feed (By Oldest, ID {0}, start = {1}, limit = {2})",
                                               chapterID,
                                               start,
                                               limit,
                                               error.exception
                                           )
                                       }.doOnFailure { delay(delayOnFailure) }
                                       .getOrNull() ?: break

                        list.forEach { event -> processNuts(now, event) }

                        if (list.size < limit) break
                        start += list.size

                        delay(delay)
                    }
                }
            }.joinAll()
        }
    }

    val STORY_LIST_REGEX =
        "books\\s*:\\s*\\[\\s*(?:\\s*\\{\\s*title\\s*:\\s*\"[^\"]+\"\\s*,\\s*chapters\\s*:\\s*\\[(?:\\s*\\{\\s*title\\s*:\\s*\"[^\"]+\"\\s*,\\s*id\\s*:\\s*\"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\"\\s*(?:,\\s*redacted\\s*:\\s*(?:true|false)\\s*)?\\}\\s*,?)*\\s*\\]\\s*,?\\s*\\}\\s*,?\\s*)*".toRegex()
    val BOOK_REGEX =
        "\\{\\s*title\\s*:\\s*\"([^\"]+)\"\\s*,\\s*chapters\\s*:\\s*\\[((?:\\s*\\{\\s*title\\s*:\\s*\"[^\"]+\"\\s*,\\s*id\\s*:\\s*\"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\"\\s*(?:,\\s*redacted\\s*:\\s*(?:true|false)\\s*)?\\}\\s*,?)*)\\s*]".toRegex()
    val CHAPTER_REGEX = "\\{\\s*title\\s*:\\s*\"([^\"]+)\"\\s*,\\s*id\\s*:\\s*\"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\"\\s*(?:,\\s*redacted\\s*:\\s*(true|false)\\s*)?\\}".toRegex()

    @OptIn(ExperimentalTime::class)
    val librarian = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.Librarian")

        val loopEvery = getIntInScope("librarian", "loop_duration_s", 60)
        val delay = getLongInScope("librarian", "delay_ms", 100)
        val delayOnFailure = getLongInScope("librarian", "delay_on_failure_ms", 100)
        val totalLimit = getLongInScope("librarian", "total_limit", Long.MAX_VALUE)

        var etag: String? = null

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
            val retrievalTime = now()
            http.getAsResult<HttpResponse>("https://raw.githubusercontent.com/xSke/blaseball-site-files/main/main.js")
                .doOnSuccess { response ->
                    val mainEtag = response.etag()

                    if (etag == null || etag != mainEtag) {
                        etag = mainEtag

                        logger.debug("Testing new books...")

                        val document = STORY_LIST_REGEX.find(response.receive<String>())
                                       ?: return@loopEvery logger.debug("No story list in document")

                        val books = BOOK_REGEX.findAll(document.value).associate { result ->
                            val name = result.groupValues[1]

                            val chapters = CHAPTER_REGEX.findAll(result.groupValues[2])
                                .associate { chapterResult -> UUID.fromString(chapterResult.groupValues[2]) to Pair(chapterResult.groupValues[1], chapterResult.groupValues.getOrNull(3)?.toBoolean() ?: false) }

                            name to chapters
                        }

                        val booksReturned = nuts.client.sql("SELECT id, chapter_title, redacted FROM library").map { row ->
                            (row["id"] as? UUID)?.let { uuid ->
                                Pair(uuid, Pair(row["chapter_title"] as? String ?: row["chapter_title"].toString(), row["redacted"]))
                            }
                        }.all()
                                                .collectList()
                                                .awaitFirstOrNull()
                                                ?.filterNotNull()
                                                ?.toMap()
                                            ?: emptyMap()

                        val chaptersAdded: MutableList<WebhookEvent.LibraryChapter> = ArrayList()
                        val chaptersUnlocked: MutableList<WebhookEvent.LibraryChapter> = ArrayList()
                        val chaptersLocked: MutableList<WebhookEvent.LibraryChapter> = ArrayList()

                        books.forEach { (bookName, chapters) ->
                            chapters.forEach { (chapterUUID, chapterDetails) ->
                                val existing = booksReturned[chapterUUID]
                                if (existing == null) {
                                    //New book just dropped

                                    logger.info("New book just dropped: {} / {}", bookName, chapterDetails.first)

                                    nuts.client.sql("INSERT INTO library (id, book_title, chapter_title, redacted) VALUES ($1, $2, $3, $4)")
                                        .bind("$1", chapterUUID)
                                        .bind("$2", bookName)
                                        .bind("$3", chapterDetails.first)
                                        .bind("$4", chapterDetails.second)
                                        .await()

                                    val chapter = WebhookEvent.LibraryChapter(bookName, chapterUUID, chapterDetails.first, null, chapterDetails.second)
                                    chaptersAdded.add(chapter)
                                    if (chapter.isRedacted) chaptersLocked.add(chapter)
                                    else chaptersUnlocked.add(chapter)

                                } else if (chapterDetails.second != existing.second) {
                                    //Book is now unredacted! -- Right ?

                                    logger.info("Book has shifted redactivity: {} -> {}, {} -> {}", existing.first, chapterDetails.first, existing.second, chapterDetails.second)

                                    nuts.client.sql("UPDATE library SET chapter_title = $1, chapter_title_redacted = $2, redacted = $3")
                                        .bind("$1", chapterDetails.first)
                                        .bind("$2", existing.first)
                                        .bind("$3", chapterDetails.second)
                                        .await()

                                    val chapter = WebhookEvent.LibraryChapter(bookName, chapterUUID, chapterDetails.first, existing.first, chapterDetails.second)
                                    if (chapter.isRedacted) chaptersLocked.add(chapter)
                                    else chaptersUnlocked.add(chapter)
                                }
                            }
                        }

                        if (chaptersAdded.isNotEmpty())
                            postEvent(WebhookEvent.NewLibraryChapters(chaptersAdded), WebhookEvent.NEW_LIBRARY_CHAPTERS)
                        if (chaptersLocked.isNotEmpty())
                            postEvent(WebhookEvent.LibraryChaptersRedacted(chaptersLocked), WebhookEvent.LIBRARY_CHAPTERS_REDACTED)
                        if (chaptersUnlocked.isNotEmpty())
                            postEvent(WebhookEvent.LibraryChaptersUnredacted(chaptersUnlocked), WebhookEvent.LIBRARY_CHAPTERS_UNREDACTED)
                    }
                }
        }
    }

    suspend fun postEvent(event: WebhookEvent, eventType: Int) {
        val now = now()

        val eventData = Json.encodeToString(event).encodeToByteArray()
        val eventAsJson = R2Json.of(eventData)

        val statement = nuts.client.sql("INSERT INTO events ( send_to, data, created, sent_at ) VALUES ( $1, $2, $3, $4 )")
            .bind("$2", eventAsJson)
            .bind("$3", now)

        coroutineScope {
            webhooksFor(eventType)
                .map { (url, token) ->
                    async { url to (postEventTo(url, token, eventData) is KorneaResult.Success<*>) }
                }.awaitAll().forEach { (url, successful) ->
                    try {
                        statement.bind("$1", url)
                            .bindNullable("$4", if (successful) now else null)
                            .await()
                    } catch (th: Throwable) {
                        th.printStackTrace()
                    }
                }
        }
    }

    suspend inline fun postEventTo(url: String, token: ByteArray, eventData: ByteArray): KorneaResult<Unit> {
        val sig = Mac.getInstance("HmacSHA256")
            .apply { init(SecretKeySpec(token, "HmacSHA256")) }
            .doFinal(eventData)
            .let(::hex)

        return http.postAsResult<Unit>(url) {
            header("X-UpNut-Signature", sig)

            body = ByteArrayContent(eventData, contentType = ContentType.Application.Json)
        }
    }

    suspend fun webhooksFor(eventType: Int): List<Pair<String, ByteArray>> =
        nuts.client.sql("SELECT url, secret_key FROM webhooks WHERE subscribed_to & $1 = $1")
            .bind("$1", eventType)
            .map { row ->
                Pair(
                    row["url"] as String, when (val secretKey = row["secret_key"]) {
                        is ByteBuffer -> ByteArray(secretKey.remaining()).also(secretKey::get)
                        is ByteArray -> secretKey
                        else -> return@map null
                    }
                )
            }
            .all()
            .collectList()
            .awaitFirstOrNull()
            ?.filterNotNull() ?: emptyList()

    inner class NutBuilder {
        suspend inline fun players(time: Long, event: UpNutEvent) {
            val players = nuts.client.sql("SELECT player_id FROM player_nuts WHERE feed_id = $1")
                              .bind("$1", event.id)
                              .fetch()
                              .all()
                              .mapNotNull { it["player_id"] as? UUID }
                              .collectList()
                              .awaitFirstOrNull() ?: emptyList()

            event.playerTags?.filterNot(players::contains)
                ?.forEach { playerID ->
                    nuts.client.sql("INSERT INTO player_nuts (feed_id, player_id) VALUES ($1, $2)")
                        .bind("$1", event.id)
                        .bind("$2", playerID)
                        .await()
                }
        }

        suspend inline fun games(time: Long, event: UpNutEvent) {
            val games = nuts.client.sql("SELECT game_id FROM game_nuts WHERE feed_id = $1")
                            .bind("$1", event.id)
                            .fetch()
                            .all()
                            .mapNotNull { it["game_id"] as? UUID }
                            .collectList()
                            .awaitFirstOrNull() ?: emptyList()

            event.gameTags?.filterNot(games::contains)
                ?.forEach { gameID ->
                    nuts.client.sql("INSERT INTO game_nuts (feed_id, game_id) VALUES ($1, $2)")
                        .bind("$1", event.id)
                        .bind("$2", gameID)
                        .await()
                }
        }

        suspend inline fun teams(time: Long, event: UpNutEvent) {
            val teams = nuts.client.sql("SELECT team_id FROM team_nuts WHERE feed_id = $1")
                            .bind("$1", event.id)
                            .fetch()
                            .all()
                            .mapNotNull { it["team_id"] as? UUID }
                            .collectList()
                            .awaitFirstOrNull() ?: emptyList()

            event.teamTags?.filterNot(teams::contains)
                ?.forEach { teamID ->
                    nuts.client.sql("INSERT INTO team_nuts (feed_id, team_id) VALUES ($1, $2)")
                        .bind("$1", event.id)
                        .bind("$2", teamID)
                        .await()
                }
        }

        suspend inline fun metadata(time: Long, event: UpNutEvent) {
            nuts.client.sql("INSERT INTO event_metadata (feed_id, created, season, tournament, type, day, phase, category) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8 ) ON CONFLICT DO NOTHING")
                .bind("$1", event.id)
                .bind("$2", event.created.utc.unixMillisLong)
                .bind("$3", event.season)
                .bind("$4", event.tournament)
                .bind("$5", event.type)
                .bind("$6", event.day)
                .bind("$7", event.phase)
                .bind("$8", event.category)
                .await()
        }

        inline operator fun Int?.minus(other: Int?): Int? =
            if (this != null && other != null) this - other else null

        suspend inline fun insert(time: Long, event: UpNutEvent): Pair<Int, Int>? {
            val atTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS nuts, SUM(scales) as scales FROM upnuts WHERE feed_id = $1 AND time <= $2 AND provider = '7fcb63bc-11f2-40b9-b465-f1d458692a63'::uuid AND source IS NULL")
                                            .bind("$1", event.id)
                                            .bind("$2", time)
                                            .map { row -> (row.get<Int>("nuts") ?: 0) to (row.get<Int>("scales") ?: 0) }
                                            .first()
                                            .awaitFirstOrNull()

            val nutsDifference = event.nuts.intOrNull - atTimeOfRecording?.first
            if (nutsDifference != null && nutsDifference > 0) {
                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                    .bind("$1", nutsDifference)
                    .bind("$2", event.id)
                    .bind("$3", THE_GAME_BAND)
                    .bind("$4", time)
                    .await()

                val missing = atTimeOfRecording!!.first - NUTS_THRESHOLD

                if (missing <= 0 && nutsDifference > missing) postEvent(WebhookEvent.ThresholdPassedNuts(NUTS_THRESHOLD, time, event), WebhookEvent.THRESHOLD_PASSED_NUTS)
            }

            val scalesDifference = event.scales.intOrNull - atTimeOfRecording?.second
            if (scalesDifference != null && scalesDifference > 0) {
                nuts.client.sql("INSERT INTO upnuts (scales, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                    .bind("$1", scalesDifference)
                    .bind("$2", event.id)
                    .bind("$3", THE_GAME_BAND)
                    .bind("$4", time)
                    .await()

                //We don't know the threshold for scales (or even how they work); making an educated guess
                val missing = atTimeOfRecording!!.second - SCALES_THRESHOLD

                if (missing <= 0 && scalesDifference > missing) postEvent(WebhookEvent.ThresholdPassedScales(SCALES_THRESHOLD, time, event), WebhookEvent.THRESHOLD_PASSED_SCALES)
            }

            return atTimeOfRecording
        }

        suspend inline fun logging(time: Long, event: UpNutEvent, logger: Logger, atTimeOfRecording: Pair<Int, Int>?) {
            if (atTimeOfRecording == null) return
            if (event.nuts.intOrNull ?: 0 > atTimeOfRecording.first) logger.info("{} +{} nuts", event.id, atTimeOfRecording.first)
            if (event.scales.intOrNull ?: 0 > atTimeOfRecording.second) logger.info("{} +{} scales", event.id, atTimeOfRecording.second)
        }

        inline fun build(): suspend (time: Long, event: UpNutEvent) -> Unit = run {
            val config = config.getJsonObjectOrNull("process")

            val loggingEnabled = config?.getBooleanOrNull("logging_enabled") ?: true
            val playerTags = config?.getBooleanOrNull("player_tags") ?: true
            val gameTags = config?.getBooleanOrNull("game_tags") ?: true
            val teamTags = config?.getBooleanOrNull("team_tags") ?: true
            val eventMetadata = config?.getBooleanOrNull("event_metadata") ?: true

            val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.Process")

            if (loggingEnabled) {
                if (playerTags) {
                    if (gameTags) {
                        if (teamTags) {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
                                    players(time, event)
                                    games(time, event)
                                    teams(time, event)
                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            } else {
                                return@run nuts@{ time, event ->
                                    players(time, event)
                                    games(time, event)
                                    teams(time, event)
//                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            }
                        } else {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
                                    players(time, event)
                                    games(time, event)
//                                    teams(time, event)
                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            } else {
                                return@run nuts@{ time, event ->
                                    players(time, event)
                                    games(time, event)
//                                    teams(time, event)
//                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            }
                        }
                    } else {
                        if (teamTags) {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
                                    players(time, event)
//                                    games(time, event)
                                    teams(time, event)
                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            } else {
                                return@run nuts@{ time, event ->
                                    players(time, event)
//                                    games(time, event)
                                    teams(time, event)
//                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            }
                        } else {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
                                    players(time, event)
//                                    games(time, event)
//                                    teams(time, event)
                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            } else {
                                return@run nuts@{ time, event ->
                                    players(time, event)
//                                    games(time, event)
//                                    teams(time, event)
//                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            }
                        }
                    }
                } else {
                    if (gameTags) {
                        if (teamTags) {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
                                    games(time, event)
                                    teams(time, event)
                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            } else {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
                                    games(time, event)
                                    teams(time, event)
//                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            }
                        } else {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
                                    games(time, event)
//                                    teams(time, event)
                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            } else {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
                                    games(time, event)
//                                    teams(time, event)
//                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            }
                        }
                    } else {
                        if (teamTags) {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
//                                    games(time, event)
                                    teams(time, event)
                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            } else {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
//                                    games(time, event)
                                    teams(time, event)
//                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            }
                        } else {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
//                                    games(time, event)
//                                    teams(time, event)
                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            } else {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
//                                    games(time, event)
//                                    teams(time, event)
//                                    metadata(time, event)

                                    logging(time, event, logger, insert(time, event))
                                }
                            }
                        }
                    }
                }
            } else {
                if (playerTags) {
                    if (gameTags) {
                        if (teamTags) {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
                                    players(time, event)
                                    games(time, event)
                                    teams(time, event)
                                    metadata(time, event)

                                    insert(time, event)
                                }
                            } else {
                                return@run nuts@{ time, event ->
                                    players(time, event)
                                    games(time, event)
                                    teams(time, event)
//                                    metadata(time, event)

                                    insert(time, event)
                                }
                            }
                        } else {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
                                    players(time, event)
                                    games(time, event)
//                                    teams(time, event)
                                    metadata(time, event)

                                    insert(time, event)
                                }
                            } else {
                                return@run nuts@{ time, event ->
                                    players(time, event)
                                    games(time, event)
//                                    teams(time, event)
//                                    metadata(time, event)

                                    insert(time, event)
                                }
                            }
                        }
                    } else {
                        if (teamTags) {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
                                    players(time, event)
//                                    games(time, event)
                                    teams(time, event)
                                    metadata(time, event)

                                    insert(time, event)
                                }
                            } else {
                                return@run nuts@{ time, event ->
                                    players(time, event)
//                                    games(time, event)
                                    teams(time, event)
//                                    metadata(time, event)

                                    insert(time, event)
                                }
                            }
                        } else {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
                                    players(time, event)
//                                    games(time, event)
//                                    teams(time, event)
                                    metadata(time, event)

                                    insert(time, event)
                                }
                            } else {
                                return@run nuts@{ time, event ->
                                    players(time, event)
//                                    games(time, event)
//                                    teams(time, event)
//                                    metadata(time, event)

                                    insert(time, event)
                                }
                            }
                        }
                    }
                } else {
                    if (gameTags) {
                        if (teamTags) {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
                                    games(time, event)
                                    teams(time, event)
                                    metadata(time, event)

                                    insert(time, event)
                                }
                            } else {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
                                    games(time, event)
                                    teams(time, event)
//                                    metadata(time, event)

                                    insert(time, event)
                                }
                            }
                        } else {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
                                    games(time, event)
//                                    teams(time, event)
                                    metadata(time, event)

                                    insert(time, event)
                                }
                            } else {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
                                    games(time, event)
//                                    teams(time, event)
//                                    metadata(time, event)

                                    insert(time, event)
                                }
                            }
                        }
                    } else {
                        if (teamTags) {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
//                                    games(time, event)
                                    teams(time, event)
                                    metadata(time, event)

                                    insert(time, event)
                                }
                            } else {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
//                                    games(time, event)
                                    teams(time, event)
//                                    metadata(time, event)

                                    insert(time, event)
                                }
                            }
                        } else {
                            if (eventMetadata) {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
//                                    games(time, event)
//                                    teams(time, event)
                                    metadata(time, event)

                                    insert(time, event)
                                }
                            } else {
                                return@run nuts@{ time, event ->
//                                    players(time, event)
//                                    games(time, event)
//                                    teams(time, event)
//                                    metadata(time, event)

                                    insert(time, event)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    val processNuts: suspend (time: Long, event: UpNutEvent) -> Unit = NutBuilder().build()

    data class ResendEvent(val id: Long, val sendTo: String, val data: ByteArray, val created: DateTimeTz) {
        constructor(row: Row) : this(row.getValue("id"), row.getValue("send_to"), row.getValue("data"), DateTime.fromUnix(row.getValue<Long>("created")).utc)
    }

    @OptIn(ExperimentalTime::class)
    val resendJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.Resend")

        val limit = getIntInScope("resend", "limit", 100)
        val loopEvery = getIntInScope("resend", "loop_duration_s", 60 * 30)
        val delay = getLongInScope("resend", "delay_ms", 100)
        val delayOnFailure = getLongInScope("resend", "delay_on_failure_ms", 100)
        val totalLimit = getLongInScope("resend", "total_limit", Long.MAX_VALUE)

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
            val eventsToDeliver = nuts.client.sql("SELECT id, send_to, data, created FROM events WHERE sent_at IS NULL")
                                      .map(::ResendEvent)
                                      .all()
                                      .collectList()
                                      .awaitFirstOrNull()
                                      ?.groupBy(ResendEvent::sendTo) ?: return@loopEvery

            logger.info("Resending {} events", eventsToDeliver.size)

            val tokens = eventsToDeliver.keys.associateWith { url ->
                nuts.client.sql("SELECT secret_key FROM webhooks WHERE url = $1")
                    .bind("$1", url)
                    .map { row -> row.getValue<ByteArray>("secret_key") }
                    .first()
                    .awaitFirstOrNull()
            }

            eventsToDeliver.forEach outer@{ (url, eventList) ->
                val token = tokens[url] ?: return@outer
                eventList.forEach { event ->
                    if (postEventTo(url, token, event.data) is KorneaResult.Success) {
                        nuts.client.sql("UPDATE events SET sent_at = $1 WHERE id = $2")
                            .bind("$1", now())
                            .bind("$2", event.id)
                            .await()
                    } else {
                        return@outer
                    }
                }
            }
        }
    }

    suspend fun join() {
        globalByTopJob.join()
        globalByHotJob.join()
        teamsByTopJob.join()
        teamsByHotJob.join()
    }
}