package dev.brella.blasement.upnut.ingest

import com.soywiz.klock.DateTime
import com.soywiz.klock.DateTimeTz
import dev.brella.blasement.upnut.common.*
import dev.brella.kornea.blaseball.BlaseballApi
import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.kornea.errors.common.getOrElse
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
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.core.await
import java.io.File
import java.nio.ByteBuffer
import java.util.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.collections.HashMap
import kotlin.coroutines.CoroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue
import kotlin.time.milliseconds
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
            val ingestFile = args.firstOrNull { str -> str.startsWith("-ingest=") }
                                 ?.substringAfter('=')
                             ?: System.getProperty("upnut.ingest")
                             ?: "ingest.json"
            val r2dbcFile = args.firstOrNull { str -> str.startsWith("-r2dbc=") }
                                ?.substringAfter('=')
                            ?: System.getProperty("upnut.r2dbc")
                            ?: "r2dbc.json"

            val baseConfig: JsonObject = File(ingestFile).takeIf(File::exists)?.readText()?.let(Json::decodeFromString) ?: JsonObject(emptyMap())
            val r2dbc = baseConfig.getJsonObjectOrNull("r2dbc") ?: File(r2dbcFile).takeIf(File::exists)?.readText()?.let(Json::decodeFromString) ?: JsonObject(emptyMap())
            val ingest = NutIngestation(baseConfig, UpNutClient(r2dbc))

            runBlocking { ingest.join() }
        }
    }

    override val coroutineContext: CoroutineContext = SupervisorJob() + Dispatchers.IO

    val processEvents: suspend (events: List<UpNutEvent>) -> Unit = NutBuilder().buildEventProcessing()
    val processNuts: suspend (time: Long, events: List<UpNutEvent>) -> Unit = NutBuilder().buildNutProcessing()

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

        nuts.client.sql("CREATE TABLE IF NOT EXISTS library (id UUID NOT NULL PRIMARY KEY, chapter_title_redacted VARCHAR(128), book_title VARCHAR(128) NOT NULL, chapter_title VARCHAR(128) NOT NULL, index_in_book INT NOT NULL, redacted BOOLEAN NOT NULL DEFAULT TRUE);")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS webhooks (id BIGSERIAL PRIMARY KEY, url VARCHAR(256) NOT NULL, subscribed_to BIGINT NOT NULL, secret_key bytea NOT NULL)")
            .await()

        nuts.client.sql("CREATE TABLE IF NOT EXISTS events (id BIGSERIAL PRIMARY KEY, send_to VARCHAR(256) NOT NULL, data json NOT NULL, created BIGINT NOT NULL, sent_at BIGINT)")
            .await()

        nuts.client.sql("CREATE INDEX IF NOT EXISTS snow_index_uuid ON snow_crystals (uuid);")
            .await()

//        nuts.client.sql("CREATE TABLE IF NOT EXISTS etags (url VARCHAR(256) NOT NULL, etag VARCHAR(256) NOT NULL);")
//            .await()
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

            timeout {
                socketTimeoutMillis = 20_000L
            }
        }
    }
    val blaseballApi = BlaseballApi(http)

    val defaultScope = config.getJsonObjectOrNull("default")

    fun getIntInScope(scope: String, key: String, default: Int) =
        config.getJsonObjectOrNull(scope)?.getIntOrNull(key)
        ?: defaultScope?.getIntOrNull(key)
        ?: default

    fun getLongInScope(scope: String, key: String, default: Long) =
        config.getJsonObjectOrNull(scope)?.getLongOrNull(key)
        ?: defaultScope?.getLongOrNull(key)
        ?: default

    fun getBooleanInScope(scope: String, key: String, default: Boolean) =
        config.getJsonObjectOrNull(scope)?.getBooleanOrNull(key)
        ?: defaultScope?.getBooleanOrNull(key)
        ?: default

    fun getIntInScope(scope: JsonObject, key: String, default: Int) =
        scope.getIntOrNull(key)
        ?: defaultScope?.getIntOrNull(key)
        ?: default

    fun getLongInScope(scope: JsonObject, key: String, default: Long) =
        scope.getLongOrNull(key)
        ?: defaultScope?.getLongOrNull(key)
        ?: default

    fun getBooleanInScope(scope: JsonObject, key: String, default: Boolean) =
        scope.getBooleanOrNull(key)
        ?: defaultScope?.getBooleanOrNull(key)
        ?: default

    fun getStringInScope(scope: JsonObject, key: String, default: String) =
        scope.getStringOrNull(key)
        ?: defaultScope?.getStringOrNull(key)
        ?: default

    fun getIntInScope(scopes: Iterable<String>, key: String, default: Int) =
        scopes.firstOrNull(config::containsKey)?.let(config::getJsonObjectOrNull)?.getIntOrNull(key)
        ?: defaultScope?.getIntOrNull(key)
        ?: default

    fun getLongInScope(scopes: Iterable<String>, key: String, default: Long) =
        scopes.firstOrNull(config::containsKey)?.let(config::getJsonObjectOrNull)?.getLongOrNull(key)
        ?: defaultScope?.getLongOrNull(key)
        ?: default

    val ingestLogger = LoggerFactory.getLogger("dev.brella.blasement.upnut.NutIngestation")

    @OptIn(ExperimentalStdlibApi::class, ExperimentalTime::class)
    val sources = buildList<ShellSource> {
        val teams = runBlocking { blaseballApi.getAllTeams().getOrElse(emptyList()) }
        val players = runBlocking { http.get<List<PlayerIdPair>>("https://www.blaseball.com/database/playerNamesIds") }
            .toReverseMap()

        config.getJsonArrayOrNull("sources")?.forEach { element ->
            if (element !is JsonObject) return@forEach

            when (val sourceType = element.getString("type").toLowerCase()) {
                "global" -> {
                    val name = element.getStringOrNull("name")

                    val logger = LoggerFactory.getLogger(element.getStringOrNull("logger_name") ?: "dev.brella.blasement.upnut.ingest.${name ?: "Global"}")

                    val limit = getIntInScope(element, "limit", 100)
                    val loopEvery = getIntInScope(element, "loop_duration_s", 60)
                    val delay = getLongInScope(element, "delay_ms", 100)
                    val delayOnFailure = getLongInScope(element, "delay_on_failure_ms", 100)
                    val totalLimit = getLongInScope(element, "total_limit", 100_000)

                    val sortBy = when (val sortBy = element.getStringOrNull("sort_by")?.toLowerCase()) {
                        "newest" -> 0
                        "oldest" -> 1
                        "top" -> 2
                        "hot" -> 3
                        null -> 3
                        else -> sortBy.toIntOrNull() ?: 3
                    }

                    val category = when (val category = element.getStringOrNull("category")?.toLowerCase()) {
                        "plays" -> 0
                        "changes" -> 1
                        "special" -> 2
                        "outcomes" -> 3
                        "book_feed" -> 4
                        "null" -> null
                        null -> null
                        else -> category.toIntOrNull()
                    }

                    add(ShellSource.GlobalFeed(loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, logger, sortBy = sortBy, category = category))
                }
                "team" -> {
                    val teamName = element.getString("team")
                    val name = element.getStringOrNull("name")

                    val logger = LoggerFactory.getLogger(element.getStringOrNull("logger_name") ?: "dev.brella.blasement.upnut.ingest.${name ?: "Team"}")

                    val limit = getIntInScope(element, "limit", 100)
                    val loopEvery = getIntInScope(element, "loop_duration_s", 60)
                    val delay = getLongInScope(element, "delay_ms", 100)
                    val delayOnFailure = getLongInScope(element, "delay_on_failure_ms", 100)
                    val totalLimit = getLongInScope(element, "total_limit", Long.MAX_VALUE)

                    val sortBy = when (val sortBy = element.getStringOrNull("sort_by")?.toLowerCase()) {
                        "newest" -> 0
                        "oldest" -> 1
                        "top" -> 2
                        "hot" -> 3
                        null -> 3
                        else -> sortBy.toIntOrNull() ?: 3
                    }

                    val category = when (val category = element.getStringOrNull("category")?.toLowerCase()) {
                        "plays" -> 0
                        "changes" -> 1
                        "special" -> 2
                        "outcomes" -> 3
                        "book_feed" -> 4
                        "null" -> null
                        null -> null
                        else -> category.toIntOrNull()
                    }

                    if (teamName.equals("all", true)) {
                        teams.forEach { team ->
                            add(ShellSource.TeamFeed(team.id.id, loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, logger, sortBy = sortBy, category = category))
                        }
                    } else {
                        teams.firstOrNull { team -> team.nickname.equals(teamName, true) || team.location.equals(teamName, true) || team.fullName.equals(teamName, true) }
                            ?.let { add(ShellSource.TeamFeed(it.id.id, loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, logger, sortBy = sortBy, category = category)) }
                        ?: add(ShellSource.TeamFeed(teamName, loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, logger, sortBy = sortBy, category = category))
                    }
                }
                "player" -> {
                    val playerName = element.getString("player")
                    val name = element.getStringOrNull("name")

                    val logger = LoggerFactory.getLogger(element.getStringOrNull("logger_name") ?: "dev.brella.blasement.upnut.ingest.${name ?: "Player"}")

                    val limit = getIntInScope(element, "limit", 100)
                    val loopEvery = getIntInScope(element, "loop_duration_s", 60)
                    val delay = getLongInScope(element, "delay_ms", 100)
                    val delayOnFailure = getLongInScope(element, "delay_on_failure_ms", 100)
                    val totalLimit = getLongInScope(element, "total_limit", Long.MAX_VALUE)

                    val sortBy = when (val sortBy = element.getStringOrNull("sort_by")?.toLowerCase()) {
                        "newest" -> 0
                        "oldest" -> 1
                        "top" -> 2
                        "hot" -> 3
                        null -> 3
                        else -> sortBy.toIntOrNull() ?: 3
                    }

                    val category = when (val category = element.getStringOrNull("category")?.toLowerCase()) {
                        "plays" -> 0
                        "changes" -> 1
                        "special" -> 2
                        "outcomes" -> 3
                        "book_feed" -> 4
                        "null" -> null
                        null -> null
                        else -> category.toIntOrNull()
                    }

                    players[playerName]?.let { playerID -> add(ShellSource.PlayerFeed(playerID.id, loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, logger, sortBy = sortBy, category = category)) }
                    ?: add(ShellSource.PlayerFeed(playerName, loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, logger, sortBy = sortBy, category = category))
                }
                "game" -> {
                    val gameID = element.getString("game")
                    val name = element.getStringOrNull("name")

                    val logger = LoggerFactory.getLogger(element.getStringOrNull("logger_name") ?: "dev.brella.blasement.upnut.ingest.${name ?: "Game"}")

                    val limit = getIntInScope(element, "limit", 100)
                    val loopEvery = getIntInScope(element, "loop_duration_s", 60)
                    val delay = getLongInScope(element, "delay_ms", 100)
                    val delayOnFailure = getLongInScope(element, "delay_on_failure_ms", 100)
                    val totalLimit = getLongInScope(element, "total_limit", Long.MAX_VALUE)

                    val sortBy = when (val sortBy = element.getStringOrNull("sort_by")?.toLowerCase()) {
                        "newest" -> 0
                        "oldest" -> 1
                        "top" -> 2
                        "hot" -> 3
                        null -> 3
                        else -> sortBy.toIntOrNull() ?: 3
                    }

                    val category = when (val category = element.getStringOrNull("category")?.toLowerCase()) {
                        "plays" -> 0
                        "changes" -> 1
                        "special" -> 2
                        "outcomes" -> 3
                        "book_feed" -> 4
                        "null" -> null
                        null -> null
                        else -> category.toIntOrNull()
                    }

                    add(ShellSource.GameFeed(gameID, loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, logger, sortBy = sortBy, category = category))
                }
                "story" -> {
                    val storyID = element.getString("story")
                    val name = element.getStringOrNull("name")

                    val logger = LoggerFactory.getLogger(element.getStringOrNull("logger_name") ?: "dev.brella.blasement.upnut.ingest.${name ?: "Story"}")

                    val limit = getIntInScope(element, "limit", 100)
                    val loopEvery = getIntInScope(element, "loop_duration_s", 60)
                    val delay = getLongInScope(element, "delay_ms", 100)
                    val delayOnFailure = getLongInScope(element, "delay_on_failure_ms", 100)
                    val totalLimit = getLongInScope(element, "total_limit", Long.MAX_VALUE)

                    val sortBy = when (val sortBy = element.getStringOrNull("sort_by")?.toLowerCase()) {
                        "newest" -> 0
                        "oldest" -> 1
                        "top" -> 2
                        "hot" -> 3
                        null -> 3
                        else -> sortBy.toIntOrNull() ?: 3
                    }

                    val category = when (val category = element.getStringOrNull("category")?.toLowerCase()) {
                        "plays" -> 0
                        "changes" -> 1
                        "special" -> 2
                        "outcomes" -> 3
                        "book_feed" -> 4
                        "null" -> null
                        null -> null
                        else -> category.toIntOrNull()
                    }

                    add(ShellSource.StoryFeed(storyID, loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, logger, sortBy = sortBy, category = category))
                }
                "librarian" -> {
                    val name = element.getStringOrNull("name")

                    val logger = LoggerFactory.getLogger(element.getStringOrNull("logger_name") ?: "dev.brella.blasement.upnut.ingest.${name ?: "Librarian"}")

                    val limit = getIntInScope(element, "limit", 100)
                    val loopEvery = getIntInScope(element, "loop_duration_s", 60)
                    val delay = getLongInScope(element, "delay_ms", 100)
                    val delayOnFailure = getLongInScope(element, "delay_on_failure_ms", 100)
                    val totalLimit = getLongInScope(element, "total_limit", Long.MAX_VALUE)

                    val sortBy = when (val sortBy = element.getStringOrNull("sort_by")?.toLowerCase()) {
                        "newest" -> 0
                        "oldest" -> 1
                        "top" -> 2
                        "hot" -> 3
                        null -> 3
                        else -> sortBy.toIntOrNull() ?: 3
                    }

                    val category = when (val category = element.getStringOrNull("category")?.toLowerCase()) {
                        "plays" -> 0
                        "changes" -> 1
                        "special" -> 2
                        "outcomes" -> 3
                        "book_feed" -> 4
                        "null" -> null
                        null -> null
                        else -> category.toIntOrNull()
                    }

                    add(ShellSource.Librarian(loopEvery.seconds, limit, delay.milliseconds, totalLimit, http, nuts.client, logger, sortBy = sortBy, category = category))
                }
                else -> ingestLogger.error("No ingest type by name of '{}'", sourceType)
            }
        }

        ingestLogger.info("Reading from the following sources: {}", this)
    }

    @OptIn(ObsoleteCoroutinesApi::class, ExperimentalTime::class)
    val actor = actor<Pair<Long, List<UpNutEvent>>> {
        var lastTime: Long = now()
        val list: MutableList<Pair<Long, List<UpNutEvent>>> = ArrayList()
        var swapping = false

        val receiveJob = receiveAsFlow().onEach { while (swapping) yield(); list.add(it) }.launchIn(this)

        initJob.join()

        loopEvery(60.seconds, { isActive }) {
            swapping = true
            val events = list.toTypedArray()
            list.clear()
            swapping = false

            println("Ingesting ${events.size} collected events")

            val existing: MutableMap<UUID, Long> = HashMap()

            println(
                "Ingest complete in ${
                    measureTime {
                        events.flatMap(Pair<Long, List<UpNutEvent>>::second)
                            .distinctBy(UpNutEvent::id)
                            .let { events ->
                                val timeTaken = measureTime { processEvents(events) }
                                ingestLogger.trace("Processed events in {}", timeTaken)
                            }

                        events
                            .groupBy(Pair<Long, List<UpNutEvent>>::first, Pair<Long, List<UpNutEvent>>::second)
                            .mapValues { (_, list) -> list.flatten().distinctBy(UpNutEvent::id) }
                            .entries
                            .sortedBy(Map.Entry<Long, List<UpNutEvent>>::key)
                            .forEach { (time, events) ->
                                if (time <= lastTime) {
                                    ingestLogger.warn("ERR: Backwards time travel; going from {} to {} ??", lastTime, time)
                                    return@forEach
                                }

                                val newEvents = events.filter { event ->
                                    val before = existing[event.id] ?: return@filter true

                                    val beforeNuts = (before and 0xFFFFFFFF).toInt()
                                    val beforeScales = (before shr 32).toInt()

                                    val eventNuts = event.nuts.intOrNull ?: 0
                                    val eventScales = event.scales.intOrNull ?: 0

                                    return@filter eventNuts > beforeNuts || eventScales > beforeScales
                                }

                                if (newEvents.isEmpty()) return@forEach

                                val timeTaken = measureTime { processNuts(time, newEvents) }
                                ingestLogger.trace("Processing complete in {}", timeTaken)
                                newEvents.forEach { event ->
                                    existing[event.id] = (event.nuts.longOrNull?.and(0xFFFFFFFF) ?: 0) or (event.scales.longOrNull?.shl(32) ?: 0)
                                }

                                lastTime = time
                            }
                    }
                }")

        }
    }

    val jobs = sources.map { shellSource -> launch { shellSource.processNuts(this, actor) } }

    @OptIn(ExperimentalTime::class)
    val librarian = launch {
        initJob.join()

        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.Librarian")

        val loopEvery = getIntInScope("librarian", "loop_duration_s", 60)
        val delay = getLongInScope("librarian", "delay_ms", 100)
        val delayOnFailure = getLongInScope("librarian", "delay_on_failure_ms", 100)
        val totalLimit = getLongInScope("librarian", "total_limit", Long.MAX_VALUE)

        var etag: String? = null

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
            try {
                val retrievalTime = now()

                val response = http.get<HttpResponse>("https://raw.githubusercontent.com/xSke/blaseball-site-files/main/data/library.json")
                val mainEtag = response.etag()

                if (etag == null || etag != mainEtag) {
                    etag = mainEtag

                    logger.debug("Testing new books...")

                    val books = Json.decodeFromString<List<LibraryBook>>(response.receive<String>())

                    val booksReturned = nuts.client.sql("SELECT id, chapter_title, redacted FROM library").map { row ->
                        Pair(row.getValue<UUID>("id"), Pair(row.getValue<String>("chapter_title"), row.get<Boolean>("redacted")))
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
                        chapters.forEachIndexed { index, chapter ->
                            val existing = booksReturned[chapter.id]
                            if (existing == null) {
                                //New book just dropped

                                logger.info("New book just dropped: {} / {}", bookName, chapter.title)

                                nuts.client.sql("INSERT INTO library (id, book_title, chapter_title, redacted, index_in_book) VALUES ($1, $2, $3, $4, $5)")
                                    .bind("$1", chapter.id)
                                    .bind("$2", bookName)
                                    .bind("$3", chapter.title)
                                    .bind("$4", chapter.redacted)
                                    .bind("$5", index)
                                    .await()

                                val chapter = WebhookEvent.LibraryChapter(bookName, chapter.id, chapter.title, null, chapter.redacted)
                                chaptersAdded.add(chapter)
                                if (chapter.isRedacted) chaptersLocked.add(chapter)
                                else chaptersUnlocked.add(chapter)

                            } else if (chapter.redacted != existing.second) {
                                //Book is now unredacted! -- Right ?

                                logger.info("Book has shifted redactivity: {} -> {}, {} -> {}", existing.first, chapter.title, existing.second, chapter.redacted)

                                nuts.client.sql("UPDATE library SET chapter_title = $1, chapter_title_redacted = $2, redacted = $3, index_in_book = $4 WHERE id = $5")
                                    .bind("$1", chapter.title)
                                    .bind("$2", existing.first)
                                    .bind("$3", chapter.redacted)
                                    .bind("$4", index)
                                    .bind("$5", chapter.id)
                                    .await()

                                val chapter = WebhookEvent.LibraryChapter(bookName, chapter.id, chapter.title, existing.first, chapter.redacted)
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
            } catch (th: Throwable) {
                th.printStackTrace()
            }
        }
    }

    suspend fun postEvent(event: WebhookEvent, eventType: Int): Job {
        val now = now()

        val eventData = Json.encodeToString(event).encodeToByteArray()
        val eventAsJson = R2Json.of(eventData)

        val statement = nuts.client.sql("INSERT INTO events ( send_to, data, created, sent_at ) VALUES ( $1, $2, $3, $4 )")
            .bind("$2", eventAsJson)
            .bind("$3", now)

        return launch {
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
        suspend inline fun players(event: UpNutEvent) {
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

        @OptIn(ExperimentalTime::class)
        suspend inline fun players(events: List<UpNutEvent>) {
            val playerMap = measureTimedValue {
                nuts.client.sql("SELECT feed_id, player_id FROM player_nuts WHERE feed_id = ANY($1)")
                    .bind("$1", Array(events.size) { events[it].id })
                    .map { row -> Pair(row.getValue<UUID>("feed_id"), row.getValue<UUID>("player_id")) }
                    .all()
                    .collectList()
                    .awaitFirstOrNull()
                    ?.groupBy(Pair<UUID, UUID>::first, Pair<UUID, UUID>::second)
            }.let { println("Players Map: ${it.duration}"); it.value }
                            ?: emptyMap()


            try {
                nuts.client.inConnectionAwait { connection ->
                    events.chunked(100).forEach { chunk ->
                        val statement = connection
                            .createStatement("INSERT INTO player_nuts (feed_id, player_id) VALUES ($1, $2)")

                        var count = 0
                        chunk.forEach { event ->
                            val players = playerMap[event.id] ?: emptyList()

                            event.playerTags?.filterNot(players::contains)
                                ?.forEach { playerID ->
                                    count++
                                    statement
                                        .bind(0, event.id)
                                        .bind(1, playerID)
                                        .add()
                                }
                        }

                        if (count > 0) statement.awaitRowsUpdated()
                    }
                }
            } catch (th: Throwable) {
                th.printStackTrace()
            }
        }

        suspend inline fun games(event: UpNutEvent) {
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

        suspend inline fun games(events: List<UpNutEvent>) {
            val gamesMap = nuts.client.sql("SELECT feed_id, game_id FROM game_nuts WHERE feed_id = ANY($1)")
                               .bind("$1", Array(events.size) { events[it].id })
                               .map { row -> Pair(row.getValue<UUID>("feed_id"), row.getValue<UUID>("game_id")) }
                               .all()
                               .collectList()
                               .awaitFirstOrNull()
                               ?.groupBy(Pair<UUID, UUID>::first, Pair<UUID, UUID>::second)
                           ?: emptyMap()

            nuts.client.inConnectionAwait { connection ->
                events.chunked(100).forEach { chunk ->
                    val statement = connection.createStatement("INSERT INTO game_nuts (feed_id, game_id) VALUES ($1, $2)")
                    var count = 0
                    chunk.forEach { event ->
                        val games = gamesMap[event.id] ?: emptyList()

                        event.gameTags?.filterNot(games::contains)
                            ?.forEach { gameID ->
                                count++
                                statement
                                    .bind("$1", event.id)
                                    .bind("$2", gameID)
                                    .add()
                            }
                    }

                    if (count > 0) statement.awaitRowsUpdated()
                }
            }
        }

        suspend inline fun teams(event: UpNutEvent) {
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

        suspend inline fun teams(events: List<UpNutEvent>) {
            val teamsMap = nuts.client.sql("SELECT feed_id, team_id FROM team_nuts WHERE feed_id = ANY($1)")
                               .bind("$1", Array(events.size) { events[it].id })
                               .map { row -> Pair(row.getValue<UUID>("feed_id"), row.getValue<UUID>("team_id")) }
                               .all()
                               .collectList()
                               .awaitFirstOrNull()
                               ?.groupBy(Pair<UUID, UUID>::first, Pair<UUID, UUID>::second)
                           ?: emptyMap()

            nuts.client.inConnectionAwait { connection ->
                events.chunked(100).forEach { chunk ->
                    val insertNuts = connection.createStatement("INSERT INTO team_nuts (feed_id, team_id) VALUES (\$1, \$2)")
                    var insertCount = 0
                    chunk.forEach { event ->
                        val teams = teamsMap[event.id] ?: emptyList()

                        event.teamTags?.filterNot(teams::contains)
                            ?.forEach { teamID ->
                                insertCount++
                                insertNuts
                                    .bind("$1", event.id)
                                    .bind("$2", teamID)
                                    .add()
                            }
                    }

                    if (insertCount > 0) insertNuts.awaitRowsUpdated()
                }
            }
        }

        suspend inline fun metadata(event: UpNutEvent) {
            nuts.client.sql("INSERT INTO event_metadata (feed_id, created, season, tournament, type, day, phase, category) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8 ) ON CONFLICT DO NOTHING")
                .bind("$1", event.id)
                .bind("$2", event.created.toEpochMilliseconds())
                .bind("$3", event.season)
                .bind("$4", event.tournament)
                .bind("$5", event.type)
                .bind("$6", event.day)
                .bind("$7", event.phase)
                .bind("$8", event.category)
                .await()
        }

        @OptIn(ExperimentalTime::class)
        suspend inline fun metadata(events: List<UpNutEvent>) {
            nuts.client.inConnectionAwait { connection ->
                events.chunked(100).forEach { chunk ->
                    val statement =
                        connection.createStatement("INSERT INTO event_metadata (feed_id, created, season, tournament, type, day, phase, category) VALUES ( \$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8 ) ON CONFLICT DO NOTHING")

                    chunk.forEach { event ->
                        statement
                            .bind("$1", event.id)
                            .bind("$2", event.created.toEpochMilliseconds())
                            .bind("$3", event.season)
                            .bind("$4", event.tournament)
                            .bind("$5", event.type)
                            .bind("$6", event.day)
                            .bind("$7", event.phase)
                            .bind("$8", event.category)
                            .add()
                    }

                    statement.awaitRowsUpdated()
                }
            }
        }

        inline operator fun Int?.minus(other: Int?): Int? =
            if (this != null) if (other != null) this - other else this else null

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

                val missing = NUTS_THRESHOLD - atTimeOfRecording!!.first

                if (missing in 1 until nutsDifference) postEvent(WebhookEvent.ThresholdPassedNuts(NUTS_THRESHOLD, time, event), WebhookEvent.THRESHOLD_PASSED_NUTS)
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
                val missing = SCALES_THRESHOLD - atTimeOfRecording!!.second

                if (missing in 1 until scalesDifference) postEvent(WebhookEvent.ThresholdPassedScales(SCALES_THRESHOLD, time, event), WebhookEvent.THRESHOLD_PASSED_SCALES)
            }

            return atTimeOfRecording
        }

        @OptIn(ExperimentalTime::class)
        suspend fun insert(time: Long, events: List<UpNutEvent>): Map<UUID, Pair<Int, Int>> {
            val atTimeOfRecordingMap =
                nuts.client.sql("SELECT feed_id, SUM(nuts) AS nuts, SUM(scales) as scales FROM upnuts WHERE feed_id = ANY($1) AND time <= $2 AND provider = '7fcb63bc-11f2-40b9-b465-f1d458692a63'::uuid AND source IS NULL GROUP BY feed_id")
                    .bind("$1", Array(events.size) { events[it].id })
                    .bind("$2", time)
                    .map { row -> Pair(row.getValue<UUID>("feed_id"), (row.get<Int>("nuts") ?: 0) to (row.get<Int>("scales") ?: 0)) }
                    .all()
                    .collectMap(Pair<UUID, Pair<Int, Int>>::first, Pair<UUID, Pair<Int, Int>>::second)
                    .awaitFirstOrNull()
                ?: emptyMap()

            println(
                "Batch Insert: ${
                    measureTime {
                        nuts.client.inConnectionAwait { connection ->
                            events.chunked(100).forEach { chunk ->
                                val insertNuts = connection.createStatement("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                var nutCount = 0

                                val insertScales = connection.createStatement("INSERT INTO upnuts (scales, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                var scaleCount = 0

                                chunk.forEach { event ->
                                    val atTimeOfRecording = atTimeOfRecordingMap[event.id]
                                    val nutsDifference = event.nuts.intOrNull - atTimeOfRecording?.first
                                    if (nutsDifference != null && nutsDifference > 0) {
                                        nutCount++
                                        insertNuts
                                            .bind("$1", nutsDifference)
                                            .bind("$2", event.id)
                                            .bind("$3", THE_GAME_BAND)
                                            .bind("$4", time)
                                            .add()

                                        val missing = NUTS_THRESHOLD - atTimeOfRecording?.first

                                        if (missing in 1 until nutsDifference) launch { postEvent(WebhookEvent.ThresholdPassedNuts(NUTS_THRESHOLD, time, event), WebhookEvent.THRESHOLD_PASSED_NUTS) }
                                    }

                                    val scalesDifference = event.scales.intOrNull - atTimeOfRecording?.second
                                    if (scalesDifference != null && scalesDifference > 0) {
                                        scaleCount++

                                        insertScales
                                            .bind("$1", scalesDifference)
                                            .bind("$2", event.id)
                                            .bind("$3", THE_GAME_BAND)
                                            .bind("$4", time)
                                            .add()

                                        //We don't know the threshold for scales (or even how they work); making an educated guess
                                        val missing = SCALES_THRESHOLD - atTimeOfRecording?.second

                                        if (missing in 1 until scalesDifference) launch { postEvent(WebhookEvent.ThresholdPassedScales(SCALES_THRESHOLD, time, event), WebhookEvent.THRESHOLD_PASSED_SCALES) }
                                    }
                                }

                                if (nutCount > 0)
                                    insertNuts.awaitRowsUpdated()

                                if (scaleCount > 0)
                                    insertScales.awaitRowsUpdated()
                            }
                        }
                    }
                }"
            )

            return atTimeOfRecordingMap
        }

        suspend inline fun logging(time: Long, event: UpNutEvent, logger: Logger, atTimeOfRecording: Pair<Int, Int>?) {
            if (atTimeOfRecording == null) return
            val nutsDifference = event.nuts.intOrNull - atTimeOfRecording.first
            if (nutsDifference != null && nutsDifference > 0) logger.info("{} +{} nuts", event.id, nutsDifference)

            val scalesDifference = event.scales.intOrNull - atTimeOfRecording.second
            if (scalesDifference != null && scalesDifference > 0) logger.info("{} +{} scales", event.id, scalesDifference)
        }

        suspend inline fun logging(time: Long, events: List<UpNutEvent>, logger: Logger, atTimeOfRecordingMap: Map<UUID, Pair<Int, Int>>) {
            events.forEach { event ->
                val atTimeOfRecording = atTimeOfRecordingMap[event.id]
                val nutsDifference = event.nuts.intOrNull - atTimeOfRecording?.first
                if (nutsDifference != null && nutsDifference > 0) logger.info("{} +{} nuts", event.id, nutsDifference)

                val scalesDifference = event.scales.intOrNull - atTimeOfRecording?.second
                if (scalesDifference != null && scalesDifference > 0) logger.info("{} +{} scales", event.id, scalesDifference)
            }
        }


        @OptIn(ExperimentalTime::class)
        inline fun buildEventProcessing(): suspend (events: List<UpNutEvent>) -> Unit = run {
            val config = config.getJsonObjectOrNull("process")

            val playerTags = config?.getBooleanOrNull("player_tags") ?: true
            val gameTags = config?.getBooleanOrNull("game_tags") ?: true
            val teamTags = config?.getBooleanOrNull("team_tags") ?: true
            val eventMetadata = config?.getBooleanOrNull("event_metadata") ?: true

            val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.EventProcessing")

            if (playerTags) {
                if (gameTags) {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ events ->
                                println("Players: ${measureTime { players(events) }}")
                                println("Games: ${measureTime { games(events) }}")
                                println("Teams: ${measureTime { teams(events) }}")
                                println("Metadata: ${measureTime { metadata(events) }}")
                            }
                        } else {
                            return@run nuts@{ events ->
                                players(events)
                                games(events)
                                teams(events)
//                                    metadata(events)
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ events ->
                                players(events)
                                games(events)
//                                    teams(events)
                                metadata(events)
                            }
                        } else {
                            return@run nuts@{ events ->
                                players(events)
                                games(events)
//                                    teams(events)
//                                    metadata(events)
                            }
                        }
                    }
                } else {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ events ->
                                players(events)
//                                    games(events)
                                teams(events)
                                metadata(events)
                            }
                        } else {
                            return@run nuts@{ events ->
                                players(events)
//                                    games(events)
                                teams(events)
//                                    metadata(events)
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ events ->
                                players(events)
//                                    games(events)
//                                    teams(events)
                                metadata(events)
                            }
                        } else {
                            return@run nuts@{ events ->
                                players(events)
//                                    games(events)
//                                    teams(events)
//                                    metadata(events)
                            }
                        }
                    }
                }
            } else {
                if (gameTags) {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ events ->
//                                    players(events)
                                games(events)
                                teams(events)
                                metadata(events)
                            }
                        } else {
                            return@run nuts@{ events ->
//                                    players(events)
                                games(events)
                                teams(events)
//                                    metadata(events)
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ events ->
//                                    players(events)
                                games(events)
//                                    teams(events)
                                metadata(events)
                            }
                        } else {
                            return@run nuts@{ events ->
//                                    players(events)
                                games(events)
//                                    teams(events)
//                                    metadata(events)
                            }
                        }
                    }
                } else {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ events ->
//                                    players(events)
//                                    games(events)
                                teams(events)
                                metadata(events)
                            }
                        } else {
                            return@run nuts@{ events ->
//                                    players(events)
//                                    games(events)
                                teams(events)
//                                    metadata(events)
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ events ->
//                                    players(events)
//                                    games(events)
//                                    teams(events)
                                metadata(events)
                            }
                        } else {
                            return@run nuts@{ events ->
//                                    players(events)
//                                    games(events)
//                                    teams(events)
//                                    metadata(events)
                            }
                        }
                    }
                }
            }
        }

        @OptIn(ExperimentalTime::class)
        inline fun buildNutProcessing(): suspend (time: Long, events: List<UpNutEvent>) -> Unit = run {
            val config = config.getJsonObjectOrNull("process")

            val loggingEnabled = config?.getBooleanOrNull("logging_enabled") ?: true

            val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.ingest.NutProcessing")

            if (loggingEnabled) {
                return@run nuts@{ time, event ->
                    println("Logging + Insert: ${measureTime { logging(time, event, logger, insert(time, event)) }}")
                }
            } else {
                return@run nuts@{ time, event ->
                    insert(time, event)
                }
            }
        }

/*        @OptIn(ExperimentalTime::class)
        inline fun buildNutProcessing(): suspend (time: Long, events: List<UpNutEvent>) -> Unit = run {
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
                                    println("Players: ${measureTime { players(time, event) }}")
                                    println("Games: ${measureTime { games(time, event) }}")
                                    println("Teams: ${measureTime { teams(time, event) }}")
                                    println("Metadata: ${measureTime { metadata(time, event) }}")

                                    println("Logging + Insert: ${measureTime { logging(time, event, logger, insert(time, event)) }}")
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
        }*/
    }

    data class ResendEvent(val id: Long, val sendTo: String, val data: ByteArray, val created: DateTimeTz) {
        constructor(row: Row) : this(row.getValue("id"), row.getValue("send_to"), row.getValue("data"), DateTime.fromUnix(row.getValue<Long>("created")).utc)
    }

    @OptIn(ExperimentalTime::class)
    val resendJob = launch {
        initJob.join()

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
//        globalByTopJob.join()
//        globalByHotJob.join()
//        teamsByTopJob.join()
//        teamsByHotJob.join()

        jobs.joinAll()
    }
}