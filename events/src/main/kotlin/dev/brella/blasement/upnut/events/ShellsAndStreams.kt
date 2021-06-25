package dev.brella.blasement.upnut.events

import dev.brella.blasement.upnut.common.UpNutClient
import dev.brella.blasement.upnut.common.WebhookEvent
import dev.brella.blasement.upnut.common.getBooleanOrNull
import dev.brella.blasement.upnut.common.getIntOrNull
import dev.brella.blasement.upnut.common.getJsonObjectOrNull
import dev.brella.blasement.upnut.common.getLongOrNull
import dev.brella.blasement.upnut.common.getStringOrNull
import dev.brella.blasement.upnut.common.getValue
import dev.brella.blasement.upnut.common.loopEvery
import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.ktornea.common.postAsResult
import io.ktor.application.*
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.compression.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.client.utils.*
import io.ktor.content.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.serialization.*
import io.ktor.util.*
import io.r2dbc.spi.Row
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.datetime.Instant
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import org.springframework.r2dbc.core.await
import java.io.File
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.coroutines.CoroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.seconds
import org.springframework.r2dbc.core.bind as bindNullable

class ShellsAndStreams : CoroutineScope {
    companion object {
        const val SHELLHOOK_TYPE_DATA = 0
        const val SHELLHOOK_TYPE_DISCORD = 1
    }

    override val coroutineContext: CoroutineContext = SupervisorJob() + Dispatchers.IO

    val configJson: JsonObject? = File(System.getProperty("upnut.events") ?: "events.json").takeIf(File::exists)?.readText()?.let(Json::decodeFromString)
    val upnutClient = UpNutClient(configJson?.getJsonObjectOrNull("r2dbc") ?: File(System.getProperty("upnut.r2dbc") ?: "r2dbc.json").readText().let(Json::decodeFromString))
    val http = HttpClient(OkHttp) {
        install(ContentEncoding) {
            gzip()
            deflate()
            identity()
        }

        install(JsonFeature) {
            serializer = KotlinxSerializer(Json(Json.Default) {
                ignoreUnknownKeys = true
            })
        }

        expectSuccess = false

        defaultRequest {
            userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0")
        }
    }

    data class PopulateEvent(val id: Long, val type: Int, val data: ByteArray, val created: Long) {
        constructor(row: Row) : this(row.getValue("id"), row.getValue("type"), row.getValue("data"), row.getValue("created"))
    }

    data class ResendEvent(val id: Long, val sendTo: String, val data: ByteArray, val created: Instant) {
        constructor(row: Row) : this(row.getValue("id"), row.getValue("send_to"), row.getValue("data"), Instant.fromEpochMilliseconds(row.getValue<Long>("created")))
    }

    data class StreamHook(val id: Long, val url: String, val secretKey: ByteArray, val type: Int) {
        constructor(row: Row): this(row.getValue("id"), row.getValue("url"), row.getValue("secret_key"), row.getValue("type"))
    }

    val defaultScope = configJson?.getJsonObjectOrNull("default")

    fun getIntInScope(scope: String, key: String, default: Int) =
        configJson?.getJsonObjectOrNull(scope)?.getIntOrNull(key)
        ?: defaultScope?.getIntOrNull(key)
        ?: default

    fun getLongInScope(scope: String, key: String, default: Long) =
        configJson?.getJsonObjectOrNull(scope)?.getLongOrNull(key)
        ?: defaultScope?.getLongOrNull(key)
        ?: default

    fun getBooleanInScope(scope: String, key: String, default: Boolean) =
        configJson?.getJsonObjectOrNull(scope)?.getBooleanOrNull(key)
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
        configJson?.let { scopes.firstOrNull(it::containsKey)?.let(it::getJsonObjectOrNull) }?.getIntOrNull(key)
        ?: defaultScope?.getIntOrNull(key)
        ?: default

    fun getLongInScope(scopes: Iterable<String>, key: String, default: Long) =
        configJson?.let { scopes.firstOrNull(it::containsKey)?.let(it::getJsonObjectOrNull) }?.getLongOrNull(key)
        ?: defaultScope?.getLongOrNull(key)
        ?: default

    val initJob = launch {
        upnutClient.client.sql("CREATE TABLE IF NOT EXISTS webhooks (id BIGSERIAL PRIMARY KEY, url VARCHAR(256) NOT NULL, subscribed_to BIGINT NOT NULL, secret_key bytea NOT NULL, type INT NOT NULL);")
            .await()

        upnutClient.client.sql("CREATE TABLE IF NOT EXISTS events (id BIGSERIAL PRIMARY KEY, webhook BIGSERIAL, event BIGSERIAL, created BIGINT NOT NULL, sent_at BIGINT);")
            .await()
    }

    inline fun now() = java.time.Clock.systemUTC().instant().toEpochMilli()

    /*suspend fun postEvent(event: WebhookEvent, eventType: Int): Job {
        val now = now()

        val eventData = Json.encodeToString(event).encodeToByteArray()
        val eventAsJson = io.r2dbc.postgresql.codec.Json.of(eventData)

        val statement = upnutClient.client.sql("INSERT INTO events ( send_to, data, created, sent_at ) VALUES ( $1, $2, $3, $4 )")
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
    }*/

    suspend inline fun postEventTo(hook: StreamHook, eventData: ByteArray): KorneaResult<EmptyContent> {
        return when (hook.type) {
            SHELLHOOK_TYPE_DATA -> {
                val sig = Mac.getInstance("HmacSHA256")
                    .apply { init(SecretKeySpec(hook.secretKey, "HmacSHA256")) }
                    .doFinal(eventData)
                    .let(::hex)

                http.postAsResult<EmptyContent>(hook.url) {
                    header("X-UpNut-Signature", sig)

                    body = ByteArrayContent(eventData, contentType = ContentType.Application.Json)
                }
            }

            SHELLHOOK_TYPE_DISCORD -> {
                val payload = Json.decodeFromString<WebhookEvent>(eventData.decodeToString())

                val discordEvent = when (payload) {
                    is WebhookEvent.LibraryChaptersRedacted -> buildJsonObject {
                        putJsonArray("embeds") {
                            addJsonObject {
                                put("title", "An old edition has been removed from Public Access")
                                putJsonObject("thumbnail") {
                                    put("url", "https://cdn.discordapp.com/attachments/818811060349566988/843769330512035851/library.png")
                                }

                                putJsonArray("fields") {
                                    payload.chapters
                                        .groupBy(WebhookEvent.LibraryChapter::bookName)
                                        .forEach { (bookName, chapters) ->
                                            addJsonObject {
                                                put("name", bookName)
                                                put("value", buildString {
                                                    chapters.forEach { event ->
                                                        val chapterNameRedacted = event.chapterNameRedacted
                                                        val chapterName = event.chapterName

                                                        if (chapterNameRedacted != null) {
                                                            if (chapterName != null) {
                                                                appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            } else {
                                                                appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            }
                                                        } else {
                                                            if (event.chapterName != null) {
                                                                appendLine("[${event.chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            } else {
                                                                appendLine("[$${event.bookIndex}/${event.bookIndex + 1}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            }
                                                        }
                                                    }
                                                })
                                            }
                                        }
                                }
                            }
                        }
                    }
                    is WebhookEvent.LibraryChaptersUnredacted -> buildJsonObject {
                        putJsonArray("embeds") {
                            addJsonObject {
                                put("title", "A new edition has been approved for Public Access")
                                putJsonObject("thumbnail") {
                                    put("url", "https://cdn.discordapp.com/attachments/818811060349566988/843769330512035851/library.png")
                                }

                                putJsonArray("fields") {
                                    payload.chapters
                                        .groupBy(WebhookEvent.LibraryChapter::bookName)
                                        .forEach { (bookName, chapters) ->
                                            addJsonObject {
                                                put("name", bookName)
                                                put("value", buildString {
                                                    chapters.forEach { event ->
                                                        val chapterNameRedacted = event.chapterNameRedacted
                                                        val chapterName = event.chapterName

                                                        if (chapterNameRedacted != null) {
                                                            if (chapterName != null) {
                                                                appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            } else {
                                                                appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            }
                                                        } else {
                                                            if (event.chapterName != null) {
                                                                appendLine("[${event.chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            } else {
                                                                appendLine("[$${event.bookIndex}/${event.bookIndex + 1}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            }
                                                        }
                                                    }
                                                })
                                            }
                                        }
                                }
                            }
                        }
                    }
                    is WebhookEvent.NewLibraryChapters -> buildJsonObject {
                        putJsonArray("embeds") {
                            addJsonObject {
                                put("title", "A new edition has been approved for Public Access")
                                putJsonObject("thumbnail") {
                                    put("url", "https://cdn.discordapp.com/attachments/818811060349566988/843769330512035851/library.png")
                                }

                                putJsonArray("fields") {
                                    payload.chapters
                                        .groupBy(WebhookEvent.LibraryChapter::bookName)
                                        .forEach { (bookName, chapters) ->
                                            addJsonObject {
                                                put("name", bookName)
                                                put("value", buildString {
                                                    chapters.forEach { event ->
                                                        val chapterNameRedacted = event.chapterNameRedacted
                                                        val chapterName = event.chapterName

                                                        if (chapterNameRedacted != null) {
                                                            if (chapterName != null) {
                                                                appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            } else {
                                                                appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            }
                                                        } else {
                                                            if (event.chapterName != null) {
                                                                appendLine("[${event.chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            } else {
                                                                appendLine("[$${event.bookIndex}/${event.bookIndex + 1}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                                                appendLine()
                                                            }
                                                        }
                                                    }
                                                })
                                            }
                                        }
                                }
                            }
                        }
                    }

                    else -> return KorneaResult.success(EmptyContent)
                }

                http.postAsResult<EmptyContent>(hook.url) {
                    header("User-Agent", "Blasement 1.0")
                    contentType(ContentType.Application.Json)
                    body = discordEvent
                }
            }

            else -> KorneaResult.success(EmptyContent)
        }
    }

    suspend fun webhooksFor(eventType: Int): List<StreamHook> =
        upnutClient.client.sql("SELECT id, url, secret_key, type FROM webhooks WHERE subscribed_to & $1 = $1")
            .bind("$1", eventType)
            .map(::StreamHook)
            .all()
            .collectList()
            .awaitFirstOrNull()
            ?.filterNotNull() ?: emptyList()

    @OptIn(ExperimentalTime::class)
    val populateJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.events.Populate")
        val loopEvery = getIntInScope("populate", "loop_duration_s", 60)

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
            val eventsToPopulate = upnutClient.client.sql("SELECT id, type, data, created FROM event_log WHERE processed = FALSE")
                                       .map(::PopulateEvent)
                                       .all()
                                       .collectList()
                                       .awaitFirstOrNull() ?: return@loopEvery

            logger.info("Populating {} events", eventsToPopulate.size)

            val insertEvent = upnutClient.client.sql("INSERT INTO events ( webhook, event, created, sent_at ) VALUES ( $1, $2, $3, $4 )")
            val updateEventLog = upnutClient.client.sql("UPDATE event_log SET processed = TRUE WHERE id = $1")

            val now = now()
            eventsToPopulate.forEach { event ->
                insertEvent.bind("$2", event.id)
                insertEvent.bind("$3", event.created)

                webhooksFor(event.type)
                    .map { hook ->
                        async { hook to (postEventTo(hook, event.data) is KorneaResult.Success<*>) }
                    }.awaitAll().forEach { (url, successful) ->
                        try {
                            insertEvent.bind("$1", url)
                                .bindNullable("$4", if (successful) now else null)
                                .await()
                        } catch (th: Throwable) {
                            th.printStackTrace()
                        }
                    }

                updateEventLog.bind("$1", event.id)
                    .await()
            }
        }
    }

    @OptIn(ExperimentalTime::class)
/*    val resendJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.events.Resend")

        val limit = getIntInScope("resend", "limit", 100)
        val loopEvery = getIntInScope("resend", "loop_duration_s", 60 * 30)
        val delay = getLongInScope("resend", "delay_ms", 100)
        val delayOnFailure = getLongInScope("resend", "delay_on_failure_ms", 100)
        val totalLimit = getLongInScope("resend", "total_limit", Long.MAX_VALUE)

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
            val eventsToDeliver = upnutClient.client.sql("SELECT id, send_to, data, created FROM events WHERE sent_at IS NULL")
                                      .map(::ResendEvent)
                                      .all()
                                      .collectList()
                                      .awaitFirstOrNull()
                                      ?.groupBy(ResendEvent::sendTo) ?: return@loopEvery

            logger.info("Resending {} events", eventsToDeliver.size)

            val tokens = eventsToDeliver.keys.associateWith { url ->
                upnutClient.client.sql("SELECT secret_key FROM webhooks WHERE url = $1")
                    .bind("$1", url)
                    .map { row -> row.getValue<ByteArray>("secret_key") }
                    .first()
                    .awaitFirstOrNull()
            }

            eventsToDeliver.forEach outer@{ (url, eventList) ->
                val token = tokens[url] ?: return@outer
                eventList.forEach { event ->
                    if (postEventTo(url, token, event.data) is KorneaResult.Success) {
                        upnutClient.client.sql("UPDATE events SET sent_at = $1 WHERE id = $2")
                            .bind("$1", now())
                            .bind("$2", event.id)
                            .await()
                    } else {
                        return@outer
                    }
                }
            }
        }
    }*/

    fun routing(routing: Routing) =
        with(routing) {

        }
}

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

@OptIn(ExperimentalTime::class, kotlin.ExperimentalStdlibApi::class)
fun Application.module() {
    val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    install(ContentNegotiation) {
        json(json)
    }

    install(CORS) {
        anyHost()
    }

    install(ConditionalHeaders)
    install(StatusPages) {
        exception<Throwable> { cause -> call.respond(HttpStatusCode.InternalServerError, cause.stackTraceToString()) }
    }
    install(CallLogging) {
        level = Level.INFO
    }

    routing(ShellsAndStreams()::routing)
}