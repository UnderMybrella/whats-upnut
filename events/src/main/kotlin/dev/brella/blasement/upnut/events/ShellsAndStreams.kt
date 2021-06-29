package dev.brella.blasement.upnut.events

import com.github.benmanes.caffeine.cache.Caffeine
import dev.brella.blasement.upnut.common.*
import dev.brella.d4j.coroutines.rest.service.awaitExecuteWebhook
import dev.brella.d4j.coroutines.rest.service.getWebhookIdFromUrl
import dev.brella.d4j.coroutines.rest.service.getWebhookTokenFromUrl
import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.ktornea.common.KorneaHttpResult
import dev.brella.ktornea.common.postAsResult
import discord4j.rest.RestClient
import discord4j.rest.http.client.ClientException
import discord4j.rest.util.WebhookMultipartRequest
import io.ktor.application.*
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.compression.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.client.request.forms.*
import io.ktor.client.statement.*
import io.ktor.client.utils.*
import io.ktor.content.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.serialization.*
import io.ktor.util.*
import io.ktor.utils.io.core.*
import io.netty.handler.codec.http.HttpResponseStatus
import io.r2dbc.spi.Row
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import org.springframework.r2dbc.core.await
import org.springframework.r2dbc.core.awaitOneOrNull
import java.io.File
import java.util.concurrent.TimeUnit
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

    val configJson: JsonObject? = File(property("upnut.events") ?: "events.json").takeIf(File::exists)?.readText()?.let(Json::decodeFromString)
    val upnutClient = UpNutClient(configJson?.getJsonObjectOrNull("r2dbc") ?: File(property("upnut.r2dbc") ?: "upnuts-r2dbc.json").readText().let(Json::decodeFromString))
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
    val discord = RestClient.create(configJson?.getStringOrNull("discord_token") ?: property("upnut.discord"))

    val teamDetailsCache = Caffeine.newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .maximumSize(100)
        .buildAsync<String, JsonObject> { uuid, _ ->
            future {
                try {
                    http.get<JsonObject>("https://www.blaseball.com/database/team") {
                        parameter("id", uuid)
                    }
                } catch (th: Throwable) {
                    th.printStackTrace()
                    throw th
                }
            }
        }

    val playerDetailsCache = Caffeine.newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .maximumSize(100)
        .buildAsync<String, JsonObject> { uuid, _ ->
            future {
                try {
                    http.get<JsonArray>("https://www.blaseball.com/database/players") {
                        parameter("ids", uuid)
                    }[0].jsonObject
                } catch (th: Throwable) {
                    th.printStackTrace()
                    throw th
                }
            }
        }

    data class PopulateEvent(val id: Long, val type: Int, val data: ByteArray, val created: Long) {
        constructor(row: Row) : this(row.getValue("id"), row.getValue("type"), row.getValue("data"), row.getValue("created"))
    }

    data class ResendEvent(val id: Long, val webhook: Long, val event: Long, val created: Instant, val sentAt: Instant?) {
        constructor(row: Row) : this(row.getValue("id"), row.getValue("webhook"), row.getValue("event"), Instant.fromEpochMilliseconds(row.getValue<Long>("created")), row.get<Long>("sent_at")?.let(Instant::fromEpochMilliseconds))
    }

    data class StreamHook(val id: Long, val url: String, val secretKey: ByteArray, val type: Int) {
        constructor(row: Row) : this(row.getValue("id"), row.getValue("url"), row.getValue("secret_key"), row.getValue("type"))
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
                    is WebhookEvent.HelloWorld -> payload.toDiscordEvent()
                    is WebhookEvent.GoodbyeWorld -> payload.toDiscordEvent()

                    is WebhookEvent.LibraryChaptersRedacted -> payload.toDiscordEvent()
                    is WebhookEvent.LibraryChaptersUnredacted -> payload.toDiscordEvent()
                    is WebhookEvent.NewLibraryChapters -> payload.toDiscordEvent()

                    is WebhookEvent.NewHerringPool -> payload.toDiscordEvent(teamDetailsCache, playerDetailsCache)

                    is WebhookEvent.ThresholdPassedNuts -> payload.toDiscordEvent(teamDetailsCache, playerDetailsCache)
                    is WebhookEvent.ThresholdPassedScales -> payload.toDiscordEvent(teamDetailsCache, playerDetailsCache)

                    else -> return KorneaResult.success(EmptyContent)
                }

                try {
                    discord.webhookService.awaitExecuteWebhook(hook.url.getWebhookIdFromUrl(), hook.url.getWebhookTokenFromUrl(), true, WebhookMultipartRequest(discordEvent))
                    return KorneaResult.success(EmptyContent)
                } catch (th: Throwable) {
                    if (th is ClientException) {
                        when (th.status) {
                            HttpResponseStatus.BAD_REQUEST -> {
                                //This is a fun one, because it means we've formulated a bad message
                                //Log an error, then return 'Success' to prevent an infinite loop

                                return KorneaResult.success(EmptyContent)
                            }
                            HttpResponseStatus.NOT_FOUND -> {
                                //Remove :)

                                upnutClient.client.sql("DELETE FROM webhooks WHERE id = $1")
                                    .bind("$1", hook.id)
                                    .await()
                            }
                        }
                    }

                    return KorneaResult.thrown(th)
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
                webhooksFor(event.type)
                    .map { hook ->
                        async { hook to (postEventTo(hook, event.data) is KorneaResult.Success<*>) }
                    }.awaitAll().forEach { (url, successful) ->
                        try {
                            insertEvent.bind("$1", url.id)
                                .bind("$2", event.id)
                                .bind("$3", event.created)
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
    val resendJob = launch {
        val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.events.Resend")

        val limit = getIntInScope("resend", "limit", 100)
        val loopEvery = getIntInScope("resend", "loop_duration_s", 600)
        val delay = getLongInScope("resend", "delay_ms", 100)
        val delayOnFailure = getLongInScope("resend", "delay_on_failure_ms", 100)
        val totalLimit = getLongInScope("resend", "total_limit", Long.MAX_VALUE)

        loopEvery(loopEvery.seconds, `while` = { isActive }) {
            val eventsToDeliver = upnutClient.client.sql("SELECT id, webhook, event, created, sent_at FROM events WHERE sent_at IS NULL")
                                      .map(::ResendEvent)
                                      .all()
                                      .collectList()
                                      .awaitFirstOrNull()
                                  ?: return@loopEvery

            logger.info("Resending {} events", eventsToDeliver.size)

            val eventData = upnutClient.client.sql("SELECT id, data FROM event_log WHERE id = ANY($1)")
                                .bind("$1", eventsToDeliver.map(ResendEvent::event).distinct().toTypedArray())
                                .map { row -> row.getValue<Long>("id") to row.getValue<ByteArray>("data") }
                                .all()
                                .collectList()
                                .awaitFirstOrNull()
                                ?.toMap() ?: emptyMap()

            val webhooks = upnutClient.client.sql("SELECT id, url, subscribed_to, secret_key, type FROM webhooks WHERE id = ANY($1)")
                               .bind("$1", eventsToDeliver.map(ResendEvent::webhook).distinct().toTypedArray())
                               .map(::StreamHook)
                               .all()
                               .collectList()
                               .awaitFirstOrNull()
                               ?.associateBy(StreamHook::id) ?: emptyMap()

            eventsToDeliver.forEach outer@{ event ->
                val response = webhooks[event.webhook]?.let { webhook ->
                    eventData[event.event]?.let { data ->
                        postEventTo(webhook, data)
                    }
                }

                if (response is KorneaResult.Success) {
                    upnutClient.client.sql("UPDATE events SET sent_at = $1 WHERE id = $2")
                        .bind("$1", now())
                        .bind("$2", event.id)
                        .await()
                } else if (response == null) {
                    upnutClient.client.sql("DELETE FROM events WHERE id = $1")
                        .bind("$1", event.id)
                        .await()
                }
            }
        }
    }

    val discordClientID = requireNotNull(configJson?.getStringOrNull("discord_client_id") ?: property("upnut.discord_client_id"))
    val discordClientSecret = requireNotNull(configJson?.getStringOrNull("discord_client_secret") ?: property("upnut.discord_client_secret"))
    val discordRedirectUri = requireNotNull(configJson?.getStringOrNull("discord_redirect") ?: property("upnut.discord_redirect"))

    fun routing(routing: Routing) =
        with(routing) {
            get("/oauth/discord") {
                val code = call.request.queryParameters.getOrFail("code")

                val postResponse = http.post<HttpResponse>("https://discord.com/api/oauth2/token") {
                    body = FormDataContent(
                        parametersOf(
                            "client_id" to listOf(discordClientID),
                            "client_secret" to listOf(discordClientSecret),
                            "grant_type" to listOf("authorization_code"),
                            "code" to listOf(code),
                            "redirect_uri" to listOf(discordRedirectUri)
                        )
                    )
                }

                if (postResponse.status != HttpStatusCode.OK) {
                    call.respondBytes(postResponse.receive<Input>().readBytes(), status = HttpStatusCode.BadRequest)
                } else {
                    val url = postResponse.receive<JsonObject>()
                        .getJsonObject("webhook")
                        .getString("url")
                        .substringAfter("/webhooks/")

                    upnutClient.client.sql("INSERT INTO webhooks (url, subscribed_to, secret_key, type) VALUES ($1, $2, $3, $4)")
                        .bind("$1", url)
                        .bind("$2", (1 shl 16) - 1)
                        .bind("$3", ByteArray(32))
                        .bind("$4", SHELLHOOK_TYPE_DISCORD)
                        .await()

                    upnutClient.client.sql("SELECT id, url, secret_key, type FROM webhooks WHERE url = $1")
                        .bind("$1", url)
                        .map(::StreamHook)
                        .first()
                        .awaitFirstOrNull()
                        ?.let { postEventTo(it, Json.encodeToString<WebhookEvent>(WebhookEvent.HelloWorld(Clock.System.now())).encodeToByteArray()) }

                    call.respondText("Success!", contentType = ContentType.Text.Html)
                }
            }

            route("/discord/{id}/{token}") {
                put {
                    val id = call.parameters.getOrFail("id")
                    val token = call.parameters.getOrFail("token")
                    val url = "$id/$token"

                    val exists = upnutClient.client.sql("SELECT EXISTS(SELECT 1 FROM webhooks WHERE type = 1 AND url = $1) as exists")
                                     .bind("$1", url)
                                     .map { row -> row.getValue<Boolean>("exists") }
                                     .first()
                                     .awaitFirstOrNull() ?: false

                    if (exists) return@put call.respond(HttpStatusCode.NoContent, message = EmptyContent)

                    upnutClient.client.sql("INSERT INTO webhooks (url, subscribed_to, secret_key, type) VALUES ($1, $2, $3, $4)")
                        .bind("$1", url)
                        .bind("$2", (1 shl 16) - 1)
                        .bind("$3", ByteArray(32))
                        .bind("$4", SHELLHOOK_TYPE_DISCORD)
                        .await()

                    upnutClient.client.sql("SELECT id, url, secret_key, type FROM webhooks WHERE url = $1")
                        .bind("$1", url)
                        .map(::StreamHook)
                        .first()
                        .awaitFirstOrNull()
                        ?.let { postEventTo(it, Json.encodeToString<WebhookEvent>(WebhookEvent.HelloWorld(Clock.System.now())).encodeToByteArray()) }

                    return@put call.respond(HttpStatusCode.Created, message = EmptyContent)
                }

                delete {
                    val id = call.parameters.getOrFail("id")
                    val token = call.parameters.getOrFail("token")
                    val url = "$id/$token"

                    val hook = upnutClient.client.sql("SELECT id, url, secret_key, type FROM webhooks WHERE url = $1 LIMIT 1")
                                   .bind("$1", url)
                                   .map(::StreamHook)
                                   .first()
                                   .awaitFirstOrNull()
                               ?: return@delete call.respond(HttpStatusCode.NoContent, message = EmptyContent)

                    upnutClient.client.sql("DELETE FROM webhooks WHERE id = $1")
                        .bind("$1", hook.id)
                        .await()

                    postEventTo(hook, Json.encodeToString<WebhookEvent>(WebhookEvent.GoodbyeWorld(Clock.System.now())).encodeToByteArray())

                    call.respond(HttpStatusCode.NoContent, message = EmptyContent)
                }
            }
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