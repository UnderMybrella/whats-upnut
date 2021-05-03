package dev.brella.blasement.upnut

import com.github.benmanes.caffeine.cache.Caffeine
import com.soywiz.klock.DateTime
import com.soywiz.klock.format
import dev.brella.blasement.upnut.common.NutDateTimeEvent
import dev.brella.blasement.upnut.common.NutEpochEvent
import dev.brella.blasement.upnut.common.NutSqlBuilder
import dev.brella.blasement.upnut.common.NutsDateTime
import dev.brella.blasement.upnut.common.NutsEpoch
import dev.brella.blasement.upnut.common.UpNutClient
import dev.brella.blasement.upnut.common.UpNutEvent
import dev.brella.blasement.upnut.common.getJsonObjectOrNull
import dev.brella.blasement.upnut.common.uuidOrNull
import dev.brella.kornea.blaseball.base.common.BLASEBALL_TIME_PATTERN
import dev.brella.kornea.errors.common.map
import dev.brella.ktornea.common.KorneaHttpResult
import dev.brella.ktornea.common.getAsResult
import dev.brella.ktornea.common.installGranularHttp
import io.jsonwebtoken.Jwt
import io.jsonwebtoken.JwtParser
import io.jsonwebtoken.JwtParserBuilder
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
import io.jsonwebtoken.impl.crypto.RsaProvider
import io.jsonwebtoken.security.Keys
import io.ktor.application.*
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.compression.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.client.utils.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.serialization.*
import io.ktor.util.*
import io.ktor.util.pipeline.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitSingleOrNull
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import org.slf4j.event.Level
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.await
import org.springframework.r2dbc.core.awaitRowsUpdated
import org.springframework.r2dbc.core.awaitSingleOrNull
import org.springframework.r2dbc.core.bind as bindNullable
import java.io.File
import java.nio.ByteBuffer
import java.security.KeyFactory
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import java.time.Clock
import java.time.Instant
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

class WhatsUpNut {
    val configJson: JsonObject? = File("upnut.json").takeIf(File::exists)?.readText()?.let(Json::decodeFromString)
    val upnut = UpNutClient(configJson?.getJsonObjectOrNull("r2dbc") ?: File("r2dbc.json").readText().let(Json::decodeFromString))
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

    val initJob = GlobalScope.launch {
        upnut.client.sql("CREATE TABLE IF NOT EXISTS auth_tokens (provider_id uuid NOT NULL, public_key bytea NOT NULL);")
            .await()
    }

    val parsers = Caffeine.newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .maximumSize(100)
        .buildAsync<UUID, JwtParser> { providerID, _ ->
            upnut.client.sql("SELECT public_key FROM auth_tokens WHERE provider_id = $1")
                .bind("$1", providerID)
                .fetch()
                .one()
                .mapNotNull { map ->
                    val key = map["public_key"] ?: return@mapNotNull null

                    return@mapNotNull when (key) {
                        is ByteBuffer -> ByteArray(key.remaining()).also(key::get)
                        is ByteArray -> key
                        else -> null
                    }
                }.map { key ->
                    Jwts.parserBuilder()
                        .requireIssuer(providerID.toString())
                        .setSigningKey(KeyFactory.getInstance("RSA").generatePublic(X509EncodedKeySpec(key)))
                        .build()
                }
                .toFuture()
        }

    val eventsCache = Caffeine.newBuilder()
        .expireAfterWrite(20, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val eventsNutCache = Caffeine.newBuilder()
        .expireAfterWrite(20, TimeUnit.SECONDS)
        .buildKotlin<String, List<Any>>()

    val feedHotGlobalCache = Caffeine.newBuilder()
        .expireAfterWrite(10, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedHotGameCache = Caffeine.newBuilder()
        .expireAfterWrite(10, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedHotPlayerCache = Caffeine.newBuilder()
        .expireAfterWrite(10, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedHotTeamCache = Caffeine.newBuilder()
        .expireAfterWrite(10, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedTopGlobalCache = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedTopGameCache = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedTopPlayerCache = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedTopTeamCache = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .buildKotlin<String, List<UpNutEvent>>()

    suspend fun PipelineContext<Unit, ApplicationCall>.processGlobal(
        cache: KotlinCache<String, List<UpNutEvent>>,
        baseFunc: suspend (
            time: Long, limit: Int,
            noneOfProviders: List<UUID>?,
            noneOfSources: List<UUID>?,
            oneOfProviders: List<UUID>?,
            oneOfSources: List<UUID>?,
            addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec
        ) -> List<Map<String, Any>>?,
//        inSources: suspend (time: Long, limit: Int, sources: List<UUID>, addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec) -> List<Map<String, Any>>?,
//        notInSources: suspend (time: Long, limit: Int, sources: List<UUID>, addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec) -> List<Map<String, Any>>?
    ) {
        val parameters = call.request.queryParameters

        cache.getAsync(parameters.toStableString(), scope = this) {
            val time = parameters["time"]?.let { time ->
                time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
            } ?: call.request.header("X-UpNut-Time")?.let { time ->
                time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
            } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

            val oneOfSources = (parameters["filter_sources"] ?: parameters["one_of_sources"]
                                ?: call.request.header("X-UpNut-FilterSources") ?: call.request.header("X-UpNut-OneOfSources"))
                ?.split(',')
                ?.mapNotNull(String::uuidOrNull)

            val noneOfSources = (parameters["filter_sources_not"] ?: parameters["none_of_sources"]
                                 ?: call.request.header("X-UpNut-FilterSourcesNot") ?: call.request.header("X-UpNut-NoneOfSources"))
                ?.split(',')
                ?.mapNotNull(String::uuidOrNull)

            val oneOfProviders = (parameters["filter_providers"] ?: parameters["one_of_providers"]
                                  ?: call.request.header("X-UpNut-FilterProviders") ?: call.request.header("X-UpNut-OneOfProviders"))
                ?.split(',')
                ?.mapNotNull(String::uuidOrNull)

            val noneOfProviders = (parameters["filter_providers_not"] ?: parameters["none_of_providers"]
                                   ?: call.request.header("X-UpNut-FilterProvidersNot") ?: call.request.header("X-UpNut-NoneOfProviders"))
                ?.split(',')
                ?.mapNotNull(String::uuidOrNull)

            val season = parameters["season"]?.toIntOrNull()
            val tournament = parameters["tournament"]?.toIntOrNull()
            val type = parameters["type"]?.toIntOrNull()
            val day = parameters["day"]?.toIntOrNull()
            val phase = parameters["phase"]?.toIntOrNull()
            val category = parameters["category"]?.toIntOrNull()

            val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

            val nuts =
                baseFunc(time, limit, noneOfProviders, noneOfSources, oneOfProviders, oneOfSources) {
                    season(season)
                        .tournament(tournament)
                        .type(type)
                        .day(day)
                        .phase(phase)
                        .category(category)
                }?.mapNotNull { map ->
                    (map["feed_id"] as? UUID)?.let { feedID ->
                        (map["sum"] as? Number)?.let { sum ->
                            Pair(feedID, sum.toInt())
                        }
                    }
                }?.toMap() ?: emptyMap()

            http.eventually(nuts, upnut, time, limit, season, tournament, type, day, phase, category)
        }.respond(call)
    }

    suspend fun PipelineContext<Unit, ApplicationCall>.processFiltered(
        cache: KotlinCache<String, List<UpNutEvent>>,
        baseFunc: suspend (
            id: UUID, time: Long, limit: Int,
            noneOfProviders: List<UUID>?,
            noneOfSources: List<UUID>?,
            oneOfProviders: List<UUID>?,
            oneOfSources: List<UUID>?,
            addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec
        ) -> List<Map<String, Any>>?,
//        inSources: suspend (id: UUID, time: Long, limit: Int, sources: List<UUID>, addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec) -> List<Map<String, Any>>?,
//        notInSources: suspend (id: UUID, time: Long, limit: Int, sources: List<UUID>, addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec) -> List<Map<String, Any>>?
    ) {
        val parameters = call.request.queryParameters

        cache.getAsync(parameters.toStableString(), scope = this) {
            val time = parameters["time"]?.let { time ->
                time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
            } ?: call.request.header("X-UpNut-Time")?.let { time ->
                time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
            } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

            val oneOfSources = (parameters["filter_sources"] ?: parameters["one_of_sources"]
                                ?: call.request.header("X-UpNut-FilterSources") ?: call.request.header("X-UpNut-OneOfSources"))
                ?.split(',')
                ?.mapNotNull(String::uuidOrNull)

            val noneOfSources = (parameters["filter_sources_not"] ?: parameters["none_of_sources"]
                                 ?: call.request.header("X-UpNut-FilterSourcesNot") ?: call.request.header("X-UpNut-NoneOfSources"))
                ?.split(',')
                ?.mapNotNull(String::uuidOrNull)

            val oneOfProviders = (parameters["filter_providers"] ?: parameters["one_of_providers"]
                                  ?: call.request.header("X-UpNut-FilterProviders") ?: call.request.header("X-UpNut-OneOfProviders"))
                ?.split(',')
                ?.mapNotNull(String::uuidOrNull)

            val noneOfProviders = (parameters["filter_providers_not"] ?: parameters["none_of_providers"]
                                   ?: call.request.header("X-UpNut-FilterProvidersNot") ?: call.request.header("X-UpNut-NoneOfProviders"))
                ?.split(',')
                ?.mapNotNull(String::uuidOrNull)

            val id = (parameters["id"] ?: call.request.header("X-UpNut-ID")).uuidOrNull()
                     ?: return@getAsync buildUserErrorJsonObject(HttpStatusCode.BadRequest) {
                         put("error", "No ID passed via query parameter (id) or header (X-UpNut-ID)")
                     }

            val season = parameters["season"]?.toIntOrNull()
            val tournament = parameters["tournament"]?.toIntOrNull()
            val type = parameters["type"]?.toIntOrNull()
            val day = parameters["day"]?.toIntOrNull()
            val phase = parameters["phase"]?.toIntOrNull()
            val category = parameters["category"]?.toIntOrNull()

            val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

            val nuts =
                baseFunc(id, time, limit, noneOfProviders, noneOfSources, oneOfProviders, oneOfSources) {
                    season(season)
                        .tournament(tournament)
                        .type(type)
                        .day(day)
                        .phase(phase)
                        .category(category)
                }?.mapNotNull { map ->
                    (map["feed_id"] as? UUID)?.let { feedID ->
                        (map["sum"] as? Number)?.let { sum ->
                            Pair(feedID, sum.toInt())
                        }
                    }
                }?.toMap() ?: emptyMap()

            http.eventually(nuts, upnut, time, limit, season, tournament, type, day, phase, category)
        }.respond(call)
    }

    fun routing(routing: Routing) =
        with(routing) {
            put("/{feed_id}/{provider}") {
                val authToken = call.request.header("Authorization")

                try {
                    val provider = call.parameters["provider"]?.uuidOrNull() ?: return@put call.respondJsonObject(HttpStatusCode.BadRequest) {
                        put("error", "Invalid provider given")
                    }

                    val parser = parsers[provider].await()
                    val authJws = parser.parseClaimsJws(authToken)

                    val queryParams = call.request.queryParameters

                    val source = queryParams["source"]?.uuidOrNull()

                    val feedID = call.parameters["feed_id"]?.uuidOrNull() ?: return@put call.respondJsonObject(HttpStatusCode.BadRequest) {
                        put("error", "Invalid feed id given")
                    }

                    val time = queryParams["time"]?.let { time ->
                        BLASEBALL_TIME_PATTERN.tryParse(time, false)?.utc?.unixMillisLong ?: time.toLongOrNull()
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    val nuts = upnut.client.sql("SELECT nuts FROM upnuts WHERE feed_id = $1 AND provider = $2 AND source IS NOT DISTINCT FROM $3")
                                   .bind("$1", feedID)
                                   .bind("$2", provider)
                                   .bind("$3", source)
                                   .fetch()
                                   .awaitSingleOrNull()
                                   ?.get("nuts")
                                   ?.let { it as? Number }
                                   ?.toInt() ?: 0

                    if (nuts > 0) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "You've already Upshelled that event.")
                        }
                    } else {
                        upnut.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, source, time) VALUES ( $1, $2, $3, $4, $5)")
                            .bind("$1", 1)
                            .bind("$2", feedID)
                            .bind("$3", provider)
                            .bindNullable("$4", source)
                            .bind("$5", time)
                            .fetch()
                            .awaitRowsUpdated()

                        call.respond(HttpStatusCode.Created, EmptyContent)
                    }
                } catch (th: Throwable) {
                    call.respondJsonObject(HttpStatusCode.Unauthorized) {
                        put("stack_trace", th.stackTraceToString())
                    }
                }
            }

            delete("/{feed_id}/{provider}") {
                val authToken = call.request.header("Authorization")
                try {
                    val provider = call.parameters["provider"]?.uuidOrNull() ?: return@delete call.respondJsonObject(HttpStatusCode.BadRequest) {
                        put("error", "Invalid provider given")
                    }

                    val parser = parsers[provider].await()
                    val authJws = parser.parseClaimsJws(authToken)

                    val queryParams = call.request.queryParameters

                    val source = queryParams["source"]?.uuidOrNull()

                    val feedID = call.parameters["feed_id"]?.uuidOrNull() ?: return@delete call.respondJsonObject(HttpStatusCode.BadRequest) {
                        put("error", "Invalid feed id given")
                    }

                    val rowsUpdated = upnut.client.sql("DELETE FROM upnuts WHERE feed_id = $1 AND provider = $2 AND source IS NOT DISTINCT FROM $3")
                        .bind("$1", feedID)
                        .bind("$2", source)
                        .bind("$3", source)
                        .fetch()
                        .awaitRowsUpdated()

                    if (rowsUpdated == 0) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "You haven't Upshelled that event.")
                        }
                    } else {
                        call.respond(HttpStatusCode.NoContent, EmptyContent)
                    }
                } catch (th: Throwable) {
                    call.respondJsonObject(HttpStatusCode.Unauthorized) {
                        put("stack_trace", th.stackTraceToString())
                    }
                }
            }

            get("/events") {
                val parameters = call.request.queryParameters
                val cacheKey = parameters.toStableString()

                eventsCache.getAsync(cacheKey, scope = this) {
                    val player = (parameters["player"] ?: call.request.header("X-UpNut-Player"))?.uuidOrNull()
                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    val oneOfSources = (parameters["filter_sources"] ?: parameters["one_of_sources"]
                                        ?: call.request.header("X-UpNut-FilterSources") ?: call.request.header("X-UpNut-OneOfSources"))
                        ?.split(',')
                        ?.mapNotNull(String::uuidOrNull)

                    val noneOfSources = (parameters["filter_sources_not"] ?: parameters["none_of_sources"]
                                         ?: call.request.header("X-UpNut-FilterSourcesNot") ?: call.request.header("X-UpNut-NoneOfSources"))
                        ?.split(',')
                        ?.mapNotNull(String::uuidOrNull)

                    val oneOfProviders = (parameters["filter_providers"] ?: parameters["one_of_providers"]
                                          ?: call.request.header("X-UpNut-FilterProviders") ?: call.request.header("X-UpNut-OneOfProviders"))
                        ?.split(',')
                        ?.mapNotNull(String::uuidOrNull)

                    val noneOfProviders = (parameters["filter_providers_not"] ?: parameters["none_of_providers"]
                                           ?: call.request.header("X-UpNut-FilterProvidersNot") ?: call.request.header("X-UpNut-NoneOfProviders"))
                        ?.split(',')
                        ?.mapNotNull(String::uuidOrNull)

                    http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                        url.parameters.appendAll(parameters)
                    }.map { list ->
                        val feedEventSources = list.map(UpNutEvent::id)
                        val map = upnut.eventually(feedEventSources, time, noneOfProviders, noneOfSources, oneOfProviders, oneOfSources) ?: emptyMap()

                        val upnuts =
                            player?.let { upnut.isUpnutted(feedEventSources, time, it) } ?: emptyMap()

                        list.map inner@{ event ->
                            val metadata: JsonElement =
                                if (upnuts[event.id] == true) {
                                    when (val metadata = event.metadata) {
                                        is JsonObject -> JsonObject(metadata + Pair("upnut", JsonPrimitive(true)))
                                        is JsonNull -> JsonObject(mapOf("upnut" to JsonPrimitive(true)))
                                        else -> event.metadata
                                    }
                                } else event.metadata

                            map[event.id]?.let {
                                if (it != event.nuts.intOrNull)
                                    return@inner event.copy(nuts = JsonPrimitive(it), metadata = metadata)
                            }

                            return@inner event.copy(metadata = metadata)
                        }
                    }
                }.respond(call)
            }

            get("/nuts") {
                val parameters = call.request.queryParameters

                val cacheKey = parameters.toStableString()

                eventsNutCache.getAsync(cacheKey, scope = this) {
                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    val oneOfSources = (parameters["filter_sources"] ?: parameters["one_of_sources"]
                                        ?: call.request.header("X-UpNut-FilterSources") ?: call.request.header("X-UpNut-OneOfSources"))
                        ?.split(',')
                        ?.mapNotNull(String::uuidOrNull)

                    val noneOfSources = (parameters["filter_sources_not"] ?: parameters["none_of_sources"]
                                         ?: call.request.header("X-UpNut-FilterSourcesNot") ?: call.request.header("X-UpNut-NoneOfSources"))
                        ?.split(',')
                        ?.mapNotNull(String::uuidOrNull)

                    val oneOfProviders = (parameters["filter_providers"] ?: parameters["one_of_providers"]
                                          ?: call.request.header("X-UpNut-FilterProviders") ?: call.request.header("X-UpNut-OneOfProviders"))
                        ?.split(',')
                        ?.mapNotNull(String::uuidOrNull)

                    val noneOfProviders = (parameters["filter_providers_not"] ?: parameters["none_of_providers"]
                                           ?: call.request.header("X-UpNut-FilterProvidersNot") ?: call.request.header("X-UpNut-NoneOfProviders"))
                        ?.split(',')
                        ?.mapNotNull(String::uuidOrNull)

                    val formatAsDateTime = (parameters["time_format"] ?: call.request.header("X-UpNut-TimeFormat")) ==
                            "datetime"

                    http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                        url.parameters.appendAll(parameters)
                    }.map { list ->
                        val map = upnut.eventuallyNutsList(list.map(UpNutEvent::id), time, noneOfProviders, noneOfSources, oneOfProviders, oneOfSources) ?: emptyMap()

                        list.map { event ->
                            val nuts = map[event.id]

                            if (formatAsDateTime) {
                                NutDateTimeEvent(
                                    event.id,
                                    event.playerTags,
                                    event.teamTags,
                                    event.gameTags,
                                    event.created,
                                    event.season,
                                    event.tournament,
                                    event.type,
                                    event.day,
                                    event.phase,
                                    event.category,
                                    event.description,
                                    nuts?.map { (nuts, provider, source, time) -> NutsDateTime(nuts, provider, source, BLASEBALL_TIME_PATTERN.format(DateTime.fromUnix(time))) } ?: emptyList(),
                                    event.metadata
                                )
                            } else {
                                NutEpochEvent(
                                    event.id,
                                    event.playerTags,
                                    event.teamTags,
                                    event.gameTags,
                                    event.created,
                                    event.season,
                                    event.tournament,
                                    event.type,
                                    event.day,
                                    event.phase,
                                    event.category,
                                    event.description,
                                    nuts ?: emptyList(),
                                    event.metadata
                                )
                            }
                        }
                    }
                }.respond(call)
            }

            route("/feed") {
                route("/hot") {
                    get("/global") { processGlobal(feedHotGlobalCache, upnut::globalHot) }

                    get("/team") { processFiltered(feedHotTeamCache, upnut::teamHot) }

                    get("/game") { processFiltered(feedHotGameCache, upnut::gameHot) }

                    get("/player") { processFiltered(feedHotPlayerCache, upnut::playerHot) }
                }

                route("/top") {
                    get("/global") { processGlobal(feedTopGlobalCache, upnut::globalTop) }

                    get("/team") { processFiltered(feedTopTeamCache, upnut::teamTop) }

                    get("/game") { processFiltered(feedTopGameCache, upnut::gameTop) }

                    get("/player") { processFiltered(feedTopPlayerCache, upnut::playerTop) }
                }

                get("/global") {
                    val category = call.parameters["category"]?.toIntOrNull()
                    val limit = call.parameters["limit"]?.toIntOrNull() ?: 100
                    val type = call.parameters["type"]?.toIntOrNull()
                    val sort = call.parameters["sort"]?.toIntOrNull()
                    val start = call.parameters["start"]?.toIntOrNull()
                    val time = call.parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    val player = (call.parameters["player"] ?: call.request.header("X-UpNut-Player"))?.uuidOrNull()

                    when (sort) {
                        /** Newest */
                        0 -> call.respondRedirect(false) {
                            path("/events")
                            with(parameters) {
                                append("before", (time / 1000).toString())
                                append("limit", limit.toString())

                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("sortorder", "desc")
                            }
                        }
                        /** Oldest */
                        1 -> call.respondRedirect(false) {
                            path("/events")
                            with(parameters) {
                                append("before", (time / 1000).toString())
                                append("limit", limit.toString())

                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("sortorder", "asc")
                            }
                        }
                        /** Top */
                        2 -> call.respondRedirect(false) {
                            path("/feed/top/global")
                            with(parameters) {
                                append("limit", limit.toString())
                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())
                            }
                        }
                        /** Hot */
                        else -> call.respondRedirect(false) {
                            path("/feed/hot/global")
                            with(parameters) {
                                append("limit", limit.toString())
                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())
                            }
                        }
                    }
                }

                get("/team") {
                    val id = call.parameters["id"] ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) { put("error", "No ID provided") }
                    val category = call.parameters["category"]?.toIntOrNull()
                    val limit = call.parameters["limit"]?.toIntOrNull() ?: 100
                    val type = call.parameters["type"]?.toIntOrNull()
                    val sort = call.parameters["sort"]?.toIntOrNull()
                    val start = call.parameters["start"]?.toIntOrNull()
                    val time = call.parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    val player = (call.parameters["player"] ?: call.request.header("X-UpNut-Player"))?.uuidOrNull()

                    when (sort) {
                        /** Newest */
                        0 -> call.respondRedirect(false) {
                            path("/events")
                            with(parameters) {
                                append("before", (time / 1000).toString())
                                append("limit", limit.toString())

                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("teamTags", id)

                                append("sortorder", "desc")
                            }
                        }
                        /** Oldest */
                        1 -> call.respondRedirect(false) {
                            path("/events")
                            with(parameters) {
                                append("before", (time / 1000).toString())
                                append("limit", limit.toString())

                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("teamTags", id)

                                append("sortorder", "asc")
                            }
                        }
                        /** Top */
                        2 -> call.respondRedirect(false) {
                            path("/feed/top/team")
                            with(parameters) {
                                append("limit", limit.toString())
                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("id", id)
                            }
                        }
                        /** Hot */
                        else -> call.respondRedirect(false) {
                            path("/feed/hot/team")
                            with(parameters) {
                                append("limit", limit.toString())
                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("id", id)
                            }
                        }
                    }
                }

                get("/player") {
                    val id = call.parameters["id"] ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) { put("error", "No ID provided") }
                    val category = call.parameters["category"]?.toIntOrNull()
                    val limit = call.parameters["limit"]?.toIntOrNull() ?: 100
                    val type = call.parameters["type"]?.toIntOrNull()
                    val sort = call.parameters["sort"]?.toIntOrNull()
                    val start = call.parameters["start"]?.toIntOrNull()
                    val time = call.parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    val player = (call.parameters["player"] ?: call.request.header("X-UpNut-Player"))?.uuidOrNull()

                    when (sort) {
                        /** Newest */
                        0 -> call.respondRedirect(false) {
                            path("/events")
                            with(parameters) {
                                append("before", (time / 1000).toString())
                                append("limit", limit.toString())

                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("playerTags", id)

                                append("sortorder", "desc")
                            }
                        }
                        /** Oldest */
                        1 -> call.respondRedirect(false) {
                            path("/events")
                            with(parameters) {
                                append("before", (time / 1000).toString())
                                append("limit", limit.toString())

                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("playerTags", id)

                                append("sortorder", "asc")
                            }
                        }
                        /** Top */
                        2 -> call.respondRedirect(false) {
                            path("/feed/top/player")
                            with(parameters) {
                                append("limit", limit.toString())
                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("id", id)
                            }
                        }
                        /** Hot */
                        else -> call.respondRedirect(false) {
                            path("/feed/hot/player")
                            with(parameters) {
                                append("limit", limit.toString())
                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("id", id)
                            }
                        }
                    }
                }

                get("/game") {
                    val id = call.parameters["id"] ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) { put("error", "No ID provided") }
                    val category = call.parameters["category"]?.toIntOrNull()
                    val limit = call.parameters["limit"]?.toIntOrNull() ?: 100
                    val type = call.parameters["type"]?.toIntOrNull()
                    val sort = call.parameters["sort"]?.toIntOrNull()
                    val start = call.parameters["start"]?.toIntOrNull()
                    val time = call.parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    val player = (call.parameters["player"] ?: call.request.header("X-UpNut-Player"))?.uuidOrNull()

                    when (sort) {
                        /** Newest */
                        0 -> call.respondRedirect(false) {
                            path("/events")
                            with(parameters) {
                                append("before", (time / 1000).toString())
                                append("limit", limit.toString())

                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("gameTags", id)

                                append("sortorder", "desc")
                            }
                        }
                        /** Oldest */
                        1 -> call.respondRedirect(false) {
                            path("/events")
                            with(parameters) {
                                append("before", (time / 1000).toString())
                                append("limit", limit.toString())

                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("gameTags", id)

                                append("sortorder", "asc")
                            }
                        }
                        /** Top */
                        2 -> call.respondRedirect(false) {
                            path("/feed/top/game")
                            with(parameters) {
                                append("limit", limit.toString())
                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("id", id)
                            }
                        }
                        /** Hot */
                        else -> call.respondRedirect(false) {
                            path("/feed/hot/game")
                            with(parameters) {
                                append("limit", limit.toString())
                                append("time", time.toString())

                                if (player != null) append("player", player.toString())

                                if (category != null) append("category", category.toString())
                                if (type != null) append("type", type.toString())
                                if (start != null) append("offset", start.toString())

                                append("id", id)
                            }
                        }
                    }
                }
            }
        }
}

@OptIn(ExperimentalTime::class, kotlin.ExperimentalStdlibApi::class)
fun Application.module(testing: Boolean = false) {
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
//        exception<Throwable> { cause -> call.respond(HttpStatusCode.InternalServerError, cause.stackTraceToString()) }
        exception<KorneaResultException> { cause ->
            val result = cause.result
            if (result is KorneaHttpResult) call.response.header("X-Response-Source", result.response.request.url.toString())
            result.respondOnFailure(call)
        }
    }
    install(CallLogging) {
        level = Level.INFO
    }

//    val upnut = BigUpNut(config)

    val upnut = WhatsUpNut()

    routing(upnut::routing)
}