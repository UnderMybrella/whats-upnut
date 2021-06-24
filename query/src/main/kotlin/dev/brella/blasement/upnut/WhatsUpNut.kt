package dev.brella.blasement.upnut

import com.github.benmanes.caffeine.cache.Caffeine
import com.soywiz.klock.DateTime
import com.soywiz.klock.format
import dev.brella.blasement.upnut.common.*
import dev.brella.kornea.blaseball.base.common.BLASEBALL_TIME_PATTERN
import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.kornea.errors.common.map
import dev.brella.ktornea.common.KorneaHttpResult
import dev.brella.ktornea.common.getAsResult
import dev.brella.ktornea.common.installGranularHttp
import io.jsonwebtoken.JwtParser
import io.jsonwebtoken.Jwts
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
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.await
import org.springframework.r2dbc.core.awaitRowsUpdated
import org.springframework.r2dbc.core.awaitSingleOrNull
import org.springframework.r2dbc.core.bind as bindNullable
import java.io.File
import java.nio.ByteBuffer
import java.security.KeyFactory
import java.security.spec.X509EncodedKeySpec
import java.time.Clock
import java.time.Instant
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.collections.LinkedHashMap
import kotlin.time.ExperimentalTime

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

class WhatsUpNut {
    companion object {
        val REMOVE_PARAMETERS_GLOBAL_EVENTUALLY = arrayOf("before", "after", "time", "sortorder", "start", "offset")
        val REMOVE_PARAMETERS_TEAM_EVENTUALLY = REMOVE_PARAMETERS_GLOBAL_EVENTUALLY + arrayOf("teamTags", "id")
        val REMOVE_PARAMETERS_PLAYER_EVENTUALLY = REMOVE_PARAMETERS_GLOBAL_EVENTUALLY + arrayOf("playerTags", "id")
        val REMOVE_PARAMETERS_GAME_EVENTUALLY = REMOVE_PARAMETERS_GLOBAL_EVENTUALLY + arrayOf("gameTags", "id")
    }

    val configJson: JsonObject? = File(System.getProperty("upnut.query") ?: "upnut.json").takeIf(File::exists)?.readText()?.let(Json::decodeFromString)
    val upnut = UpNutClient(configJson?.getJsonObjectOrNull("upnuts_r2dbc") ?: File(System.getProperty("upnut.r2dbc") ?: "upnuts-r2dbc.json").readText().let(Json::decodeFromString))
    val eventuallie = Eventuallie(configJson?.getJsonObjectOrNull("eventually_r2dbc") ?: File(System.getProperty("upnut.eventually") ?: "eventually-r2dbc.json").readText().let(Json::decodeFromString))

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
        .expireAfterWrite(1, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedTopGameCache = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedTopPlayerCache = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val feedTopTeamCache = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.SECONDS)
        .buildKotlin<String, List<UpNutEvent>>()

    val logger = LoggerFactory.getLogger("dev.brella.blasement.upnut.query.Query")

    suspend fun PipelineContext<Unit, ApplicationCall>.processGlobal(
        cache: KotlinCache<String, List<UpNutEvent>>,
        baseFunc: suspend (
            time: Long, limit: Int, start: Int,
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

            val provider = parameters["provider"]?.uuidOrNull()
            val source = (parameters["player"] ?: parameters["player"])?.uuidOrNull()

            val limit = (parameters["count"]?.toIntOrNull() ?: parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Count")?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceAtLeast(1) ?: 50
            val offset = (parameters["start"]?.toIntOrNull() ?: parameters["offset"]?.toIntOrNull() ?: call.request.header("X-UpNut-Start")?.toIntOrNull() ?: call.request.header("X-UpNut-Offset")?.toIntOrNull())?.coerceAtLeast(0) ?: 0

            val nuts =
                baseFunc(time, limit, offset, noneOfProviders, noneOfSources, oneOfProviders, oneOfSources) {
                    season(season)
                        .tournament(tournament)
                        .type(type)
                        .day(day)
                        .phase(phase)
                        .category(category)
                }?.mapNotNull { map ->
                    (map["feed_id"] as? UUID)?.let { feedID ->
                        Pair(feedID, Pair((map["nuts"] as? Number)?.toInt() ?: 0, (map["scales"] as? Number)?.toInt() ?: 0))
                    }
                }?.toMap(LinkedHashMap()) ?: emptyMap()

            try {
                KorneaResult.success(eventuallie.mergeFeedWithNuts(nuts, upnut, logger, time, limit, offset, season, tournament, type, day, phase, category, provider, source))
            } catch (th: Throwable) {
                th.printStackTrace()
                KorneaResult.thrown(th)
            }
        }.respond(call)
    }

    suspend fun PipelineContext<Unit, ApplicationCall>.processFiltered(
        cache: KotlinCache<String, List<UpNutEvent>>,
        baseFunc: suspend (
            id: UUID, time: Long, limit: Int, offset: Int,
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

            val provider = parameters["provider"]?.uuidOrNull()
            val source = (parameters["player"] ?: parameters["player"])?.uuidOrNull()

            val limit = (parameters["count"]?.toIntOrNull() ?: parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Count")?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceAtLeast(1) ?: 50
            val offset = (parameters["start"]?.toIntOrNull() ?: parameters["offset"]?.toIntOrNull() ?: call.request.header("X-UpNut-Start")?.toIntOrNull() ?: call.request.header("X-UpNut-Offset")?.toIntOrNull())?.coerceAtLeast(0) ?: 0

            val nuts =
                baseFunc(id, time, limit, offset, noneOfProviders, noneOfSources, oneOfProviders, oneOfSources) {
                    season(season)
                        .tournament(tournament)
                        .type(type)
                        .day(day)
                        .phase(phase)
                        .category(category)
                }?.mapNotNull { map ->
                    (map["feed_id"] as? UUID)?.let { feedID ->
                        Pair(feedID, Pair((map["nuts"] as? Number)?.toInt() ?: 0, (map["scales"] as? Number)?.toInt() ?: 0))
                    }
                }?.toMap(LinkedHashMap()) ?: emptyMap()

            try {
                KorneaResult.success(eventuallie.mergeFeedWithNuts(nuts, upnut, logger, time, limit, offset, season, tournament, type, day, phase, category, provider, source))
            } catch (th: Throwable) {
                KorneaResult.thrown(th)
            }
        }.respond(call)
    }

    @Serializable
    data class LibraryBook(val bookTitle: String?, val chapters: List<LibraryChapter>)

    @Serializable
    data class LibraryChapter(val feedID: String, val chapterTitle: String, val chapterTitleRedacted: String?, val indexInBook: Int, val isRedacted: Boolean)

    fun routing(routing: Routing) =
        with(routing) {
            get("/library") {
                val books = upnut.client.sql("SELECT id, chapter_title_redacted, chapter_title, book_title, index_in_book, redacted FROM library").map { row ->
                    (row["id"] as? UUID)?.let { uuid ->
                        Pair(
                            row["book_title"] as? String,
                            LibraryChapter(
                                uuid.toString(),
                                row["chapter_title"] as? String ?: row["chapter_title"].toString(),
                                row["chapter_title_redacted"] as? String,
                                row.getValue("index_in_book"),
                                row["redacted"] as? Boolean ?: true
                            )
                        )
                    }
                }
                                .all()
                                .collectList()
                                .awaitFirstOrNull()
                                ?.filterNotNull()
                                ?.groupBy(Pair<String?, LibraryChapter>::first, Pair<String?, LibraryChapter>::second)
                                ?.map { (name, chapters) -> LibraryBook(name, chapters.sortedBy(LibraryChapter::indexInBook)) }
                            ?: emptyList()

                call.respond(books)
            }

            route("/gc") {
                get("/pending") {
                    call.respond(
                        upnut.client.sql("SELECT feed_id FROM metadata_collection WHERE data IS NULL")
                            .map { row -> row.getValue<UUID>("feed_id").toString() }
                            .all()
                            .collectList()
                            .awaitFirstOrNull()
                        ?: emptyList()
                    )
                }

                get("/ingested") {
                    call.respond(
                        upnut.client.sql("SELECT data FROM metadata_collection WHERE data IS NOT NULL AND cleared = false")
                            .map { row -> Json.parseToJsonElement(row.getValue("data")) }
                            .all()
                            .collectList()
                            .awaitFirstOrNull()
                        ?: emptyList()
                    )
                }

                get("/cleared") {
                    call.respond(
                        upnut.client.sql("SELECT data FROM metadata_collection WHERE data IS NOT NULL AND cleared = true")
                            .map { row -> Json.parseToJsonElement(row.getValue("data")) }
                            .all()
                            .collectList()
                            .awaitFirstOrNull()
                        ?: emptyList()
                    )
                }
            }

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
                                   .bindNullable("$3", source)
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
                        .bind("$2", provider)
                        .bindNullable("$3", source)
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
                    val provider = (parameters["provider"] ?: call.request.header("X-UpNut-Provider"))?.uuidOrNull()
                    val source = (parameters["source"] ?: parameters["player"] ?: call.request.header("X-UpNut-Source") ?: call.request.header("X-UpNut-Player"))?.uuidOrNull()


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

                    http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/v2/events") {
                        url.parameters.appendAll(parameters)
                    }.map { list ->
                        val feedEventSources = list.map(UpNutEvent::id)
                        val map = upnut.eventually(feedEventSources, time, noneOfProviders, noneOfSources, oneOfProviders, oneOfSources) ?: emptyMap()

                        val upnuts =
                            provider?.let { upnut.isUpnutted(feedEventSources, time, it, source) } ?: emptyMap()

                        list.map inner@{ event ->
                            val upnutted = upnuts[event.id]
                            val metadata: JsonElement =
                                if (upnutted?.first == true || upnutted?.second == true) {
                                    //TODO: Triple check that it is, in fact, upnut for scales
                                    when (val metadata = event.metadata) {
                                        is JsonObject -> JsonObject(metadata + Pair("upnut", JsonPrimitive(true)))
                                        is JsonNull -> JsonObject(mapOf("upnut" to JsonPrimitive(true)))
                                        else -> event.metadata
                                    }
                                } else event.metadata

                            map[event.id]?.let { (nuts, scales) ->
                                if (nuts != event.nuts.intOrNull)
                                    return@inner event.copy(nuts = JsonPrimitive(nuts), metadata = metadata)
                                else if (scales != event.scales.intOrNull)
                                    return@inner event.copy(metadata = metadata).apply {
                                        this.scales = JsonPrimitive(scales)
                                    }
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

                    http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/v2/events") {
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
                                    nuts?.map { (nuts, scales, provider, source, time) -> NutsDateTime(nuts, scales, provider, source, BLASEBALL_TIME_PATTERN.format(DateTime.fromUnix(time))) } ?: emptyList(),
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
                    val parameters = call.request.queryParameters

                    val after = parameters["after"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    }
                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val start = parameters["start"]?.toIntOrNull()
                    val sort = parameters["sort"]?.toIntOrNull()

                    when (sort) {
                        /** Oldest */
                        1 -> call.redirectInternally("/events") {
                            append("before", (time / 1000).toString())
                            after?.let { append("after", (it / 1000).toString()) }
                            append("time", time.toString())
                            append("sortorder", "asc")
                            if (start != null) append("offset", start.toString())

                            appendAll(parameters.filter { k, _ -> k.toLowerCase() !in REMOVE_PARAMETERS_GLOBAL_EVENTUALLY })
                        }
                        /** Top */
                        2 -> call.redirectInternally("/feed/top/global") {
                            appendAll(parameters)
                        }
                        /** Hot */
                        3 -> call.redirectInternally("/feed/hot/global") {
                            appendAll(parameters)
                        }

                        /** Newest */
                        else -> call.redirectInternally("/events") {
                            append("before", (time / 1000).toString())
                            after?.let { append("after", (it / 1000).toString()) }
                            append("time", time.toString())
                            append("sortorder", "desc")
                            if (start != null) append("offset", start.toString())

                            appendAll(parameters.filter { k, _ -> k.toLowerCase() !in REMOVE_PARAMETERS_GLOBAL_EVENTUALLY })
                        }
                    }
                }

                get("/team") {
                    val parameters = call.request.queryParameters

                    val id = parameters["id"] ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) { put("error", "No ID provided") }
                    val sort = parameters["sort"]?.toIntOrNull()
                    val start = parameters["start"]?.toIntOrNull()

                    val after = parameters["after"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    }
                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    when (sort) {
                        /** Oldest */
                        1 -> call.redirectInternally("/events") {
                            append("before", (time / 1000).toString())
                            after?.let { append("after", (it / 1000).toString()) }
                            append("time", time.toString())
                            if (start != null) append("offset", start.toString())
                            append("teamTags", id)
                            append("sortorder", "asc")

                            appendAll(parameters.filter { k, _ -> k.toLowerCase() !in REMOVE_PARAMETERS_TEAM_EVENTUALLY })
                        }
                        /** Top */
                        2 -> call.redirectInternally("/feed/top/team") {
                            appendAll(parameters)
                        }
                        /** Hot */
                        3 -> call.redirectInternally("/feed/hot/team") {
                            appendAll(parameters)
                        }

                        /** Newest */
                        else -> call.redirectInternally("/events") {
                            append("before", (time / 1000).toString())
                            after?.let { append("after", (it / 1000).toString()) }
                            append("time", time.toString())
                            if (start != null) append("offset", start.toString())
                            append("teamTags", id)
                            append("sortorder", "desc")

                            appendAll(parameters.filter { k, _ -> k.toLowerCase() !in REMOVE_PARAMETERS_TEAM_EVENTUALLY })
                        }
                    }
                }

                get("/player") {
                    val parameters = call.request.queryParameters

                    val id = parameters["id"] ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) { put("error", "No ID provided") }
                    val sort = parameters["sort"]?.toIntOrNull()
                    val start = parameters["start"]?.toIntOrNull()
                    val after = parameters["after"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    }
                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    when (sort) {
                        /** Oldest */
                        1 -> call.redirectInternally("/events") {
                            append("before", (time / 1000).toString())
                            after?.let { append("after", (it / 1000).toString()) }
                            if (start != null) append("offset", start.toString())
                            append("playerTags", id)
                            append("sortorder", "asc")

                            appendAll(parameters.filter { k, _ -> k.toLowerCase() !in REMOVE_PARAMETERS_PLAYER_EVENTUALLY })
                        }
                        /** Top */
                        2 -> call.redirectInternally("/feed/top/player") {
                            appendAll(parameters)
                        }
                        /** Hot */
                        3 -> call.redirectInternally("/feed/hot/player") {
                            appendAll(parameters)
                        }

                        /** Newest */
                        else -> call.redirectInternally("/events") {
                            append("before", (time / 1000).toString())
                            after?.let { append("after", (it / 1000).toString()) }
                            if (start != null) append("offset", start.toString())
                            append("playerTags", id)
                            append("sortorder", "desc")

                            appendAll(parameters.filter { k, _ -> k.toLowerCase() !in REMOVE_PARAMETERS_PLAYER_EVENTUALLY })
                        }
                    }
                }

                get("/game") {
                    val parameters = call.request.queryParameters
                    val id = parameters["id"] ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) { put("error", "No ID provided") }
                    val category = parameters["category"]?.toIntOrNull()
                    val limit = parameters["limit"]?.toIntOrNull() ?: 100
                    val type = parameters["type"]?.toIntOrNull()
                    val sort = parameters["sort"]?.toIntOrNull()
                    val start = parameters["start"]?.toIntOrNull()
                    val after = parameters["after"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    }
                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                    val player = (parameters["player"] ?: call.request.header("X-UpNut-Player"))?.uuidOrNull()

                    when (sort) {
                        /** Oldest */
                        1 -> call.redirectInternally("/events") {
                            append("before", (time / 1000).toString())
                            after?.let { append("after", (it / 1000).toString()) }
                            if (start != null) append("offset", start.toString())
                            append("gameTags", id)
                            append("sortorder", "asc")

                            appendAll(parameters.filter { k, _ -> k.toLowerCase() !in REMOVE_PARAMETERS_GAME_EVENTUALLY })
                        }
                        /** Top */
                        2 -> call.redirectInternally("/feed/top/game") {
                            appendAll(parameters)
                        }
                        /** Hot */
                        3 -> call.redirectInternally("/feed/hot/game") {
                            appendAll(parameters)
                        }

                        /** Newest */
                        else -> call.redirectInternally("/events") {
                            append("before", (time / 1000).toString())
                            after?.let { append("after", (it / 1000).toString()) }
                            if (start != null) append("offset", start.toString())
                            append("gameTags", id)
                            append("sortorder", "desc")

                            appendAll(parameters.filter { k, _ -> k.toLowerCase() !in REMOVE_PARAMETERS_GAME_EVENTUALLY })
                        }
                    }
                }
            }
        }
}

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