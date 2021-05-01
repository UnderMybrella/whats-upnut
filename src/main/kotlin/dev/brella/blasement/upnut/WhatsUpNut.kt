package dev.brella.blasement.upnut

import com.soywiz.klock.DateTime
import com.soywiz.klock.format
import dev.brella.kornea.blaseball.BlaseballApi
import dev.brella.kornea.blaseball.base.common.BLASEBALL_TIME_PATTERN
import dev.brella.kornea.blaseball.endpoints.BlaseballDatabaseService
import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.kornea.errors.common.doOnFailure
import dev.brella.kornea.errors.common.doOnThrown
import dev.brella.kornea.errors.common.getOrNull
import dev.brella.kornea.errors.common.map
import dev.brella.ktornea.common.getAsResult
import dev.brella.ktornea.common.installGranularHttp
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
import io.jsonwebtoken.security.Keys
import io.ktor.application.*
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.compression.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.client.utils.*
import io.ktor.config.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.serialization.*
import io.ktor.util.*
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions.*
import io.r2dbc.spi.Option
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingleOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import org.intellij.lang.annotations.Language
import org.slf4j.event.Level
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.await
import org.springframework.r2dbc.core.awaitRowsUpdated
import org.springframework.r2dbc.core.awaitSingleOrNull
import java.io.File
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.seconds
import org.springframework.r2dbc.core.bind as bindNullable

class BigUpNut(config: ApplicationConfig) : CoroutineScope {
    override val coroutineContext: CoroutineContext = SupervisorJob()

    //    val connectionFactory = ConnectionFactories.get("r2dbc:h2:file:///./upnuts?options=DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE");
    val connectionFactory: ConnectionFactory = ConnectionFactories.get(
        config.config("r2dbc").run {
            val builder = propertyOrNull("url")?.getString()?.let { parse(it).mutate() }
                          ?: builder().option(DRIVER, "pool").option(PROTOCOL, "postgresql")

            propertyOrNull("connectTimeout")?.getString()
                ?.let { builder.option(CONNECT_TIMEOUT, Duration.parse(it)) }

            propertyOrNull("database")?.getString()
                ?.let { builder.option(DATABASE, it) }

            propertyOrNull("driver")?.getString()
                ?.let { builder.option(DRIVER, it) }

            propertyOrNull("host")?.getString()
                ?.let { builder.option(HOST, it) }

            propertyOrNull("password")?.getString()
                ?.let { builder.option(PASSWORD, it) }

            propertyOrNull("port")?.getString()?.toIntOrNull()
                ?.let { builder.option(PORT, it) }

            propertyOrNull("protocol")?.getString()
                ?.let { builder.option(PROTOCOL, it) }

            propertyOrNull("ssl")?.getString()?.toBoolean()
                ?.let { builder.option(SSL, it) }

            propertyOrNull("user")?.getString()
                ?.let { builder.option(USER, it) }

            propertyOrNull("options")?.getList()?.forEach { option ->
                val (key, value) = option.split('=', limit = 2)

                builder.option(Option.valueOf(key), value)
            }


//                .option(HOST, "localhost")
//                .option(PORT, 5433) // optional, defaults to 5432
//                .option(USER, "upnuts")
//                .option(PASSWORD, "password")
//                .option(DATABASE, "upnuts") // optional

            builder.build()
        }
    )
    val client = DatabaseClient.create(connectionFactory);

    val initJob = launch {
        client.sql("CREATE TABLE IF NOT EXISTS upnuts (id BIGSERIAL PRIMARY KEY, nuts INT NOT NULL DEFAULT 1, feed_id uuid NOT NULL, source uuid NOT NULL, time BIGINT NOT NULL)")
            .fetch()
            .rowsUpdated()
            .awaitFirst()

        client.sql("CREATE TABLE IF NOT EXISTS game_nuts (id BIGSERIAL PRIMARY KEY, feed_id uuid NOT NULL, game_id uuid NOT NULL);")
            .fetch()
            .rowsUpdated()
            .awaitFirst()

        client.sql("CREATE TABLE IF NOT EXISTS player_nuts (id BIGSERIAL PRIMARY KEY, feed_id uuid NOT NULL, player_id uuid NOT NULL);")
            .fetch()
            .rowsUpdated()
            .awaitFirst()

        client.sql("CREATE TABLE IF NOT EXISTS team_nuts (id BIGSERIAL PRIMARY KEY, feed_id uuid NOT NULL, team_id uuid NOT NULL);")
            .fetch()
            .rowsUpdated()
            .awaitFirst()
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

    @OptIn(ExperimentalTime::class)
    val globalByTopJob = launch {
        val limit = 100
        loopEvery(60.seconds, `while` = { isActive }) {
//            getFeed(limit = limit, start = lastID?.let(BLASEBALL_TIME_PATTERN::format))
            var start = 0
            val now = Instant.now(Clock.systemUTC()).toEpochMilli()

            while (isActive) {
                val list = getGlobalFeed(limit = limit, sort = 2, start = start.toString())
                               .doOnThrown { error -> error.exception.printStackTrace() }
                               .doOnFailure { delay(100L) }
                               .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 } ?: break

                list.forEach { event -> processNuts(now, event) }

                if (list.size < limit) break
                start += list.size

                delay(100L)
            }
        }
    }

    @OptIn(ExperimentalTime::class)
    val globalByHotJob = launch {
        val limit = 100
        loopEvery(60.seconds, `while` = { isActive }) {
            val now = Instant.now(Clock.systemUTC()).toEpochMilli()

            val list = getGlobalFeed(limit = limit, sort = 3)
                .doOnThrown { error -> error.exception.printStackTrace() }
                .doOnFailure { delay(100L) }
                .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 }
                ?.reversed()

            list?.forEach { event -> processNuts(now, event) }
        }
    }

    @OptIn(ExperimentalTime::class)
    val teamsByTopJob = launch {
        blaseballApi.getAllTeams()
            .getOrNull()
            ?.map { team ->
                launch {
                    val limit = 100
                    loopEvery(60.seconds, `while` = { isActive }) {
                        var start = 0
                        val now = Instant.now(Clock.systemUTC()).toEpochMilli()

                        while (isActive) {
                            val list = getTeamFeed(team.id.id, limit = limit, sort = 2, start = start.toString())
                                           .doOnThrown { error -> error.exception.printStackTrace() }
                                           .doOnFailure { delay(100L) }
                                           .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 } ?: break

                            list.forEach { event -> processNuts(now, event) }

                            if (list.size < limit) break
                            start += list.size

                            delay(100)
                        }
                    }
                }
            }
            ?.joinAll()
    }

    @OptIn(ExperimentalTime::class)
    val teamsByHotJob = launch {
        blaseballApi.getAllTeams()
            .getOrNull()
            ?.map { team ->
                launch {
                    val limit = 100
                    loopEvery(60.seconds, `while` = { isActive }) {
                        val now = Instant.now(Clock.systemUTC()).toEpochMilli()

                        val list = getTeamFeed(team.id.id, limit = limit, sort = 3)
                            .doOnThrown { error -> error.exception.printStackTrace() }
                            .doOnFailure { delay(100L) }
                            .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 }
                            ?.reversed()

                        list?.forEach { event -> processNuts(now, event) }
                    }
                }
            }
            ?.joinAll()
    }

//    val newNuts: MutableSharedFlow<Pair<Long, UpNutEvent>> = MutableSharedFlow(extraBufferCapacity = Channel.UNLIMITED)
//    val nutsAtTime: MutableMap<String, Int> = HashMap()

//    val newNutsProcessing = newNuts.onEach { (time, event) ->

    suspend fun processNuts(time: Long, event: UpNutEvent) {
        val players = client.sql("SELECT player_id FROM player_nuts WHERE feed_id = $1")
                          .bind("$1", event.id)
                          .fetch()
                          .all()
                          .mapNotNull { it["player_id"] as? String }
                          .collectList()
                          .awaitFirstOrNull() ?: emptyList()

        event.playerTags?.filterNot(players::contains)
            ?.forEach { playerID ->
                client.sql("INSERT INTO player_nuts (feed_id, player_id) VALUES ($1, $2)")
                    .bind("$1", event.id)
                    .bind("$2", playerID)
                    .await()
            }

        val games = client.sql("SELECT game_id FROM game_nuts WHERE feed_id = $1")
                        .bind("$1", event.id)
                        .fetch()
                        .all()
                        .mapNotNull { it["game_id"] as? String }
                        .collectList()
                        .awaitFirstOrNull() ?: emptyList()

        event.gameTags?.filterNot(games::contains)
            ?.forEach { gameID ->
                client.sql("INSERT INTO game_nuts (feed_id, game_id) VALUES ($1, $2)")
                    .bind("$1", event.id)
                    .bind("$2", gameID)
                    .await()
            }

        val teams = client.sql("SELECT team_id FROM team_nuts WHERE feed_id = $1")
                        .bind("$1", event.id)
                        .fetch()
                        .all()
                        .mapNotNull { it["team_id"] as? String }
                        .collectList()
                        .awaitFirstOrNull() ?: emptyList()

        event.teamTags?.filterNot(teams::contains)
            ?.forEach { teamID ->
                client.sql("INSERT INTO team_nuts (feed_id, team_id) VALUES ($1, $2)")
                    .bind("$1", event.id)
                    .bind("$2", teamID)
                    .await()
            }

        val nutsAtTimeOfRecording = client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                        .bind("$1", event.id)
                                        .bind("$2", time)
                                        .fetch()
                                        .first()
                                        .awaitFirstOrNull()
                                        ?.values
                                        ?.let { it.firstOrNull() as? Number }
                                        ?.toInt() ?: 0

        val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
        if (difference <= 0) return

        client.sql("INSERT INTO upnuts (nuts, feed_id, source, time) VALUES ( $1, $2, $3, $4 )")
            .bind("$1", difference)
            .bind("$2", event.id)
            .bind("$3", "7fcb63bc-11f2-40b9-b465-f1d458692a63")
            .bind("$4", time)
            .fetch()
            .awaitRowsUpdated()


        println("${event.id} +$difference")
    }

/*    suspend inline fun processNuts(time: Long, events: Iterable<UpNutEvent>) {
        val nutsAtTimeOfRecording = client.sql("SELECT nuts, source FROM upnuts WHERE feed_id IN ($1) AND time <= $2")
                                        .bind("$1", events.joinToString { it.id })
                                        .bind("$2", time)
                                        .fetch()
                                        .all()
                                        .collectList()
                                        .awaitSingleOrNull()
                                        ?.mapNotNull { map ->
                                            (map["nuts"] as? Number)?.let { nuts ->
                                                (map["source"] as? String)?.let { source ->
                                                    Pair(nuts.toInt(), source)
                                                }
                                            }
                                        }?.groupBy(Pair<Int, String>::second, Pair<Int, String>::first)
                                        ?.mapValues { (_, list) -> list.sum() } ?: emptyMap()

        val difference = event.nuts - nutsAtTimeOfRecording
        if (difference <= 0) return

        client.sql("INSERT INTO upnuts (nuts, feed_id, source, time) VALUES ( $1, $2, $3, $4 )")
            .bind("$1", difference)
            .bind("$2", event.id)
            .bind("$3", "7fcb63bc-11f2-40b9-b465-f1d458692a63")
            .bind("$4", time)
            .fetch()
            .awaitRowsUpdated()


        println("${event.id} +$difference")
    }*/
//    }.launchIn(this)
}

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

@OptIn(ExperimentalTime::class)
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
    install(StatusPages)
    install(CallLogging) {
        level = Level.INFO
    }

    val config = environment.config.config("upnut")

    val upnut = BigUpNut(config)

    val key = File(".secretkey").let { file ->
        if (file.exists()) Keys.hmacShaKeyFor(file.readBytes())
        else {
            val key = Keys.secretKeyFor(SignatureAlgorithm.HS512)
            file.writeBytes(key.encoded)
            key
        }
    }
    val parser = Jwts.parserBuilder().setSigningKey(key).build()

    routing {
        put("/{feed_id}/{source}") {
            val authToken = call.request.header("Authorization")
            try {
                val authJws = parser.parseClaimsJws(authToken)
                val id = authJws.body.id

                val feedID = call.parameters.getOrFail("feed_id")
                val source = call.parameters.getOrFail("source")

                val time = call.parameters["time"]?.let { time -> BLASEBALL_TIME_PATTERN.tryParse(time, false)?.utc?.unixMillisLong ?: time.toLongOrNull() }
                           ?: Instant.now(Clock.systemUTC()).toEpochMilli()

                if (source != id) {
                    call.respondJsonObject(HttpStatusCode.Forbidden) {
                        put("error", "Source ID does not match auth ID")
                        put("auth_id", id)
                        put("source_id", source)
                    }
                } else {
                    val nuts = upnut.client.sql("SELECT nuts FROM upnuts WHERE feed_id = $1 AND source = $2")
                                   .bind("$1", feedID)
                                   .bind("$2", source)
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
                        upnut.client.sql("INSERT INTO upnuts (nuts, feed_id, source, time) VALUES ( $1, $2, $3, $4 )")
                            .bind("$1", 1)
                            .bind("$2", feedID)
                            .bind("$3", source)
                            .bind("$4", time)
                            .fetch()
                            .awaitRowsUpdated()

                        call.respond(HttpStatusCode.Created, EmptyContent)
                    }
                }
            } catch (th: Throwable) {
                call.respondJsonObject(HttpStatusCode.Unauthorized) {
                    put("stack_trace", th.stackTraceToString())
                }
            }
        }

        delete("/{feed_id}/{source}") {
            val authToken = call.request.header("Authorization")
            try {
                val authJws = parser.parseClaimsJws(authToken)
                val id = authJws.body.id

                val feedID = call.parameters.getOrFail("feed_id")
                val source = call.parameters.getOrFail("source")

                if (source != id) {
                    call.respondJsonObject(HttpStatusCode.Forbidden) {
                        put("error", "Source ID does not match auth ID")
                        put("auth_id", id)
                        put("source_id", source)
                    }
                } else {
                    val rowsUpdated = upnut.client.sql("DELETE FROM upnuts WHERE feed_id = $1 AND source = $2")
                        .bind("$1", feedID)
                        .bind("$2", source)
                        .fetch()
                        .awaitRowsUpdated()

                    if (rowsUpdated == 0) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "You haven't Upshelled that event.")
                        }
                    } else {
                        call.respond(HttpStatusCode.NoContent, EmptyContent)
                    }
                }
            } catch (th: Throwable) {
                call.respondJsonObject(HttpStatusCode.Unauthorized) {
                    put("stack_trace", th.stackTraceToString())
                }
            }
        }

        get("/events") {
            val parameters = call.request.queryParameters

            val player = parameters["player"] ?: call.request.header("X-UpNut-Player")
            val time = parameters["time"]?.let { time ->
                time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
            } ?: call.request.header("X-UpNut-Time")?.let { time ->
                time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
            } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
            val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
            val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

            upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                url.parameters.appendAll(parameters)
            }.map { list ->
                list.map { event ->
                    val sum = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT SUM(nuts) FROM upnuts WHERE feed_id = $1 AND time <= $2 AND source IN ($3)")
                                .bind("$1", event.id)
                                .bind("$2", time)
                                .bind("$3", filterSources)
                                .fetch()
                                .awaitSingleOrNull()
                                ?.values
                                ?.run { firstOrNull() as? Number }
                                ?.toInt() ?: event.nuts.intOrNull ?: 0
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT SUM(nuts) FROM upnuts WHERE feed_id = $1 AND time <= $2 AND source NOT IN ($3)")
                                .bind("$1", event.id)
                                .bind("$2", time)
                                .bind("$3", filterSourcesNot)
                                .fetch()
                                .awaitSingleOrNull()
                                ?.values
                                ?.run { firstOrNull() as? Number }
                                ?.toInt() ?: event.nuts.intOrNull ?: 0
                        }
                        else -> {
                            upnut.client.sql("SELECT SUM(nuts) FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                .bind("$1", event.id)
                                .bind("$2", time)
                                .fetch()
                                .awaitSingleOrNull()
                                ?.values
                                ?.run { firstOrNull() as? Number }
                                ?.toInt() ?: event.nuts.intOrNull ?: 0
                        }
                    }

                    val metadata: JsonElement

                    if (player != null) {
                        val upNut = upnut.client.sql("SELECT SUM(nuts) FROM upnuts WHERE feed_id = $1 AND time <= $2 AND source = $3")
                            .bind("$1", event.id)
                            .bind("$2", time)
                            .bindNullable("$3", player)
                            .fetch()
                            .awaitSingleOrNull()
                            ?.values
                            ?.run { firstOrNull() as? Number }
                            ?.toInt()

                        metadata = if (upNut != null && upNut > 0) when (event.metadata) {
                            is JsonObject -> JsonObject(event.metadata + Pair("upnut", JsonPrimitive(true)))
                            is JsonNull -> JsonObject(mapOf("upnut" to JsonPrimitive(true)))
                            else -> event.metadata
                        } else event.metadata
                    } else {
                        metadata = event.metadata
                    }

                    if (sum != event.nuts.intOrNull) {
                        event.copy(nuts = JsonPrimitive(sum), metadata = metadata)
                    } else {
                        event
                    }
                }
            }.respond(call)
        }

        get("/nuts") {
            val parameters = call.request.queryParameters

            val time = parameters["time"]?.let { time ->
                time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
            } ?: call.request.header("X-UpNut-Time")?.let { time ->
                time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
            } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
            val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
            val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

            val formatAsDateTime = (parameters["time_format"] ?: call.request.header("X-UpNut-TimeFormat")) ==
                    "datetime"

            upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                url.parameters.appendAll(parameters)
            }.map { list ->
                list.map { event ->
                    val nuts = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT nuts, source, time FROM upnuts WHERE feed_id = $1 AND time <= $2 AND source IN ($3)")
                                .bind("$1", event.id)
                                .bind("$2", time)
                                .bind("$3", filterSources)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                                ?.mapNotNull { map ->
                                    (map["nuts"] as? Number)?.let { nuts ->
                                        (map["source"] as? String)?.let { source ->
                                            (map["time"] as? Number)?.let { time ->
                                                NutsEpoch(nuts, source, time)
                                            }
                                        }
                                    }
                                }
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT nuts, source, time FROM upnuts WHERE feed_id = $1 AND time <= $2 AND source NOT IN ($3)")
                                .bind("$1", event.id)
                                .bind("$2", time)
                                .bind("$3", filterSourcesNot)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                                ?.mapNotNull { map ->
                                    (map["nuts"] as? Number)?.let { nuts ->
                                        (map["source"] as? String)?.let { source ->
                                            (map["time"] as? Number)?.let { time ->
                                                NutsEpoch(nuts, source, time)
                                            }
                                        }
                                    }
                                }
                        }
                        else -> {
                            upnut.client.sql("SELECT nuts, source, time FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                .bind("$1", event.id)
                                .bind("$2", time)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                                ?.mapNotNull { map ->
                                    (map["nuts"] as? Number)?.let { nuts ->
                                        (map["source"] as? String)?.let { source ->
                                            (map["time"] as? Number)?.let { time ->
                                                NutsEpoch(nuts, source, time)
                                            }
                                        }
                                    }
                                }
                        }
                    }

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
                            nuts?.map { (nuts, source, time) -> NutsDateTime(nuts, source, BLASEBALL_TIME_PATTERN.format(DateTime.fromUnix(time))) } ?: emptyList(),
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
            }.respond(call)
        }

        route("/feed") {
            route("/hot") {
                get("/global") {
                    val parameters = call.request.queryParameters

                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
                    val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

                    val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

                    val nuts = when {
                        filterSources != null -> {
                            SqlQueries.baseHotInSources(upnut.client, time = time, limit = limit, filterSources)

                            /*upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND source IN ($2) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $3) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", filterSources)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()*/
                        }
                        filterSourcesNot != null -> {
                            SqlQueries.baseHotNotInSources(upnut.client, time = time, limit = limit, filterSources)

                            /*upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND source NOT IN ($2) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $3) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", filterSourcesNot)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()*/
                        }
                        else -> {
                            SqlQueries.baseHot(upnut.client, time = time, limit = limit)

/*                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $2) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()*/
                        }
                    }?.mapNotNull { map ->
                        (map["feed_id"] as? String)?.let { feedID ->
                            (map["sum"] as? Number)?.let { sum ->
                                Pair(feedID, sum.toInt())
                            }
                        }
                    }

                    if (nuts == null) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "No PSQL Results")
                        }
                    } else {
                        upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                            parameter("ids", nuts.joinToString(",", transform = Pair<String, Int>::first))
                        }.map { list ->
                            val map = list.associateBy(UpNutEvent::id)

                            nuts.mapNotNull { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
                        }.respond(call)
                    }
                }

                get("/team") {
                    val parameters = call.request.queryParameters

                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
                    val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

                    val teamID = parameters["id"]
                                 ?: call.request.header("X-UpNut-ID")
                                 ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) {
                                     put("error", "No ID passed via query parameter (id) or header (X-UpNut-ID)")
                                 }

                    val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

                    val nuts = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND source IN ($2) AND feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $3) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $4) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", filterSources)
                                .bind("$3", teamID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND source NOT IN ($2) AND feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $3) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $4) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", filterSourcesNot)
                                .bind("$3", teamID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        else -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $2) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $3) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", teamID)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                    }?.mapNotNull { map ->
                        (map["feed_id"] as? String)?.let { feedID ->
                            (map["sum"] as? Number)?.let { sum ->
                                Pair(feedID, sum.toInt())
                            }
                        }
                    }

                    if (nuts == null) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "No PSQL Results")
                        }
                    } else {
                        upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                            parameter("ids", nuts.joinToString(",", transform = Pair<String, Int>::first))
                        }.map { list ->
                            val map = list.associateBy(UpNutEvent::id)

                            nuts.mapNotNull { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
                        }.respond(call)
                    }
                }

                get("/game") {
                    val parameters = call.request.queryParameters

                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
                    val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

                    val gameID = parameters["id"]
                                 ?: call.request.header("X-UpNut-ID")
                                 ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) {
                                     put("error", "No ID passed via query parameter (id) or header (X-UpNut-ID)")
                                 }

                    val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

                    val nuts = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND source IN ($2) AND feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $3) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $4) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", filterSources)
                                .bind("$3", gameID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND source NOT IN ($2) AND feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $3) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $4) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", filterSourcesNot)
                                .bind("$3", gameID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        else -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $2) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $3) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", gameID)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                    }?.mapNotNull { map ->
                        (map["feed_id"] as? String)?.let { feedID ->
                            (map["sum"] as? Number)?.let { sum ->
                                Pair(feedID, sum.toInt())
                            }
                        }
                    }

                    if (nuts == null) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "No PSQL Results")
                        }
                    } else {
                        upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                            parameter("ids", nuts.joinToString(",", transform = Pair<String, Int>::first))
                        }.map { list ->
                            val map = list.associateBy(UpNutEvent::id)

                            nuts.mapNotNull { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
                        }.respond(call)
                    }
                }

                get("/player") {
                    val parameters = call.request.queryParameters

                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
                    val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

                    val playerID = parameters["id"]
                                   ?: call.request.header("X-UpNut-ID")
                                   ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) {
                                       put("error", "No ID passed via query parameter (id) or header (X-UpNut-ID)")
                                   }

                    val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

                    val nuts = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND source IN ($2) AND feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $3) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $4) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", filterSources)
                                .bind("$3", playerID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND source NOT IN ($2) AND feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $3) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $4) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", filterSourcesNot)
                                .bind("$3", playerID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        else -> {
                            upnut.client.sql("SELECT * FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE time <= $1 AND feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $2) GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $3) as list ORDER BY list.sum DESC")
                                .bind("$1", time)
                                .bind("$2", playerID)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                    }?.mapNotNull { map ->
                        (map["feed_id"] as? String)?.let { feedID ->
                            (map["sum"] as? Number)?.let { sum ->
                                Pair(feedID, sum.toInt())
                            }
                        }
                    }

                    if (nuts == null) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "No PSQL Results")
                        }
                    } else {
                        upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                            parameter("ids", nuts.joinToString(",", transform = Pair<String, Int>::first))
                        }.map { list ->
                            val map = list.associateBy(UpNutEvent::id)

                            nuts.mapNotNull { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
                        }.respond(call)
                    }
                }
            }

            route("/top") {
                get("/global") {
                    val parameters = call.request.queryParameters

                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
                    val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

                    val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

                    val nuts = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND source IN ($2) GROUP BY feed_id ORDER BY sum DESC LIMIT $3")
                                .bind("$1", time)
                                .bind("$2", filterSources)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND source NOT IN ($2) GROUP BY feed_id ORDER BY sum DESC LIMIT $3")
                                .bind("$1", time)
                                .bind("$2", filterSourcesNot)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        else -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 GROUP BY feed_id ORDER BY sum DESC LIMIT $2")
                                .bind("$1", time)
                                .bind("$2", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                    }?.mapNotNull { map ->
                        (map["feed_id"] as? String)?.let { feedID ->
                            (map["sum"] as? Number)?.let { sum ->
                                Pair(feedID, sum.toInt())
                            }
                        }
                    }

                    if (nuts == null) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "No PSQL Results")
                        }
                    } else {
                        upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                            parameter("ids", nuts.joinToString(",", transform = Pair<String, Int>::first))
                        }.map { list ->
                            val map = list.associateBy(UpNutEvent::id)

                            nuts.mapNotNull { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
                        }.respond(call)
                    }
                }

                get("/team") {
                    val parameters = call.request.queryParameters

                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
                    val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

                    val teamID = parameters["id"]
                                 ?: call.request.header("X-UpNut-ID")
                                 ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) {
                                     put("error", "No ID passed via query parameter (id) or header (X-UpNut-ID)")
                                 }

                    val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

                    val nuts = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND source IN ($2) AND feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $3) GROUP BY feed_id ORDER BY sum DESC LIMIT $4")
                                .bind("$1", time)
                                .bind("$2", filterSources)
                                .bind("$3", teamID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND source NOT IN ($2) AND feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $3) GROUP BY feed_id ORDER BY sum DESC LIMIT $4")
                                .bind("$1", time)
                                .bind("$2", filterSourcesNot)
                                .bind("$3", teamID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        else -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $2) GROUP BY feed_id ORDER BY sum DESC LIMIT $3")
                                .bind("$1", time)
                                .bind("$2", teamID)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                    }?.mapNotNull { map ->
                        (map["feed_id"] as? String)?.let { feedID ->
                            (map["sum"] as? Number)?.let { sum ->
                                Pair(feedID, sum.toInt())
                            }
                        }
                    }

                    if (nuts == null) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "No PSQL Results")
                        }
                    } else {
                        upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                            parameter("ids", nuts.joinToString(",", transform = Pair<String, Int>::first))
                        }.map { list ->
                            val map = list.associateBy(UpNutEvent::id)

                            nuts.mapNotNull { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
                        }.respond(call)
                    }
                }

                get("/game") {
                    val parameters = call.request.queryParameters

                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
                    val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

                    val gameID = parameters["id"]
                                 ?: call.request.header("X-UpNut-ID")
                                 ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) {
                                     put("error", "No ID passed via query parameter (id) or header (X-UpNut-ID)")
                                 }

                    val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

                    val nuts = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND source IN ($2) AND feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $3) GROUP BY feed_id ORDER BY sum DESC LIMIT $4")
                                .bind("$1", time)
                                .bind("$2", filterSources)
                                .bind("$3", gameID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND source NOT IN ($2) AND feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $3) GROUP BY feed_id ORDER BY sum DESC LIMIT $4")
                                .bind("$1", time)
                                .bind("$2", filterSourcesNot)
                                .bind("$3", gameID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        else -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $2) GROUP BY feed_id ORDER BY sum DESC LIMIT $3")
                                .bind("$1", time)
                                .bind("$2", gameID)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                    }?.mapNotNull { map ->
                        (map["feed_id"] as? String)?.let { feedID ->
                            (map["sum"] as? Number)?.let { sum ->
                                Pair(feedID, sum.toInt())
                            }
                        }
                    }

                    if (nuts == null) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "No PSQL Results")
                        }
                    } else {
                        upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                            parameter("ids", nuts.joinToString(",", transform = Pair<String, Int>::first))
                        }.map { list ->
                            val map = list.associateBy(UpNutEvent::id)

                            nuts.mapNotNull { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
                        }.respond(call)
                    }
                }

                get("/player") {
                    val parameters = call.request.queryParameters

                    val time = parameters["time"]?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: call.request.header("X-UpNut-Time")?.let { time ->
                        time.toLongOrNull() ?: BLASEBALL_TIME_PATTERN.tryParse(time)?.utc?.unixMillisLong
                    } ?: Instant.now(Clock.systemUTC()).toEpochMilli()
                    val filterSources = parameters["filter_sources"] ?: call.request.header("X-UpNut-FilterSources")
                    val filterSourcesNot = parameters["filter_sources_not"] ?: call.request.header("X-UpNut-FilterSourcesNot")

                    val playerID = parameters["id"]
                                   ?: call.request.header("X-UpNut-ID")
                                   ?: return@get call.respondJsonObject(HttpStatusCode.BadRequest) {
                                       put("error", "No ID passed via query parameter (id) or header (X-UpNut-ID)")
                                   }

                    val limit = (parameters["limit"]?.toIntOrNull() ?: call.request.header("X-UpNut-Limit")?.toIntOrNull())?.coerceIn(1, 50) ?: 50

                    val nuts = when {
                        filterSources != null -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND source IN ($2) AND feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $3) GROUP BY feed_id ORDER BY sum DESC LIMIT $4")
                                .bind("$1", time)
                                .bind("$2", filterSources)
                                .bind("$3", playerID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        filterSourcesNot != null -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND source NOT IN ($2) AND feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $3) GROUP BY feed_id ORDER BY sum DESC LIMIT $4")
                                .bind("$1", time)
                                .bind("$2", filterSourcesNot)
                                .bind("$3", playerID)
                                .bind("$4", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                        else -> {
                            upnut.client.sql("SELECT feed_id, SUM(nuts) AS sum FROM upnuts WHERE time <= $1 AND feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $2) GROUP BY feed_id ORDER BY sum DESC LIMIT $3")
                                .bind("$1", time)
                                .bind("$2", playerID)
                                .bind("$3", limit)
                                .fetch()
                                .all()
                                .collectList()
                                .awaitSingleOrNull()
                        }
                    }?.mapNotNull { map ->
                        (map["feed_id"] as? String)?.let { feedID ->
                            (map["sum"] as? Number)?.let { sum ->
                                Pair(feedID, sum.toInt())
                            }
                        }
                    }

                    if (nuts == null) {
                        call.respondJsonObject(HttpStatusCode.BadRequest) {
                            put("error", "No PSQL Results")
                        }
                    } else {
                        upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                            parameter("ids", nuts.joinToString(",", transform = Pair<String, Int>::first))
                        }.map { list ->
                            val map = list.associateBy(UpNutEvent::id)

                            nuts.mapNotNull { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
                        }.respond(call)
                    }
                }
            }
        }
    }
}

object SqlQueries {
    const val LIMIT_VAR = "\$limit"
    const val SOURCE_VAR = "\$source"
    const val TIME_VAR = "\$time"
    const val TEAM_VAR = "\$team_id"
    const val GAME_VAR = "\$game_id"
    const val PLAYER_VAR = "\$player_id"

    const val TIME_FILTER = "time <= $TIME_VAR"
    const val TEAM_FILTER = "feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $TEAM_VAR)"
    const val GAME_FILTER = "feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $GAME_VAR)"
    const val PLAYER_FILTER = "feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $PLAYER_VAR)"
    const val SOURCES_FILTER = "source IN ($SOURCE_VAR)"
    const val NOT_SOURCES_FILTER = "source NOT IN ($SOURCE_VAR)"

    val BASE_HOT = hotPSQLWithTime()
    val BASE_HOT_SOURCES = hotPSQLWithTime(SOURCES_FILTER)
    val BASE_HOT_NOT_SOURCES = hotPSQLWithTime(NOT_SOURCES_FILTER)

    val TEAM_HOT = hotPSQLWithTimeAndTeam()
    val TEAM_HOT_SOURCES = hotPSQLWithTimeAndTeam(SOURCES_FILTER)
    val TEAM_HOT_NOT_SOURCES = hotPSQLWithTimeAndTeam(NOT_SOURCES_FILTER)

    val GAME_HOT = hotPSQLWithTimeAndGame()
    val GAME_HOT_SOURCES = hotPSQLWithTimeAndGame(SOURCES_FILTER)
    val GAME_HOT_NOT_SOURCES = hotPSQLWithTimeAndGame(NOT_SOURCES_FILTER)

    val PLAYER_HOT = hotPSQLWithTimeAndPlayer()
    val PLAYER_HOT_SOURCES = hotPSQLWithTimeAndPlayer(SOURCES_FILTER)
    val PLAYER_HOT_NOT_SOURCES = hotPSQLWithTimeAndPlayer(NOT_SOURCES_FILTER)


    inline fun hotPSQLWithTime(and: String? = null): String =
        hotPSQL(if (and == null) TIME_FILTER else "$TIME_FILTER AND $and")

    inline fun hotPSQLWithTimeAndTeam(and: String? = null): String =
        hotPSQL(if (and == null) "$TIME_FILTER AND $TEAM_FILTER" else "$TIME_FILTER AND $and AND $TEAM_FILTER")

    inline fun hotPSQLWithTimeAndGame(and: String? = null): String =
        hotPSQL(if (and == null) "$TIME_FILTER AND $GAME_FILTER" else "$TIME_FILTER AND $and AND $GAME_FILTER")

    inline fun hotPSQLWithTimeAndPlayer(and: String? = null): String =
        hotPSQL(if (and == null) "$TIME_FILTER AND $PLAYER_FILTER" else "$TIME_FILTER AND $and AND $PLAYER_FILTER")

    @Language("PostgreSQL")
    inline fun hotPSQL(where: String): String =
        "SELECT list.feed_id, list.sum, list.time FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE $where GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT \$limit) as list ORDER BY list.sum DESC"

    suspend fun baseHot(client: DatabaseClient, time: Long, limit: Int) =
        client.sql(BASE_HOT)
            .bind(TIME_VAR, time)
            .bind(LIMIT_VAR, limit)
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun baseHotInSources(client: DatabaseClient, time: Long, limit: Int, sources: List<String>) = baseHotInSources(client, time, limit, sources.joinToString(","))
    suspend fun baseHotInSources(client: DatabaseClient, time: Long, limit: Int, sources: String) =
        client.sql(BASE_HOT_NOT_SOURCES)
            .bind(TIME_VAR, time)
            .bind(LIMIT_VAR, limit)
            .bind(SOURCE_VAR, sources)
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun baseHotNotInSources(client: DatabaseClient, time: Long, limit: Int, sources: List<String>) = baseHotInSources(client, time, limit, sources.joinToString(","))
    suspend fun baseHotNotInSources(client: DatabaseClient, time: Long, limit: Int, sources: String) =
        client.sql(BASE_HOT_NOT_SOURCES)
            .bind(TIME_VAR, time)
            .bind(LIMIT_VAR, limit)
            .bind(SOURCE_VAR, sources)
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()
}