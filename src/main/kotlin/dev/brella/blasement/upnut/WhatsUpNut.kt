package dev.brella.blasement.upnut

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
import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.ConnectionFactoryOptions.*
import io.r2dbc.spi.Option
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.put
import org.slf4j.event.Level
import org.springframework.r2dbc.core.DatabaseClient
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
                          ?: builder().option(DRIVER, "postgresql")

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
        client.sql("CREATE TABLE IF NOT EXISTS upnuts (id BIGSERIAL PRIMARY KEY, nuts INT NOT NULL DEFAULT 1, feed_id VARCHAR(64) NOT NULL, source VARCHAR(64) NOT NULL, time BIGINT NOT NULL)")
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
        val limit = 1000
        loopEvery(1.seconds, `while` = { isActive }) {
//            getFeed(limit = limit, start = lastID?.let(BLASEBALL_TIME_PATTERN::format))
            var start = 0
            val now = Instant.now(Clock.systemUTC()).toEpochMilli()

            while (isActive) {
                val list = getGlobalFeed(limit = limit, sort = 2, start = start.toString())
                               .doOnThrown { error -> error.exception.printStackTrace() }
                               .doOnFailure { delay(100L) }
                               .getOrNull()?.takeWhile { event -> event.nuts > 0 } ?: break

                launch { list.forEach { event -> newNuts.emit(now to event) } }

                if (list.size < limit) break
                start += list.size
            }
        }
    }

    @OptIn(ExperimentalTime::class)
    val globalByHotJob = launch {
        val limit = 1000
        loopEvery(1.seconds, `while` = { isActive }) {
            val now = Instant.now(Clock.systemUTC()).toEpochMilli()

            val list = getGlobalFeed(limit = limit, sort = 3)
                .doOnThrown { error -> error.exception.printStackTrace() }
                .doOnFailure { delay(100L) }
                .getOrNull()?.takeWhile { event -> event.nuts > 0 }
                ?.reversed()

            launch { list?.forEach { event -> newNuts.emit(now to event) } }
        }
    }

    @OptIn(ExperimentalTime::class)
    val teamsByTopJob = launch {
        blaseballApi.getAllTeams()
            .getOrNull()
            ?.map { team ->
                launch {
                    val limit = 1000
                    loopEvery(1.seconds, `while` = { isActive }) {
                        var start = 0
                        val now = Instant.now(Clock.systemUTC()).toEpochMilli()

                        while (isActive) {
                            val list = getTeamFeed(team.id.id, limit = limit, sort = 2, start = start.toString())
                                           .doOnThrown { error -> error.exception.printStackTrace() }
                                           .doOnFailure { delay(100L) }
                                           .getOrNull()?.takeWhile { event -> event.nuts > 0 } ?: break

                            launch { list.forEach { event -> newNuts.emit(now to event) } }

                            if (list.size < limit) break
                            start += list.size
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
                    val limit = 1000
                    loopEvery(1.seconds, `while` = { isActive }) {
                        val now = Instant.now(Clock.systemUTC()).toEpochMilli()

                        val list = getTeamFeed(team.id.id, limit = limit, sort = 3)
                            .doOnThrown { error -> error.exception.printStackTrace() }
                            .doOnFailure { delay(100L) }
                            .getOrNull()?.takeWhile { event -> event.nuts > 0 }
                            ?.reversed()

                        launch { list?.forEach { event -> newNuts.emit(now to event) } }
                    }
                }
            }
            ?.joinAll()
    }

    val newNuts: MutableSharedFlow<Pair<Long, UpNutEvent>> = MutableSharedFlow(extraBufferCapacity = Channel.UNLIMITED)
    val nutsAtTime: MutableMap<String, Int> = HashMap()

    val newNutsProcessing = newNuts.onEach { (time, event) ->
        val nutsAtTimeOfRecording = if (event.id in nutsAtTime) nutsAtTime.getValue(event.id)
        else {
            val num = client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                          .bind("$1", event.id)
                          .bind("$2", time)
                          .fetch()
                          .first()
                          .awaitFirstOrNull()
                          ?.values
                          ?.let { it.firstOrNull() as? Number }
                          ?.toInt() ?: 0

            nutsAtTime[event.id] = num
            num
        }

        val difference = event.nuts - nutsAtTimeOfRecording
        if (difference <= 0) return@onEach
        nutsAtTime[event.id] = event.nuts

        client.sql("INSERT INTO upnuts (nuts, feed_id, source, time) VALUES ( $1, $2, $3, $4 )")
            .bind("$1", difference)
            .bind("$2", event.id)
            .bind("$3", "7fcb63bc-11f2-40b9-b465-f1d458692a63")
            .bind("$4", time)
            .fetch()
            .awaitRowsUpdated()


        println("${event.id} +$difference")
    }.launchIn(this)
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

//                    BigUpNut.client.sql("SELECT nuts, source FROM upnuts WHERE feed_id = $1 AND time <= $2;")
//                        .bind("$1", event.id)
//                        .bind("$2", time ?: System.currentTimeMillis())
//                        .fetch()
//                        .all()
//                        .mapNotNull { map ->
//                            val nuts = (map["nuts"] ?: map["NUTS"]) as? Int
//                            val playerID = (map["source"] ?: map["source"]) as? String
//
//                            if (nuts != null && playerID != null) Pair(nuts, playerID)
//                            else null
//                        }.collectList()
//                        .awaitSingleOrNull()
//                        ?.filterNotNull()
//                        ?.groupBy(Pair<Int, String>::second, Pair<Int, String>::first)
//                        ?.mapValues { (_, list) -> list.sum() }

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
                    upnut.client.sql("INSERT INTO upnuts (nuts, feed_id, source, time) VALUES ( $1, $2, $3, $4 )")
                        .bind("$1", 1)
                        .bind("$2", feedID)
                        .bind("$3", source)
                        .bind("$4", time)
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
                    upnut.client.sql("DELETE FROM upnuts WHERE feed_id = $1 AND source = $2")
                        .bind("$1", feedID)
                        .bind("$2", source)
                        .fetch()
                        .awaitRowsUpdated()

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

            val player = parameters["user"] ?: call.request.header("X-UpNut-Player")
            val time = parameters["time"]?.toLongOrNull() ?: call.request.header("X-UpNut-Time")?.toLongOrNull()
            val filterUsers = parameters["filter_users"] ?: call.request.header("X-UpNut-FilterUsers")
            val filterUsersNot = parameters["filter_users_not"] ?: call.request.header("X-UpNut-FilterUsersNot")

            upnut.http.getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/events") {
                url.parameters.appendAll(parameters)
            }.map { list ->
                list.map { event ->
                    val sum = when {
                        filterUsers != null -> {
                            upnut.client.sql("SELECT SUM(nuts) FROM upnuts WHERE feed_id = $1 AND time <= $2 AND source IN ($3)")
                                .bind("$1", event.id)
                                .bind("$2", time ?: System.currentTimeMillis())
                                .bind("$3", filterUsers)
                                .fetch()
                                .awaitSingleOrNull()
                                ?.values
                                ?.run { firstOrNull() as? Number }
                                ?.toInt() ?: event.nuts
                        }
                        filterUsersNot != null -> {
                            upnut.client.sql("SELECT SUM(nuts) FROM upnuts WHERE feed_id = $1 AND time <= $2 AND source NOT IN ($3)")
                                .bind("$1", event.id)
                                .bind("$2", time ?: System.currentTimeMillis())
                                .bind("$3", filterUsersNot)
                                .fetch()
                                .awaitSingleOrNull()
                                ?.values
                                ?.run { firstOrNull() as? Number }
                                ?.toInt() ?: event.nuts
                        }
                        else -> {
                            upnut.client.sql("SELECT SUM(nuts) FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                .bind("$1", event.id)
                                .bind("$2", time ?: System.currentTimeMillis())
                                .fetch()
                                .awaitSingleOrNull()
                                ?.values
                                ?.run { firstOrNull() as? Number }
                                ?.toInt() ?: event.nuts
                        }
                    }

                    val metadata: JsonElement

                    if (player != null) {
                        val upNut = upnut.client.sql("SELECT SUM(nuts) FROM upnuts WHERE feed_id = $1 AND time <= $2 AND source = $3")
                            .bind("$1", event.id)
                            .bind("$2", time ?: System.currentTimeMillis())
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

                    if (sum != event.nuts) {
                        event.copy(nuts = sum, metadata = metadata)
                    } else {
                        event
                    }
                }
            }.respond(call)
        }
    }
}