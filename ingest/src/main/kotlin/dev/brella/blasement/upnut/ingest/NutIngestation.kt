package dev.brella.blasement.upnut.ingest

import dev.brella.blasement.upnut.common.UpNutClient
import dev.brella.blasement.upnut.common.UpNutEvent
import dev.brella.blasement.upnut.common.getBooleanOrNull
import dev.brella.blasement.upnut.common.getIntOrNull
import dev.brella.blasement.upnut.common.getJsonObjectOrNull
import dev.brella.blasement.upnut.common.getLongOrNull
import dev.brella.blasement.upnut.common.loopEvery
import dev.brella.kornea.blaseball.BlaseballApi
import dev.brella.kornea.blaseball.endpoints.BlaseballDatabaseService
import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.kornea.errors.common.doOnFailure
import dev.brella.kornea.errors.common.doOnThrown
import dev.brella.kornea.errors.common.getOrNull
import dev.brella.ktornea.common.getAsResult
import dev.brella.ktornea.common.installGranularHttp
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.compression.*
import io.ktor.client.features.json.*
import io.ktor.client.features.json.serializer.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.intOrNull
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.core.await
import java.io.File
import java.time.Clock
import java.time.Instant
import java.util.*
import kotlin.coroutines.CoroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

class NutIngestation(val config: JsonObject, val nuts: UpNutClient) : CoroutineScope {
    companion object {
        val THE_GAME_BAND = UUID.fromString("7fcb63bc-11f2-40b9-b465-f1d458692a63")

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
        nuts.client.sql("CREATE TABLE IF NOT EXISTS upnuts (id BIGSERIAL PRIMARY KEY, nuts INT NOT NULL DEFAULT 1, feed_id uuid NOT NULL, source uuid, provider uuid NOT NULL, time BIGINT NOT NULL)")
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

        nuts.client.sql("CREATE TABLE IF NOT EXISTS library (id UUID NOT NULL PRIMARY KEY, book_title VARCHAR(128) NOT NULL, chapter_title VARCHAR(128) NOT NULL, redacted BOOLEAN NOT NULL DEFAULT TRUE);")
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
                               .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 } ?: break

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
                .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 }
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
                                           .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 } ?: break

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
                            .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 }
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
                                       .getOrNull()?.takeWhile { event -> event.nuts.intOrNull ?: 0 > 0 } ?: break

                        list.forEach { event -> processNuts(now, event) }

                        if (list.size < limit) break
                        start += list.size

                        delay(delay)
                    }
                }
            }.joinAll()
        }
    }

    val processNuts: suspend (time: Long, event: UpNutEvent) -> Unit = run {
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

//                                if (nuts.client.sql("SELECT feed_id FROM event_metadata WHERE feed_id = $1")
//                                        .bind("$1", event.id)
//                                        .fetch()
//                                        .awaitOneOrNull()
//                                    == null
//                                ) {
//                                    nuts.client.sql("INSERT INTO event_metadata (feed_id, created, season, tournament, type, day, phase, category) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8 )")
//                                        .bind("$1", event.id)
//                                        .bind("$2", event.created.utc.unixMillisLong)
//                                        .bind("$3", event.season)
//                                        .bind("$4", event.tournament)
//                                        .bind("$5", event.type)
//                                        .bind("$6", event.day)
//                                        .bind("$7", event.phase)
//                                        .bind("$8", event.category)
//                                        .await()
//                                }

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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        }
                    }
                } else {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        }
                    }
                }
            } else {
                if (gameTags) {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        }
                    }
                } else {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
                            }
                        } else {
                            return@run nuts@{ time, event ->
                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()

                                logger.info("${event.id} +$difference")
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()
                            }
                        }
                    }
                } else {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()
                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()
                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()
                            }
                        }
                    }
                }
            } else {
                if (gameTags) {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        }
                    }
                } else {
                    if (teamTags) {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        } else {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        }
                    } else {
                        if (eventMetadata) {
                            return@run nuts@{ time, event ->
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

                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        } else {
                            return@run nuts@{ time, event ->
                                val nutsAtTimeOfRecording = nuts.client.sql("SELECT SUM(nuts) AS sum FROM upnuts WHERE feed_id = $1 AND time <= $2")
                                                                .bind("$1", event.id)
                                                                .bind("$2", time)
                                                                .fetch()
                                                                .first()
                                                                .awaitFirstOrNull()
                                                                ?.values
                                                                ?.let { it.firstOrNull() as? Number }
                                                                ?.toInt() ?: 0

                                val difference = (event.nuts.intOrNull ?: 0) - nutsAtTimeOfRecording
                                if (difference <= 0) return@nuts

                                nuts.client.sql("INSERT INTO upnuts (nuts, feed_id, provider, time) VALUES ( $1, $2, $3, $4 )")
                                    .bind("$1", difference)
                                    .bind("$2", event.id)
                                    .bind("$3", THE_GAME_BAND)
                                    .bind("$4", time)
                                    .await()


                            }
                        }
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