package dev.brella.blasement.upnut

import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Option
import kotlinx.coroutines.reactive.awaitSingleOrNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import org.intellij.lang.annotations.Language
import org.springframework.r2dbc.core.DatabaseClient
import java.time.Duration
import java.util.*

class UpNutClient(config: JsonObject) {
    companion object {
        const val TIME_VAR = ":time"
        const val LIMIT_VAR = ":limit"
        const val SOURCE_VAR = ":source"
        const val TEAM_VAR = ":team"
        const val GAME_VAR = ":game"
        const val PLAYER_VAR = ":player"

        const val CREATED_VAR = ":created"
        const val SEASON_VAR = ":season"
        const val TOURNAMENT_VAR = ":tournament"
        const val TYPE_VAR = ":type"
        const val DAY_VAR = ":day"
        const val PHASE_VAR = ":phase"
        const val CATEGORY_VAR = ":category"

        const val TIME_FILTER = "time <= $TIME_VAR"
        const val TEAM_FILTER = "feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $TEAM_VAR)"
        const val GAME_FILTER = "feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $GAME_VAR)"
        const val PLAYER_FILTER = "feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $PLAYER_VAR)"
        const val SOURCES_FILTER = "source IN ($SOURCE_VAR)"
        const val NOT_SOURCES_FILTER = "source NOT IN ($SOURCE_VAR)"

        const val CREATED_FILTER = "($CREATED_VAR IS NULL OR category = $CREATED_VAR)"
        const val CATEGORY_FILTER = "($CATEGORY_VAR IS NULL OR category = $CATEGORY_VAR)"
        const val SEASON_FILTER = "($SEASON_VAR IS NULL OR season = $SEASON_VAR)"
        const val TOURNAMENT_FILTER = "($TOURNAMENT_VAR IS NULL OR season = $TOURNAMENT_VAR)"
        const val TYPE_FILTER = "($TYPE_VAR IS NULL OR season = $TYPE_VAR)"
        const val DAY_FILTER = "($DAY_VAR IS NULL OR day = $DAY_VAR)"
        const val PHASE_FILTER = "($PHASE_VAR IS NULL OR phase = $PHASE_VAR)"

        const val METADATA_FILTER = "feed_id IN (SELECT feed_id FROM event_metadata WHERE $CREATED_FILTER AND $SEASON_FILTER AND $TOURNAMENT_FILTER AND $TYPE_FILTER AND $DAY_FILTER AND $PHASE_FILTER AND $CATEGORY_FILTER)"

        val BASE_HOT = hotPSQLWithTime(METADATA_FILTER)
        val BASE_HOT_SOURCES = hotPSQLWithTime(SOURCES_FILTER, METADATA_FILTER)
        val BASE_HOT_NOT_SOURCES = hotPSQLWithTime(NOT_SOURCES_FILTER, METADATA_FILTER)

        val TEAM_HOT = hotPSQLWithTimeAndTeam(METADATA_FILTER)
        val TEAM_HOT_SOURCES = hotPSQLWithTimeAndTeam(SOURCES_FILTER, METADATA_FILTER)
        val TEAM_HOT_NOT_SOURCES = hotPSQLWithTimeAndTeam(NOT_SOURCES_FILTER, METADATA_FILTER)

        val GAME_HOT = hotPSQLWithTimeAndGame()
        val GAME_HOT_SOURCES = hotPSQLWithTimeAndGame(SOURCES_FILTER, METADATA_FILTER)
        val GAME_HOT_NOT_SOURCES = hotPSQLWithTimeAndGame(NOT_SOURCES_FILTER, METADATA_FILTER)

        val PLAYER_HOT = hotPSQLWithTimeAndPlayer(METADATA_FILTER)
        val PLAYER_HOT_SOURCES = hotPSQLWithTimeAndPlayer(SOURCES_FILTER, METADATA_FILTER)
        val PLAYER_HOT_NOT_SOURCES = hotPSQLWithTimeAndPlayer(NOT_SOURCES_FILTER, METADATA_FILTER)

//        @Language("PostgreSQL")
        inline fun hotPSQL(vararg where: String) =
            NutSqlStatement("SELECT list.feed_id, list.sum, list.time FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts ${if (where.isEmpty()) "" else where.joinToString(prefix = "WHERE ", separator = " AND ")} GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $LIMIT_VAR) as list ORDER BY list.sum DESC")

        inline fun hotPSQLWithTime(vararg and: String) =
            hotPSQL(TIME_FILTER, *and)

        inline fun hotPSQLWithTimeAndTeam(vararg and: String) =
            hotPSQL(TIME_FILTER, TEAM_FILTER, *and)

        inline fun hotPSQLWithTimeAndGame(vararg and: String) =
            hotPSQL(TIME_FILTER, GAME_FILTER, *and)

        inline fun hotPSQLWithTimeAndPlayer(vararg and: String) =
            hotPSQL(TIME_FILTER, PLAYER_FILTER, *and)
    }

    val connectionFactory: ConnectionFactory = ConnectionFactories.get(
        config.run {
            val builder = getStringOrNull("url")?.let { ConnectionFactoryOptions.parse(it).mutate() }
                          ?: ConnectionFactoryOptions.builder().option(ConnectionFactoryOptions.DRIVER, "pool").option(ConnectionFactoryOptions.PROTOCOL, "postgresql")

            getStringOrNull("connectTimeout")
                ?.let { builder.option(ConnectionFactoryOptions.CONNECT_TIMEOUT, Duration.parse(it)) }

            getStringOrNull("database")
                ?.let { builder.option(ConnectionFactoryOptions.DATABASE, it) }

            getStringOrNull("driver")
                ?.let { builder.option(ConnectionFactoryOptions.DRIVER, it) }

            getStringOrNull("host")
                ?.let { builder.option(ConnectionFactoryOptions.HOST, it) }

            getStringOrNull("password")
                ?.let { builder.option(ConnectionFactoryOptions.PASSWORD, it) }

            getStringOrNull("port")?.toIntOrNull()
                ?.let { builder.option(ConnectionFactoryOptions.PORT, it) }

            getStringOrNull("protocol")
                ?.let { builder.option(ConnectionFactoryOptions.PROTOCOL, it) }

            getStringOrNull("ssl")?.toBoolean()
                ?.let { builder.option(ConnectionFactoryOptions.SSL, it) }

            getStringOrNull("user")
                ?.let { builder.option(ConnectionFactoryOptions.USER, it) }

            getJsonObjectOrNull("options")?.forEach { (key, value) ->
                val value = value as? JsonPrimitive ?: return@forEach
                value.longOrNull?.let { builder.option(Option.valueOf(key), it) }
                ?: value.doubleOrNull?.let { builder.option(Option.valueOf(key), it) }
                ?: value.booleanOrNull?.let { builder.option(Option.valueOf(key), it) }
                ?: value.contentOrNull?.let { builder.option(Option.valueOf(key), it) }
            }

            builder.build()
        }
    )

    val client = DatabaseClient.create(connectionFactory)

    suspend fun globalHot(time: Long, limit: Int) =
        BASE_HOT(client)
            .time(time)
            .limit(limit)
            .metadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun globalHotInSources(time: Long, limit: Int, sources: List<UUID>) =
        BASE_HOT_SOURCES(client)
            .time(time)
            .limit(limit)
            .sources(sources)
            .metadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun globalHotNotInSources(time: Long, limit: Int, sources: List<UUID>) =
        BASE_HOT_NOT_SOURCES(client)
            .time(time)
            .limit(limit)
            .sources(sources)
            .metadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()


    suspend fun teamHot(teamID: UUID, time: Long, limit: Int) =
        TEAM_HOT(client)
            .team(teamID)
            .time(time)
            .limit(limit)
            .metadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun teamHotInSources(teamID: UUID, time: Long, limit: Int, sources: List<UUID>) =
        TEAM_HOT_SOURCES(client)
            .team(teamID)
            .time(time)
            .limit(limit)
            .sources(sources)
            .metadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun teamHotNotInSources(teamID: UUID, time: Long, limit: Int, sources: List<UUID>) =
        TEAM_HOT_NOT_SOURCES(client)
            .team(teamID)
            .time(time)
            .limit(limit)
            .sources(sources)
            .metadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()
}