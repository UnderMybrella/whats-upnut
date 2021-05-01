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

        @Language("PostgreSQL")
        inline fun hotPSQL(where: String) =
            NutSqlStatement("SELECT list.feed_id, list.sum, list.time FROM (SELECT feed_id, SUM(nuts) AS sum, MAX(time) as time FROM upnuts WHERE $where GROUP BY feed_id ORDER BY time DESC, sum DESC LIMIT $LIMIT_VAR) as list ORDER BY list.sum DESC")

        inline fun hotPSQLWithTime(and: String? = null) =
            hotPSQL(if (and == null) TIME_FILTER else "$TIME_FILTER AND $and")

        inline fun hotPSQLWithTimeAndTeam(and: String? = null) =
            hotPSQL(if (and == null) "$TIME_FILTER AND $TEAM_FILTER" else "$TIME_FILTER AND $and AND $TEAM_FILTER")

        inline fun hotPSQLWithTimeAndGame(and: String? = null) =
            hotPSQL(if (and == null) "$TIME_FILTER AND $GAME_FILTER" else "$TIME_FILTER AND $and AND $GAME_FILTER")

        inline fun hotPSQLWithTimeAndPlayer(and: String? = null) =
            hotPSQL(if (and == null) "$TIME_FILTER AND $PLAYER_FILTER" else "$TIME_FILTER AND $and AND $PLAYER_FILTER")
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
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun globalHotInSources(time: Long, limit: Int, sources: List<UUID>) =
        BASE_HOT_SOURCES(client)
            .time(time)
            .limit(limit)
            .sources(sources)
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun globalHotNotInSources(time: Long, limit: Int, sources: List<UUID>) =
        BASE_HOT_NOT_SOURCES(client)
            .time(time)
            .limit(limit)
            .sources(sources)
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()


    suspend fun teamHot(teamID: UUID, time: Long, limit: Int) =
        TEAM_HOT(client)
            .team(teamID)
            .time(time)
            .limit(limit)
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
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()
}