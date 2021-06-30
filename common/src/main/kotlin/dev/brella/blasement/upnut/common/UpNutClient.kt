package dev.brella.blasement.upnut.common

import io.netty.channel.epoll.Epoll
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Option
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingleOrNull
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.longOrNull
import org.springframework.r2dbc.core.DatabaseClient
import java.time.Duration
import java.util.*

class UpNutClient(config: JsonObject) {
    companion object {
        const val TIME_VAR = ":time:"
        const val LIMIT_VAR = ":limit:"
        const val OFFSET_VAR = ":offset:"
        const val SINGLE_PROVIDER_VAR = ":provider:"
        const val SINGLE_SOURCE_VAR = ":source:"
        const val ONE_OF_SOURCES_VAR = ":one_of_sources:"
        const val NONE_OF_SOURCES_VAR = ":none_of_sources:"
        const val ONE_OF_PROVIDERS_VAR = ":one_of_providers:"
        const val NONE_OF_PROVIDERS_VAR = ":none_of_providers:"
        const val TEAM_VAR = ":team:"
        const val GAME_VAR = ":game:"
        const val PLAYER_VAR = ":player:"

        const val FEED_ID_VAR = ":feed_id:"

        const val CREATED_VAR = ":created:"
        const val SEASON_VAR = ":season:"
        const val TOURNAMENT_VAR = ":tournament:"
        const val TYPE_VAR = ":type:"
        const val DAY_VAR = ":day:"
        const val PHASE_VAR = ":phase:"
        const val CATEGORY_VAR = ":category:"

        const val FEEDS_VAR = ":feed_ids:"

        const val UUID_VAR = ":uuid:"

        const val TIME_FILTER = "($TIME_VAR IS NULL OR time <= $TIME_VAR)"
        const val LIBRARY_TIME_FILTER =
            "($TIME_VAR IS NULL OR (NOT EXISTS(SELECT 1 FROM feed_sources WHERE feed_sources.feed_id = upnuts.feed_id AND feed_sources.source_type = 4) OR EXISTS(SELECT 1 FROM library WHERE id = (SELECT source_id FROM feed_sources WHERE feed_sources.feed_id = upnuts.feed_id AND source_type = 4 LIMIT 1) AND unredacted_since IS NOT NULL AND unredacted_since < $TIME_VAR)))"
        const val TEAM_FILTER = "feed_id IN (SELECT feed_id FROM team_nuts WHERE team_id = $TEAM_VAR)"
        const val GAME_FILTER = "feed_id IN (SELECT feed_id FROM game_nuts WHERE game_id = $GAME_VAR)"
        const val PLAYER_FILTER = "feed_id IN (SELECT feed_id FROM player_nuts WHERE player_id = $PLAYER_VAR)"
        const val SINGLE_SOURCE_FILTER = "source IS NOT DISTINCT FROM $SINGLE_SOURCE_VAR"
        const val SINGLE_PROVIDER_FILTER = "provider = $SINGLE_PROVIDER_VAR"

        const val ONE_OF_SOURCES_FILTER = "($ONE_OF_SOURCES_VAR IS NULL OR source = ANY($ONE_OF_SOURCES_VAR))"
        const val NONE_OF_SOURCES_FILTER = "($NONE_OF_SOURCES_VAR IS NULL OR source != ALL($NONE_OF_SOURCES_VAR))"

        const val ONE_OF_PROVIDERS_FILTER = "($ONE_OF_PROVIDERS_VAR IS NULL OR provider = ANY($ONE_OF_PROVIDERS_VAR))"
        const val NONE_OF_PROVIDERS_FILTER = "($NONE_OF_PROVIDERS_VAR IS NULL OR provider != ALL($NONE_OF_PROVIDERS_VAR))"


        const val IN_FEED_ID_FILTER = "feed_id = ANY($FEEDS_VAR)"
        const val NOT_IN_FEED_ID_FILTER = "feed_id != ALL($FEEDS_VAR)"

        const val CREATED_TIME_FILTER = "created <= $CREATED_VAR "

        const val CREATED_FILTER = "($CREATED_VAR IS NULL OR created = $CREATED_VAR)"
        const val CATEGORY_FILTER = "($CATEGORY_VAR IS NULL OR category = $CATEGORY_VAR)"
        const val SEASON_FILTER = "($SEASON_VAR IS NULL OR season = $SEASON_VAR)"
        const val TOURNAMENT_FILTER = "($TOURNAMENT_VAR IS NULL OR tournament = $TOURNAMENT_VAR)"
        const val TYPE_FILTER = "($TYPE_VAR IS NULL OR type = $TYPE_VAR)"
        const val DAY_FILTER = "($DAY_VAR IS NULL OR day = $DAY_VAR)"
        const val PHASE_FILTER = "($PHASE_VAR IS NULL OR phase = $PHASE_VAR)"

        const val METADATA_FILTER = "feed_id IN (SELECT feed_id FROM event_metadata WHERE $CREATED_FILTER AND $SEASON_FILTER AND $TOURNAMENT_FILTER AND $TYPE_FILTER AND $DAY_FILTER AND $PHASE_FILTER AND $CATEGORY_FILTER)"
        const val METADATA_NO_CREATED_FILTER = "feed_id IN (SELECT feed_id FROM event_metadata WHERE $SEASON_FILTER AND $TOURNAMENT_FILTER AND $TYPE_FILTER AND $DAY_FILTER AND $PHASE_FILTER AND $CATEGORY_FILTER)"

        val GLOBAL_HOT = hotPSQLWithTime(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)
        val GLOBAL_HOT_SCALES = hotScalesPSQLWithTime(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)

        val GLOBAL_TOP = topPSQLWithTime(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)
        val GLOBAL_TOP_SCALES = topScalesPSQLWithTime(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)

        val GLOBAL_EVENT_IDS = getEventIDs(CREATED_TIME_FILTER, NOT_IN_FEED_ID_FILTER, METADATA_NO_CREATED_FILTER)

        val TEAM_HOT = hotPSQLWithTimeAndTeam(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)
        val TEAM_HOT_SCALES = hotScalesPSQLWithTimeAndTeam(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)

        val TEAM_TOP = topPSQLWithTimeAndTeam(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)
        val TEAM_TOP_SCALES = topScalesPSQLWithTimeAndTeam(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)

        val TEAM_EVENT_IDS = getEventIDs(CREATED_TIME_FILTER, TEAM_FILTER, METADATA_NO_CREATED_FILTER)

        val GAME_HOT = hotPSQLWithTimeAndGame(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER)
        val GAME_HOT_SCALES = hotScalesPSQLWithTimeAndGame(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER)

        val GAME_TOP = topPSQLWithTimeAndGame(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER)
        val GAME_TOP_SCALES = topScalesPSQLWithTimeAndGame(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER)

        val GAME_EVENT_IDS = getEventIDs(CREATED_TIME_FILTER, GAME_FILTER, METADATA_NO_CREATED_FILTER)

        val PLAYER_HOT = hotPSQLWithTimeAndPlayer(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)
        val PLAYER_HOT_SCALES = hotScalesPSQLWithTimeAndPlayer(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)

        val PLAYER_TOP = topPSQLWithTimeAndPlayer(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)
        val PLAYER_TOP_SCALES = topScalesPSQLWithTimeAndPlayer(NONE_OF_PROVIDERS_FILTER, NONE_OF_SOURCES_FILTER, ONE_OF_PROVIDERS_FILTER, ONE_OF_SOURCES_FILTER, METADATA_FILTER)
        val PLAYER_EVENT_IDS = getEventIDs(CREATED_TIME_FILTER, PLAYER_FILTER, METADATA_NO_CREATED_FILTER)

        val EVENTUALLY_EVENTS =
            NutSqlStatement("SELECT feed_id, SUM(nuts) as nuts, SUM(scales) as scales FROM upnuts WHERE $IN_FEED_ID_FILTER AND $TIME_FILTER AND $NONE_OF_PROVIDERS_FILTER AND $NONE_OF_SOURCES_FILTER AND $ONE_OF_PROVIDERS_FILTER AND $ONE_OF_SOURCES_FILTER GROUP BY feed_id")

        val EVENTUALLY_EVENTS_NUTS_LIST =
            NutSqlStatement("SELECT feed_id, nuts, scales, provider, source, time FROM upnuts WHERE $IN_FEED_ID_FILTER AND $TIME_FILTER AND $NONE_OF_PROVIDERS_FILTER AND $NONE_OF_SOURCES_FILTER AND $ONE_OF_PROVIDERS_FILTER AND $ONE_OF_SOURCES_FILTER")


        val EVENTUALLY_EVENTS_WITH_SOURCES =
            NutSqlStatement("SELECT feed_id, SUM(nuts) as nuts, SUM(scales) as scales FROM upnuts WHERE $IN_FEED_ID_FILTER AND $TIME_FILTER AND $ONE_OF_SOURCES_FILTER GROUP BY feed_id")
        val EVENTUALLY_EVENTS_WITHOUT_SOURCES =
            NutSqlStatement("SELECT feed_id, SUM(nuts) as nuts, SUM(scales) as scales FROM upnuts WHERE $IN_FEED_ID_FILTER AND $TIME_FILTER AND $NONE_OF_SOURCES_FILTER GROUP BY feed_id")

        val IS_UPNUT_FOR_SOURCE =
            NutSqlStatement("SELECT feed_id, SUM(nuts) as nuts, SUM(scales) as scales FROM upnuts WHERE $IN_FEED_ID_FILTER AND $TIME_FILTER AND $SINGLE_PROVIDER_FILTER AND $SINGLE_SOURCE_FILTER GROUP BY feed_id")

        //        @Language("PostgreSQL")
        inline fun getEventIDs(vararg where: String) =
            NutSqlStatement("SELECT feed_id FROM event_metadata ${if (where.isEmpty()) "" else where.joinToString(prefix = "WHERE ", separator = " AND ")} ORDER BY created DESC LIMIT $LIMIT_VAR OFFSET $OFFSET_VAR")

        //        @Language("PostgreSQL")
        inline fun hotPSQL(vararg where: String) =
            NutSqlStatement(
                "SELECT list.feed_id, list.nuts, list.scales, list.time, floor(log(10, list.nuts) + (meta.created / 1000 / 604800)) as sort FROM (SELECT feed_id, SUM(nuts) AS nuts, SUM(scales) as scales, MAX(time) as time FROM upnuts ${
                    if (where.isEmpty()) "" else where.joinToString(
                        prefix = "WHERE ",
                        separator = " AND "
                    )
                } GROUP BY feed_id) as list JOIN (SELECT feed_id, created FROM event_metadata) as meta ON list.feed_id = meta.feed_id ORDER BY sort DESC, list.nuts DESC, list.scales DESC, list.time DESC LIMIT $LIMIT_VAR OFFSET $OFFSET_VAR"
            )

        inline fun hotPSQLWithTime(vararg and: String) =
            hotPSQL(TIME_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun hotPSQLWithTimeAndTeam(vararg and: String) =
            hotPSQL(TIME_FILTER, TEAM_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun hotPSQLWithTimeAndGame(vararg and: String) =
            hotPSQL(TIME_FILTER, GAME_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun hotPSQLWithTimeAndPlayer(vararg and: String) =
            hotPSQL(TIME_FILTER, PLAYER_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun topPSQL(vararg where: String) =
            NutSqlStatement(
                "SELECT feed_id, SUM(nuts) AS nuts, SUM(scales) as scales FROM upnuts ${
                    if (where.isEmpty()) "" else where.joinToString(
                        prefix = "WHERE ",
                        separator = " AND "
                    )
                } GROUP BY feed_id ORDER BY nuts DESC, scales DESC LIMIT $LIMIT_VAR OFFSET $OFFSET_VAR"
            )

        inline fun topPSQLWithTime(vararg and: String) =
            topPSQL(TIME_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun topPSQLWithTimeAndTeam(vararg and: String) =
            topPSQL(TIME_FILTER, TEAM_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun topPSQLWithTimeAndGame(vararg and: String) =
            topPSQL(TIME_FILTER, GAME_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun topPSQLWithTimeAndPlayer(vararg and: String) =
            topPSQL(TIME_FILTER, PLAYER_FILTER, *and, LIBRARY_TIME_FILTER)

        /** Scales */

        inline fun hotScalesPSQL(vararg where: String) =
            NutSqlStatement {
                appendLine("SELECT list.feed_id, list.nuts, list.scales, list.time, floor(log(10, list.scales) + (meta.created / 1000 / 604800)) as sort")
                appendLine("FROM (")

                appendLine("SELECT feed_id, SUM(nuts) AS nuts, SUM(scales) as scales, MAX(time) as time")
                appendLine("FROM upnuts")
                if (where.isNotEmpty()) where.joinTo(this, prefix = "WHERE ", separator = " AND ", postfix = "\n")
                appendLine("GROUP BY feed_id")
                appendLine("HAVING SUM(scales) < 1000")

                appendLine(") as list")
                appendLine("JOIN (")

                appendLine("SELECT feed_id, created FROM event_metadata")

                appendLine(") as meta ON list.feed_id = meta.feed_id")
                appendLine("ORDER BY sort DESC, list.scales DESC, list.nuts DESC, list.time DESC")
                appendLine("LIMIT $LIMIT_VAR OFFSET $OFFSET_VAR")
            }

        inline fun hotScalesPSQLWithTime(vararg and: String) =
            hotScalesPSQL(TIME_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun hotScalesPSQLWithTimeAndTeam(vararg and: String) =
            hotScalesPSQL(TIME_FILTER, TEAM_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun hotScalesPSQLWithTimeAndGame(vararg and: String) =
            hotScalesPSQL(TIME_FILTER, GAME_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun hotScalesPSQLWithTimeAndPlayer(vararg and: String) =
            hotScalesPSQL(TIME_FILTER, PLAYER_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun topScalesPSQL(vararg where: String) =
            NutSqlStatement {
                appendLine("SELECT feed_id, SUM(nuts) AS nuts, SUM(scales) as scales")
                appendLine("FROM upnuts")
                if (where.isNotEmpty()) where.joinTo(this, prefix = "WHERE ", separator = " AND ", postfix = "\n")
                appendLine("GROUP BY feed_id")
                appendLine("HAVING SUM(scales) < 1000")
                appendLine("ORDER BY scales DESC, nuts DESC")
                appendLine("LIMIT $LIMIT_VAR OFFSET $OFFSET_VAR")
            }

        inline fun topScalesPSQLWithTime(vararg and: String) =
            topScalesPSQL(TIME_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun topScalesPSQLWithTimeAndTeam(vararg and: String) =
            topScalesPSQL(TIME_FILTER, TEAM_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun topScalesPSQLWithTimeAndGame(vararg and: String) =
            topScalesPSQL(TIME_FILTER, GAME_FILTER, *and, LIBRARY_TIME_FILTER)

        inline fun topScalesPSQLWithTimeAndPlayer(vararg and: String) =
            topScalesPSQL(TIME_FILTER, PLAYER_FILTER, *and, LIBRARY_TIME_FILTER)
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

    suspend fun eventually(
        feedIDs: Iterable<UUID>, time: Long?,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null
    ) =
        try {
            EVENTUALLY_EVENTS(client)
                .feedIDs(feedIDs)
                .time(time)
                .noneOfProviders(noneOfProviders)
                .noneOfSources(noneOfSources)
                .oneOfProviders(oneOfProviders)
                .oneOfSources(oneOfSources)
                .map { row -> Pair(row.getValue<UUID>("feed_id"), Pair(row.get<Int?>("nuts"), row.get<Int?>("scales"))) }
                .all()
                .collectList()
                .awaitFirst()
                .toMap()
        } catch (th: Throwable) {
            th.printStackTrace()
            throw th
        }

    suspend fun eventuallyNutsList(
        feedIDs: Iterable<UUID>, time: Long?,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null
    ) =
        EVENTUALLY_EVENTS_NUTS_LIST(client)
            .feedIDs(feedIDs)
            .time(time)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .map { row ->
                row.getValue<UUID>("feed_id") to
                        NutsEpoch(
                            row.get<Int?>("nuts"),
                            row.get<Int?>("scales"),
                            row.getValue<UUID>("provider"),
                            row.get<UUID>("source"),
                            row.getValue<Long>("time")
                        )
            }.all()
            .collectList()
            .awaitSingleOrNull()
            ?.filterNotNull()
            ?.groupBy(Pair<UUID, NutsEpoch>::first, Pair<UUID, NutsEpoch>::second)

    suspend fun eventuallyWithSources(feedIDs: Iterable<UUID>, time: Long?, sources: List<UUID>) =
        EVENTUALLY_EVENTS_WITH_SOURCES(client)
            .feedIDs(feedIDs)
            .time(time)
            .oneOfSources(sources)
            .map { row -> Pair(row.getValue<UUID>("feed_id"), Pair(row.get<Int?>("nuts"), row.get<Int?>("scales"))) }
            .all()
            .collectList()
            .awaitSingleOrNull()
            ?.toMap()

    suspend fun eventuallyWithoutSources(feedIDs: Iterable<UUID>, time: Long?, sources: List<UUID>) =
        EVENTUALLY_EVENTS_WITHOUT_SOURCES(client)
            .feedIDs(feedIDs)
            .time(time)
            .oneOfSources(sources)
            .map { row -> Pair(row.getValue<UUID>("feed_id"), Pair(row.get<Int?>("nuts"), row.get<Int?>("scales"))) }
            .all()
            .collectList()
            .awaitSingleOrNull()
            ?.toMap()

    suspend fun isUpnutted(feedIDs: Iterable<UUID>, time: Long?, provider: UUID, source: UUID?) =
        IS_UPNUT_FOR_SOURCE(client)
            .feedIDs(feedIDs)
            .time(time)
            .provider(provider)
            .source(source)
            .map { row -> Pair(row.getValue<UUID>("feed_id"), Pair(row.get<Int?>("nuts")?.let { it > 0 }, row.get<Int?>("scales")?.let { it > 0 })) }
            .all()
            .collectList()
            .awaitSingleOrNull()
            ?.filterNotNull()
            ?.toMap()

    suspend fun globalEventsBefore(time: Long?, limit: Int, offset: Int, feedIDs: Iterable<UUID>, addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec) =
        GLOBAL_EVENT_IDS(client)
            .metadata()
            .created(time)
            .limit(limit)
            .offset(offset)
            .feedIDs(feedIDs)
            .addMetadata()
            .map { row -> row.getValue<UUID>("feed_id") }
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun globalHot(
        time: Long?,
        limit: Int,
        offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        GLOBAL_HOT(client)
            .metadata()
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitFirstOrNull()

    suspend fun globalHotScales(
        time: Long?,
        limit: Int,
        offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        GLOBAL_HOT_SCALES(client)
            .metadata()
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitFirstOrNull()

    suspend fun globalTop(
        time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        GLOBAL_TOP(client)
            .metadata()
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun globalTopScales(
        time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        GLOBAL_TOP_SCALES(client)
            .metadata()
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun teamEventsBefore(teamID: UUID, time: Long?, limit: Int, offset: Int, feedIDs: Iterable<UUID>, addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec) =
        TEAM_EVENT_IDS(client)
            .metadata()
            .team(teamID)
            .created(time)
            .limit(limit)
            .offset(offset)
            .feedIDs(feedIDs)
            .addMetadata()
            .fetch()
            .all()
            .mapNotNull { map -> map["feed_id"] as? UUID }
            .collectList()
            .awaitSingleOrNull()

    suspend fun teamHot(
        teamID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        TEAM_HOT(client)
            .metadata()
            .team(teamID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun teamTop(
        teamID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        TEAM_TOP(client)
            .metadata()
            .team(teamID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun teamHotScales(
        teamID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        TEAM_HOT_SCALES(client)
            .metadata()
            .team(teamID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun teamTopScales(
        teamID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        TEAM_TOP_SCALES(client)
            .metadata()
            .team(teamID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()


    suspend fun gameEventsBefore(gameID: UUID, time: Long?, limit: Int, offset: Int, feedIDs: Iterable<UUID>, addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec) =
        GAME_EVENT_IDS(client)
            .metadata()
            .game(gameID)
            .created(time)
            .limit(limit)
            .offset(offset)
            .feedIDs(feedIDs)
            .addMetadata()
            .fetch()
            .all()
            .mapNotNull { map -> map["feed_id"] as? UUID }
            .collectList()
            .awaitSingleOrNull()

    suspend fun gameHot(
        gameID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        GAME_HOT(client)
            .metadata()
            .game(gameID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun gameTop(
        gameID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        GAME_TOP(client)
            .metadata()
            .game(gameID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun gameHotScales(
        gameID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        GAME_HOT_SCALES(client)
            .metadata()
            .game(gameID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun gameTopScales(
        gameID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        GAME_TOP_SCALES(client)
            .metadata()
            .game(gameID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun playerEventsBefore(playerID: UUID, time: Long?, limit: Int, offset: Int, feedIDs: Iterable<UUID>, addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec) =
        PLAYER_EVENT_IDS(client)
            .metadata()
            .player(playerID)
            .created(time)
            .limit(limit)
            .offset(offset)
            .feedIDs(feedIDs)
            .addMetadata()
            .fetch()
            .all()
            .mapNotNull { map -> map["feed_id"] as? UUID }
            .collectList()
            .awaitSingleOrNull()

    suspend fun playerHot(
        playerID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        PLAYER_HOT(client)
            .metadata()
            .player(playerID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun playerTop(
        playerID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        PLAYER_TOP(client)
            .metadata()
            .player(playerID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun playerHotScales(
        playerID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        PLAYER_HOT_SCALES(client)
            .metadata()
            .player(playerID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()

    suspend fun playerTopScales(
        playerID: UUID, time: Long?, limit: Int, offset: Int,
        noneOfProviders: List<UUID>? = null,
        noneOfSources: List<UUID>? = null,
        oneOfProviders: List<UUID>? = null,
        oneOfSources: List<UUID>? = null,
        addMetadata: NutSqlBuilder.() -> DatabaseClient.GenericExecuteSpec = { this }
    ) =
        PLAYER_TOP_SCALES(client)
            .metadata()
            .player(playerID)
            .time(time)
            .limit(limit)
            .offset(offset)
            .noneOfProviders(noneOfProviders)
            .noneOfSources(noneOfSources)
            .oneOfProviders(oneOfProviders)
            .oneOfSources(oneOfSources)
            .addMetadata()
            .fetch()
            .all()
            .collectList()
            .awaitSingleOrNull()
}