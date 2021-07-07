package dev.brella.blasement.upnut.common

import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.kornea.errors.common.map
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Option
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.*
import org.slf4j.Logger
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.await
import org.springframework.r2dbc.core.awaitOneOrNull
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class Eventuallie(config: JsonObject) {
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

    suspend inline fun feedIDsPresent(feedIDs: List<UpNutEvent>) =
        feedIDsPresent(Array(feedIDs.size) { feedIDs[it].id })

    suspend inline fun feedIDsPresent(feedIDs: Array<UUID>) =
        client.sql("SELECT doc_id FROM documents WHERE doc_id = ANY($1)")
            .bind("$1", feedIDs)
            .map { row -> row.getValue<UUID>("doc_id") }
            .all()
            .collectList()
            .awaitFirstOrNull()
        ?: emptyList()

    suspend inline fun getFeedEvents(feedIDs: List<UUID>) = getFeedEvents(feedIDs.toTypedArray())
    suspend inline fun getFeedEvents(feedIDs: Array<UUID>): Map<UUID, UpNutEvent> =
        client.sql("SELECT doc_id, object FROM documents WHERE doc_id = ANY($1)")
            .bind("$1", feedIDs)
            .map { row -> row.getValue<UUID>("doc_id") to row.getValue<String>("object") }
            .all()
            .collectMap(Pair<UUID, String>::first) { (_, second) -> Json.decodeFromString<EventuallieEvent>(second).toUpNutEvent() }
            .awaitFirstOrNull() ?: emptyMap()

    suspend fun mergeFeedWithNuts(
        nuts: Map<UUID, Pair<Int, Int>>,
        upnut: UpNutClient,
        logger: Logger,
        time: Long?,
        limit: Int,
        offset: Int,
        season: Int?,
        tournament: Int?,
        type: Int?,
        day: Int?,
        phase: Int?,
        category: Int?,
        provider: UUID? = null,
        source: UUID? = null
    ): List<UpNutEvent> {
        val missingAmount = limit - nuts.size
        val remainingList = if (missingAmount > 0) upnut.globalEventsBefore(time, missingAmount, offset + nuts.size, nuts.keys) {
            season(season)
                .tournament(tournament)
                .type(type)
                .day(day)
                .phase(phase)
                .category(category)
        } ?: emptyList() else emptyList()

        val feedIDs = (nuts.keys.toList() + remainingList).filterNotNull()
        val map = getFeedEvents(feedIDs)
        val hrefs = upnut.sourcesForFeed(feedIDs)

        val upnuts =
            provider?.let { upnut.isUpnutted(feedIDs, time, it, source) } ?: emptyMap()

//        nuts.entries.mapNotNullTo(list) { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
//        remainingList.mapNotNullTo(list) { uuid -> map[uuid]?.copy(nuts = JsonPrimitive(0)) }

        return feedIDs.mapNotNull { feedID ->
            val nutPair = nuts[feedID]
            val event = (map[feedID] ?: run {
                logger.warn("Eventually is missing {}, marking for collection", feedID)
                upnut.client.sql("INSERT INTO metadata_collection (feed_id) VALUES ($1) ON CONFLICT DO NOTHING")
                    .bind("$1", feedID)
                    .await()

                return@mapNotNull null
            }).copy(nuts = JsonPrimitive(nutPair?.first ?: 0))

            val upnutted = upnuts[event.id]

            event withMetadata {
                if (upnutted?.first == true || upnutted?.second == true) put("upnut", true)
            }

            hrefs[feedID]?.let { sources ->
                if (sources.isEmpty()) return@let

                event withMetadata {
                    sources.sortedByDescending(BlaseballSource::sourceType)
                        .mapNotNull { toHref(it, upnut) }
                        .let { list ->
                            if (list.isEmpty()) return@let
                            putJsonArray("_upnuts_hrefs") {
                                list.distinct().forEach { add(it) }
                            }
                        }
                }
            }

            event.apply {
                if (scales == JsonNull) {
                    if (nutPair != null && nutPair.second > 0) {
                        scales = JsonPrimitive(nutPair.second)
                    }
                } else {
                    scales = JsonPrimitive(nutPair?.second ?: 0)
                }
            }
        }
    }

    suspend fun mergeHrefsForEvent(upnut: UpNutClient, event: UpNutEvent): UpNutEvent =
        event withMetadata {
            upnut.sourcesForFeed(event.id)
                .takeIf { it.isNotEmpty() }
                ?.sortedByDescending(BlaseballSource::sourceType)
                ?.mapNotNull { toHref(it, upnut) }
                ?.let { list ->
                    if (list.isEmpty()) return@let
                    putJsonArray("_upnuts_hrefs") {
                        list.distinct().forEach { add(it) }
                    }
                }
        }

    @PublishedApi
    internal val library: MutableMap<UUID, Pair<Int, Int>> = ConcurrentHashMap()

    suspend inline fun toHref(source: BlaseballSource, upnuts: UpNutClient): String? {
        return when (source.sourceType) {
            BlaseballSource.GLOBAL_FEED -> "https://www.blaseball.com/global"
            BlaseballSource.PLAYER_FEED -> "https://www.blaseball.com/player/${source.sourceID}"
            BlaseballSource.TEAM_FEED -> "https://www.blaseball.com/team/${source.sourceID}"
            BlaseballSource.STORY_CHAPTER -> {
                if ((source.sourceID ?: return null) !in library) {
                    library[source.sourceID] =
                        upnuts.client.sql("SELECT index_in_book, book_index FROM library WHERE id = $1")
                            .bind("$1", source.sourceID)
                            .map { row -> Pair(row.getValue<Int>("book_index"), row.getValue<Int>("index_in_book") + 1) }
                            .awaitOneOrNull() ?: return null
                }

                library[source.sourceID]?.let { "https://www.blaseball.com/library/${it.first}/${it.second}" }
            }

            else -> null
        }
    }

}