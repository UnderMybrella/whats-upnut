package dev.brella.blasement.upnut.common

import dev.brella.blasement.upnut.common.UpNutClient.Companion.CATEGORY_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.CREATED_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.DAY_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.FEEDS_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.GAME_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.LIMIT_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.NONE_OF_PROVIDERS_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.NONE_OF_SOURCES_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.ONE_OF_PROVIDERS_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.ONE_OF_SOURCES_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.PHASE_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.PLAYER_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.SEASON_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.SINGLE_PROVIDER_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.SINGLE_SOURCE_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.TEAM_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.TIME_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.TOURNAMENT_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.TYPE_VAR
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.Parameter
import java.util.*

class NutSqlBuilder(val statement: DatabaseClient.GenericExecuteSpec, val mappings: Map<String, String>) : DatabaseClient.GenericExecuteSpec by statement {
    inline fun <reified T : Any> _bind(variable: String, value: T?): NutSqlBuilder =
        NutSqlBuilder(statement.bind(mappings[variable] ?: variable, Parameter.fromOrEmpty(value, T::class.java)), mappings)

    inline fun time(long: Long) = _bind(TIME_VAR, long)
    inline fun limit(limit: Int) = _bind(LIMIT_VAR, limit)
    inline fun provider(uuid: UUID) = _bind(SINGLE_PROVIDER_VAR, uuid)
    inline fun source(uuid: UUID?) = _bind(SINGLE_SOURCE_VAR, uuid)

    inline fun oneOfSources(list: List<UUID>?) = _bind(ONE_OF_SOURCES_VAR, list?.toTypedArray())
    inline fun noneOfSources(list: List<UUID>?) = _bind(NONE_OF_SOURCES_VAR, list?.toTypedArray())
    inline fun oneOfProviders(list: List<UUID>?) = _bind(ONE_OF_PROVIDERS_VAR, list?.toTypedArray())
    inline fun noneOfProviders(list: List<UUID>?) = _bind(NONE_OF_PROVIDERS_VAR, list?.toTypedArray())

    inline fun team(id: UUID) = _bind(TEAM_VAR, id)
    inline fun game(id: UUID) = _bind(GAME_VAR, id)
    inline fun player(id: UUID) = _bind(PLAYER_VAR, id)

    inline fun feedIDs(ids: Iterable<UUID>) = _bind(FEEDS_VAR, ids.toList().toTypedArray())

    inline fun created(time: Long?) = _bind(CREATED_VAR, time)
    inline fun season(season: Int?) = _bind(SEASON_VAR, season)
    inline fun tournament(tournament: Int?) = _bind(TOURNAMENT_VAR, tournament)
    inline fun type(type: Int?) = _bind(TYPE_VAR, type)
    inline fun day(day: Int?) = _bind(DAY_VAR, day)
    inline fun phase(phase: Int?) = _bind(PHASE_VAR, phase)
    inline fun category(category: Int?) = _bind(CATEGORY_VAR, category)

    fun metadata() =
        this
            .created(null)
            .season(null)
            .tournament(null)
            .type(null)
            .day(null)
            .phase(null)
            .category(null)
}