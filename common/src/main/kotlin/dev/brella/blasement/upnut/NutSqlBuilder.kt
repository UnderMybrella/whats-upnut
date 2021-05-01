package dev.brella.blasement.upnut

import dev.brella.blasement.upnut.UpNutClient.Companion.CATEGORY_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.CREATED_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.DAY_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.PHASE_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.SEASON_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.TOURNAMENT_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.TYPE_VAR
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.Parameter
import java.util.*

class NutSqlBuilder(val statement: DatabaseClient.GenericExecuteSpec, val mappings: Map<String, String>) : DatabaseClient.GenericExecuteSpec by statement {
    inline fun <reified T : Any> _bind(variable: String, value: T?): NutSqlBuilder =
        NutSqlBuilder(statement.bind(mappings[variable] ?: variable, Parameter.fromOrEmpty(value, T::class.java)), mappings)

    inline fun time(long: Long) = _bind(UpNutClient.TIME_VAR, long)
    inline fun limit(limit: Int) = _bind(UpNutClient.LIMIT_VAR, limit)
    inline fun sources(list: List<UUID>) = _bind(UpNutClient.SOURCE_VAR, list)
    inline fun team(id: UUID) = _bind(UpNutClient.TEAM_VAR, id)
    inline fun game(id: UUID) = _bind(UpNutClient.GAME_VAR, id)
    inline fun player(id: UUID) = _bind(UpNutClient.PLAYER_VAR, id)

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