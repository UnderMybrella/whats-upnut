package dev.brella.blasement.upnut

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
}