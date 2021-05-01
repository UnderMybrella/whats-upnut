package dev.brella.blasement.upnut

import dev.brella.blasement.upnut.UpNutClient.Companion.CATEGORY_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.CREATED_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.DAY_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.PHASE_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.SEASON_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.TOURNAMENT_VAR
import dev.brella.blasement.upnut.UpNutClient.Companion.TYPE_VAR
import org.intellij.lang.annotations.Language
import org.springframework.r2dbc.core.DatabaseClient

class NutSqlStatement(@Language("PostgreSQL") val psql: String, val mappings: Map<String, String>) {
    companion object {
        val ALL_MAPPINGS = listOf(
            UpNutClient.TIME_VAR,
            UpNutClient.LIMIT_VAR,
            UpNutClient.SOURCE_VAR,
            UpNutClient.TEAM_VAR,
            UpNutClient.GAME_VAR,
            UpNutClient.PLAYER_VAR,
            CREATED_VAR,
            SEASON_VAR,
            TOURNAMENT_VAR,
            TYPE_VAR,
            DAY_VAR,
            PHASE_VAR,
            CATEGORY_VAR
        )

        operator fun invoke(@Language("PostgreSQL") psql: String): NutSqlStatement {
            var index = 1
            val mapping: MutableMap<String, String> = HashMap()

            return NutSqlStatement(ALL_MAPPINGS.fold(psql) { psql, variable ->
                if (variable in psql) {
                    val newVariable = "\$${index++}"
                    mapping[variable] = newVariable
                    psql.replace(variable, newVariable)
                } else {
                    psql
                }
            }, mapping)
        }
    }

    operator fun invoke(client: DatabaseClient) =
        NutSqlBuilder(client.sql(psql), mappings)
}