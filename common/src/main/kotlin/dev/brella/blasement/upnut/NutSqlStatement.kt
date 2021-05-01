package dev.brella.blasement.upnut

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
            UpNutClient.PLAYER_VAR
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