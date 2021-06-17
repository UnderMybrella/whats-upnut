package dev.brella.blasement.upnut.common

import dev.brella.blasement.upnut.common.UpNutClient.Companion.CATEGORY_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.CREATED_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.DAY_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.FEEDS_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.FEED_ID_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.GAME_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.LIMIT_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.NONE_OF_PROVIDERS_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.NONE_OF_SOURCES_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.OFFSET_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.ONE_OF_PROVIDERS_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.ONE_OF_SOURCES_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.PHASE_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.PLAYER_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.SEASON_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.SINGLE_SOURCE_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.TEAM_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.TIME_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.TOURNAMENT_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.TYPE_VAR
import dev.brella.blasement.upnut.common.UpNutClient.Companion.UUID_VAR
import org.intellij.lang.annotations.Language
import org.springframework.r2dbc.core.DatabaseClient

class NutSqlStatement(@Language("PostgreSQL") val psql: String, val mappings: Map<String, String>) {
    companion object {
        val ALL_MAPPINGS = listOf(
            TIME_VAR,
            LIMIT_VAR,
            OFFSET_VAR,
            ONE_OF_SOURCES_VAR,
            NONE_OF_SOURCES_VAR,
            ONE_OF_PROVIDERS_VAR,
            NONE_OF_PROVIDERS_VAR,
            SINGLE_SOURCE_VAR,
            TEAM_VAR,
            GAME_VAR,
            PLAYER_VAR,
            FEED_ID_VAR,

            FEEDS_VAR,

            CREATED_VAR,
            SEASON_VAR,
            TOURNAMENT_VAR,
            TYPE_VAR,
            DAY_VAR,
            PHASE_VAR,
            CATEGORY_VAR,

            UUID_VAR
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