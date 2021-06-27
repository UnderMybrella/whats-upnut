package dev.brella.blasement.upnut.ingest

import dev.brella.blasement.upnut.common.UpNutEvent
import dev.brella.kornea.blaseball.endpoints.BlaseballDatabaseService
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

suspend inline fun HttpClient.getGlobalFeed(category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): List<UpNutEvent> =
    getGlobalFeedAs(category, limit, type, sort, start)

suspend inline fun HttpClient.getGlobalFeedAsResponse(category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): HttpResponse =
    getGlobalFeedAs(category, limit, type, sort, start)


suspend inline fun <reified T> HttpClient.getGlobalFeedAs(category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): T =
    get("https://blaseball.com/database/feed/global") {
        if (category != null) parameter("category", category)
        if (type != null) parameter("type", type)
        if (limit != BlaseballDatabaseService.YES_BRELLA_I_WOULD_LIKE_UNLIMITED_EVENTS) parameter("limit", limit)
        if (sort != null) parameter("sort", sort)
        if (start != null) parameter("start", start)
    }

suspend inline fun HttpClient.getTeamFeed(teamID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): List<UpNutEvent> =
    getTeamFeedAs(teamID, category, limit, type, sort, start)

suspend inline fun HttpClient.getTeamFeedAsResponse(teamID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): HttpResponse =
    getTeamFeedAs(teamID, category, limit, type, sort, start)

suspend inline fun <reified T> HttpClient.getTeamFeedAs(teamID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): T =
    get("https://blaseball.com/database/feed/team") {
        parameter("id", teamID)

        if (category != null) parameter("category", category)
        if (type != null) parameter("type", type)
        if (limit != BlaseballDatabaseService.YES_BRELLA_I_WOULD_LIKE_UNLIMITED_EVENTS) parameter("limit", limit)
        if (sort != null) parameter("sort", sort)
        if (start != null) parameter("start", start)
    }

suspend inline fun HttpClient.getGameFeed(gameID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): List<UpNutEvent> =
    getGameFeedAs(gameID, category, limit, type, sort, start)

suspend inline fun HttpClient.getGameFeedAsResponse(gameID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): HttpResponse =
    getGameFeedAs(gameID, category, limit, type, sort, start)

suspend inline fun <reified T> HttpClient.getGameFeedAs(gameID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): T =
    get("https://blaseball.com/database/feed/game") {
        parameter("id", gameID)

        if (category != null) parameter("category", category)
        if (type != null) parameter("type", type)
        if (limit != BlaseballDatabaseService.YES_BRELLA_I_WOULD_LIKE_UNLIMITED_EVENTS) parameter("limit", limit)
        if (sort != null) parameter("sort", sort)
        if (start != null) parameter("start", start)
    }

suspend inline fun HttpClient.getPlayerFeed(playerID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): List<UpNutEvent> =
    getPlayerFeedAs(playerID, category, limit, type, sort, start)

suspend inline fun HttpClient.getPlayerFeedAsResponse(playerID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): HttpResponse =
    getPlayerFeedAs(playerID, category, limit, type, sort, start)

suspend inline fun <reified T> HttpClient.getPlayerFeedAs(playerID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): T =
    get("https://blaseball.com/database/feed/player") {
        parameter("id", playerID)

        if (category != null) parameter("category", category)
        if (type != null) parameter("type", type)
        if (limit != BlaseballDatabaseService.YES_BRELLA_I_WOULD_LIKE_UNLIMITED_EVENTS) parameter("limit", limit)
        if (sort != null) parameter("sort", sort)
        if (start != null) parameter("start", start)
    }

suspend inline fun HttpClient.getStoryFeed(chapterID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): List<UpNutEvent> =
    getStoryFeedAs(chapterID, category, limit, type, sort, start)

suspend inline fun HttpClient.getStoryFeedAsResponse(chapterID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): HttpResponse =
    getStoryFeedAs(chapterID, category, limit, type, sort, start)

suspend inline fun <reified T> HttpClient.getStoryFeedAs(chapterID: String, category: Int? = null, limit: Int = 100, type: Int? = null, sort: Int? = null, start: Int? = null): T =
    get("https://blaseball.com/database/feed/story") {
        parameter("id", chapterID)

        if (category != null) parameter("category", category)
        if (type != null) parameter("type", type)
        if (limit != BlaseballDatabaseService.YES_BRELLA_I_WOULD_LIKE_UNLIMITED_EVENTS) parameter("limit", limit)
        if (sort != null) parameter("sort", sort)
        if (start != null) parameter("start", start)
    }

@Serializable
data class BlaseballPlayerName(val id: String, val name: String)

suspend inline fun HttpClient.getAllPlayers(): List<BlaseballPlayerName> =
    get("https://www.blaseball.com/database/playerNamesIds")

suspend inline fun HttpClient.getTodaysGames(): List<JsonObject> =
    get("https://api.sibr.dev/corsmechanics/stream/games/schedule") //thanks, me!