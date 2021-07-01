package dev.brella.blasement.upnut.ingest

import dev.brella.blasement.upnut.common.PlayerIdPair
import dev.brella.blasement.upnut.common.UpNutEvent
import dev.brella.blasement.upnut.common.getJsonObjectOrNull
import dev.brella.blasement.upnut.common.getStringOrNull
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import java.util.*
import kotlin.collections.HashMap

fun interface EventFilter {
    suspend fun interestingEventsIn(events: List<UpNutEvent>): List<UpNutEvent>
}

object StoleFifthBase {

}

/** For FK reasons, KLoNG */
class KennedyLosersNewGuys(val http: HttpClient) : EventFilter {
    var players: MutableMap<String, String> = HashMap()
    var etag: String? = null
    override suspend fun interestingEventsIn(events: List<UpNutEvent>): List<UpNutEvent> {
        http.get<HttpResponse>("https://www.blaseball.com/database/playerNamesIds") {
            etag?.let { parameter("If-None-Match", it) }
        }.let { response ->
            //If we failed, don't update, but also -- if we don't have an update, don't update !
            if (!response.status.isSuccess()) return@let

            val newPlayers = response.receive<List<PlayerIdPair>>()
            players.clear()
            newPlayers.forEach { (id, name) -> players[id.toString()] = name }
            response.headers[HttpHeaders.ETag]?.let { etag = it }
        }

        val hauntings = events.filter { event ->
            //First of all, KLoNGs are of event type 106 - modifier changes
            if (event.type != 106) return@filter false

            //Then, we need to check the modifier
            val mod = (event.metadata as? JsonObject)?.getStringOrNull("mod") ?: return@filter false
            if (!mod.equals("INHABITING", true)) return@filter false

            //Now, let's check if the player exists in our cache
            val player = event.playerTags?.firstOrNull()?.toString() ?: return@filter false
            if (player in players) return@filter false

            //So we have a player inhabiting 'someone', that's not in our cache (we'll double check it later)

            true
        }.distinctBy { it.playerTags?.firstOrNull() }.toMutableList()

        http.get<JsonArray>("https://www.blaseball.com/database/players") {
            parameter("ids", hauntings.joinToString(",") { it.playerTags!!.first().toString() })
        }.forEach { element ->
            val id = (element as? JsonObject)?.getStringOrNull("id") ?: return@forEach
            hauntings.removeIf { it.playerTags?.firstOrNull()?.toString() == id }
        }

        //We should definitely only have unknown players now

        return hauntings
    }
}

object VaultOfTheRoamer: EventFilter {
    val PARKER_MACMILLAN = UUID.fromString("38a1e7ce-64ed-433d-b29c-becc4720caa1")
    override suspend fun interestingEventsIn(events: List<UpNutEvent>): List<UpNutEvent> =
        events.filter { it.playerTags?.contains(PARKER_MACMILLAN) == true }
}