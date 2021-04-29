package dev.brella.blasement.upnut

import com.soywiz.klock.DateTimeTz
import dev.brella.kornea.blaseball.base.common.FeedID
import dev.brella.kornea.blaseball.base.common.GameID
import dev.brella.kornea.blaseball.base.common.PlayerID
import dev.brella.kornea.blaseball.base.common.TeamID
import dev.brella.kornea.blaseball.base.common.beans.BlaseballFeedMetadata
import dev.brella.kornea.blaseball.base.common.json.BlaseballDateTimeSerialiser
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class UpNutEvent(
    val id: String,
    val playerTags: List<String>?,
    val teamTags: List<String>?,
    val gameTags: List<String>?,
    val created: @Serializable(BlaseballDateTimeSerialiser::class) DateTimeTz,
    val season: Int,
    val tournament: Int,
    val type: Int,
    val day: Int,
    val phase: Int,
    val category: Int,
    val description: String,
    var nuts: Int,
    val metadata: JsonElement
)