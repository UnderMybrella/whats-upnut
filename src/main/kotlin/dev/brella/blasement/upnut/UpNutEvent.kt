package dev.brella.blasement.upnut

import com.soywiz.klock.DateTimeTz
import dev.brella.kornea.blaseball.base.common.FeedID
import dev.brella.kornea.blaseball.base.common.GameID
import dev.brella.kornea.blaseball.base.common.PlayerID
import dev.brella.kornea.blaseball.base.common.TeamID
import dev.brella.kornea.blaseball.base.common.beans.BlaseballFeedMetadata
import dev.brella.kornea.blaseball.base.common.json.BlaseballDateTimeSerialiser
import dev.brella.kornea.blaseball.base.common.json.CoercedIntSerialiser
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

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
    var nuts: JsonPrimitive,
    val metadata: JsonElement
)

@Serializable
data class NutEpochEvent(
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
    val nuts: List<NutsEpoch>,
    val metadata: JsonElement
)

@Serializable
data class NutDateTimeEvent(
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
    val nuts: List<NutsDateTime>,
    val metadata: JsonElement
)

@Serializable
data class NutsEpoch(val nuts: Int, val source: String, val time: Long) {
    constructor(nuts: Number, source: String, time: Number): this(nuts.toInt(), source, time.toLong())
}

@Serializable
data class NutsDateTime(val nuts: Int, val source: String, val time: String) {
    constructor(nuts: Number, source: String, time: String): this(nuts.toInt(), source, time)
}