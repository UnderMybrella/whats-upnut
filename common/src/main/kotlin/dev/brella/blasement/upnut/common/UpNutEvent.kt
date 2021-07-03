package dev.brella.blasement.upnut.common

import kotlinx.datetime.Instant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import java.util.*

data class UpNutIngest(
    val time: Long,
    val source: BlaseballSource,
    val events: List<UpNutEvent>
)

@Serializable
data class UpNutEvent(
    val id: @Serializable(UUIDSerialiser::class) UUID,
    val playerTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val teamTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val gameTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val created: Instant,
    val season: Int,
    val tournament: Int,
    val type: Int,
    val day: Int,
    val phase: Int,
    val category: Int,
    val description: String,
    var nuts: JsonPrimitive = JsonNull,
//    var scales: JsonPrimitive = JsonNull,
    var metadata: JsonElement
) {
    var scales: JsonPrimitive
        get() = (metadata as? JsonObject)?.get("scales") as? JsonPrimitive ?: JsonNull
        set(value) {
            metadata = (metadata as? JsonObject)?.let { json ->
                JsonObject(json.plus("scales" to value))
            } ?: metadata
        }

    inline infix fun withMetadata(builder: JsonObjectBuilder.() -> Unit): UpNutEvent {
        metadata = when (val metadata = metadata) {
            is JsonNull -> buildJsonObject(builder)
            is JsonObject -> buildJsonObject {
                metadata.forEach { k, v -> put(k, v) }
                builder()
            }
            else -> metadata
        }

        return this
    }

    inline infix fun copyWithMetadata(builder: JsonObjectBuilder.() -> Unit): UpNutEvent =
        copy(metadata = when (val metadata = metadata) {
            is JsonNull -> buildJsonObject(builder)
            is JsonObject -> buildJsonObject {
                metadata.forEach { k, v -> put(k, v) }
                builder()
            }
            else -> metadata
        })
}

object UnixTimestampSerialiser : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("UnixTimestamp", PrimitiveKind.LONG)

    override fun deserialize(decoder: Decoder): Instant =
        Instant.fromEpochSeconds(decoder.decodeLong())

    override fun serialize(encoder: Encoder, value: Instant) {
        encoder.encodeLong(value.epochSeconds)
    }
}

@Serializable
data class EventuallieEvent(
    val id: @Serializable(UUIDSerialiser::class) UUID,
    val playerTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val teamTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val gameTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val created: Long,
    val season: Int,
    val tournament: Int,
    val type: Int,
    val day: Int,
    val phase: Int,
    val category: Int,
    val description: String,
    var nuts: JsonPrimitive = JsonNull,
//    var scales: JsonPrimitive = JsonNull,
    var metadata: JsonElement
) {
    var scales: JsonPrimitive
        get() = (metadata as? JsonObject)?.get("scales") as? JsonPrimitive ?: JsonNull
        set(value) {
            metadata = (metadata as? JsonObject)?.let { json ->
                JsonObject(json.plus("scales" to value))
            } ?: metadata
        }

    inline fun toUpNutEvent() = UpNutEvent(id, playerTags, teamTags, gameTags, Instant.fromEpochSeconds(created), season, tournament, type, day, phase, category, description, nuts, metadata)
}

@Serializable
data class NutEpochEvent(
    val id: @Serializable(UUIDSerialiser::class) UUID,
    val playerTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val teamTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val gameTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val created: Instant,
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
    val id: @Serializable(UUIDSerialiser::class) UUID,
    val playerTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val teamTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val gameTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val created: Instant,
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
data class NutsEpoch(
    val nuts: Int?,
    val scales: Int?,
    val provider: @Serializable(UUIDSerialiser::class) UUID,
    val source: @Serializable(UUIDSerialiser::class) UUID?,
    val time: Long
) {
    constructor(nuts: Number?, scales: Number?, provider: UUID, source: UUID?, time: Number) : this(nuts?.toInt(), scales?.toInt(), provider, source, time.toLong())
}

@Serializable
data class NutsDateTime(
    val nuts: Int?,
    val scales: Int?,
    val provider: @Serializable(UUIDSerialiser::class) UUID,
    val source: @Serializable(UUIDSerialiser::class) UUID?,
    val time: String
) {
    constructor(nuts: Number?, scales: Number?, provider: UUID, source: UUID?, time: String) : this(nuts?.toInt(), scales?.toInt(), provider, source, time)
}

object UUIDSerialiser : KSerializer<UUID> {
    override val descriptor: SerialDescriptor = String.serializer().descriptor

    override fun serialize(encoder: Encoder, value: UUID) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): UUID = UUID.fromString(decoder.decodeString())
}

object UUIDListSerialiser : KSerializer<List<UUID>> {
    val base = ListSerializer(String.serializer()).nullable


    override val descriptor: SerialDescriptor = base.descriptor

    override fun deserialize(decoder: Decoder): List<UUID> =
        base.deserialize(decoder)?.mapNotNull(String::uuidOrNull) ?: emptyList()

    override fun serialize(encoder: Encoder, value: List<UUID>) =
        base.serialize(encoder, value.map(UUID::toString))
}

fun String?.uuidOrNull(): UUID? {
    if (this == null) return null

    val len: Int = this.length
    if (len > 36) return null

    val dash1: Int = this.indexOf('-', 0)
    val dash2: Int = this.indexOf('-', dash1 + 1)
    val dash3: Int = this.indexOf('-', dash2 + 1)
    val dash4: Int = this.indexOf('-', dash3 + 1)
    val dash5: Int = this.indexOf('-', dash4 + 1)

    // For any valid input, dash1 through dash4 will be positive and dash5
    // negative, but it's enough to check dash4 and dash5:
    // - if dash1 is -1, dash4 will be -1
    // - if dash1 is positive but dash2 is -1, dash4 will be -1
    // - if dash1 and dash2 is positive, dash3 will be -1, dash4 will be
    //   positive, but so will dash5

    // For any valid input, dash1 through dash4 will be positive and dash5
    // negative, but it's enough to check dash4 and dash5:
    // - if dash1 is -1, dash4 will be -1
    // - if dash1 is positive but dash2 is -1, dash4 will be -1
    // - if dash1 and dash2 is positive, dash3 will be -1, dash4 will be
    //   positive, but so will dash5
    if (dash4 < 0 || dash5 >= 0) return null

    var mostSigBits: Long = java.lang.Long.parseLong(this, 0, dash1, 16) and 0xffffffffL
    mostSigBits = mostSigBits shl 16
    mostSigBits = mostSigBits or (java.lang.Long.parseLong(this, dash1 + 1, dash2, 16) and 0xffffL)
    mostSigBits = mostSigBits shl 16
    mostSigBits = mostSigBits or (java.lang.Long.parseLong(this, dash2 + 1, dash3, 16) and 0xffffL)
    var leastSigBits: Long = java.lang.Long.parseLong(this, dash3 + 1, dash4, 16) and 0xffffL
    leastSigBits = leastSigBits shl 48
    leastSigBits = leastSigBits or (java.lang.Long.parseLong(this, dash4 + 1, len, 16) and 0xffffffffffffL)

    return UUID(mostSigBits, leastSigBits)
}