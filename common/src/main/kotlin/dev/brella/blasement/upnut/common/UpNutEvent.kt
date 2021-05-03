package dev.brella.blasement.upnut.common

import com.soywiz.klock.DateTimeTz
import dev.brella.kornea.blaseball.base.common.json.BlaseballDateTimeSerialiser
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import java.util.*

@Serializable
data class UpNutEvent(
    val id: @Serializable(UUIDSerialiser::class) UUID,
    val playerTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val teamTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val gameTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
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
    val id: @Serializable(UUIDSerialiser::class) UUID,
    val playerTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val teamTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val gameTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
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
    val id: @Serializable(UUIDSerialiser::class) UUID,
    val playerTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val teamTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
    val gameTags: @Serializable(UUIDListSerialiser::class) List<@Serializable(UUIDSerialiser::class) UUID>?,
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
data class NutsEpoch(
    val nuts: Int,
    val provider: @Serializable(UUIDSerialiser::class) UUID,
    val source: @Serializable(UUIDSerialiser::class) UUID?,
    val time: Long
) {
    constructor(nuts: Number, provider: UUID, source: UUID?, time: Number) : this(nuts.toInt(), provider, source, time.toLong())
}

@Serializable
data class NutsDateTime(
    val nuts: Int,
    val provider: @Serializable(UUIDSerialiser::class) UUID,
    val source: @Serializable(UUIDSerialiser::class) UUID?,
    val time: String
) {
    constructor(nuts: Number, provider: UUID, source: UUID?, time: String) : this(nuts.toInt(), provider, source, time)
}

object UUIDSerialiser : KSerializer<UUID> {
    override val descriptor: SerialDescriptor = String.serializer().descriptor

    override fun serialize(encoder: Encoder, value: UUID) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): UUID = UUID.fromString(decoder.decodeString())
}

object UUIDListSerialiser : KSerializer<List<UUID>> {
    val base = ListSerializer(String.serializer())


    override val descriptor: SerialDescriptor = base.descriptor

    override fun deserialize(decoder: Decoder): List<UUID> =
        base.deserialize(decoder).mapNotNull(String::uuidOrNull)

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