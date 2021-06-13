package dev.brella.blasement.upnut.common

import dev.brella.kornea.blaseball.base.common.PlayerID
import kotlinx.serialization.Serializable

@Serializable
data class PlayerIdPair(val id: PlayerID, val name: String)


/**
 * Returns a new map containing all key-value pairs from the given collection of pairs.
 *
 * The returned map preserves the entry iteration order of the original collection.
 * If any of two pairs would have the same key the last one gets added to the map.
 */
public fun Iterable<PlayerIdPair>.toMap(): Map<PlayerID, String> {
    if (this is Collection) {
        return when (size) {
            0 -> emptyMap()
            1 -> mapOf(if (this is List) this[0].let { it.id to it.name } else iterator().next().let { it.id to it.name })
            else -> toMap(LinkedHashMap())
        }
    }
    return toMap(LinkedHashMap())
}


/**
 * Populates and returns the [destination] mutable map with key-value pairs from the given collection of pairs.
 */
public fun <M : MutableMap<in PlayerID, in String>> Iterable<PlayerIdPair>.toMap(destination: M): M =
    destination.apply { this@toMap.forEach { (id, name) -> put(id, name) } }




/**
 * Returns a new map containing all key-value pairs from the given collection of pairs.
 *
 * The returned map preserves the entry iteration order of the original collection.
 * If any of two pairs would have the same key the last one gets added to the map.
 */
public fun Iterable<PlayerIdPair>.toReverseMap(): Map<String, PlayerID> {
    if (this is Collection) {
        return when (size) {
            0 -> emptyMap()
            1 -> mapOf(if (this is List) this[0].let { it.name to it.id } else iterator().next().let { it.name to it.id })
            else -> toReverseMap(LinkedHashMap())
        }
    }
    return toReverseMap(LinkedHashMap())
}


/**
 * Populates and returns the [destination] mutable map with key-value pairs from the given collection of pairs.
 */
public fun <M : MutableMap<in String, in PlayerID>> Iterable<PlayerIdPair>.toReverseMap(destination: M): M =
    destination.apply { this@toReverseMap.forEach { (id, name) -> put(name, id) } }