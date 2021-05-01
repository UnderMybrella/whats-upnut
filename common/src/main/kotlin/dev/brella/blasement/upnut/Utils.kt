package dev.brella.blasement.upnut

import kotlinx.coroutines.delay
import kotlinx.serialization.json.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

@OptIn(ExperimentalTime::class)
suspend fun <T> T.loopEvery(time: Duration, `while`: suspend T.() -> Boolean, block: suspend () -> Unit) {
    while (`while`()) {
        val timeTaken = measureTime { block() }
//        println("Took ${timeTaken.inSeconds}s, waiting ${(time - timeTaken).inSeconds}s")
        delay((time - timeTaken).toLongMilliseconds().coerceAtLeast(0L))
    }
}


public inline fun JsonObject.getJsonObject(key: String) =
    getValue(key).jsonObject

public inline fun JsonObject.getJsonArray(key: String) =
    getValue(key).jsonArray

public inline fun JsonObject.getJsonPrimitive(key: String) =
    getValue(key).jsonPrimitive

public inline fun JsonObject.getString(key: String) =
    getValue(key).jsonPrimitive.content

public inline fun JsonObject.getInt(key: String) =
    getValue(key).jsonPrimitive.int

public inline fun JsonObject.getLong(key: String) =
    getValue(key).jsonPrimitive.long


public inline fun JsonObject.getJsonObjectOrNull(key: String) =
    get(key) as? JsonObject

public inline fun JsonObject.getJsonArrayOrNull(key: String) =
    get(key) as? JsonArray

public inline fun JsonObject.getJsonPrimitiveOrNull(key: String) =
    (get(key) as? JsonPrimitive)

public inline fun JsonObject.getStringOrNull(key: String) =
    (get(key) as? JsonPrimitive)?.contentOrNull

public inline fun JsonObject.getIntOrNull(key: String) =
    (get(key) as? JsonPrimitive)?.intOrNull

public inline fun JsonObject.getLongOrNull(key: String) =
    (get(key) as? JsonPrimitive)?.longOrNull

public inline fun JsonObject.getBooleanOrNull(key: String) =
    (get(key) as? JsonPrimitive)?.booleanOrNull
