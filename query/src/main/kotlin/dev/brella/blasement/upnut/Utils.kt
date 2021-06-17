package dev.brella.blasement.upnut

import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import dev.brella.blasement.upnut.common.UpNutClient
import dev.brella.blasement.upnut.common.UpNutEvent
import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.kornea.errors.common.doOnFailure
import dev.brella.kornea.errors.common.doOnSuccess
import dev.brella.kornea.errors.common.doOnThrown
import dev.brella.kornea.errors.common.map
import dev.brella.ktornea.common.KorneaHttpResult
import dev.brella.ktornea.common.getAsResult
import io.ktor.application.*
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.util.*
import io.ktor.utils.io.*
import io.netty.handler.codec.http.QueryStringDecoder
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.serialization.json.JsonArrayBuilder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.slf4j.Logger
import org.springframework.r2dbc.core.await
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import kotlin.Comparator
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.jvm.jvmName

sealed class KorneaResponseResult : KorneaResult.Failure {
    abstract val httpResponseCode: HttpStatusCode
    abstract val contentType: ContentType?

    class UserErrorJson(override val httpResponseCode: HttpStatusCode, val json: JsonElement) : KorneaResponseResult() {
        override val contentType: ContentType = ContentType.Application.Json

        override suspend fun writeTo(call: ApplicationCall) =
            call.respond(httpResponseCode, json)
    }

    override fun get(): Nothing = throw IllegalStateException("Failed Response @ $this")

    abstract suspend fun writeTo(call: ApplicationCall)
}

inline fun buildUserErrorJsonObject(httpResponseCode: HttpStatusCode = HttpStatusCode.BadRequest, builder: JsonObjectBuilder.() -> Unit) =
    KorneaResponseResult.UserErrorJson(httpResponseCode, buildJsonObject(builder))

suspend inline fun KorneaResult<*>.respondOnFailure(call: ApplicationCall) =
    this.doOnFailure { failure ->
        when (failure) {
            is KorneaHttpResult<*> -> {
                call.response.header("X-Call-URL", failure.response.request.url.toURI().toASCIIString())
                call.respondBytesWriter(failure.response.contentType(), failure.response.status) {
                    failure.response.content.copyTo(this)
                }
            }
            is KorneaResponseResult -> failure.writeTo(call)
            else -> {
                call.respond(HttpStatusCode.InternalServerError, buildJsonObject {
                    put("error_type", failure::class.jvmName)
                    put("error", failure.toString())

                    failure.doOnThrown { withException -> put("stack_trace", withException.exception.stackTraceToString()) }
                })
            }
        }
    }

suspend inline fun <reified T : Any> KorneaResult<T>.respond(call: ApplicationCall) =
    this.doOnSuccess { call.respond(it) }
        .respondOnFailure(call)

suspend inline fun <T, reified R : Any> KorneaResult<T>.respond(call: ApplicationCall, transform: (T) -> R) =
    this.doOnSuccess { call.respond(transform(it)) }
        .respondOnFailure(call)


public suspend inline fun ApplicationCall.respondJsonObject(statusCode: HttpStatusCode = HttpStatusCode.OK, producer: JsonObjectBuilder.() -> Unit) =
    respond(statusCode, buildJsonObject(producer))

public suspend inline fun ApplicationCall.respondJsonArray(statusCode: HttpStatusCode = HttpStatusCode.OK, producer: JsonArrayBuilder.() -> Unit) =
    respond(statusCode, buildJsonArray(producer))

object ParametersComparator : Comparator<Pair<String, String>> {
    override fun compare(o1: Pair<String, String>, o2: Pair<String, String>): Int {
        val a = o1.first.compareTo(o2.first)
        if (a != 0) return a

        return o1.second.compareTo(o2.second)
    }
}

inline fun Parameters.toStableString(): String =
    flattenEntries().sortedWith(ParametersComparator)
        .joinToString("&") { (k, v) -> "$k=$v" }

val DISPATCHER_CACHE = Caffeine.newBuilder()
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .buildAsync<Executor, CoroutineDispatcher>(Executor::asCoroutineDispatcher)

typealias KotlinCache<K, V> = Cache<K, Deferred<KorneaResult<V>>>

inline fun <K, V> Caffeine<Any, Any>.buildKotlin(): KotlinCache<K, V> = build()

suspend fun <K, V> KotlinCache<K, V>.getAsync(key: K, scope: CoroutineScope = GlobalScope, context: CoroutineContext = scope.coroutineContext, mappingFunction: suspend (key: K) -> KorneaResult<V>): KorneaResult<V> {
    try {
        val result = get(key) { k -> scope.async(context) { mappingFunction(k) } }.await()

        return result.doOnFailure { invalidate(key) }
    } catch (th: Throwable) {
        invalidate(key)
        throw th
    }
}


suspend fun <K, V> AsyncCache<K, V>.getAsync(key: K, scope: CoroutineScope = GlobalScope, mappingFunction: suspend (key: K) -> V): V {
    val cache = Caffeine.newBuilder().build<String, String>()

    return get(key) { key: K, executor: Executor ->
        DISPATCHER_CACHE[executor].thenCompose { dispatcher ->
            scope.future(dispatcher) { mappingFunction(key) }
        }
    }.await()
}

suspend fun <K, V> AsyncCache<K, V>.getAsyncResult(key: K, scope: CoroutineScope = GlobalScope, mappingFunction: suspend (key: K) -> KorneaResult<V>): V =
    get(key) { key: K, executor: Executor ->
        DISPATCHER_CACHE[executor].thenCompose { dispatcher ->
            scope.future(dispatcher) { mappingFunction(key) }
                .thenCompose(KorneaResult<V>::getAsStage)
        }
    }.await()

suspend fun HttpClient.eventually(
    nuts: Map<UUID, Pair<Int, Int>>,
    upnut: UpNutClient,
    logger: Logger,
    time: Long,
    limit: Int,
    offset: Int,
    season: Int?,
    tournament: Int?,
    type: Int?,
    day: Int?,
    phase: Int?,
    category: Int?,
    provider: UUID? = null,
    source: UUID? = null
): KorneaResult<List<UpNutEvent>> {
    val missingAmount = limit - nuts.size
    val remainingList = if (missingAmount > 0) upnut.globalEventsBefore(time, missingAmount, offset + nuts.size, nuts.keys) {
        season(season)
            .tournament(tournament)
            .type(type)
            .day(day)
            .phase(phase)
            .category(category)
    } ?: emptyList() else emptyList()

    val feedIDs = (nuts.keys.toList() + remainingList).filterNotNull()

    return getAsResult<List<UpNutEvent>>("https://api.sibr.dev/eventually/v2/events") {
        parameter("id", feedIDs.joinToString("_or_"))
        parameter("limit", feedIDs.size)
    }.map { list ->
        val map = list.associateBy(UpNutEvent::id)

        val upnuts =
            provider?.let { upnut.isUpnutted(feedIDs, time, it, source) } ?: emptyMap()

//        nuts.entries.mapNotNullTo(list) { (feedID, nuts) -> map[feedID]?.copy(nuts = JsonPrimitive(nuts)) }
//        remainingList.mapNotNullTo(list) { uuid -> map[uuid]?.copy(nuts = JsonPrimitive(0)) }

        feedIDs.mapNotNull { feedID ->
            val event = map[feedID] ?: run {
                logger.warn("Eventually is missing {}, marking for collection", feedID)
                upnut.client.sql("INSERT INTO metadata_collection (feed_id) VALUES ($1) ON CONFLICT DO NOTHING")
                    .bind("$1", feedID)
                    .await()

                return@mapNotNull null
            }
            val upnutted = upnuts[event.id]

            val metadata: JsonElement =
                if (upnutted?.first == true || upnutted?.second == true) {
                    when (val metadata = event.metadata) {
                        is JsonObject -> JsonObject(metadata + Pair("upnut", JsonPrimitive(true)))
                        is JsonNull -> JsonObject(mapOf("upnut" to JsonPrimitive(true)))
                        else -> event.metadata
                    }
                } else event.metadata

            val nutPair = nuts[feedID]

            event.copy(nuts = JsonPrimitive(nutPair?.first ?: 0), metadata = metadata).apply {
                if (scales == JsonNull) {
                    if (nutPair != null && nutPair.second > 0) {
                        scales = JsonPrimitive(nutPair.second)
                    }
                } else {
                    scales = JsonPrimitive(nutPair?.second ?: 0)
                }
            }
        }
    }
}

class KorneaResultException(val result: KorneaResult<*>) : Throwable((result as? KorneaResult.WithException<*>)?.exception)

inline fun <T> KorneaResult<T>.getOrThrow(): T =
    if (this is KorneaResult.Success<T>) get()
    else throw KorneaResultException(this)

inline fun <T> KorneaResult<T>.getAsStage(): CompletionStage<T> =
    if (this is KorneaResult.Success<T>) CompletableFuture.completedStage(get())
    else CompletableFuture.failedStage(KorneaResultException(this))

suspend inline fun ApplicationCall.redirectInternally(path: String, builder: ParametersBuilder.() -> Unit) =
    redirectInternally("$path?${ParametersBuilder().apply(builder).build().formUrlEncode()}")

suspend fun ApplicationCall.redirectInternally(path: String) {
    val cp = object : RequestConnectionPoint by this.request.local {
        override val uri: String = path
    }
    val req = object : ApplicationRequest by this.request {
        override val local: RequestConnectionPoint = cp
        override val queryParameters: Parameters = object : Parameters {
            private val decoder = QueryStringDecoder(uri)
            override val caseInsensitiveName: Boolean get() = true
            override fun getAll(name: String) = decoder.parameters()[name]
            override fun names() = decoder.parameters().keys
            override fun entries() = decoder.parameters().entries
            override fun isEmpty() = decoder.parameters().isEmpty()
        }
    }
    val call = object : ApplicationCall by this {
        override val request: ApplicationRequest = req
    }

    this.application.execute(call, Unit)
}