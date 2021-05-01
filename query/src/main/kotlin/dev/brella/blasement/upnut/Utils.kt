package dev.brella.blasement.upnut

import dev.brella.kornea.errors.common.KorneaResult
import dev.brella.kornea.errors.common.doOnFailure
import dev.brella.kornea.errors.common.doOnSuccess
import dev.brella.ktornea.common.KorneaHttpResult
import io.ktor.application.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.utils.io.*
import kotlinx.coroutines.delay
import kotlinx.serialization.json.JsonArrayBuilder
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.reflect.jvm.jvmName
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

suspend inline fun KorneaResult<*>.respondOnFailure(call: ApplicationCall) =
    this.doOnFailure { failure ->
        when (failure) {
            is KorneaHttpResult<*> -> {
                call.response.header("X-Call-URL", failure.response.request.url.toURI().toASCIIString())
                call.respondBytesWriter(failure.response.contentType(), failure.response.status) {
                    failure.response.content.copyTo(this)
                }
            }
            else -> {
                call.respond(HttpStatusCode.InternalServerError, buildJsonObject {
                    put("error_type", failure::class.jvmName)
                    put("error", failure.toString())
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
