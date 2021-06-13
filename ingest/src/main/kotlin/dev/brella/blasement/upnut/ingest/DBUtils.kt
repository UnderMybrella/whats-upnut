package dev.brella.blasement.upnut.ingest

import io.r2dbc.spi.Connection
import io.r2dbc.spi.Result
import io.r2dbc.spi.Statement
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.awaitSingleOrNull
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.reactivestreams.Publisher
import org.springframework.dao.DataAccessException
import org.springframework.r2dbc.core.ConnectionAccessor
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.function.Function
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

suspend inline fun <T> ConnectionAccessor.inConnectionAwait(
    context: CoroutineContext = EmptyCoroutineContext,
    crossinline block: suspend CoroutineScope.(connection: Connection) -> T?
): T? =
    inConnection { connection -> mono(context) { block(connection) } }.await()

suspend inline fun Statement.await() =
    execute().await()

suspend inline fun Statement.awaitRowsUpdated() =
    Flux.from(execute())
        .flatMap(Result::getRowsUpdated)
        .collectList()
        .await()
        .sum()

suspend inline fun <T> Publisher<T>.await() =
    if (this is Mono<T>) awaitSingle() else awaitFirst()

suspend inline fun <T> Publisher<T>.awaitOrNull() =
    if (this is Mono<T>) awaitSingleOrNull() else awaitFirstOrNull()

suspend inline fun <T> Mono<T>.await() = awaitSingle()
suspend inline fun <T> Mono<T>.awaitOrNull() = awaitSingleOrNull()