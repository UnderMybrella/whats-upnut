package dev.brella.blasement.upnut.common

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono

class InitialisedConnectionFactory(val backing: ConnectionFactory, val onCreate: (Connection) -> Publisher<*>) : ConnectionFactory by backing {
    override fun create(): Publisher<out Connection> =
        Mono.from(backing.create())
            .flatMap { connection ->
                Mono.from(onCreate(connection))
                    .thenReturn(connection)
            }
}