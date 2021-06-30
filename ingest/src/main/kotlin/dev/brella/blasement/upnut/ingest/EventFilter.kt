package dev.brella.blasement.upnut.ingest

import dev.brella.blasement.upnut.common.UpNutEvent
import java.util.*

fun interface EventFilter {
    suspend fun interestingEventsIn(events: List<UpNutEvent>): List<UUID>
}