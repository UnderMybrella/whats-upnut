package dev.brella.blasement.upnut.common

import com.soywiz.klock.DateTimeTz
import dev.brella.kornea.blaseball.base.common.json.BlaseballDateTimeSerialiser
import kotlinx.datetime.Instant
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.util.*

@Serializable
sealed class WebhookEvent {
    companion object {
        const val PING = 1 shl 0

        const val NEW_LIBRARY_CHAPTERS = 1 shl 1
        const val LIBRARY_CHAPTERS_REDACTED = 1 shl 2
        const val LIBRARY_CHAPTERS_UNREDACTED = 1 shl 3

        const val THRESHOLD_PASSED_NUTS = 1 shl 4
        const val THRESHOLD_PASSED_SCALES = 1 shl 5

        const val LIBRARY_CHAPTER_REMOVED = 1 shl 6

        const val NEW_HERRING_POOL = 1 shl 7
    }

    @Serializable
    data class LibraryChapter(val bookName: String, val bookIndex: Int, val chapterUUID: @Serializable(UUIDSerialiser::class) UUID, val chapterName: String?, val chapterNameRedacted: String?, val chapterIndex: Int, val unredactedSince: Instant?)

    @Serializable
    @SerialName("PING")
    data class Ping(val time: Instant): WebhookEvent()

    @Serializable
    @SerialName("HELLO_WORLD")
    data class HelloWorld(val time: Instant): WebhookEvent()

    @Serializable
    @SerialName("GOODBYE_WORLD")
    data class GoodbyeWorld(val time: Instant): WebhookEvent()

    @Serializable
    @SerialName("NEW_LIBRARY_CHAPTERS")
    data class NewLibraryChapters(val chapters: List<LibraryChapter>): WebhookEvent()

    @Serializable
    @SerialName("LIBRARY_CHAPTERS_REDACTED")
    data class LibraryChaptersRedacted(val chapters: List<LibraryChapter>): WebhookEvent()

    @Serializable
    @SerialName("LIBRARY_CHAPTERS_UNREDACTED")
    data class LibraryChaptersUnredacted(val chapters: List<LibraryChapter>): WebhookEvent()

    @Serializable
    @SerialName("LIBRARY_CHAPTERS_REMOVED")
    data class LibraryChaptersRemoved(val chapters: List<LibraryChapter>): WebhookEvent()

    @Serializable
    @SerialName("THRESHOLD_PASSED_NUTS")
    data class ThresholdPassedNuts(val threshold: Int, val time: Long, val event: UpNutEvent): WebhookEvent()

    @Serializable
    @SerialName("THRESHOLD_PASSED_SCALES")
    data class ThresholdPassedScales(val threshold: Int, val time: Long, val event: UpNutEvent): WebhookEvent()

    @Serializable
    @SerialName("NEW_HERRING_POOL")
    data class NewHerringPool(val event: UpNutEvent, val firstDiscovered: Long): WebhookEvent()
}