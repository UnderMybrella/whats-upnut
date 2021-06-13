package dev.brella.blasement.upnut.common

import kotlinx.serialization.Serializable
import java.util.*

@Serializable
data class LibraryBook(
    val title: String,
    val chapters: List<LibraryBookChapter>
)

@Serializable
data class LibraryBookChapter(
    val title: String,
    val id: @Serializable(UUIDSerialiser::class) UUID,
    val redacted: Boolean = false
)