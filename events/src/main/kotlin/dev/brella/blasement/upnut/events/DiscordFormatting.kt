package dev.brella.blasement.upnut.events

import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import dev.brella.blasement.upnut.common.WebhookEvent
import dev.brella.blasement.upnut.common.getIntOrNull
import dev.brella.blasement.upnut.common.getStringOrNull
import dev.brella.d4j.coroutines.json.addAllFields
import dev.brella.d4j.coroutines.json.addEmbed
import dev.brella.d4j.coroutines.json.addField
import dev.brella.d4j.coroutines.json.author
import dev.brella.d4j.coroutines.json.buildWebhookExecuteRequest
import dev.brella.d4j.coroutines.json.footer
import dev.brella.d4j.coroutines.json.thumbnail
import discord4j.discordjson.json.ImmutableEmbedData
import kotlinx.coroutines.future.await
import kotlinx.datetime.Instant
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject

const val LIBRARY = "https://cdn.discordapp.com/attachments/818811060349566988/843769330512035851/library.png"
const val HERRING = "https://cdn.discordapp.com/attachments/818811060349566988/858816135759921182/fish.png"

const val LOOTCRATES = "https://cdn.discordapp.com/attachments/818811060349566988/858825678317551626/lootcrates.png"
const val EQUITY = "https://d35iw2jmbg6ut8.cloudfront.net/static/media/Equity.c533a2ae.png"
const val MONITOR = "https://cdn.discordapp.com/attachments/818811060349566988/859010381506412557/monitor.png"

val AUTHORS = arrayOf(
    "",
    "https://cdn.discordapp.com/attachments/818811060349566988/859036227649798164/MonitorAuthor.png",
    "https://cdn.discordapp.com/attachments/818811060349566988/859036230545178654/EquityAuthor.png",
    "https://cdn.discordapp.com/attachments/818811060349566988/859036227188555796/ReaderAuthor.png",
    "https://cdn.discordapp.com/attachments/818811060349566988/859036229676302346/MicrophoneAuthor.png",
    "https://cdn.discordapp.com/attachments/818811060349566988/859036228316692520/LootcratesAuthor.png"
)

inline fun WebhookEvent.LibraryChaptersRedacted.toDiscordEvent() =
    buildWebhookExecuteRequest {
        addEmbed {
            title("History Rewritten; Old Editions have been Redacted")
            thumbnail { url(LIBRARY) }

            chapters.formatFieldsTo(this)

//            timestamp()
        }
    }

inline fun WebhookEvent.LibraryChaptersUnredacted.toDiscordEvent() =
    buildWebhookExecuteRequest {
        addEmbed {
            title("SECRETS SPILLED / TALES SPUN / CHAPTERS REVEALED")
            thumbnail { url(LIBRARY) }

            chapters.formatFieldsTo(this)

//            timestamp()
        }
    }

inline fun WebhookEvent.NewLibraryChapters.toDiscordEvent() =
    buildWebhookExecuteRequest {
        addEmbed {
            title("IN THE STACKS / RECORDS SET / CHAPTERS ADDED")
            thumbnail { url(LIBRARY) }

            chapters.formatFieldsTo(this)

//            timestamp()
        }
    }

inline fun List<WebhookEvent.LibraryChapter>.formatFields() =
    buildJsonArray {
        groupBy(WebhookEvent.LibraryChapter::bookName)
            .forEach { (bookName, chapters) ->
                addJsonObject {
                    put("name", bookName)
                    put("value", buildString {
                        chapters.forEach { event ->
                            val chapterNameRedacted = event.chapterNameRedacted
                            val chapterName = event.chapterName

                            if (chapterNameRedacted != null) {
                                if (chapterName != null) {
                                    appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                    appendLine()
                                } else {
                                    appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                    appendLine()
                                }
                            } else {
                                if (event.chapterName != null) {
                                    appendLine("[${event.chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                    appendLine()
                                } else {
                                    appendLine("[$${event.bookIndex}/${event.bookIndex + 1}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                    appendLine()
                                }
                            }
                        }
                    })
                }
            }
    }

inline fun List<WebhookEvent.LibraryChapter>.formatFieldsTo(builder: ImmutableEmbedData.Builder) =
    with(builder) {
        groupBy(WebhookEvent.LibraryChapter::bookName)
            .forEach { (bookName, chapters) ->
                addField {
                    name(bookName)
                    value(buildString {
                        chapters.forEach { event ->
                            val chapterNameRedacted = event.chapterNameRedacted
                            val chapterName = event.chapterName

                            if (chapterNameRedacted != null) {
                                if (chapterName != null) {
                                    appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                    appendLine()
                                } else {
                                    appendLine("[||${chapterNameRedacted.replace('|', '\u2588')}|| -> ${chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                    appendLine()
                                }
                            } else {
                                if (event.chapterName != null) {
                                    appendLine("[${event.chapterName}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                    appendLine()
                                } else {
                                    appendLine("[$${event.bookIndex}/${event.bookIndex + 1}](https://www.blaseball.com/library/${event.bookIndex}/${event.chapterIndex + 1})")
                                    appendLine()
                                }
                            }
                        }
                    })
                }
            }
    }

suspend inline fun WebhookEvent.NewHerringPool.toDiscordEvent(teamCache: AsyncLoadingCache<String, JsonObject>, playerCache: AsyncLoadingCache<String, JsonObject>) =
    buildWebhookExecuteRequest {
        addEmbed {
            title("Something seems fishy...")
            thumbnail { url(HERRING) }
            description(event.description)

            val tags: MutableList<String> = ArrayList()

            event.gameTags?.forEach { uuid -> tags.add("[Game](https://blaseball.com/game/$uuid)") }
            event.teamTags?.forEach { uuid ->
                tags.add(
                    "[${
                        teamCache[uuid.toString()]
                            .await()
                            .getStringOrNull("fullName")
                        ?: "% Team %"
                    }](https://blaseball.com/team/$uuid)"
                )
            }
            event.playerTags?.forEach { uuid ->
                tags.add(
                    "[${
                        playerCache[uuid.toString()]
                            .await()
                            .getStringOrNull("name")
                        ?: "% Player %"
                    }](https://blaseball.com/player/$uuid)"
                )
            }

            tags.chunked(10)
                .forEachIndexed { index, list ->
                    addField {
                        name(if (index == 0) "Tags" else "\u200B")
                        value(list.joinToString(", "))
                    }
                }
        }
    }

suspend inline fun WebhookEvent.ThresholdPassedNuts.toDiscordEvent(teamCache: AsyncLoadingCache<String, JsonObject>, playerCache: AsyncLoadingCache<String, JsonObject>) =
    buildWebhookExecuteRequest {
        addEmbed {
//            title("RECORDS SET")
            thumbnail { url(LOOTCRATES) }

            val being = (event.metadata as? JsonObject)?.getIntOrNull("being")
            description(event.description)

            author { name("RECORDS SET").iconUrl(being?.let(AUTHORS::getOrNull) ?: "") }

            footer { text("Passed threshold at") }
            timestamp(Instant.fromEpochMilliseconds(time).toString())

            val tags: MutableList<String> = ArrayList()

            event.gameTags?.forEach { uuid -> tags.add("[Game](https://blaseball.com/game/$uuid)") }
            event.teamTags?.forEach { uuid ->
                tags.add(
                    "[${
                        teamCache[uuid.toString()]
                            .await()
                            .getStringOrNull("fullName")
                        ?: "% Team %"
                    }](https://blaseball.com/team/$uuid)"
                )
            }
            event.playerTags?.forEach { uuid ->
                tags.add(
                    "[${
                        playerCache[uuid.toString()]
                            .await()
                            .getStringOrNull("name")
                        ?: "% Player %"
                    }](https://blaseball.com/player/$uuid)"
                )
            }

            tags.chunked(10)
                .forEachIndexed { index, list ->
                    addField {
                        name(if (index == 0) "Tags" else "\u200B")
                        value(list.joinToString(", "))
                    }
                }
        }
    }

suspend inline fun WebhookEvent.ThresholdPassedScales.toDiscordEvent(teamCache: AsyncLoadingCache<String, JsonObject>, playerCache: AsyncLoadingCache<String, JsonObject>) =
    buildWebhookExecuteRequest {
        addEmbed {
            thumbnail { url(LOOTCRATES) }

            val being = (event.metadata as? JsonObject)?.getIntOrNull("being")
            description(event.description)

            author { name("SECRETS SPILLED").iconUrl(being?.let(AUTHORS::getOrNull) ?: "") }

            footer { text("Passed threshold at") }
            timestamp(Instant.fromEpochMilliseconds(time).toString())

            val tags: MutableList<String> = ArrayList()

            event.gameTags?.forEach { uuid -> tags.add("[Game](https://blaseball.com/game/$uuid)") }
            event.teamTags?.forEach { uuid ->
                tags.add(
                    "[${
                        teamCache[uuid.toString()]
                            .await()
                            .getStringOrNull("fullName")
                        ?: "% Team %"
                    }](https://blaseball.com/team/$uuid)"
                )
            }
            event.playerTags?.forEach { uuid ->
                tags.add(
                    "[${
                        playerCache[uuid.toString()]
                            .await()
                            .getStringOrNull("name")
                        ?: "% Player %"
                    }](https://blaseball.com/player/$uuid)"
                )
            }

            tags.chunked(10)
                .forEachIndexed { index, list ->
                    addField {
                        name(if (index == 0) "Tags" else "\u200B")
                        value(list.joinToString(", "))
                    }
                }
        }
    }

inline fun WebhookEvent.HelloWorld.toDiscordEvent() =
    buildWebhookExecuteRequest {
        addEmbed {
            author { iconUrl(AUTHORS.random()).name("Upnuts") }
            description("Hello, World! Added to the stream <t:${time.epochSeconds}:R>")
        }
    }

inline fun WebhookEvent.GoodbyeWorld.toDiscordEvent() =
    buildWebhookExecuteRequest {
        addEmbed {
            author { iconUrl(AUTHORS.random()).name("Upnuts") }
            description("Goodbye, World! Removed from the stream <t:${time.epochSeconds}:R>")
        }
    }