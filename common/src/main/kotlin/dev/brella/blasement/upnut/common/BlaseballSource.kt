package dev.brella.blasement.upnut.common

import java.util.*

data class BlaseballSource(val sourceID: UUID?, val sourceType: Int) {
    companion object {
        const val GLOBAL_FEED = 0
        const val PLAYER_FEED = 1
        const val GAME_FEED = 2
        const val TEAM_FEED = 3
        const val STORY_CHAPTER = 4

        @PublishedApi
        internal val GLOBAL_SOURCE by lazy { BlaseballSource(null, GLOBAL_FEED) }

        inline fun global() = GLOBAL_SOURCE
        inline fun player(id: UUID) = BlaseballSource(id, PLAYER_FEED)
        inline fun game(id: UUID) = BlaseballSource(id, GAME_FEED)
        inline fun team(id: UUID) = BlaseballSource(id, TEAM_FEED)
        inline fun story(id: UUID) = BlaseballSource(id, STORY_CHAPTER)
    }
}