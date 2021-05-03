package dev.brella.blasement.upnut.common

const val BLASEBALL_EPOCH = 1595203200000L

inline class Snowflake(val backing: Long) {
    fun timestamp(epoch: Long) =
        (backing shr 22) + epoch

    val workerID get() = (backing and 0x3E0000) shr 17
    val uuidType get() = (backing and 0x1F000) shr 12
    val increment get() = backing and 0xFFF
}

inline val Snowflake.blaseballTimestamp
    get() = timestamp(BLASEBALL_EPOCH)

class SnowCrystal(val epoch: Long, workerID: Long, uuidType: Long) {
    var lastTimestamp: Long = -1L
    var sequence: Long = 0

    private val workerIDBits = 5
    private val uuidTypeBits = 5
    private val maxWorkerID = -1L xor (-1L shl workerIDBits)
    private val maxUUIDType = -1L xor (-1L shl uuidTypeBits)
    private val sequenceBits = 12

    val workerIDShift = sequenceBits
    val uuidTypeShift = sequenceBits + workerIDBits
    val timestampLeftShift = sequenceBits + workerIDBits + uuidTypeBits
    val sequenceMask = -1L xor (-1L shl sequenceBits)

    val workerID = workerID and maxWorkerID
    val uuidType = uuidType and maxUUIDType

    private val staticIDComponents = (uuidType shl uuidTypeShift) or (workerID shl workerIDShift)

    inline fun now() = System.currentTimeMillis()

    fun gen(): Snowflake {
        var timestamp = now()

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) and sequenceMask
            if (sequence == 0L) {
                timestamp = now()
                while (timestamp <= lastTimestamp) {
                    timestamp = now()
                }
            }
        } else {
            sequence = 0
        }

        lastTimestamp = timestamp
        return Snowflake((timestamp - epoch shl timestampLeftShift) or staticIDComponents or sequence)
    }
}