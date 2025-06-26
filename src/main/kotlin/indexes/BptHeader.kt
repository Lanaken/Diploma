package org.bmstu.indexes

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel

const val MAGIC = 0x42505431 // 'BPT1'
const val HEADER_SIZE = 32

data class BptHeader(
    val pageSize: Int,
    val keyType: Byte,
    val rootPid: Long,
    val pagesUsed: Long
) {
    fun toBuffer(): ByteBuffer = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN).apply {
        putInt(MAGIC)
        putInt(pageSize)
        put(keyType)
        put(ByteArray(3)) // reserved
        putLong(rootPid)
        putLong(pagesUsed)
        flip()
    }

    companion object {
        fun read(ch: FileChannel): BptHeader {
            val buf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN)
            ch.read(buf, 0)
            buf.flip()
            require(buf.int == MAGIC) { "Bad magic, not .bpt" }
            val ps = buf.int
            val kt = buf.get()
            buf.position(buf.position() + 3) // skip reserved
            val root = buf.long
            val used = buf.long
            return BptHeader(ps, kt, root, used)
        }
    }
}