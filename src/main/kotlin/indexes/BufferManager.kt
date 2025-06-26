package org.bmstu.indexes

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.util.LinkedHashMap

class BufferManager(
    private val ch: FileChannel,
    private val pageSize: Int,
    capacity: Int = 64
) {
    private val cache: LinkedHashMap<Long, ByteBuffer> = object : LinkedHashMap<Long, ByteBuffer>(capacity, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Long, ByteBuffer>?): Boolean = size > capacity
    }

    fun page(pid: Long): ByteBuffer {
        return cache.getOrPut(pid) {
            val buf = ByteBuffer.allocate(pageSize).order(ByteOrder.LITTLE_ENDIAN)
            ch.read(buf, HEADER_SIZE.toLong() + pid * pageSize)
            buf.flip(); buf
        }
    }
}