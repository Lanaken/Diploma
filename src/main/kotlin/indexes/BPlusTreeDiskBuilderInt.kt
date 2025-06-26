package org.bmstu.indexes

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE
import org.bmstu.reader.TablesLoader
import org.bmstu.reader.YSchema
import org.bmstu.util.MemoryUtil
import reader.Rid

object BPlusTreeDiskBuilderInt {

    fun build(
        tbl: Path,
        schema: YSchema,
        keyCol: String,
        indexPath: Path,
        pageSize: Int = 8192,
        order: Int = 128
    ) {
        Files.createDirectories(indexPath.parent)

        FileChannel.open(indexPath, CREATE, WRITE, READ, TRUNCATE_EXISTING).use { ch ->
            /* 0) зарезервировать место под header */
            ch.write(ByteBuffer.allocate(HEADER_SIZE))

            /* 1) формируем отсортированный поток (key,Rid) */
            val sortedSeq: Sequence<Pair<Int, Rid>> = if (MemoryUtil.isMemoryEnough(tbl)) {
                val all = mutableListOf<Pair<Int, Rid>>()
                TablesLoader.readTblWithOffsets(tbl, schema, keyCol) { keyAny, rid ->
                    all += (keyAny as Int) to rid
                }
                all.sortBy { it.first }
                all.asSequence()
            } else {
                ExternalIntRidSorter.sort(tbl, schema, keyCol,)
            }

            /* 2) строим листовые страницы, связывая их вперёд */
            val leaves = mutableListOf<Long>()
            val leafBuf = LeafBuildBuffer(order, pageSize)
            for ((k, r) in sortedSeq) {
                if (leafBuf.add(k, r)) {
                    leaves += leafBuf.flush(ch)
                    leafBuf.add(k, r)
                }
            }
            if (!leafBuf.isEmpty()) leaves += leafBuf.flush(ch)

            /* 3) внутренние уровни */
            var currentLevel = leaves.toMutableList()
            while (currentLevel.size > 1) {
                val next = mutableListOf<Pair<Int, Long>>()
                var i = 0
                while (i < currentLevel.size) {
                    val chunk = currentLevel.subList(i, minOf(i + order + 1, currentLevel.size))
                    val childPids = chunk.toList()

                    val keysHere = mutableListOf<Int>()
                    for ((idx, pid) in childPids.withIndex()) {
                        if (idx > 0) keysHere += readMinKey(ch, pid, pageSize)
                    }

                    val buf = ByteBuffer.allocate(pageSize).order(ByteOrder.LITTLE_ENDIAN)
                    buf.put(0, 0)                      // INTERNAL
                    buf.putShort(1, keysHere.size.toShort())
                    var pos = 3
                    keysHere.forEach { buf.putInt(pos, it); pos += 4 }
                    childPids.forEach { buf.putLong(pos, it); pos += 8 }
                    buf.position(pageSize); buf.flip()

                    val pid = writePage(ch, buf)
                    next += readMinKey(ch, pid, pageSize) to pid
                    i += chunk.size
                }
                currentLevel = next.map { it.second }.toMutableList()
            }

            /* 4) header */
            val pagesUsed = (ch.size() - HEADER_SIZE) / pageSize
            ch.write(BptHeader(pageSize, 0, currentLevel.first(), pagesUsed).toBuffer(), 0)
        }
    }

    /* ----------------------- helpers ----------------------- */

    private fun writePage(ch: FileChannel, buf: ByteBuffer): Long {
        val pid = (ch.size() - HEADER_SIZE) / buf.capacity()
        ch.write(buf, HEADER_SIZE + pid * buf.capacity().toLong())
        return pid
    }

    private fun readMinKey(ch: FileChannel, pid: Long, pageSize: Int): Int {
        val tmp = ByteBuffer.allocate(7).order(ByteOrder.LITTLE_ENDIAN)
        ch.read(tmp, HEADER_SIZE + pid * pageSize)
        tmp.flip(); tmp.get(); // type
        val cnt = tmp.short
        return if (cnt.toInt() == 0) Int.MIN_VALUE else {
            val kBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
            ch.read(kBuf, HEADER_SIZE + pid * pageSize + 3)
            kBuf.flip(); kBuf.int
        }
    }

    /* -------- leaf build buffer (контроль по байтам) ------- */
    private class LeafBuildBuffer(
        private val order: Int,
        private val pageSize: Int
    ) {
        private val keys = mutableListOf<Int>()
        private val ridLists = mutableListOf<MutableList<Rid>>()
        private var prevLeafPid: Long? = null
        private var estBytes = 3  // 1B type + 2B count

        /** true, если надо flush() ПЕРЕД добавлением */
        fun add(key: Int, rid: Rid): Boolean {
            val isNewKey = keys.isEmpty() || key != keys.last()
            val needBytes = if (isNewKey) 4 + 2 + 8 else 8  // key(4) + cnt(2) + 1‑й RID(8)  |  лишь RID
            if (estBytes + needBytes > pageSize - 8) return true
            estBytes += needBytes
            if (isNewKey) {
                keys += key
                ridLists += mutableListOf(rid)
            } else {
                ridLists.last() += rid
            }
            return keys.size > order
        }

        fun flush(ch: FileChannel): Long {
            val buf = ByteBuffer.allocate(pageSize).order(ByteOrder.LITTLE_ENDIAN)
            buf.put(0, 1) // LEAF
            buf.putShort(1, keys.size.toShort())
            var pos = 3
            for (i in keys.indices) {
                buf.putInt(pos, keys[i]); pos += 4
                buf.putShort(pos, ridLists[i].size.toShort()); pos += 2
                for (r in ridLists[i]) {
                    buf.putLong(pos, r.pos); pos += 8
                }
            }
            buf.putLong(pageSize - 8, -1L) // nextLeaf = -1 (запатчим позже)
            buf.position(pageSize); buf.flip()

            val pid = writePage(ch, buf)

            /* патчим предыдущий лист */
            prevLeafPid?.let { prev ->
                val patch = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).apply {
                    putLong(pid); flip()
                }
                ch.write(patch, HEADER_SIZE + prev * pageSize + (pageSize - 8))
            }
            prevLeafPid = pid

            keys.clear(); ridLists.clear(); estBytes = 3
            return pid
        }

        fun isEmpty() = keys.isEmpty()
    }
}