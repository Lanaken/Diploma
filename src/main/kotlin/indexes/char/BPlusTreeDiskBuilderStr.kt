package org.bmstu.indexes.char

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE
import org.bmstu.indexes.BptHeader
import org.bmstu.indexes.HEADER_SIZE
import reader.Rid
import org.bmstu.reader.TablesLoader
import org.bmstu.reader.YSchema
import sort.ExternalStringRidSorter
import org.bmstu.util.MemoryUtil

object BPlusTreeDiskBuilderStr {
    fun build(
        tbl: Path,
        schema: YSchema,
        keyCol: String,
        indexPath: Path,
        keyLen: Int = 256,
        pageSize: Int = 8192,
        order: Int = 64
    ) {
        require(keyLen in 1..255) { "keyLen must be 1..255" }
        Files.createDirectories(indexPath.parent)

        FileChannel.open(indexPath, CREATE, WRITE, READ, TRUNCATE_EXISTING).use { ch ->
            ch.write(ByteBuffer.allocate(HEADER_SIZE))

            val sortedSeq: Sequence<Pair<String, Rid>> = if (
                MemoryUtil.isMemoryEnough(tbl)
            ) {
                val all = mutableListOf<Pair<String, Rid>>()
                TablesLoader.readTblWithOffsets(tbl, schema, keyCol) { keyAny, rid ->
                    val s = keyAny as String
                    val norm = s.padEnd(keyLen, ' ').take(keyLen)
                    all += norm to rid
                }
                all.sortBy { it.first }
                all.asSequence()
            } else {
                // external k-way merge sort
                ExternalStringRidSorter().sort(tbl, schema, keyCol, keyLen)
            }

            // 3) строим листовые страницы, связывая их в цепочку
            val leaves = mutableListOf<Long>()
            val leafBuf = LeafBuildBuffer(order, keyLen, pageSize)
            for ((k, r) in sortedSeq) {
                if (leafBuf.add(k, r)) {
                    leaves += leafBuf.flush(ch)
                    leafBuf.add(k, r)
                }
            }
            if (!leafBuf.isEmpty()) {
                leaves += leafBuf.flush(ch)
            }
            println(leafBuf.automobileCount)
            // 4) строим внутренние уровни (как обычно)
            var current = leaves.toMutableList()
            while (current.size > 1) {
                val next = mutableListOf<Pair<String, Long>>()
                var i = 0
                while (i < current.size) {
                    val chunk = current.subList(i, minOf(i + order + 1, current.size))
                    val children = chunk.toList()
                    val keysHere = mutableListOf<String>()
                    // ключи: minKey каждой ветви, кроме первой
                    for ((idx, pid) in children.withIndex()) {
                        if (idx > 0)
                            keysHere += readMinKeyStr(ch, pid, pageSize, keyLen)
                    }
                    // пишем страницу
                    val buf = ByteBuffer.allocate(pageSize).order(ByteOrder.LITTLE_ENDIAN)
                    buf.put(0, 0.toByte())
                    buf.putShort(1, keysHere.size.toShort())
                    var pos = 3
                    // ключи
                    for (k2 in keysHere) {
                        val b = k2.toByteArray(Charsets.UTF_8)
                        val cell = ByteArray(keyLen) { ' '.code.toByte() }
                        System.arraycopy(b, 0, cell, 0, minOf(b.size, keyLen))
                        buf.position(pos); buf.put(cell); pos += keyLen
                    }
                    // указатели на детей
                    for (pid in children) {
                        buf.putLong(pos, pid); pos += 8
                    }
                    buf.position(pageSize); buf.flip()
                    val pid = writePage(ch, buf)
                    next += readMinKeyStr(ch, pid, pageSize, keyLen) to pid
                    i += chunk.size
                }
                current = next.map { it.second }.toMutableList()
            }

            // 5) записать header
            val root = current.first()
            val pagesUsed = (ch.size() - HEADER_SIZE) / pageSize
            ch.write(BptHeader(pageSize, 2, root, pagesUsed).toBuffer(), 0)
        }
    }

    private fun writePage(ch: FileChannel, buf: ByteBuffer): Long {
        val pid = (ch.size() - HEADER_SIZE) / buf.capacity()
        ch.write(buf, HEADER_SIZE + pid * buf.capacity().toLong())
        return pid
    }

    private fun readMinKeyStr(
        ch: FileChannel, pid: Long, pageSize: Int, keyLen: Int
    ): String {
        val buf = ByteBuffer.allocate(3 + keyLen).order(ByteOrder.LITTLE_ENDIAN)
        ch.read(buf, HEADER_SIZE + pid * pageSize)
        buf.flip()
        buf.get()        // pageType
        buf.getShort()   // keysCnt
        val bytes = ByteArray(keyLen)
        buf.get(bytes)
        return bytes.decodeToString().trimEnd(' ')
    }

    private class LeafBuildBuffer(
        private val order: Int,
        private val keyLen: Int,
        private val pageSize: Int
    ) {
        private val keys = mutableListOf<String>()
        private val ridLists = mutableListOf<MutableList<Rid>>()
        private var prevLeaf: Long? = null
        private var est = 3 // 1B type + 2B count
        var automobileCount = 0

        /** Добавить; вернуть true, если перед добавлением нужно flush(). */
        fun add(key: String, rid: Rid): Boolean {
            if (key == "AUTOMOBILE")
                automobileCount++
            val isNew = keys.isEmpty() || key != keys.last()
            val need = if (isNew) keyLen + 2 + 8 else 8
            if (est + need > pageSize - 8) return true
            est += need
            if (isNew) {
                keys += key
                ridLists += mutableListOf(rid)
            } else {
                ridLists.last().add(rid)
            }
            return keys.size > order
        }

        fun flush(ch: FileChannel): Long {
            val buf = ByteBuffer.allocate(pageSize).order(ByteOrder.LITTLE_ENDIAN)
            buf.put(0, 1.toByte())
            buf.putShort(1, keys.size.toShort())
            var pos = 3
            for ((i, k) in keys.withIndex()) {
                val b = k.toByteArray(Charsets.UTF_8)
                val cell = ByteArray(keyLen) { ' '.code.toByte() }
                System.arraycopy(b, 0, cell, 0, minOf(b.size, keyLen))
                buf.position(pos); buf.put(cell); pos += keyLen
                buf.putShort(pos, ridLists[i].size.toShort()); pos += 2
                for (r in ridLists[i]) {
                    buf.putLong(pos, r.pos); pos += 8
                }
            }
            buf.putLong(pageSize - 8, -1L)
            buf.position(pageSize); buf.flip()

            val pid = writePage(ch, buf)
            prevLeaf?.let { p ->
                val patch = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).apply {
                    putLong(pid)
                    flip()
                }
                ch.write(patch, HEADER_SIZE + p * pageSize + (pageSize - 8))
            }
            prevLeaf = pid
            keys.clear()
            ridLists.clear()
            est = 3
            return pid
        }

        fun isEmpty() = keys.isEmpty()
    }
}
