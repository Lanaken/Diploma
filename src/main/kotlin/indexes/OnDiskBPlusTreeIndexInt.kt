package org.bmstu.indexes

import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.ArrayList
import java.util.NoSuchElementException
import reader.Rid
import util.accumulateTime

class OnDiskBPlusTreeIndexInt private constructor(
    private val ch: FileChannel,
    private val header: BptHeader,
    private val bufMgr: BufferManager
) : Index<Int> {

    companion object {
        fun open(path: Path): OnDiskBPlusTreeIndexInt {
            val ch = FileChannel.open(path, READ)
            val hdr = BptHeader.read(ch)
            require(hdr.keyType.toInt() == 0) { "KeyType != INT" }
            return OnDiskBPlusTreeIndexInt(ch, hdr, BufferManager(ch, hdr.pageSize))
        }
    }

    override fun seekEqual(key: Int): List<Rid> {
            var pid = header.rootPid
            while (true) {
                val page = bufMgr.page(pid)
                val type = page.get(0)
                val keysCount = page.getShort(1).toInt()
                if (type.toInt() == 0) { // internal
                    // binary search
                    var low = 0
                    var high = keysCount - 1
                    var idx = keysCount
                    while (low <= high) {
                        val mid = (low + high) ushr 1
                        val midKey = page.getInt(3 + mid * 4)
                        if (key <= midKey) {
                            idx = mid;
                            high = mid - 1
                        } else low = mid + 1
                    }
                    val childPos = 3 + keysCount * 4 + idx * 8
                    pid = page.getLong(childPos)
                } else { // leaf
                    // linear scan leaf (cnt usually <= 128)
                    var pos = 3
                    repeat(keysCount) {
                        val k = page.getInt(pos)
                        val listSize = page.getShort(pos + 4).toInt()
                        pos += 6
                        if (k == key) {
                            val rids = ArrayList<Rid>(listSize)
                            repeat(listSize) {
                                val off = page.getLong(pos)
                                rids.add(Rid(off)); pos += 8
                            }
                            return rids
                        } else {
                            pos += listSize * 8
                        }
                    }
                    return emptyList()
                }
            }
    }

    override fun seekRange(range: KeyRange<Int>): Iterator<Pair<Int, List<Rid>>> = object : Iterator<Pair<Int, List<Rid>>> {
        private var leafPid: Long = findFirstLeaf(range.lower)
        private var buffer: List<Pair<Int, List<Rid>>> = emptyList()
        private var idx = 0
        override fun hasNext(): Boolean {
            while (idx >= buffer.size && leafPid != -1L) fillBuffer()
            return idx < buffer.size
        }
        override fun next(): Pair<Int, List<Rid>> {
            if (!hasNext()) throw NoSuchElementException()
            return buffer[idx++]
        }
        private fun fillBuffer() {
            if (leafPid == -1L) return
            val page = bufMgr.page(leafPid)
            val cnt = page.getShort(1).toInt()
            val list = ArrayList<Pair<Int, List<Rid>>>(cnt)
            var pos = 3
            repeat(cnt) {
                val k = page.getInt(pos); pos += 4
                val sz = page.getShort(pos).toInt(); pos += 2
                val r = mutableListOf<Rid>()
                repeat(sz) { r.add(Rid(page.getLong(pos))); pos += 8 }
                if (range.contains(k)) list.add(k to r) else if (range.upper != null && k > range.upper) { leafPid = -1; return@repeat }
            }
            buffer = list; idx = 0
            leafPid = page.getLong(header.pageSize - 8) // nextLeafId stored at end
            if (range.upper != null && (list.lastOrNull()?.first ?: Int.MIN_VALUE) > range.upper) leafPid = -1
        }
        private fun findFirstLeaf(lower: Int?): Long {
            var pid = header.rootPid
            val l = lower ?: Int.MIN_VALUE
            while (true) {
                val page = bufMgr.page(pid)
                val type = page.get(0)
                if (type.toInt() == 1) return pid // leaf
                val cnt = page.getShort(1).toInt()
                var low = 0; var high = cnt - 1; var idx = cnt
                while (low <= high) {
                    val mid = (low + high) ushr 1
                    val midKey = page.getInt(3 + mid * 4)
                    if (l <= midKey) { idx = mid; high = mid - 1 } else low = mid + 1
                }
                val childPos = 3 + cnt * 4 + idx * 8
                pid = page.getLong(childPos)
            }
        }
    }
}