    package org.bmstu.indexes.char

    import java.nio.ByteBuffer
    import java.nio.channels.FileChannel
    import java.nio.file.Path
    import java.nio.file.StandardOpenOption.READ
    import org.bmstu.indexes.BptHeader
    import org.bmstu.indexes.BufferManager
    import org.bmstu.indexes.Index
    import org.bmstu.indexes.KeyRange
    import reader.Rid

    class OnDiskBPlusTreeIndexStr private constructor(
        private val header: BptHeader,
        private val bufMgr: BufferManager,
        private val keyLen: Int = 256
    ) : Index<String> {

        companion object {
            fun open(path: Path, keyLen: Int = 256): OnDiskBPlusTreeIndexStr {
                val ch = FileChannel.open(path, READ)
                val hdr = BptHeader.read(ch)
                require(hdr.keyType.toInt() == 2) { "KeyType != STRING" }
                return OnDiskBPlusTreeIndexStr(hdr, BufferManager(ch, hdr.pageSize), keyLen)
            }
        }

        private fun readKey(buf: ByteBuffer, pos: Int): String {
            val bytes = ByteArray(keyLen)
            buf.position(pos)
            buf.get(bytes)
            return bytes.decodeToString()
        }

        override fun seekEqual(key: String): List<Rid> {
            val normKey = key.padEnd(keyLen, ' ').take(keyLen)
            var pid = header.rootPid
            println(">>> seekEqual: found first leaf pid = $pid")
            while (true) {
                val page = bufMgr.page(pid)
                val type = page.get(0)
                val cnt  = page.getShort(1).toInt()
                if (type.toInt() == 0) {
                    var low = 0; var high = cnt - 1; var idx = cnt
                    while (low <= high) {
                        val mid    = (low + high) ushr 1
                        val midKey = readKey(page, 3 + mid * keyLen)
                        if (normKey <= midKey) { idx = mid; high = mid - 1 } else low = mid + 1
                    }
                    val childPos = 3 + cnt * keyLen + idx * 8
                    pid = page.getLong(childPos)
                } else {
                    break
                }
            }
            // collect RIDs across leaf chain
            val result = mutableListOf<Rid>()
            var leafPid = pid
            while (leafPid != -1L) {
                println(">>> visiting leaf pid = $leafPid")
                val page = bufMgr.page(leafPid)
                val cnt  = page.getShort(1).toInt()
                println("    entries on this leaf: $cnt slots")
                var pos  = 3
                var found = false
                repeat(cnt) {
                    val k  = readKey(page, pos)
                    pos += keyLen
                    val sz = page.getShort(pos).toInt(); pos += 2
                    if (k == normKey) {
                        println("    KEY MATCH in this leaf: $sz RIDs")
                        repeat(sz) {
                            result += Rid(page.getLong(pos)); pos += 8
                        }
                        found = true
                        return@repeat
                    } else {
                        pos += sz * 8
                    }
                }
                if (!found) {
                    println("    KEY NOT FOUND in this leaf → stopping")
                    break
                }
                // move to next leaf via pointer stored at end
                val next = page.getLong(header.pageSize - 8)
                println("    nextLeafPid = $next")
                leafPid = next
            }
            return result
        }

        override fun seekRange(range: KeyRange<String>): Iterator<Pair<String, List<Rid>>> = object : Iterator<Pair<String, List<Rid>>> {
            private var leafPid: Long = findFirstLeaf(range.lower)
            private var buffer: List<Pair<String, List<Rid>>> = emptyList()
            private var idx = 0
            override fun hasNext(): Boolean {
                while (idx >= buffer.size && leafPid != -1L) fillBuffer(range)
                return idx < buffer.size
            }
            override fun next(): Pair<String, List<Rid>> {
                if (!hasNext()) throw NoSuchElementException()
                return buffer[idx++] }
            private fun fillBuffer(range: KeyRange<String>) {
                if (leafPid == -1L) return
                val page = bufMgr.page(leafPid)
                val cnt  = page.getShort(1).toInt()
                val lst  = mutableListOf<Pair<String, List<Rid>>>()
                var pos  = 3
                val upper = range.upper?.padEnd(keyLen, ' ')
                repeat(cnt) {
                    val k  = readKey(page, pos); pos += keyLen
                    val sz = page.getShort(pos).toInt(); pos += 2
                    val r  = mutableListOf<Rid>()
                    repeat(sz) { r += Rid(page.getLong(pos)); pos += 8 }
                    if (range.contains(k.trimEnd(' '))) lst += k.trimEnd(' ') to r
                    else if (upper != null && k > upper) { leafPid = -1L; return@repeat }
                }
                buffer = lst; idx = 0
                leafPid = page.getLong(header.pageSize - 8)
            }
            private fun findFirstLeaf(lower: String?): Long {
                var pid = header.rootPid
                val low = lower?.padEnd(keyLen, ' ') ?: " ".repeat(keyLen)
                while (true) {
                    val page = bufMgr.page(pid)
                    if (page.get(0).toInt() == 1) return pid
                    val cnt = page.getShort(1).toInt()
                    var lowI = 0; var highI = cnt - 1; var idx = cnt
                    while (lowI <= highI) {
                        val mid    = (lowI + highI) ushr 1
                        val midKey = readKey(page, 3 + mid * keyLen)
                        if (low <= midKey) { idx = mid; highI = mid - 1 } else lowI = mid + 1
                    }
                    pid = page.getLong(3 + cnt * keyLen + idx * 8)
                }
            }
        }
    }
