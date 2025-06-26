package org.bmstu.indexes

import reader.Rid
import org.bmstu.reader.TablesLoader
import org.bmstu.reader.YSchema
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.PriorityQueue
import kotlin.io.path.createTempDirectory

/**
 * Внешняя сортировка (Int key, Rid) – бинарный формат записи:
 *   4 байта key  + 8 байт ridPos   (Little-Endian)
 */
object ExternalIntRidSorter {

    /**
     * Читает таблицу `tbl`, вытаскивает (key,Rid) из колонки `keyCol`,
     * сортирует поток и возвращает ленивую последовательность пар
     * в порядке возрастания ключа.
     *
     * При достаточной памяти делает in-memory sort, иначе – k-way merge.
     */
    fun sort(
        tbl: Path,
        schema: YSchema,
        keyCol: String,
        memLimitBytes: Long = Runtime.getRuntime().maxMemory() / 4,  // как в строковой версии
    ): Sequence<Pair<Int, Rid>> {

        /* ---------- 1. Читаем .tbl и режем на отсортированные чанки ---------- */
        val tmpDir     = createTempDirectory("ext-sort-int")
        var chunkFiles = mutableListOf<Path>()

        val maxRecsInMem = (memLimitBytes / 12).coerceAtLeast(1)          // 12 байт на запись
        val bufKeys      = IntArray(maxRecsInMem.toInt())
        val bufRids      = LongArray(maxRecsInMem.toInt())
        var buffered     = 0

        fun flushChunk() {
            if (buffered == 0) return
            // сортировка индексов (чтобы вместе переставить key и rid)
            val order = (0 until buffered).sortedBy { bufKeys[it] }
            val tmpKeys = IntArray(buffered)  { bufKeys[order[it]] }
            val tmpRids = LongArray(buffered) { bufRids[order[it]] }
            System.arraycopy(tmpKeys, 0, bufKeys, 0, buffered)
            System.arraycopy(tmpRids, 0, bufRids, 0, buffered)

            // бинарная запись
            val chunk = Files.createTempFile(tmpDir, "chunk", ".bin")
            BufferedOutputStream(Files.newOutputStream(chunk, CREATE, WRITE)).use { out ->
                val bb = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                repeat(buffered) { i ->
                    bb.clear()
                    bb.putInt(bufKeys[i]).putLong(bufRids[i]).flip()
                    out.write(bb.array(), 0, 12)
                }
            }
            chunkFiles.add(chunk)
            buffered = 0
        }

        TablesLoader.readTblWithOffsets(tbl, schema, keyCol) { keyAny, rid ->
            bufKeys[buffered] = keyAny as Int
            bufRids[buffered] = rid.pos
            buffered++
            if (buffered == bufKeys.size) flushChunk()
        }
        flushChunk()

        /* ---------- 2. k-way merge по min-heap ---------- */
        data class Node(val key: Int, val ridPos: Long, val stream: BufferedInputStream)

        val pq = PriorityQueue<Node>(compareBy { it.key })

        fun readNext(s: BufferedInputStream): Node? {
            val arr = ByteArray(12)
            val read = s.readNBytes(arr, 0, 12)
            if (read < 12) return null
            val bb = ByteBuffer.wrap(arr).order(ByteOrder.LITTLE_ENDIAN)
            return Node(bb.int, bb.long, s)
        }

        // инициализируем PQ первым элементом каждого чанка
        for (file in chunkFiles) {
            val ins = BufferedInputStream(Files.newInputStream(file, READ))
            readNext(ins)?.let { pq += it } ?: ins.close()
        }

        return sequence {
            while (pq.isNotEmpty()) {
                val n = pq.poll()
                yield(n.key to Rid(n.ridPos))
                readNext(n.stream)?.let { pq += it } ?: n.stream.close()
            }
            // clean-up
            chunkFiles.forEach { Files.deleteIfExists(it) }
            Files.deleteIfExists(tmpDir)
        }
    }
}
