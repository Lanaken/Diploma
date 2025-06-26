package sort

import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.PriorityQueue
import org.bmstu.reader.TablesLoader
import org.bmstu.reader.YSchema
import reader.Rid


class ExternalStringRidSorter(
    private val tmpDir: Path = Files.createTempDirectory("ext-sort"),
    private val memLimitBytes: Long = Runtime.getRuntime().maxMemory() / 4
) {
    fun sort(
        tbl: Path,
        schema: YSchema,
        keyCol: String,
        keyLen: Int
    ): Sequence<Pair<String, Rid>> {
        val chunkFiles = mutableListOf<Path>()
        val buffer = mutableListOf<Pair<String, Rid>>()
        var bufferedBytes = 0L
        TablesLoader.Companion.readTblWithOffsets(tbl, schema, keyCol) { keyAny, rid ->
            val raw = keyAny as String
            val norm = raw.padEnd(keyLen, ' ').take(keyLen)
            buffer.add(norm to rid)
            bufferedBytes += norm.toByteArray(StandardCharsets.UTF_8).size + 8
            if (bufferedBytes > memLimitBytes) {
                chunkFiles.add(writeChunk(buffer, keyLen))
                buffer.clear()
                bufferedBytes = 0L
            }
        }
        if (buffer.isNotEmpty()) {
            chunkFiles.add(writeChunk(buffer, keyLen))
            buffer.clear()
        }

        data class Node(val key: String, val rid: Rid, val reader: BufferedReader)
        val pq = PriorityQueue<Node>(compareBy { it.key })
        val readers = chunkFiles.map { path ->
            Files.newBufferedReader(path, StandardCharsets.UTF_8).apply {
                readLine()?.let { line ->
                    val (k, pos) = line.split(',', limit = 2)
                    pq.add(Node(k, Rid(pos.toLong()), this))
                }
            }
        }

        return sequence {
            while (pq.isNotEmpty()) {
                val node = pq.poll()
                yield(node.key to node.rid)
                node.reader.readLine()?.let { line ->
                    val (k, pos) = line.split(',', limit = 2)
                    pq.add(Node(k, Rid(pos.toLong()), node.reader))
                } ?: node.reader.close()
            }
            readers.forEach { it.close() }
            chunkFiles.forEach { Files.deleteIfExists(it) }
            Files.deleteIfExists(tmpDir)
        }
    }

    private fun writeChunk(
        buffer: List<Pair<String, Rid>>,
        keyLen: Int
    ): Path {
        val sorted = buffer.sortedBy { it.first }
        val chunk = Files.createTempFile(tmpDir, "chunk", ".tmp")
        Files.newBufferedWriter(chunk, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
        ).use { w ->
            for ((k, r) in sorted) {
                w.append(k)
                w.append(',')
                w.append(r.pos.toString())
                w.newLine()
            }
        }
        return chunk
    }
}