package org.bmstu.reader

import java.io.FileInputStream
import java.lang.AutoCloseable
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate
import org.bmstu.tables.Row
import reader.Rid
import util.accumulateTime

class TablesReader(
    private val tblPath: Path,
    private val schema: YSchema
): AutoCloseable {
    private val fileStream = Files.newBufferedReader(tblPath)
    private val bufferedFileStream = Files.newInputStream(tblPath).buffered()
    private val accessRandomFile = FileInputStream(tblPath.toFile()).channel
    private val parsers: List<ColParser> = schema.columns.map { c ->
        when (c.type) {
            CType.INT -> { s: String -> s.toInt() }
            CType.BIGINT -> { s: String -> s.toLong() }
            CType.DECIMAL -> { s: String -> BigDecimal(s) }
            CType.DATE -> { s: String -> LocalDate.parse(s) }
            CType.CHAR -> { s: String -> s }
            CType.STRING -> { s: String -> s }
        }
    }
    private val buffer = ByteBuffer.allocate(16192)

    fun readTbl(consume: (Row) -> Unit) {

        fileStream.useLines { lines ->
            lines.forEach { line ->
                if (line.isEmpty()) return@forEach
                val raw = line.split('|')
                val row = HashMap<String, Any?>(schema.columns.size)
                schema.columns.forEachIndexed { i, col ->
                    row[col.name] = parsers[i].invoke(raw[i])
                }
                consume(Row(row))
            }
        }
    }

    fun readTblWithOffsets(
        tbl: Path,
        schema: YSchema,
        keyCol: String,
        consume: (key: Any, rid: Rid) -> Unit
    ) {
        val keyPos = schema.columns.indexOfFirst { it.name == keyCol }
        require(keyPos >= 0) { "Колонка $keyCol не найдена" }

        bufferedFileStream.let { input ->
            var offset = 0L
            input.bufferedReader().forEachLine { line ->
                if (line.isEmpty()) {
                    offset += 1; return@forEachLine
                }
                val raw = line.split('|')
                val key = parsers[keyPos].invoke(raw[keyPos])
                consume(key, Rid(offset))
                offset += line.toByteArray().size + 1
            }
        }
    }

    fun readRowByRid(
        rid: Rid,
        schema: YSchema,
    ): Row {
        val ch = accessRandomFile
        buffer.clear()

        accumulateTime("io operations position") {
            ch.position(rid.pos)
        }
        accumulateTime("io operations read") {
            ch.read(buffer)
        }

        buffer.flip()
        val limit = buffer.limit()
        var lineEnd = 0
        accumulateTime("Пополнение буффера") {
            while (lineEnd < limit) {
                if (buffer.get(lineEnd) == '\n'.code.toByte()) break
                lineEnd++
            }
        }


        val lineBytes = ByteArray(lineEnd)
        accumulateTime("Получение данных из буфера") {
            buffer.position(0)
            buffer.get(lineBytes, 0, lineEnd)
        }

        lateinit var raw: List<String>
        accumulateTime("Разбиение строк") {
            raw = String(lineBytes).split('|')
        }
        lateinit var values: HashMap<String, Any?>
        accumulateTime("Создание хэш таблицы для разбора строк") {
            values = HashMap<String, Any?>(schema.columns.size)
        }
        accumulateTime("Разбор строк") {
            schema.columns.forEachIndexed { idx, col ->
                values[col.name] = parsers[idx](raw[idx])
            }
        }

        return Row(values)
    }



    fun readTblWithRids(
        tbl: Path,
        schema: YSchema,
        consume: (Row, Rid) -> Unit
    ) {
        bufferedFileStream.let { input ->
            var offset = 0L
            input.bufferedReader().forEachLine { line ->
                if (line.isEmpty()) {
                    offset += 1; return@forEachLine
                }
                val raw = line.split('|')
                val row = HashMap<String, Any?>(schema.columns.size)
                schema.columns.forEachIndexed { i, col ->
                    row[col.name] = parsers[i](raw[i])
                }
                consume(Row(row), Rid(offset))
                offset += line.toByteArray().size + 1
            }
        }
    }

    fun isSortedBy(tbl: Path, colIdx: Int): Boolean {
        var prev: Long? = null
        Files.newBufferedReader(tbl).useLines { lines ->
            lines.forEach { line ->
                val key = line.split('|')[colIdx].toLong()
                if (prev != null && key < prev!!) return false
                prev = key
            }
        }
        return true
    }

    override fun close() {
        fileStream.close()
        bufferedFileStream.close()
        accessRandomFile.close()
    }
}
