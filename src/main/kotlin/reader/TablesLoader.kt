package org.bmstu.reader

import java.io.RandomAccessFile
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate
import org.bmstu.tables.Row
import reader.Rid

class TablesLoader {
    companion object {
        fun listTableNamesFromDir(dir: String): List<Path> {
            val path = Path.of(dir)

            return if (Files.exists(path) && Files.isDirectory(path)) {
                Files.list(path)
                    .filter { Files.isRegularFile(it) && it.toString().endsWith(".tbl") }
                    .toList()
            } else {
                emptyList()
            }
        }

        fun readTbl(tbl: Path, schema: YSchema, consume: (Row) -> Unit) {
            val parsers = makeParsers(schema.columns)

            Files.newBufferedReader(tbl).useLines { lines ->
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

        private fun makeParsers(cols: List<Col>): List<ColParser> = cols.map { c ->
            when (c.type) {
                CType.INT -> { s: String -> s.toInt() }
                CType.BIGINT -> { s: String -> s.toLong() }
                CType.DECIMAL -> { s: String -> BigDecimal(s) }
                CType.DATE -> { s: String -> LocalDate.parse(s) }
                CType.CHAR -> { s: String -> s }
                CType.STRING -> { s: String -> s }
            }
        }

        fun readTblWithOffsets(
            tbl: Path,
            schema: YSchema,
            keyCol: String,
            consume: (key: Any, rid: Rid) -> Unit
        ) {
            val parsers = makeParsers(schema.columns)
            val keyPos  = schema.columns.indexOfFirst { it.name == keyCol }
            require(keyPos >= 0) { "Колонка $keyCol не найдена" }

            Files.newInputStream(tbl).buffered().use { input ->
                var offset = 0L
                input.bufferedReader().forEachLine { line ->
                    if (line.isEmpty()) { offset += 1; return@forEachLine }
                    val raw = line.split('|')
                    val key = parsers[keyPos].invoke(raw[keyPos])
                    consume(key, Rid(offset))
                    offset += line.toByteArray().size + 1
                }
            }
        }

        fun readRowByRid(tbl: Path, rid: Rid, schema: YSchema): Row {
            val parsers = makeParsers(schema.columns)

            RandomAccessFile(tbl.toFile(), "r").use { raf ->
                raf.seek(rid.pos)
                val line = raf.readLine() ?: ""
                val raw  = line.split('|')
                val values = HashMap<String, Any?>(schema.columns.size)
                schema.columns.forEachIndexed { i, col ->
                    values[col.name] = parsers[i](raw[i])
                }
                return Row(values)
            }
        }

        fun readTblWithRids(
            tbl: Path,
            schema: YSchema,
            consume: (Row, Rid) -> Unit
        ) {
            val parsers = makeParsers(schema.columns)

            Files.newInputStream(tbl).buffered().use { input ->
                var offset = 0L
                input.bufferedReader().forEachLine { line ->
                    if (line.isEmpty()) { offset += 1; return@forEachLine }
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

}