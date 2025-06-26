package org.bmstu.joins.algorithms

import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate
import org.bmstu.reader.CType
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row
import util.accumulateTime

class ScanIterator2(
    private val tbl: Path,
    private val schema: YSchema
) : TupleIterator {
    private var lineIterator: Iterator<String>? = null
    private val parsers: List<(String) -> Any> = schema.columns.map { col ->
        when (col.type) {
            CType.INT -> { s: String -> s.toInt() }
            CType.BIGINT -> { s: String -> s.toLong() }
            CType.DECIMAL -> { s: String -> BigDecimal(s) }
            CType.DATE -> { s: String -> LocalDate.parse(s) }
            CType.CHAR,
            CType.STRING -> { s: String -> s }
        }
    }

    private var reader: java.io.BufferedReader? = null

    override fun open() {
        reader = Files.newBufferedReader(tbl)
        lineIterator = reader!!.lineSequence().iterator()
    }

    override fun next(): Row? {
        val iter = lineIterator ?: return null
        while (iter.hasNext()) {
            val line = iter.next()
            if (line.isEmpty()) continue
            val raw = line.split('|')
            val values = LinkedHashMap<String, Any?>(schema.columns.size)
            schema.columns.forEachIndexed { i, col ->
                val token = raw.getOrNull(i) ?: ""
                values[col.name] = token
                    .ifEmpty { null }
                    ?.let { parsers[i](it) }
            }
            return Row(values)
        }
        return null
    }


    override fun close() {
        reader?.close()
        lineIterator = null
    }
}
