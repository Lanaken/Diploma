package org.bmstu.joins.algorithms

import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate
import org.bmstu.reader.CType
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row

/**
 * Optimised version of [ScanIterator].
 * Implements optimisation items 1-3 & 5 only:
 *  1) one-pass delimiter scan instead of String.split();
 *  2) reuses a pre-allocated token buffer and per-column parsers;
 *  3) pre-sizes the HashMap and skips allocation for empty tokens;
 *  5) no behavioural change for ScanIterator itself.
 */
class ScanIterator(
    private val tbl: Path,
    private val schema: YSchema
) : TupleIterator {

    private var lineIter: Iterator<String>? = null
    private var reader: java.io.BufferedReader? = null

    /** One parser per column (pre-computed once) */
    private val parsers: Array<(String) -> Any?> = Array(schema.columns.size) { idx ->
        when (val t = schema.columns[idx].type) {
            CType.INT     -> { s: String -> s.toInt() }
            CType.BIGINT  -> { s: String -> s.toLong() }
            CType.DECIMAL -> { s: String -> BigDecimal(s) }
            CType.DATE    -> { s: String -> LocalDate.parse(s) }
            CType.CHAR, CType.STRING -> { s: String -> s }
        }
    }

    // Scratch buffers reused between next() calls
    private val tokens    = Array<String?>(schema.columns.size) { null }
    private val valuesBuf = LinkedHashMap<String, Any?>(schema.columns.size)

    override fun open() {
        reader = Files.newBufferedReader(tbl)
        lineIter = reader!!.lineSequence().iterator()
    }

    override fun next(): Row? {
        val it = lineIter ?: return null
        while (it.hasNext()) {
            val line = it.next()
            if (line.isEmpty()) continue

            // ---------------- quick manual tokenizer ----------------
            var col = 0
            var lastPos = 0
            val len = line.length
            while (col < tokens.size && lastPos <= len) {
                val nextSep = line.indexOf('|', startIndex = lastPos)
                val end = if (nextSep == -1) len else nextSep
                tokens[col] = if (end == lastPos) null else line.substring(lastPos, end)
                col++
                lastPos = end + 1        // skip separator
            }
            // ----------------------------------------------------------------

            valuesBuf.clear()
            schema.columns.forEachIndexed { idx, column ->
                val tok = tokens[idx]
                if (tok != null) {                   // skip empty ⇒ NULL
                    valuesBuf[column.name] = parsers[idx](tok)
                }
            }
            return Row(HashMap(valuesBuf))           // copy to detach from buffer
        }
        return null
    }

    override fun close() {
        reader?.close()
        lineIter = null
    }
}