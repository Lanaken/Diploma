package org.bmstu.joins.algorithms

import org.bmstu.tables.Row
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption


class TblWriter(private val outputPath: Path) {

    // Открываем BufferedWriter сразу в CREATE + TRUNCATE_EXISTING (то есть, файл очищается)
    private var writer = Files.newBufferedWriter(
        outputPath,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE
    )

    /**
     * Записать одну строку (Row) в файл.
     * Если columnOrder != null, будем писать именно в том порядке (имен столбцов),
     * что передали. Иначе – просто row.columns.values в произвольном порядке.
     */
    @Synchronized
    fun writeRow(row: Row, columnOrder: List<String>? = null) {
        val cols = row.columns
        val line = if (columnOrder != null) {
            columnOrder.joinToString("|") { colName ->
                // если в этой строке нет колонки с именем colName, запишем пустую
                cols[colName]?.toString() ?: ""
            }
        } else {
            // без явного порядка – просто все значения из Map
            cols.values.joinToString("|") { it.toString() }
        }
        writer.write(line)
        writer.newLine()
    }

    /**
     * Записать сразу несколько строк.
     * Повторно вызывает writeRow(...) для каждой строки.
     */
    @Synchronized
    fun writeRows(rows: List<Row>, columnOrder: List<String>? = null) {
        for (row in rows) {
            writeRow(row, columnOrder)
        }
    }

    /**
     * Важно: когда все join-итераторы (или другие куски кода) дописали
     * свои строки, необходимо вызвать close(), чтобы закрыть файл.
     */
    fun close() {
        writer.close()
    }
}
