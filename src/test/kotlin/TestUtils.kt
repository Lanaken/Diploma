package org.bmstu.util

import java.nio.file.Files
import java.nio.file.Path
import logical.Memo
import operators.ScanOp

fun createTbl(lines: List<String>): Path {
    val tmp = Files.createTempFile("tbltest", ".tbl")
    Files.newBufferedWriter(tmp).use { w ->
        lines.forEach { w.write(it); w.newLine() }
    }
    return tmp
}

fun Memo.scanGroup(table: String, card: Long) =
    insert(ScanOp(table, card), emptyList())!!
        .let { groupOf(setOf(table)) }

