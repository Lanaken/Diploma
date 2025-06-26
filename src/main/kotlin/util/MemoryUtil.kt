package org.bmstu.util

import java.nio.file.Files
import java.nio.file.Path
import statistic.Catalog

object MemoryUtil {

    fun isMemoryEnough(tbl: Path, overheadFactor: Double = 1.5): Boolean {
        val rt = Runtime.getRuntime()
        val maxHeap   = rt.maxMemory()
        val usedHeap  = rt.totalMemory() - rt.freeMemory()
        val availHeap = maxHeap - usedHeap
        val fileSize  = Files.size(tbl)
        return availHeap > fileSize * overheadFactor
    }

    fun freeHeap(): Long {
        val rt = Runtime.getRuntime()
        return rt.maxMemory() - (rt.totalMemory() - rt.freeMemory())
    }
    fun isMemoryEnough(bytesNeeded: Long, overhead: Double = 1.2): Boolean =
        freeHeap() > bytesNeeded * overhead

    fun canFitInRam(table: String): Boolean {
        val need = Catalog.stats(table).rows * Catalog.stats(table).rowSizeBytes
        return freeHeap() > need * 2.5
    }

    fun canFitInRam(group: logical.Group, overhead: Double = 2.5): Boolean {
        val bytesNeeded = group.tables.sumOf { tbl ->
            val st = Catalog.stats(tbl)
            (st.rows * st.rowSizeBytes)
        }
        return freeHeap() > bytesNeeded * overhead
    }
}