package org.bmstu.util

import kotlin.test.Test
import kotlin.test.assertEquals
import org.bmstu.reader.CType
import org.bmstu.reader.Col
import org.bmstu.reader.YSchema
import sort.ExternalStringRidSorter

class ExternalStringRidSorterTest {
    private val schema = YSchema(
        table = "test",
        columns = listOf(
            Col("key", CType.STRING),
            Col("value", CType.INT)
        )
    )

    @Test
    fun `sort small in memory`() {
        val lines = listOf(
            "b|2",
            "a|1",
            "c|3",
            "b|4"
        )
        val tbl = createTbl(lines)

        val sorter = ExternalStringRidSorter(memLimitBytes = Long.MAX_VALUE)
        val seq = sorter.sort(tbl, schema, "key", keyLen = 1)
        val result = seq.toList()

        // keys should be sorted and padded/truncated to length 1
        val keys = result.map { it.first }
        assertEquals(listOf("a", "b", "b", "c"), keys)
        // check RIDs correspond to original offsets
        val values = result.map { it.second.pos }
        // compute offsets: each line + \n, offset of each line start
        val offsets = mutableListOf<Long>()
        var off = 0L
        lines.forEach { line ->
            offsets.add(off)
            off += line.toByteArray().size + 1
        }
        val expected = listOf(
            offsets[1], // a
            offsets[0], offsets[3], // b occurrences
            offsets[2]  // c
        )
        assertEquals(expected, values)
    }

    @Test
    fun `sort with external chunks`() {
        val lines = List(50) { if (it % 2 == 0) "x|${it}" else "y|${it}" }
        val tbl = createTbl(lines)

        val sorter = ExternalStringRidSorter(memLimitBytes = 10)
        val seq = sorter.sort(tbl, schema, "key", keyLen = 1)
        val result = seq.toList()

        val keys = result.map { it.first }
        assertEquals(List(25) { "x" } + List(25) { "y" }, keys)
    }
}
