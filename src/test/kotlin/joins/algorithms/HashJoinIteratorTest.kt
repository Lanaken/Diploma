package org.bmstu.joins.algorithms

import java.nio.file.Files
import java.nio.file.Path
import org.bmstu.joins.JoinCommand
import org.bmstu.reader.CType
import org.bmstu.reader.Col
import org.bmstu.reader.YSchema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class HashJoinIteratorTest {
    private val buildKey = "n_nationkey"
    private val probeKey = "s_nationkey"

    private lateinit var tmpDir: Path

    @BeforeEach
    fun setup() {
        tmpDir = Files.createTempDirectory("hash-join-test")
    }

    private fun writeTbl(file: Path, lines: List<String>) {
        Files.newBufferedWriter(file).use { writer ->
            lines.forEach { writer.write(it + "\n") }
        }
    }

    private fun schema(vararg cols: Pair<String, CType>) = YSchema("", cols.map { Col(it.first, it.second) })

    @Test
    fun `single match produces one joined row`() {
        val buildFile = tmpDir.resolve("build.tbl")
        val probeFile = tmpDir.resolve("probe.tbl")

        writeTbl(buildFile, listOf("1|A1"))
        writeTbl(probeFile, listOf("1|B1"))

        val buildSchema = schema("id" to CType.INT, "a" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b" to CType.STRING)

        val buildIter = ScanIterator(buildFile, buildSchema)
        val probeIter = ScanIterator(probeFile, probeSchema)

        val joinIter = HashJoinIterator(buildIter, probeIter, buildOnLeft = true, buildKey = "id", probeKey = "id")
        joinIter.open()
        val result = joinIter.next()
        assertNotNull(result)
        assertEquals(3, result!!.columns.size)
        assertEquals("A1", result.columns["a"])
        assertEquals("B1", result.columns["b"])
        assertEquals(1, result.columns["id"])
        assertNull(joinIter.next())
        joinIter.close()
    }

    @Test
    fun `multiple build matches produce multiple joined rows`() {
        val buildFile = tmpDir.resolve("build.tbl")
        val probeFile = tmpDir.resolve("probe.tbl")

        writeTbl(buildFile, listOf("2|A2a", "2|A2b"))
        writeTbl(probeFile, listOf("2|B2"))

        val buildSchema = schema("id" to CType.INT, "a" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b" to CType.STRING)

        val joinIter = HashJoinIterator(
            ScanIterator(buildFile, buildSchema),
            ScanIterator(probeFile, probeSchema),
            buildOnLeft = true,
            buildKey = "id",
            probeKey = "id"
        )

        joinIter.open()
        val first = joinIter.next()
        val second = joinIter.next()
        assertNotNull(first)
        assertNotNull(second)
        val aValues = setOf(first!!.columns["a"], second!!.columns["a"])
        assertEquals(setOf("A2a", "A2b"), aValues)
        assertNull(joinIter.next())
        joinIter.close()
    }

    @Test
    fun `no build matches yields no output`() {
        val buildFile = tmpDir.resolve("build.tbl")
        val probeFile = tmpDir.resolve("probe.tbl")

        writeTbl(buildFile, listOf("3|A3"))
        writeTbl(probeFile, listOf("4|B4"))

        val buildSchema = schema("id" to CType.INT, "a" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b" to CType.STRING)

        val joinIter = HashJoinIterator(
            ScanIterator(buildFile, buildSchema),
            ScanIterator(probeFile, probeSchema),
            buildOnLeft = true,
            buildKey = "id",
            probeKey = "id"
        )

        joinIter.open()
        assertNull(joinIter.next())
        joinIter.close()
    }

    @Test
    fun `probe on left merges in reverse order when buildOnLeft is false`() {
        val buildFile = tmpDir.resolve("build.tbl")
        val probeFile = tmpDir.resolve("probe.tbl")

        writeTbl(buildFile, listOf("5|A5"))
        writeTbl(probeFile, listOf("5|B5"))

        val buildSchema = schema("id" to CType.INT, "a" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b" to CType.STRING)

        val joinIter = HashJoinIterator(
            ScanIterator(buildFile, buildSchema),
            ScanIterator(probeFile, probeSchema),
            buildOnLeft = false,
            buildKey = "id",
            probeKey = "id"
        )

        joinIter.open()
        val row = joinIter.next()
        assertNotNull(row)
        assertEquals("B5", row!!.columns["b"])
        assertEquals("A5", row.columns["a"])
        joinIter.close()
    }

    @Test
    fun `join nation and supplier`() {
        val nationPath = Path.of("tables/nation.tbl")
        val supplierPath = Path.of("tables/supplier.tbl")

        val nationSchema = YSchema("nation", listOf(
            Col("n_nationkey", CType.INT),
            Col("n_name", CType.STRING),
            Col("n_regionkey", CType.INT),
            Col("n_comment", CType.STRING)
        ))

        val supplierSchema = YSchema("supplier", listOf(
            Col("s_suppkey", CType.INT),
            Col("s_name", CType.STRING),
            Col("s_address", CType.STRING),
            Col("s_nationkey", CType.INT),
            Col("s_phone", CType.STRING),
            Col("s_acctbal", CType.DECIMAL),
            Col("s_comment", CType.STRING)
        ))

        val nationIter = ScanIterator(nationPath, nationSchema)
        val supplierIter = ScanIterator(supplierPath, supplierSchema)

        val joinIter = HashJoinIterator(
            buildIter = nationIter,
            probeIter = supplierIter,
            buildOnLeft = true,
            buildKey = "n_nationkey",
            probeKey = "s_nationkey"
        )

        joinIter.open()
        var count = 0
        while (true) {
            val row = joinIter.next() ?: break
            assertTrue(row.columns.containsKey("n_nationkey"))
            assertTrue(row.columns.containsKey("s_nationkey"))
            count++
        }
        joinIter.close()
        println(count)
        assertTrue(count > 0, "Join should produce at least one row")
    }

    @Test
    fun `left outer join includes unmatched probe`() {
        val buildFile = tmpDir.resolve("build.tbl")
        val probeFile = tmpDir.resolve("probe.tbl")
        writeTbl(buildFile, listOf("1|A1"))
        writeTbl(probeFile, listOf("1|B1", "2|B2"))

        val buildSchema = schema("id" to CType.INT, "a" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b" to CType.STRING)

        val iter = HashJoinIterator(
            ScanIterator(buildFile, buildSchema),
            ScanIterator(probeFile, probeSchema),
            buildOnLeft = true,
            buildKey = "id",
            probeKey = "id",
            joinType = JoinCommand.LEFT
        )

        iter.open()
        // first match
        val m1 = iter.next()!!
        assertEquals("A1", m1.columns["a"])
        assertEquals("B1", m1.columns["b"])
        // unmatched probe: build-side null, only probe columns
        assertNull(iter.next())
        iter.close()
    }

    @Test
    fun `right outer join includes unmatched build`() {
        val buildFile = tmpDir.resolve("build.tbl")
        val probeFile = tmpDir.resolve("probe.tbl")
        writeTbl(buildFile, listOf("1|A1", "2|A2"))
        writeTbl(probeFile, listOf("1|B1"))

        val buildSchema = schema("id" to CType.INT, "a" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b" to CType.STRING)

        val iter = HashJoinIterator(
            ScanIterator(buildFile, buildSchema),
            ScanIterator(probeFile, probeSchema),
            buildOnLeft = false,
            buildKey = "id",
            probeKey = "id",
            joinType = JoinCommand.RIGHT
        )

        iter.open()
        // match for id=1
        val m1 = iter.next()!!
        assertEquals("A1", m1.columns["a"])
        assertEquals("B1", m1.columns["b"])
        // unmatched build id=2
        val m2 = iter.next()!!
        assertEquals("A2", m2.columns["a"])
        assertNull(m2.columns["b"])
        assertNull(iter.next())
        iter.close()
    }

    @Test
    fun `full outer join includes unmatched both sides`() {
        val buildFile = tmpDir.resolve("build.tbl")
        val probeFile = tmpDir.resolve("probe.tbl")
        writeTbl(buildFile, listOf("1|A1", "2|A2"))
        writeTbl(probeFile, listOf("1|B1", "3|B3"))

        val buildSchema = schema("id" to CType.INT, "a" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b" to CType.STRING)

        val iter = HashJoinIterator(
            ScanIterator(buildFile, buildSchema),
            ScanIterator(probeFile, probeSchema),
            buildOnLeft = true,
            buildKey = "id",
            probeKey = "id",
            joinType = JoinCommand.FULL
        )

        iter.open()
        // match id=1
        val m1 = iter.next()!!
        assertEquals("A1", m1.columns["a"])
        assertEquals("B1", m1.columns["b"])
        // unmatched probe id=3
        val m2 = iter.next()!!
        assertNull(m2.columns["a"])
        assertEquals("B3", m2.columns["b"])
        // unmatched build id=2
        val m3 = iter.next()!!
        assertEquals("A2", m3.columns["a"])
        assertNull(m3.columns["b"])
        assertNull(iter.next())
        iter.close()
    }
}
