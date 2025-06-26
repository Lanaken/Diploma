package org.bmstu.joins.algorithms

import org.bmstu.reader.YSchema
import org.bmstu.reader.Col
import org.bmstu.reader.CType
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand
import org.bmstu.tables.Row
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

class PartitionHashJoinIteratorTest {
    private lateinit var tmpDir: Path

    @BeforeEach
    fun setup() {
        tmpDir = Files.createTempDirectory("hybrid_hash_join_test")
    }

    private fun writeTbl(file: Path, lines: List<String>) {
        Files.newBufferedWriter(file).use { w ->
            lines.forEach { w.write(it + "\n") }
        }
    }

    private fun schema(vararg cols: Pair<String, CType>): YSchema =
        YSchema("", cols.map { Col(it.first, it.second) })

    @Test
    fun `inner join equals simple`() {
        val buildFile = tmpDir.resolve("build.tbl")
        val probeFile = tmpDir.resolve("probe.tbl")

        writeTbl(buildFile, listOf(
            "1|A1",
            "2|A2",
            "3|A3"
        ))
        writeTbl(probeFile, listOf(
            "2|B2",
            "3|B3",
            "4|B4"
        ))

        val buildSchema = schema("id" to CType.INT, "a_val" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b_val" to CType.STRING)

        val joinIter = PartitionHashJoinIterator(
            buildPath    = buildFile,
            buildSchema  = buildSchema,
            probePath    = probeFile,
            probeSchema  = probeSchema,
            joinType     = JoinCommand.INNER,
            buildKey     = "id",
            probeKey     = "id",
            condOp       = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        assertEquals(2, results.size)
        val pairs = results.map {
            (it.columns["id"] as Int) to (it.columns["b_val"] as String)
        }.toSet()
        assertTrue(pairs.contains(2 to "B2"), "Должен быть результат (2, B2)")
        assertTrue(pairs.contains(3 to "B3"), "Должен быть результат (3, B3)")
    }

    @Test
    fun `left outer join equals includes unmatched build`() {
        val buildFile = tmpDir.resolve("build_l.tbl")
        val probeFile = tmpDir.resolve("probe_l.tbl")

        writeTbl(buildFile, listOf(
            "1|A1",
            "2|A2",
            "3|A3"
        ))
        writeTbl(probeFile, listOf("2|B2"))

        val buildSchema = schema("id" to CType.INT, "a_val" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b_val" to CType.STRING)

        val joinIter = PartitionHashJoinIterator(
            buildPath    = buildFile,
            buildSchema  = buildSchema,
            probePath    = probeFile,
            probeSchema  = probeSchema,
            joinType     = JoinCommand.LEFT,
            buildKey     = "id",
            probeKey     = "id",
            condOp       = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        assertEquals(3, results.size)
        assertTrue(results.any {
            it.columns["id"] == 2 && it.columns["b_val"] == "B2"
        })
        assertTrue(results.any {
            it.columns["id"] == 1 && !it.columns.containsKey("b_val")
        })
        assertTrue(results.any {
            it.columns["id"] == 3 && !it.columns.containsKey("b_val")
        })
    }

    @Test
    fun `right outer join equals includes unmatched probe`() {
        val buildFile = tmpDir.resolve("build_r.tbl")
        val probeFile = tmpDir.resolve("probe_r.tbl")

        writeTbl(buildFile, listOf("1|A1"))
        writeTbl(probeFile, listOf(
            "1|B1",
            "2|B2"
        ))

        val buildSchema = schema("id" to CType.INT, "a_val" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b_val" to CType.STRING)

        val joinIter = PartitionHashJoinIterator(
            buildPath    = buildFile,
            buildSchema  = buildSchema,
            probePath    = probeFile,
            probeSchema  = probeSchema,
            joinType     = JoinCommand.RIGHT,
            buildKey     = "id",
            probeKey     = "id",
            condOp       = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        assertEquals(2, results.size)
        assertTrue(results.any {
            it.columns["id"] == 1 && it.columns["b_val"] == "B1"
        })
        assertTrue(results.any {
            it.columns["id"] == 2 && !it.columns.containsKey("a_val")
        })
    }

    @Test
    fun `full outer join equals includes unmatched both sides`() {
        val buildFile = tmpDir.resolve("build_f.tbl")
        val probeFile = tmpDir.resolve("probe_f.tbl")

        writeTbl(buildFile, listOf(
            "1|A1",
            "2|A2"
        ))
        writeTbl(probeFile, listOf(
            "2|B2",
            "3|B3"
        ))

        val buildSchema = schema("id" to CType.INT, "a_val" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b_val" to CType.STRING)

        val joinIter = PartitionHashJoinIterator(
            buildPath    = buildFile,
            buildSchema  = buildSchema,
            probePath    = probeFile,
            probeSchema  = probeSchema,
            joinType     = JoinCommand.FULL,
            buildKey     = "id",
            probeKey     = "id",
            condOp       = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        assertEquals(3, results.size)
        assertTrue(results.any {
            it.columns["id"] == 2 && it.columns["b_val"] == "B2"
        })
        assertTrue(results.any {
            it.columns["id"] == 1 && !it.columns.containsKey("b_val")
        })
        assertTrue(results.any {
            it.columns["id"] == 3 && !it.columns.containsKey("a_val")
        })
    }

    @Test
    fun `inner join equals with large threshold behaves like in-memory`() {
        val buildFile = tmpDir.resolve("build_large.tbl")
        val probeFile = tmpDir.resolve("probe_large.tbl")

        writeTbl(buildFile, listOf(
            "10|X10",
            "20|X20"
        ))
        writeTbl(probeFile, listOf(
            "20|Y20",
            "30|Y30"
        ))

        val buildSchema = schema("id" to CType.INT, "a_val" to CType.STRING)
        val probeSchema = schema("id" to CType.INT, "b_val" to CType.STRING)

        // Порог памяти заведомо висок, чтобы buildSize < freeHeap*MEMORY_FACTOR
        val joinIter = PartitionHashJoinIterator(
            buildPath    = buildFile,
            buildSchema  = buildSchema,
            probePath    = probeFile,
            probeSchema  = probeSchema,
            joinType     = JoinCommand.INNER,
            buildKey     = "id",
            probeKey     = "id",
            condOp       = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        assertEquals(1, results.size)
        val row = results[0]
        assertEquals(20, row.columns["id"])
        assertEquals("Y20", row.columns["b_val"])
    }
}
