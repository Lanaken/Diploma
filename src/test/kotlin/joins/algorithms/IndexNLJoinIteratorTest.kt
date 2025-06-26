package org.bmstu.joins.algorithms

import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import org.bmstu.execution.Executor.Companion.writeIteratorToFile
import org.bmstu.indexes.BPlusTreeDiskBuilderInt
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand
import org.bmstu.reader.CType
import org.bmstu.reader.Col
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IndexNLJoinIteratorTest {

    @TempDir
    lateinit var tmpDir: Path

    private lateinit var outerFile: Path
    private lateinit var innerFile: Path

    private lateinit var outerSchema: YSchema
    private lateinit var innerSchema: YSchema

    @TempDir
    private lateinit var indexPath: Path
    @TempDir
    private lateinit var resultFile: Path

    @BeforeEach
    fun setup(testInfo: TestInfo) {
        if (testInfo.displayName.contains("real customer-orders INNER join executes successfully")) {
            outerFile = Path.of("tables/customer.tbl")
            innerFile = Path.of("tables/orders.tbl")

            outerSchema = YSchema("customer", listOf(
                Col("c_custkey", CType.INT),
                Col("c_name", CType.CHAR, length = 25),
                Col("c_address", CType.CHAR, length = 40),
                Col("c_nationkey", CType.INT),
                Col("c_phone", CType.CHAR, length = 15),
                Col("c_acctbal", CType.DECIMAL),
                Col("c_mktsegment", CType.CHAR, length = 10),
                Col("c_comment", CType.CHAR, length = 117)
            ))
            innerSchema = YSchema("orders", listOf(
                Col("o_orderkey", CType.INT),
                Col("o_custkey", CType.INT),
                Col("o_status", CType.CHAR, length = 25),
                Col("o_totalprice", CType.DECIMAL),
                Col("o_orderdate", CType.DATE),
                Col("o_comment", CType.STRING)
            ))

            indexPath = tmpDir.resolve("orders_index.bpt")
            BPlusTreeDiskBuilderInt.build(innerFile, innerSchema, "o_custkey", indexPath)
            resultFile = Files.createTempFile("join-result", ".txt")
        }
    }

    private fun writeTbl(path: Path, lines: List<String>) {
        Files.newBufferedWriter(path).use { w ->
            lines.forEach { w.write(it + "\n") }
        }
    }

    @Test
    fun `real customer-orders INNER join executes successfully`() {
        val joinIter = IndexNLJoinIterator(
            outerPath = outerFile,
            outerSchema = outerSchema,
            innerPath = innerFile,
            innerSchema = innerSchema,
            indexPath = indexPath,
            joinType = JoinCommand.INNER,
            outerKey = "c_custkey",
            innerKey = "o_custkey",
            condOp = ConditionOperator.EQUALS
        )

        writeIteratorToFile(joinIter, resultFile, outerSchema, innerSchema)
        var check = false
        val keyIdxOuter = outerSchema.columns.indexOfFirst { it.name == "c_custkey" }
        val keyIdxInner = outerSchema.columns.size + innerSchema.columns.indexOfFirst { it.name == "o_custkey" }
        val totalPrice = outerSchema.columns.size + innerSchema.columns.indexOfFirst { it.name == "o_totalprice" }
        val nationKey = outerSchema.columns.indexOfFirst { it.name == "c_nationkey" }

        Files.newBufferedReader(resultFile).useLines { lines ->
            lines.forEach {
                val parts = it.split("|")
                assertTrue(parts[keyIdxOuter].isNotEmpty(), "Expected c_custkey to be present")
                assertTrue(parts[keyIdxInner].isNotEmpty(), "Expected o_custkey to be present")
                if (parts[keyIdxInner] == "115846" && parts[keyIdxOuter] == "115846") {
                    assertEquals("91495.19", parts[totalPrice])
                    assertEquals("20", parts[nationKey])
                    check = true
                    return@forEach
                }
            }
        }
        assertTrue(check)
    }

    @Test
    fun `orders RIGHT JOIN customer on o_custkey = c_custkey returns all matches`() {
        val customerTbl = tmpDir.resolve("customer.tbl")
        val ordersTbl = tmpDir.resolve("orders.tbl")

        val customerLines = listOf(
            "1|Alice|addr1|10|111|100.00|MKT1|comment1",
            "2|Bob|addr2|20|222|200.00|MKT2|comment2",
            "3|Charlie|addr3|30|333|300.00|MKT3|comment3"
        )

        val ordersLines = listOf(
            "1|1|F|300.00|2024-01-01|PRIORITY1|CLERK1|1|order1",   // Alice
            "2|1|F|320.00|2024-01-02|PRIORITY2|CLERK2|1|order2",   // Alice
            "3|2|O|150.00|2024-01-03|PRIORITY3|CLERK3|2|order3",   // Bob
            "4|2|O|170.00|2024-01-04|PRIORITY4|CLERK4|2|order4",   // Bob
            "5|3|F|200.00|2024-01-05|PRIORITY5|CLERK5|3|order5",   // Charlie
            "6|3|F|220.00|2024-01-06|PRIORITY6|CLERK6|3|order6"    // Charlie
        )

        writeTbl(customerTbl, customerLines)
        writeTbl(ordersTbl, ordersLines)

        val customerSchema = YSchema("customer", listOf(
            Col("c_custkey", CType.INT),
            Col("c_name", CType.CHAR, length = 25),
            Col("c_address", CType.CHAR, length = 40),
            Col("c_nationkey", CType.INT),
            Col("c_phone", CType.CHAR, length = 15),
            Col("c_acctbal", CType.DECIMAL, precision = 15, scale = 2),
            Col("c_mktsegment", CType.CHAR, length = 10),
            Col("c_comment", CType.CHAR, length = 117)
        ))

        val ordersSchema = YSchema("orders", listOf(
            Col("o_orderkey", CType.INT),
            Col("o_custkey", CType.INT),
            Col("o_orderstatus", CType.CHAR, length = 1),
            Col("o_totalprice", CType.DECIMAL, precision = 15, scale = 2),
            Col("o_orderdate", CType.DATE),
            Col("o_orderpriority", CType.CHAR, length = 15),
            Col("o_clerk", CType.CHAR, length = 15),
            Col("o_shippriority", CType.INT),
            Col("o_comment", CType.STRING, length = 79)
        ))

        val indexFile = tmpDir.resolve("customer_index.bpt")
        BPlusTreeDiskBuilderInt.build(customerTbl, customerSchema, "c_custkey", indexFile)

        val joinIter = IndexNLJoinIterator(
            outerPath = ordersTbl,
            outerSchema = ordersSchema,
            innerPath = customerTbl,
            innerSchema = customerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.RIGHT,
            outerKey = "o_custkey",
            innerKey = "c_custkey",
            condOp = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = generateSequence { joinIter.next() }.toList()
        joinIter.close()

        // Должно быть 6 строк: каждая строка заказов имеет соответствующего customer
        assertEquals(6, results.size)

        val joinedPairs = results.map {
            it.columns["o_orderkey"] to it.columns["c_custkey"]
        }.toSet()

        val expected = setOf(
            1 to 1, 2 to 1, // Alice
            3 to 2, 4 to 2, // Bob
            5 to 3, 6 to 3  // Charlie
        )
        assertEquals(expected, joinedPairs)
        results.forEach { row ->
            val oKey = row.columns["o_orderkey"] as Int
            val cKey = row.columns["c_custkey"] as Int
            val cName = row.columns["c_name"] as String

            when (oKey) {
                1, 2 -> {
                    assertEquals(1, cKey)
                    assertEquals("Alice", cName.trim())
                }
                3, 4 -> {
                    assertEquals(2, cKey)
                    assertEquals("Bob", cName.trim())
                }
                5, 6 -> {
                    assertEquals(3, cKey)
                    assertEquals("Charlie", cName.trim())
                }
                else -> fail("Unexpected o_orderkey: $oKey")
            }
        }

    }


    @Test
    fun `customer INNER JOIN orders on c_custkey = o_custkey returns all matches`() {
        val customerTbl = tmpDir.resolve("customer.tbl")
        val ordersTbl = tmpDir.resolve("orders.tbl")

        val customerLines = listOf(
            "1|Alice|addr1|10|111|100.00|MKT1|comment1",
            "2|Bob|addr2|20|222|200.00|MKT2|comment2",
            "3|Charlie|addr3|30|333|300.00|MKT3|comment3"
        )

        val ordersLines = listOf(
            "1|1|F|300.00|2024-01-01|PRIORITY1|CLERK1|1|order1", // Alice
            "2|1|F|320.00|2024-01-02|PRIORITY2|CLERK2|1|order2", // Alice
            "3|2|O|150.00|2024-01-03|PRIORITY3|CLERK3|2|order3", // Bob
            "4|2|O|170.00|2024-01-04|PRIORITY4|CLERK4|2|order4", // Bob
            "5|3|F|200.00|2024-01-05|PRIORITY5|CLERK5|3|order5", // Charlie
            "6|3|F|220.00|2024-01-06|PRIORITY6|CLERK6|3|order6"  // Charlie
        )

        writeTbl(customerTbl, customerLines)
        writeTbl(ordersTbl, ordersLines)

        val customerSchema = YSchema("customer", listOf(
            Col("c_custkey", CType.INT),
            Col("c_name", CType.CHAR, length = 25),
            Col("c_address", CType.CHAR, length = 40),
            Col("c_nationkey", CType.INT),
            Col("c_phone", CType.CHAR, length = 15),
            Col("c_acctbal", CType.DECIMAL, precision = 15, scale = 2),
            Col("c_mktsegment", CType.CHAR, length = 10),
            Col("c_comment", CType.CHAR, length = 117)
        ))

        val ordersSchema = YSchema("orders", listOf(
            Col("o_orderkey", CType.INT),
            Col("o_custkey", CType.INT),
            Col("o_orderstatus", CType.CHAR, length = 1),
            Col("o_totalprice", CType.DECIMAL, precision = 15, scale = 2),
            Col("o_orderdate", CType.DATE),
            Col("o_orderpriority", CType.CHAR, length = 15),
            Col("o_clerk", CType.CHAR, length = 15),
            Col("o_shippriority", CType.INT),
            Col("o_comment", CType.STRING, length = 79)
        ))

        val indexFile = tmpDir.resolve("orders_index.bpt")
        BPlusTreeDiskBuilderInt.build(ordersTbl, ordersSchema, "o_custkey", indexFile)

        val joinIter = IndexNLJoinIterator(
            outerPath = customerTbl,
            outerSchema = customerSchema,
            innerPath = ordersTbl,
            innerSchema = ordersSchema,
            indexPath = indexFile,
            joinType = JoinCommand.INNER,
            outerKey = "c_custkey",
            innerKey = "o_custkey",
            condOp = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = generateSequence { joinIter.next() }.toList()
        joinIter.close()

        // Ожидается 6 строк: по 2 на каждого customer
        assertEquals(6, results.size)

        val expectedPairs = setOf(
            1 to 1, 1 to 2, // Alice
            2 to 3, 2 to 4, // Bob
            3 to 5, 3 to 6  // Charlie
        )

        val actualPairs = results.map { it.columns["c_custkey"] to it.columns["o_orderkey"] }.toSet()
        assertEquals(expectedPairs, actualPairs)
    }



    private fun intSchema(tableName: String, keyCol: String, vararg otherCols: String): YSchema {
        // Название таблицы не влияет на логику join, можно оставить пустым или сделать equals keyCol
        val cols = mutableListOf(Col(keyCol, CType.INT))
        otherCols.forEach { cols += Col(it, CType.STRING, length = 10) }
        return YSchema(tableName, cols)
    }

    @Test
    fun `INNER join returns only matching pairs`() {
        val innerTbl = tmpDir.resolve("inner.tbl")
        val innerLines = listOf("100|X100", "200|X200", "300|X300")
        writeTbl(innerTbl, innerLines)

        val outerTbl = tmpDir.resolve("outer.tbl")
        val outerLines = listOf("100|O100", "150|O150", "200|O200", "400|O400")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            table = "inner",
            columns = listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            table = "outer",
            columns = listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.INNER,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        assertEquals(2, results.size)
        assertEquals(mapOf<String, Any?>("id" to 100, "val" to "X100"), results[0].columns)
        assertEquals(mapOf<String, Any?>("id" to 200, "val" to "X200"), results[1].columns)
        results.forEach { row ->
            val key = row.columns["id"] as Int
            val xval = row.columns["val"] as String
            when (key) {
                100 -> assertEquals("X100", xval)
                200 -> assertEquals("X200", xval)
                else -> Assertions.fail<Unit>("Unexpected key $key in INNER result")
            }
        }
        results.forEach { row ->
            val keyCount = row.columns.keys.count { it == "id" }
            assertEquals(1, keyCount, "Expected 'id' key only once in merged row")
        }
    }

    @Test
    @Disabled
    fun `LEFT join returns matching and unmatched-outer rows`() {
        val innerTbl = tmpDir.resolve("inner2.tbl")
        val innerLines = listOf("10|A10", "20|A20")
        writeTbl(innerTbl, innerLines)

        val outerTbl = tmpDir.resolve("outer2.tbl")
        val outerLines = listOf("10|B10", "15|B15", "30|B30")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            "inner2",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            "outer2",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner2_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.LEFT,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        assertEquals(3, results.size)
        assertTrue(results.any { row ->
            (row.columns["id"] as Int) == 10 && (row.columns["val"] as String) == "A10"
        })
        val unmatched = results.filter { row ->
            val k = row.columns["id"] as Int
            (k == 15 || k == 30) && row.columns.containsKey("val") && row.columns["val"] == null
        }
        assertEquals(2, unmatched.size)
    }

    @Test
    @Disabled
    fun `RIGHT join returns matching and unmatched-inner rows`() {
        val innerTbl = tmpDir.resolve("inner3.tbl")
        val innerLines = listOf("5|C5", "25|C25", "50|C50")
        writeTbl(innerTbl, innerLines)

        val outerTbl = tmpDir.resolve("outer3.tbl")
        val outerLines = listOf("5|D5", "30|D30")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            "inner3",
            listOf(Col("id", CType.INT), Col("val1", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            "outer3",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner3_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.RIGHT,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        assertEquals(3, results.size)
        assertTrue(results.any { row ->
            (row.columns["id"] as Int) == 5 && (row.columns["val"] as String) == "C5"
        })
        val unmatched = results.filter { row ->
            (row.columns["id"] as Int) in setOf(25, 50) && !row.columns.containsKey("val")
        }
        assertEquals(2, unmatched.size)
    }

    @Test
    fun `FULL join returns all matching, unmatched-outer and unmatched-inner`() {
        val innerTbl = tmpDir.resolve("inner4.tbl")
        val innerLines = listOf("7|F7", "14|F14", "21|F21")
        writeTbl(innerTbl, innerLines)

        val outerTbl = tmpDir.resolve("outer4.tbl")
        val outerLines = listOf("7|G7", "10|G10")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            "inner4",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            "outer4",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner4_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.FULL,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        assertEquals(4, results.size)
        assertTrue(results.any { row ->
            (row.columns["id"] as Int) == 7 && (row.columns["val"] as String) == "F7"
        })
        assertTrue(results.any { row ->
            (row.columns["id"] as Int) == 10 && !row.columns.containsKey("val")
        })
        assertTrue(results.any { row ->
            (row.columns["id"] as Int) == 14 && !row.columns.containsKey("val")
        })
        assertTrue(results.any { row ->
            (row.columns["id"] as Int) == 21 && !row.columns.containsKey("val")
        })
    }


    @Test
    fun `INNER join LESS_THAN returns correct pairs`() {
        // Inner keys: 2,3
        val innerTbl = tmpDir.resolve("inner_lt.tbl")
        val innerLines = listOf("2|I2", "3|I3")
        writeTbl(innerTbl, innerLines)

        // Outer keys: 1,2
        val outerTbl = tmpDir.resolve("outer_lt.tbl")
        val outerLines = listOf("1|O1", "2|O2")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            "inner_lt",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            "outer_lt",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner_lt_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.INNER,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.LESS_THAN
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        // Ожидаем (1<2)->(1,I2), (1<3)->(1,I3), (2<3)->(2,I3)
        assertEquals(3, results.size)
        val pairs = results.map { row ->
            (row.columns["id"] as Int) to (row.columns["val"] as String)
        }.toSet()
        assertTrue(pairs.contains(1 to "I2"))
        assertTrue(pairs.contains(1 to "I3"))
        assertTrue(pairs.contains(2 to "I3"))
    }

    @Test
    fun `INNER join GREATER_THAN_OR_EQUALS returns correct pairs`() {
        // Inner keys: 1,2,3
        val innerTbl = tmpDir.resolve("inner_ge.tbl")
        val innerLines = listOf("1|I1", "2|I2", "3|I3")
        writeTbl(innerTbl, innerLines)

        // Outer keys: 1,2
        val outerTbl = tmpDir.resolve("outer_ge.tbl")
        val outerLines = listOf("1|O1", "2|O2")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            "inner_ge",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            "outer_ge",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner_ge_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.INNER,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.GREATER_THAN_OR_EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        // Ожидаем для outer=1 → inner 1,2,3; для outer=2 → inner 2,3
        assertEquals(5, results.size)
        val multiset = results.map { row ->
            (row.columns["id"] as Int) to (row.columns["val"] as String)
        }
        // Проверим парочку примеров:
        assertTrue(multiset.contains(1 to "I1"))
        assertTrue(multiset.contains(1 to "I3"))
        assertTrue(multiset.count { it.first == 2 } == 2)
    }

    @Test
    @Disabled
    fun `LEFT join LESS_THAN returns unmatched outer when no match`() {
        // Inner keys: 2
        val innerTbl = tmpDir.resolve("inner_lt2.tbl")
        val innerLines = listOf("2|I2")
        writeTbl(innerTbl, innerLines)

        // Outer keys: 2,3
        val outerTbl = tmpDir.resolve("outer_lt2.tbl")
        val outerLines = listOf("2|O2", "3|O3")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            "inner_lt2",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            "outer_lt2",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner_lt2_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.LEFT,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.LESS_THAN
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        // outer=2: ищем inner>2 → нет → (2,null)
        // outer=3: ищем inner>3 → нет → (3,null)
        assertEquals(2, results.size)
        results.forEach { row ->
            val k = row.columns["id"] as Int
            assertTrue(k in listOf(2, 3))
            assertTrue(!row.columns.containsKey("val"))
        }
    }

    @Test
    @Disabled
    fun `RIGHT join GREATER_THAN returns unmatched inner when no match`() {
        // Inner keys: 1,3
        val innerTbl = tmpDir.resolve("inner_gt.tbl")
        val innerLines = listOf("1|I1", "3|I3")
        writeTbl(innerTbl, innerLines)

        // Outer keys: 2
        val outerTbl = tmpDir.resolve("outer_gt.tbl")
        val outerLines = listOf("2|O2")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            "inner_gt",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            "outer_gt",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner_gt_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.RIGHT,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.GREATER_THAN
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        // outer=2: ищем inner<2 → inner=1 → (2,I1)
        // unmatched-inner: inner=3 → (null,3)
        assertEquals(2, results.size)
        assertTrue(results.any { row ->
            (row.columns["id"] as Int) == 2 && (row.columns["val"] as String) == "I1"
        })
        assertTrue(results.any { row ->
            (row.columns["id"] as Int) == 3 && !row.columns.containsKey("val")
        })
    }

    @Test
    fun `FULL join NOT_EQUALS returns all non-equal pairs and no unmatched`() {
        // Inner keys: 1,3
        val innerTbl = tmpDir.resolve("inner_ne.tbl")
        val innerLines = listOf("1|I1", "3|I3")
        writeTbl(innerTbl, innerLines)

        // Outer keys: 1,2
        val outerTbl = tmpDir.resolve("outer_ne.tbl")
        val outerLines = listOf("1|O1", "2|O2")
        writeTbl(outerTbl, outerLines)

        val innerSchema = YSchema(
            "inner_ne",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )
        val outerSchema = YSchema(
            "outer_ne",
            listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner_ne_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "id",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath = outerTbl,
            outerSchema = outerSchema,
            innerPath = innerTbl,
            innerSchema = innerSchema,
            indexPath = indexFile,
            joinType = JoinCommand.FULL,
            outerKey = "id",
            innerKey = "id",
            condOp = ConditionOperator.NOT_EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val r = joinIter.next() ?: break
            results += r
        }
        joinIter.close()

        // пары, где outer.id != inner.id:
        //  outer=1 → inner=3 => (1,3)
        //  outer=2 → inner=1,inner=3 => (2,1),(2,3)
        // unmatched: нет, потому что для обоих outer есть пара, и для обоих inner тоже
        assertEquals(3, results.size)
        val pairs = results.map { row ->
            (row.columns["id"] as Int) to (row.columns["val"] as String)
        }.toSet()
        assertTrue(pairs.contains(1 to "I3"))
        assertTrue(pairs.contains(2 to "I1"))
        assertTrue(pairs.contains(2 to "I3"))
    }

    @Test
    fun `RIGHT join with duplicate inner keys returns all matches and unmatched`() {
        val innerTbl = tmpDir.resolve("inner_dup.tbl")
        val innerLines = listOf("5|A", "5|B", "25|C")  // key 5 дублируется
        writeTbl(innerTbl, innerLines)

        val outerTbl = tmpDir.resolve("outer_dup.tbl")
        val outerLines = listOf("5|Z")
        writeTbl(outerTbl, outerLines)

        val outerSchema = YSchema("outer",
            listOf(Col("oid", CType.INT), Col("oval", CType.STRING, length = 10))
        )
        val innerSchema = YSchema("inner",
            listOf(Col("iid", CType.INT), Col("ival", CType.STRING, length = 10))
        )

        val indexFile = tmpDir.resolve("inner_dup_index.bpt")
        BPlusTreeDiskBuilderInt.build(
            tbl = innerTbl,
            schema = innerSchema,
            keyCol = "iid",
            indexPath = indexFile
        )

        val joinIter = IndexNLJoinIterator(
            outerPath   = outerTbl,
            outerSchema = outerSchema,
            innerPath   = innerTbl,
            innerSchema = innerSchema,
            indexPath   = indexFile,
            joinType    = JoinCommand.RIGHT,
            outerKey    = "oid",
            innerKey    = "iid",
            condOp      = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = generateSequence { joinIter.next() }.toList()
        joinIter.close()

        // Проверяем, что обе строки с id = 5 попали
        val matches = results.filter { it.columns["iid"] == 5 }
        assertEquals(2, matches.size)

        val matchedValues = matches.map { it.columns["ival"] }
        assertTrue(matchedValues.contains("A"))
        assertTrue(matchedValues.contains("B"))
        assertTrue(matches.all { it.columns["oid"] == 5 && it.columns["oval"] == "Z" })

        // Проверяем unmatched inner строку с id = 25
        val unmatched = results.find { it.columns["iid"] == 25 }
        assertNotNull(unmatched, "Expected unmatched RIGHT join row for inner.id = 25")
        assertEquals("C", unmatched.columns["ival"])
        assertNull(unmatched.columns["oid"], "Unmatched RIGHT row must have null outer ID")
        assertNull(unmatched.columns["oval"], "Unmatched RIGHT row must have null outer value")
    }


    @Test
    fun `INNER join with duplicate keys on both sides`() {
        val innerTbl = tmpDir.resolve("inner_dup2.tbl")
        val innerLines = listOf("1|I1", "1|I1b")
        writeTbl(innerTbl, innerLines)

        val outerTbl = tmpDir.resolve("outer_dup2.tbl")
        val outerLines = listOf("1|O1", "1|O1b")
        writeTbl(outerTbl, outerLines)

        val schema = YSchema("t", listOf(Col("id", CType.INT), Col("val", CType.STRING, length = 10)))

        val indexFile = tmpDir.resolve("index_dup2.bpt")
        BPlusTreeDiskBuilderInt.build(innerTbl, schema, "id", indexFile)

        val joinIter = IndexNLJoinIterator(
            outerTbl, schema,
            innerTbl, schema,
            indexFile,
            JoinCommand.INNER,
            "id", "id",
            ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = generateSequence { joinIter.next() }.toList()
        joinIter.close()

        // 2 outer x 2 inner → 4 результата
        assertEquals(4, results.size)
    }

    @Test
    fun `LEFT join with different key names`() {
        val outerTbl = tmpDir.resolve("outer_diff.tbl")
        val outerLines = listOf("1|O1")
        writeTbl(outerTbl, outerLines)

        val innerTbl = tmpDir.resolve("inner_diff.tbl")
        val innerLines = listOf("1|I1")
        writeTbl(innerTbl, innerLines)

        val outerSchema = YSchema("outer", listOf(Col("oid", CType.INT), Col("oval", CType.STRING, length = 10)))
        val innerSchema = YSchema("inner", listOf(Col("iid", CType.INT), Col("ival", CType.STRING, length = 10)))

        val indexFile = tmpDir.resolve("diff_key_index.bpt")
        BPlusTreeDiskBuilderInt.build(innerTbl, innerSchema, "iid", indexFile)

        val joinIter = IndexNLJoinIterator(
            outerTbl, outerSchema,
            innerTbl, innerSchema,
            indexFile,
            JoinCommand.LEFT,
            "oid", "iid",
            ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = generateSequence { joinIter.next() }.toList()
        joinIter.close()

        val row = results.single()
        assertTrue("oid" in row.columns)
        assertTrue("iid" in row.columns)
        assertTrue("oval" in row.columns)
        assertTrue("ival" in row.columns)
        assertEquals(1, row.columns["oid"])
        assertEquals(1, row.columns["iid"])
        assertEquals("O1", row.columns["oval"])
        assertEquals("I1", row.columns["ival"])
    }
}
