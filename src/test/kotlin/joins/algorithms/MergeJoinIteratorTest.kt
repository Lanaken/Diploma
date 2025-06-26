package org.bmstu.joins.algorithms

import org.bmstu.tables.Row
import org.bmstu.reader.YSchema
import org.bmstu.reader.Col
import org.bmstu.reader.CType
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand


class MergeJoinIteratorTest {
    private lateinit var tmpDir: Path

    @BeforeEach
    fun setup() {
        tmpDir = Files.createTempDirectory("merge-join-test")
    }

    private fun writeTbl(file: Path, lines: List<String>) {
        Files.newBufferedWriter(file).use { w ->
            lines.forEach { w.write(it + "\n") }
        }
    }

    private fun schema(vararg cols: Pair<String, CType>): YSchema =
        YSchema("", cols.map { Col(it.first, it.second) })

    @Test
    fun `inner join equals returns correct pairs`() {
        val leftFile = tmpDir.resolve("left.tbl")
        val rightFile = tmpDir.resolve("right.tbl")

        // строки вида "<ключ>|<значение>"
        writeTbl(leftFile, listOf(
            "1|L1",
            "2|L2",
            "3|L3"
        ))
        writeTbl(rightFile, listOf(
            "2|R2",
            "3|R3",
            "4|R4"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.INNER,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // Ожидаем пары только для ключей 2 и 3
        assertEquals(2, results.size)

        val pairs = results.map {
            // В выходе "id" сохраняется из левой таблицы
            (it.columns["id"] as Int) to (it.columns["r_val"] as String)
        }.toSet()
        assertTrue(pairs.contains(2 to "R2"))
        assertTrue(pairs.contains(3 to "R3"))
    }

    @Test
    fun `inner less-than join returns correct pairs`() {
        val leftFile = tmpDir.resolve("left_lt.tbl")
        val rightFile = tmpDir.resolve("right_lt.tbl")

        writeTbl(leftFile, listOf(
            "1|A1",
            "2|A2"
        ))
        writeTbl(rightFile, listOf(
            "2|B2",
            "3|B3"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.INNER,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.LESS_THAN
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // Пары (l.id < r.id) → (1,2), (1,3), (2,3)
        assertEquals(3, results.size)

        // Проверяем, что в каждой строке "id" из левой
        val pairs = results.map {
            (it.columns["id"] as Int) to (it.columns["r_val"] as String)
        }.toSet()
        assertTrue(pairs.contains(1 to "B2"))
        assertTrue(pairs.contains(1 to "B3"))
        assertTrue(pairs.contains(2 to "B3"))
    }

    //============================================================
    // 3) INNER JOIN, оператор GREATER_THAN_OR_EQUALS
    //============================================================
    @Test
    fun `inner greater-than-or-equals join returns correct pairs`() {
        val leftFile = tmpDir.resolve("left_ge.tbl")
        val rightFile = tmpDir.resolve("right_ge.tbl")

        writeTbl(leftFile, listOf(
            "1|X1",
            "2|X2"
        ))
        writeTbl(rightFile, listOf(
            "1|Y1",
            "2|Y2",
            "3|Y3"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        // Объект MergeJoinIterator подразумевает, что обе таблицы уже
        // отсортированы по "id" — наша генерация и так по возрастанию.

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.INNER,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.GREATER_THAN_OR_EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // Условие (l.id >= r.id):
        // для l=1 → совпадения r.id ≤ 1 → r.id=1 → (1,Y1)
        // для l=2 → совпадения r.id ≤ 2 → r.id=1,2 → (2,Y1), (2,Y2)
        assertEquals(3, results.size)

        val pairs = results.map {
            (it.columns["id"] as Int) to (it.columns["r_val"] as String)
        }.toSet()
        assertTrue(pairs.contains(1 to "Y1"))
        assertTrue(pairs.contains(2 to "Y1"))
        assertTrue(pairs.contains(2 to "Y2"))
    }

    //============================================================
    // 4) LEFT OUTER JOIN, оператор EQUALS
    //============================================================
    @Test
    fun `left outer join equals returns unmatched left`() {
        val leftFile = tmpDir.resolve("left_l.tbl")
        val rightFile = tmpDir.resolve("right_l.tbl")

        writeTbl(leftFile, listOf(
            "1|L1",
            "2|L2",
            "3|L3"
        ))
        writeTbl(rightFile, listOf(
            "2|R2"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.LEFT,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // Совпадение (2—2) а для 1 и 3 нет правых → (1,null), (3,null)
        // Всего 3 строки: (2,R2), (1,null), (3,null)
        assertEquals(3, results.size)

        // Проверим, что среди результатов есть точное совпадение (2,R2)
        assertTrue(results.any {
            it.columns["id"] == 2 && it.columns["r_val"] == "R2"
        })
        // А также строки, где r_val отсутствует
        assertTrue(results.any { it.columns["id"] == 1 && !it.columns.containsKey("r_val") })
        assertTrue(results.any { it.columns["id"] == 3 && !it.columns.containsKey("r_val") })
    }

    //============================================================
    // 5) LEFT OUTER JOIN, оператор LESS_THAN
    //============================================================
    @Test
    fun `left outer join less-than produces unmatched left`() {
        val leftFile = tmpDir.resolve("left_l2.tbl")
        val rightFile = tmpDir.resolve("right_l2.tbl")

        writeTbl(leftFile, listOf(
            "1|A1",
            "3|A3"
        ))
        writeTbl(rightFile, listOf(
            "2|B2"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.LEFT,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.LESS_THAN
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // Условие (l.id < r.id):
        // для l=1 → r=2 дает совпадение (1,B2)
        // для l=3 → нет r.id > 3 → выдаём (3,null)
        assertEquals(2, results.size)

        assertTrue(results.any {
            it.columns["id"] == 1 && it.columns["r_val"] == "B2"
        })
        assertTrue(results.any {
            it.columns["id"] == 3 && !it.columns.containsKey("r_val")
        })
    }

    //============================================================
    // 6) RIGHT OUTER JOIN, оператор EQUALS
    //============================================================
    @Test
    fun `right outer join equals includes unmatched right`() {
        val leftFile = tmpDir.resolve("left_r.tbl")
        val rightFile = tmpDir.resolve("right_r.tbl")

        writeTbl(leftFile, listOf(
            "1|L1"
        ))
        writeTbl(rightFile, listOf(
            "1|R1",
            "2|R2"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.RIGHT,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // Ожидаем: (1,R1) и (null, R2) для ключа 2
        assertEquals(2, results.size)

        assertTrue(results.any {
            it.columns["id"] == 1 && it.columns["r_val"] == "R1"
        })
        assertTrue(results.any {
            it.columns["id"] == 2 && !it.columns.containsKey("l_val")
        })
    }

    //============================================================
    // 7) RIGHT OUTER JOIN, оператор GREATER_THAN
    //============================================================
    @Test
    fun `right outer join greater-than includes unmatched right`() {
        val leftFile = tmpDir.resolve("left_r2.tbl")
        val rightFile = tmpDir.resolve("right_r2.tbl")

        writeTbl(leftFile, listOf(
            "2|L2"
        ))
        writeTbl(rightFile, listOf(
            "1|R1",
            "3|R3"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.RIGHT,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.GREATER_THAN
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // Условие (l.id > r.id):
        // для r=1 → l=2 подходит → (2,R1)
        // для r=3 → нет l.id > 3 → выдаём (null, R3)
        assertEquals(2, results.size)
        assertTrue(results.any {
            it.columns["id"] == 2 && it.columns["r_val"] == "R1"
        })
        assertTrue(results.any {
            it.columns["id"] == 3 && !it.columns.containsKey("l_val")
        })
    }

    //============================================================
    // 8) FULL OUTER JOIN, оператор EQUALS
    //============================================================
    @Test
    fun `full outer join equals includes unmatched both sides`() {
        val leftFile = tmpDir.resolve("left_f.tbl")
        val rightFile = tmpDir.resolve("right_f.tbl")

        writeTbl(leftFile, listOf(
            "1|L1",
            "2|L2"
        ))
        writeTbl(rightFile, listOf(
            "2|R2",
            "3|R3"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.FULL,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // Пары:
        // (2,R2) — совпадает
        // (1,null) — нет правого для l=1
        // (null,3) — нет левого для r=3
        assertEquals(3, results.size)
        assertTrue(results.any {
            it.columns["id"] == 2 && it.columns["r_val"] == "R2"
        })
        assertTrue(results.any {
            it.columns["id"] == 1 && !it.columns.containsKey("r_val")
        })
        assertTrue(results.any {
            it.columns["id"] == 3 && !it.columns.containsKey("l_val")
        })
    }

    //============================================================
    // 9) FULL OUTER JOIN, оператор NOT_EQUALS
    //============================================================
    @Test
    fun `full outer join not-equals includes all pairs and unmatched`() {
        val leftFile = tmpDir.resolve("left_f2.tbl")
        val rightFile = tmpDir.resolve("right_f2.tbl")

        writeTbl(leftFile, listOf(
            "1|X1",
            "2|X2"
        ))
        writeTbl(rightFile, listOf(
            "1|Y1",
            "3|Y3"
        ))

        val leftSchema  = schema("id" to CType.INT, "l_val" to CType.STRING)
        val rightSchema = schema("id" to CType.INT, "r_val" to CType.STRING)

        val joinIter = MergeJoinIterator(
            leftIter   = ScanIterator(leftFile, leftSchema),
            rightIter  = ScanIterator(rightFile, rightSchema),
            joinType   = JoinCommand.FULL,
            leftKey    = "id",
            rightKey   = "id",
            condOp     = ConditionOperator.NOT_EQUALS
        )

        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        /**
         * Условие NOT_EQUALS:
         * пары, где l.id != r.id:
         *   (1,Y3), (2,Y1), (2,Y3)
         * А также (1,null)?  (1,Y1) исключается, так как 1==1
         * И (null,3)? (нет, т.к. r=3 matched с l=1 и 2)
         * На самом деле matchedLeft = {0,1}; matchedRight={0,1};
         * -> нет одиночных, потому что все индексы попали в пары.
         */
        assertEquals(3, results.size)

        val pairs = results.map {
            (it.columns["id"] as Int) to (it.columns["r_val"] as String)
        }.toSet()
        assertTrue(pairs.contains(1 to "Y3"))  // 1 != 3
        assertTrue(pairs.contains(2 to "Y1"))  // 2 != 1
        assertTrue(pairs.contains(2 to "Y3"))  // 2 != 3
    }

    @Test
    fun `inner join part and partsupp returns non-empty result`() {
        val partPath = Path.of("tables/part.tbl")
        val partsuppPath = Path.of("tables/partsupp.tbl")

        // Описываем схемы: часть полей достаточно для join
        val partSchema = YSchema("part", listOf(
            Col("p_partkey", CType.INT),
            Col("p_name", CType.STRING),
            Col("p_mfgr", CType.STRING),
            // остальные колонки неважны для теста, можно указать, но optional
            Col("p_brand", CType.STRING),
            Col("p_type", CType.STRING),
            Col("p_size", CType.INT),
            Col("p_container", CType.STRING),
            Col("p_retailprice", CType.DECIMAL),
            Col("p_comment", CType.STRING)
        ))

        val partsuppSchema = YSchema("partsupp", listOf(
            Col("ps_partkey", CType.INT),
            Col("ps_suppkey", CType.INT),
            Col("ps_availqty", CType.INT),
            Col("ps_supplycost", CType.DECIMAL),
            Col("ps_comment", CType.STRING)
        ))

        val joinIter = MergeJoinIterator(
            leftIter    = ScanIterator(partPath, partSchema),
            rightIter   = ScanIterator(partsuppPath, partsuppSchema),
            joinType    = JoinCommand.INNER,
            leftKey     = "p_partkey",
            rightKey    = "ps_partkey",
            condOp      = ConditionOperator.EQUALS
        )

        joinIter.open()
        var count = 0
        while (true) {
            val row = joinIter.next() ?: break
            // Убедимся, что у нас в результате присутствуют обе колонки-ключи:
            assertTrue(row.columns.containsKey("p_partkey"))
            assertTrue(row.columns.containsKey("ps_partkey"))
            count++
        }
        joinIter.close()

        assertTrue(count > 0, "INNER-join part.tbl и partsupp.tbl должен вернуть хотя бы одну строку")
    }

    @Test
    fun `left outer join part and partsupp includes all part rows`() {
        val partPath = Path.of("tables/part.tbl")
        val orderPath = Path.of("tables/orders.tbl")

        val partSchema = YSchema("part", listOf(
            Col("p_partkey", CType.INT),
            Col("p_name", CType.STRING),
            Col("p_mfgr", CType.STRING),
            Col("p_brand", CType.STRING),
            Col("p_type", CType.STRING),
            Col("p_size", CType.INT),
            Col("p_container", CType.STRING),
            Col("p_retailprice", CType.DECIMAL),
            Col("p_comment", CType.STRING)
        ))


        val orderSchema = YSchema(
            table = "orders",
            columns = listOf(
                Col("o_orderkey", CType.INT),
                Col("o_custkey", CType.INT),
                Col("o_status", CType.CHAR, length = 25),
                Col("o_totalprice", CType.DECIMAL, precision = 15, scale = 2),
                Col("o_orderdate", CType.DATE),
                Col("o_comment", CType.STRING)
            )
        )

        // Сначала подсчитаем, сколько строк в файле part.tbl
        var partCount = 0
        ScanIterator(partPath, partSchema).let { iter ->
            iter.open()
            while (iter.next() != null) partCount++
            iter.close()
        }

        // Выполним LEFT OUTER join
        val joinIter = MergeJoinIterator(
            leftIter    = ScanIterator(partPath, partSchema),
            rightIter   = ScanIterator(orderPath, orderSchema),
            joinType    = JoinCommand.LEFT,
            leftKey     = "p_partkey",
            rightKey    = "o_orderkey",
            condOp      = ConditionOperator.EQUALS
        )

        // Соберём результат и отсортируем его по ключу part для более точной проверки
        joinIter.open()
        val results = mutableListOf<Row>()
        while (true) {
            val row = joinIter.next() ?: break
            results.add(row)
        }
        joinIter.close()

        // В LEFT OUTER join каждая строка из part должна появиться хотя бы один раз в выходе
        // (либо со значениями из partsupp, либо с null-значениями). Поэтому общее число выходных
        // строк должно быть >= partCount.
        assertTrue(results.size >= partCount,
            "Результат LEFT OUTER join должен содержать хотя бы $partCount строк, но было ${results.size}")

        // Дополнительно проверим, что хотя бы одна строка из part без соответствия partsupp
        // выдаёт null по полю partsupp (например, по ps_suppkey).
        val hasNullMatch = results.any { row ->
            row.columns.containsKey("p_partkey") &&
                    !row.columns.containsKey("ps_partkey")
        }
        assertTrue(hasNullMatch,
            "Должна присутствовать хотя бы одна строка part без совпадения в partsupp (ps_partkey == null)")
    }
}
