package org.bmstu.joins.algorithms

import io.mockk.every
import io.mockk.mockkObject
import io.mockk.unmockkObject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.bmstu.reader.YSchema
import org.bmstu.reader.Col
import org.bmstu.reader.CType
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import net.bytebuddy.matcher.ElementMatchers.any
import org.bmstu.tables.Row
import org.bmstu.util.MemoryUtil

class BlockNLJoinIteratorTest {

    private lateinit var buildFile: Path
    private lateinit var probeFile: Path

    private lateinit var buildSchema: YSchema
    private lateinit var probeSchema: YSchema

    @BeforeEach
    fun setUp() {
        mockkObject(MemoryUtil)
       // every { MemoryUtil.canFitInRam(any()) } returnsMany listOf(true, false, true)
        // Создаем временные файлы для build и probe
        buildFile = Files.createTempFile("build_table", ".tbl")
        val buildLines = listOf(
            "1|A",
            "2|B",
            "3|C",
            "4|D"
        )
        Files.write(buildFile, buildLines)

        probeFile = Files.createTempFile("probe_table", ".tbl")
        val probeLines = listOf(
            "2|X",
            "3|Y",
            "5|Z"
        )
        Files.write(probeFile, probeLines)

        buildSchema = YSchema(
            table = buildFile.fileName.toString(),
            columns = listOf(
                Col(name = "k", type = CType.INT),
                Col(name = "b", type = CType.STRING, length = 10)
            )
        )

        probeSchema = YSchema(
            table = probeFile.fileName.toString(),
            columns = listOf(
                Col(name = "k", type = CType.INT),
                Col(name = "p", type = CType.STRING, length = 10)
            )
        )
    }

    @AfterEach
    fun tearDown() {
        unmockkObject(MemoryUtil)
        Files.deleteIfExists(buildFile)
        Files.deleteIfExists(probeFile)
    }

    /**
     * Служебный метод: из Row делаем строку "build.k|build.b|probe.k|probe.p",
     * подставляя пустую строку, если какие-то поля отсутствуют.
     */
    private fun serializeRow(
        row: Row,
        buildSchema: YSchema,
        probeSchema: YSchema,
        buildKey: String,
        buildVal: String,
        probeKey: String,
        probeVal: String
    ): String {
        // Если build-части нет (null), то в словаре не окажется поля buildKey и buildVal
        val bk = row.columns["k"] as? Int
        val bv = row.columns["b"] as? String

        // Чтобы точно понять, откуда данные, поступаем так:
        // — если в Row есть ключ "k" и есть "b", значит это matched или unmatched-build;
        //   тогда serialize as "<k>|<b>|<maybe probe.k>|<maybe probe.p>"
        // — если в Row нет "b" но есть "k" (результат unmatched-probe),
        //   то значение "k" в row.columns — уже ключ из probe, поэтому build-часть пустая
        //   и probe-часть берём из row.columns["k"] и row.columns["p"].

        // Проверим, есть ли в row.build-поле "b":
        // (при matched и unmatched-build оно точно присутствует)
        val hasBuildPart = bv != null

        val buildKPart: String
        val buildValPart: String
        val probeKPart: String
        val probeValPart: String

        if (hasBuildPart) {
            // build-ключ и build-значение
            buildKPart = (row.columns[buildKey] as Int).toString()
            buildValPart = bv!!
            // probe-часть, если она есть:
            if (row.columns.containsKey(probeVal)) {
                // matched-случай
                probeKPart = (row.columns[probeKey] as Int).toString()
                probeValPart = row.columns[probeVal] as String
            } else {
                // unmatched-build
                probeKPart = ""
                probeValPart = ""
            }
        } else {
            // unmatched-probe: build-часть пустая
            buildKPart = ""
            buildValPart = ""
            // probe-ключ и probe-значение
            probeKPart = (row.columns[probeKey] as Int).toString()
            probeValPart = row.columns[probeVal] as String
        }

        return listOf(buildKPart, buildValPart, probeKPart, probeValPart).joinToString("|")
    }

    @Test
    fun `INNER join записывает только совпадающие ключи через iterator`() {
        val iter = BlockNLJoinIterator(
            buildPath   = buildFile,
            buildSchema = buildSchema,
            probePath   = probeFile,
            probeSchema = probeSchema,
            joinType    = JoinCommand.INNER,
            buildKey    = "k",
            probeKey    = "k",
            condOp      = ConditionOperator.EQUALS
        )

        val actualSet = mutableSetOf<String>()
        iter.open()
        while (true) {
            val row = iter.next() ?: break
            actualSet.add(serializeRow(
                row,
                buildSchema, probeSchema,
                buildKey = "k", buildVal = "b",
                probeKey = "k", probeVal = "p"
            ))
        }
        iter.close()

        // Ожидаем только "2|B|2|X" и "3|C|3|Y"
        val expected = setOf("2|B|2|X", "3|C|3|Y")
        assertEquals(expected, actualSet)
    }

    @Test
    fun `LEFT join записывает совпадающие и unmatched-build строки через iterator`() {
        val iter = BlockNLJoinIterator(
            buildPath   = buildFile,
            buildSchema = buildSchema,
            probePath   = probeFile,
            probeSchema = probeSchema,
            joinType    = JoinCommand.LEFT,
            buildKey    = "k",
            probeKey    = "k",
            condOp      = ConditionOperator.EQUALS
        )

        val actualSet = mutableSetOf<String>()
        iter.open()
        while (true) {
            val row = iter.next() ?: break
            actualSet.add(serializeRow(
                row,
                buildSchema, probeSchema,
                buildKey = "k", buildVal = "b",
                probeKey = "k", probeVal = "p"
            ))
        }
        iter.close()

        // Ожидаем "2|B|2|X", "3|C|3|Y", "1|A||", "4|D||"
        val expected = setOf("2|B|2|X", "3|C|3|Y", "1|A||", "4|D||")
        assertEquals(expected, actualSet)
    }

    @Test
    fun `RIGHT join записывает совпадающие и unmatched-probe строки через iterator`() {
        val iter = BlockNLJoinIterator(
            buildPath   = buildFile,
            buildSchema = buildSchema,
            probePath   = probeFile,
            probeSchema = probeSchema,
            joinType    = JoinCommand.RIGHT,
            buildKey    = "k",
            probeKey    = "k",
            condOp      = ConditionOperator.EQUALS
        )

        val actualSet = mutableSetOf<String>()
        iter.open()
        while (true) {
            val row = iter.next() ?: break
            actualSet.add(serializeRow(
                row,
                buildSchema, probeSchema,
                buildKey = "k", buildVal = "b",
                probeKey = "k", probeVal = "p"
            ))
        }
        iter.close()

        // Ожидаем "2|B|2|X", "3|C|3|Y", "||5|Z"
        val expected = setOf("2|B|2|X", "3|C|3|Y", "||5|Z")
        assertEquals(expected, actualSet)
    }

    @Test
    fun `FULL join записывает все unmatched-строки обеих таблиц через iterator`() {
        val iter = BlockNLJoinIterator(
            buildPath   = buildFile,
            buildSchema = buildSchema,
            probePath   = probeFile,
            probeSchema = probeSchema,
            joinType    = JoinCommand.FULL,
            buildKey    = "k",
            probeKey    = "k",
            condOp      = ConditionOperator.EQUALS
        )

        val actualSet = mutableSetOf<String>()
        iter.open()
        while (true) {
            val row = iter.next() ?: break
            actualSet.add(serializeRow(
                row,
                buildSchema, probeSchema,
                buildKey = "k", buildVal = "b",
                probeKey = "k", probeVal = "p"
            ))
        }
        iter.close()

        // Ожидаем: "2|B|2|X", "3|C|3|Y", "1|A||", "4|D||", "||5|Z"
        val expected = setOf("2|B|2|X", "3|C|3|Y", "1|A||", "4|D||", "||5|Z")
        assertEquals(expected, actualSet)
    }

    @Test
    fun `пустая build-таблица с INNER join даёт пустой результат`() {
        // делаем buildFile пустым
        Files.write(buildFile, emptyList())

        val iter = BlockNLJoinIterator(
            buildPath   = buildFile,
            buildSchema = buildSchema,
            probePath   = probeFile,
            probeSchema = probeSchema,
            joinType    = JoinCommand.INNER,
            buildKey    = "k",
            probeKey    = "k",
            condOp      = ConditionOperator.EQUALS
        )

        val actualSet = mutableSetOf<String>()
        iter.open()
        while (true) {
            val row = iter.next() ?: break
            actualSet.add(serializeRow(
                row,
                buildSchema, probeSchema,
                buildKey = "k", buildVal = "b",
                probeKey = "k", probeVal = "p"
            ))
        }
        iter.close()

        assertEquals(emptySet<String>(), actualSet)
    }

    @Test
    fun `пустая probe-таблица с LEFT join возвращает все build-строки`() {
        // делаем probeFile пустым
        Files.write(probeFile, emptyList())

        val iter = BlockNLJoinIterator(
            buildPath   = buildFile,
            buildSchema = buildSchema,
            probePath   = probeFile,
            probeSchema = probeSchema,
            joinType    = JoinCommand.LEFT,
            buildKey    = "k",
            probeKey    = "k",
            condOp      = ConditionOperator.EQUALS
        )

        val actualSet = mutableSetOf<String>()
        iter.open()
        while (true) {
            val row = iter.next() ?: break
            actualSet.add(serializeRow(
                row,
                buildSchema, probeSchema,
                buildKey = "k", buildVal = "b",
                probeKey = "k", probeVal = "p"
            ))
        }
        iter.close()

        // Ожидаем только unmatched-build (1|A||), (2|B||), (3|C||), (4|D||)
        val expected = setOf("1|A||", "2|B||", "3|C||", "4|D||")
        assertEquals(expected, actualSet)
    }

    //
    // Примеры TPC-H: INNER и FULL join для lineitem ⨝ orders.
    //

    private fun makeLineitemSchema(): YSchema {
        return YSchema(
            table = "lineitem",
            columns = listOf(
                Col("l_orderkey",     CType.INT),
                Col("l_partkey",      CType.INT),
                Col("l_suppkey",      CType.INT),
                Col("l_linenumber",   CType.INT),
                Col("l_quantity",     CType.DECIMAL, precision = 15, scale = 2),
                Col("l_extendedprice",CType.DECIMAL, precision = 15, scale = 2),
                Col("l_discount",     CType.DECIMAL, precision = 15, scale = 2),
                Col("l_tax",          CType.DECIMAL, precision = 15, scale = 2),
                Col("l_returnflag",   CType.CHAR, length = 1),
                Col("l_linestatus",   CType.CHAR, length = 1),
                Col("l_shipdate",     CType.DATE),
                Col("l_commitdate",   CType.DATE),
                Col("l_receiptdate",  CType.DATE),
                Col("l_shipinstruct", CType.CHAR, length = 25),
                Col("l_shipmode",     CType.CHAR, length = 10),
                Col("l_comment",      CType.CHAR, length = 44)
            )
        )
    }

    private fun makeOrdersSchema(): YSchema {
        return YSchema(
            table = "orders",
            columns = listOf(
                Col("o_orderkey",   CType.INT),
                Col("o_custkey",    CType.INT),
                Col("o_status",     CType.CHAR, length = 25),
                Col("o_totalprice", CType.DECIMAL, precision = 15, scale = 2),
                Col("o_orderdate",  CType.DATE),
                Col("o_comment",    CType.STRING, length = 256)
            )
        )
    }

    @Test
    fun `INNER join lineitem and orders записывает только совпадающие записи через iterator`() {
        // --------- готовим lineitem и orders -------
        buildFile = Files.createTempFile("lineitem", ".tbl")
        probeFile = Files.createTempFile("orders", ".tbl")

        val lineitemLines = listOf(
            // Две строки: одна совпадёт по l_orderkey=100, вторая — нет
            "100|1|10|1|5.00|100.00|0.00|0.00|N|O|1995-03-15|1995-02-12|1995-03-20|DELIVER IN PERSON|AIR|Comment A",
            "200|2|20|2|3.00|200.00|0.10|0.05|R|F|1995-04-01|1995-03-10|1995-04-10|TAKE BACK RETURN|RAIL|Comment B"
        )
        Files.write(buildFile, lineitemLines)

        val ordersLines = listOf(
            "100|1000|O|1000.00|1995-02-15|OrderComment X",
            "300|2000|F|2000.00|1995-05-01|OrderComment Y"
        )
        Files.write(probeFile, ordersLines)

        buildSchema = makeLineitemSchema()
        probeSchema = makeOrdersSchema()

        val iter = BlockNLJoinIterator(
            buildPath   = buildFile,
            buildSchema = buildSchema,
            probePath   = probeFile,
            probeSchema = probeSchema,
            joinType    = JoinCommand.INNER,
            buildKey    = "l_orderkey",
            probeKey    = "o_orderkey",
            condOp      = ConditionOperator.EQUALS
        )

        val actualList = mutableListOf<String>()
        iter.open()
        while (true) {
            val row = iter.next() ?: break
            actualList.add(
                // Теперь у нас 16 полей из lineitem и 6 полей из orders.
                // Собираем их в правильном порядке:
                (0 until buildSchema.columns.size).joinToString("|") { idx ->
                    val colName = buildSchema.columns[idx].name
                    val raw = row.columns[colName]
                    raw?.toString() ?: ""
                } + "|" +
                        (0 until probeSchema.columns.size).joinToString("|") { idx ->
                            val colName = probeSchema.columns[idx].name
                            val raw = row.columns[colName]
                            raw?.toString() ?: ""
                        }
            )
        }
        iter.close()

        // Ожидаем ровно одну строку: ключ=100
        assertEquals(1, actualList.size)

        val expectedLine = listOf(
            "100","1","10","1","5.00","100.00","0.00","0.00","N","O","1995-03-15","1995-02-12","1995-03-20","DELIVER IN PERSON","AIR","Comment A",
            "100","1000","O","1000.00","1995-02-15","OrderComment X"
        ).joinToString("|")
        assertEquals(expectedLine, actualList.first())
    }

    @Test
    fun `FULL join lineitem and orders записывает unmatched-строки обеих таблиц через iterator`() {
        // --------- готовим lineitem_full и orders_full -------
        buildFile = Files.createTempFile("lineitem_full", ".tbl")
        probeFile = Files.createTempFile("orders_full", ".tbl")

        val lineitemLines = listOf(
            "100|1|10|1|5.00|100.00|0.00|0.00|N|O|1995-03-15|1995-02-12|1995-03-20|DELIVER IN PERSON|AIR|Comment A",
            "200|2|20|2|3.00|200.00|0.10|0.05|R|F|1995-04-01|1995-03-10|1995-04-10|TAKE BACK RETURN|RAIL|Comment B",
            "400|3|30|3|7.50|300.00|0.05|0.02|N|O|1995-05-01|1995-04-15|1995-05-10|NONE SPECIAL|FOB|Comment C"
        )
        Files.write(buildFile, lineitemLines)

        val ordersLines = listOf(
            "100|1000|O|1000.00|1995-02-15|OrderComment X",
            "300|2000|F|2000.00|1995-05-01|OrderComment Y",
            "500|3000|P|3000.00|1995-06-01|OrderComment Z"
        )
        Files.write(probeFile, ordersLines)

        buildSchema = makeLineitemSchema()
        probeSchema = makeOrdersSchema()

        val iter = BlockNLJoinIterator(
            buildPath   = buildFile,
            buildSchema = buildSchema,
            probePath   = probeFile,
            probeSchema = probeSchema,
            joinType    = JoinCommand.FULL,
            buildKey    = "l_orderkey",
            probeKey    = "o_orderkey",
            condOp      = ConditionOperator.EQUALS
        )

        val actualSet = mutableSetOf<String>()
        iter.open()
        while (true) {
            val row = iter.next() ?: break
            // Собираем 16 build-полей, затем 6 probe-полей. Если незаполнено — пустая строка.
            val line = (0 until buildSchema.columns.size).joinToString("|") { idx ->
                val colName = buildSchema.columns[idx].name
                row.columns[colName]?.toString() ?: ""
            } + "|" +
                    (0 until probeSchema.columns.size).joinToString("|") { idx ->
                        val colName = probeSchema.columns[idx].name
                        row.columns[colName]?.toString() ?: ""
                    }
            actualSet.add(line)
        }
        iter.close()

        // Ожидаем 5 строк:
        // 1) совпадение (ключ=100)
        val match100 = listOf(
            "100","1","10","1","5.00","100.00","0.00","0.00","N","O","1995-03-15","1995-02-12","1995-03-20","DELIVER IN PERSON","AIR","Comment A",
            "100","1000","O","1000.00","1995-02-15","OrderComment X"
        ).joinToString("|")

        // 2) unmatched-build (200 → пустые probe-поля)
        val unmatched200 = listOf(
            "200","2","20","2","3.00","200.00","0.10","0.05","R","F","1995-04-01","1995-03-10","1995-04-10","TAKE BACK RETURN","RAIL","Comment B",
            "","","","","",""
        ).joinToString("|")

        // 3) unmatched-build (400 → пустые probe-поля)
        val unmatched400 = listOf(
            "400","3","30","3","7.50","300.00","0.05","0.02","N","O","1995-05-01","1995-04-15","1995-05-10","NONE SPECIAL","FOB","Comment C",
            "","","","","",""
        ).joinToString("|")

        // 4) unmatched-probe (300 → пустые build-поля)
        val unmatched300 = listOf(
            "","","","","","","","","","","","","","","","",
            "300","2000","F","2000.00","1995-05-01","OrderComment Y"
        ).joinToString("|")

        // 5) unmatched-probe (500 → пустые build-поля)
        val unmatched500 = listOf(
            "","","","","","","","","","","","","","","","",
            "500","3000","P","3000.00","1995-06-01","OrderComment Z"
        ).joinToString("|")

        val expectedSet = setOf(match100, unmatched200, unmatched400, unmatched300, unmatched500)
        assertEquals(expectedSet, actualSet)
    }
}
