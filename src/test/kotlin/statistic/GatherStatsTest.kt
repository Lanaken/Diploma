package statistic

import java.math.BigDecimal
import kotlin.test.*
import java.nio.file.Files
import java.nio.file.Paths
import org.bmstu.reader.CType
import org.bmstu.reader.Col
import org.bmstu.reader.YSchema

class GatherStatsTest {

    private val tbl = Paths.get("tables/customer.tbl")
    private val schema = makeSchema(
        Col("c_custkey",   CType.INT,     index = true),
        Col("c_name",      CType.CHAR,    length = 25, index = true),
        Col("c_address",   CType.CHAR,    length = 40),
        Col("c_nationkey", CType.INT),
        Col("c_phone",     CType.CHAR,    length = 15),
        Col("c_acctbal",   CType.DECIMAL, precision = 15, scale = 2),
        Col("c_mktsegment",CType.CHAR,    length = 10),
        Col("c_comment",   CType.CHAR,    length = 117)
    )

    private fun makeSchema(vararg cols: Col) = YSchema(
        table = "t",
        columns = cols.toList()
    )

    @Test
    fun `INT and BIGINT stats`() {
        val tbl = Files.createTempFile("intbig", ".tbl")
        Files.write(tbl, listOf("1|100", "2|200"))
        val schema = makeSchema(
            Col("a", CType.INT),
            Col("b", CType.BIGINT)
        )

        val stats = gatherStats(tbl, schema)

        assertEquals(2L, stats.rows)
        assertTrue(stats.filePages >= 1, "Должна быть хотя бы 1 страница")

        val sa = stats.col.getValue("a")
        assertEquals(2L, sa.ndv, "NDV для {1,2}")
        assertEquals(1L, sa.min)
        assertEquals(2L, sa.max)
        assertTrue(sa.wasSorted, "1<=2 => sorted=true")

        val sb = stats.col.getValue("b")
        assertEquals(2L, sb.ndv, "NDV для {100,200}")
        assertEquals(100L, sb.min)
        assertEquals(200L, sb.max)
        assertTrue(sb.wasSorted)
    }

    @Test
    fun `DECIMAL stats`() {
        val tbl = Files.createTempFile("dec", ".tbl")
        Files.write(tbl, listOf("1.5|2.5", "3.5|4.5"))
        val schema = makeSchema(
            Col("d1", CType.DECIMAL),
            Col("d2", CType.DECIMAL)
        )

        val stats = gatherStats(tbl, schema)
        assertEquals(2L, stats.rows)

        val s1 = stats.col.getValue("d1")
        assertTrue(s1.ndv >= 2, "NDV десятичных ≥2")
        assertEquals(BigDecimal.valueOf(1.5), s1.minDec)
        assertEquals(BigDecimal.valueOf(3.5), s1.maxDec)
        assertTrue(s1.wasSorted)

        val s2 = stats.col.getValue("d2")
        assertTrue(s2.ndv >= 2)
        assertEquals(BigDecimal.valueOf(2.5), s2.minDec)
        assertEquals(BigDecimal.valueOf(4.5), s2.maxDec)
        assertTrue(s2.wasSorted)
    }

    @Test
    fun `DATE stats and sorted flag`() {
        val tbl = Files.createTempFile("date", ".tbl")
        Files.write(tbl, listOf("2020-01-01|2020-01-02", "2020-02-01|2020-03-01"))
        val schema = makeSchema(
            Col("d1", CType.DATE),
            Col("d2", CType.DATE)
        )

        val stats = gatherStats(tbl, schema)
        assertEquals(2L, stats.rows)

        val sd1 = stats.col.getValue("d1")
        assertTrue(sd1.wasSorted, "2020-01-01<=2020-02-01")
        assertTrue(sd1.min <= sd1.max)

        val sd2 = stats.col.getValue("d2")
        assertTrue(sd2.wasSorted)
    }

    @Test
    fun `CHAR and STRING stats`() {
        val tbl = Files.createTempFile("str", ".tbl")
        Files.write(tbl, listOf("apple|zebra", "banana|yak"))
        val schema = makeSchema(
            Col("c1", CType.CHAR, length = 10),
            Col("c2", CType.STRING)
        )

        val stats = gatherStats(tbl, schema)
        assertEquals(2L, stats.rows)

        val sc1 = stats.col.getValue("c1")
        assertEquals("apple", sc1.minStr)
        assertEquals("banana", sc1.maxStr)
        assertTrue(sc1.wasSorted, "apple<=banana")

        val sc2 = stats.col.getValue("c2")
        assertEquals("yak", sc2.minStr, "yak<zebra")
        assertEquals("zebra", sc2.maxStr)
        assertFalse(sc2.wasSorted, "zebra>yak → sorted=false")
    }

    @Test
    fun `customer tbl basic stats`() {
        val stats = gatherStats(tbl, schema)

        // общее число строк
        assertEquals(150_000L, stats.rows, "rows")

        // вспомогательная функция для проверки NDV с допуском
        fun assertNdvApprox(colName: String, expected: Long) {
            val actual = stats.col.getValue(colName).ndv
            val tol = (expected * 0.01).toLong() // 1%
            assertTrue(
                kotlin.math.abs(actual - expected) <= tol,
                "NDV для $colName ($actual) должно быть в пределах ±1% от $expected"
            )
        }

        // первичный ключ
        val ck = stats.col.getValue("c_custkey")
        assertNdvApprox("c_custkey", stats.rows)
        assertEquals(1L,       ck.min,   "min для c_custkey")
        assertEquals(150_000L, ck.max,   "max для c_custkey")
        assertTrue(ck.wasSorted,          "c_custkey отсортирован")

        // foreign key
        val nk = stats.col.getValue("c_nationkey")
        assertNdvApprox("c_nationkey", 25L)
        assertEquals(0L, nk.min,        "min для c_nationkey")
        assertEquals(24L, nk.max,       "max для c_nationkey")
        assertFalse(nk.wasSorted,        "c_nationkey отсортирован")

        // баланс (десятичный)
        val ba = stats.col.getValue("c_acctbal")
        // здесь достаточно, чтобы ndv было не слишком маленьким
        assertTrue(
            ba.ndv >= (stats.rows * 0.9).toLong(),
            "примерно столько разных c_acctbal (>=90% от ${stats.rows})"
        )
        assertEquals(BigDecimal("-999.99"), ba.minDec, "minDec для c_acctbal")
        assertEquals(BigDecimal("9999.99"),  ba.maxDec, "maxDec для c_acctbal")
        assertFalse(ba.wasSorted,                     "c_acctbal отсортирован")

        // маркет-сегменты
        val ms = stats.col.getValue("c_mktsegment")
        assertNdvApprox("c_mktsegment", 5L)
        assertEquals("AUTOMOBILE", ms.minStr, "минимальный сегмент")
        assertEquals("MACHINERY",  ms.maxStr, "максимальный сегмент")
        assertFalse(ms.wasSorted,               "c_mktsegment отсортирован")
    }
}
