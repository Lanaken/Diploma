import org.bmstu.reader.*
import org.bmstu.tables.Row
import java.math.BigDecimal
import java.nio.file.Path
import java.time.LocalDate
import kotlin.test.*
import org.bmstu.indexes.BPlusTreeDiskBuilderInt
import org.bmstu.indexes.OnDiskBPlusTreeIndexInt
import org.bmstu.indexes.char.BPlusTreeDiskBuilderStr
import org.bmstu.indexes.char.OnDiskBPlusTreeIndexStr
import reader.Rid

class OrdersTableTest {

    private val schema = YSchema(
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

    private val customerSchema = YSchema(
        table = "customer",
        columns = listOf(
            Col("c_custkey",   CType.INT,    index = true),
            Col("c_name",      CType.STRING, length = 25),
            Col("c_address",   CType.STRING, length = 40),
            Col("c_nationkey", CType.INT),
            Col("c_phone",     CType.STRING, length = 15),
            Col("c_acctbal",   CType.DECIMAL, precision = 15, scale = 2),
            Col("c_mktsegment",CType.STRING, length = 10),
            Col("c_comment",   CType.STRING, length = 117)
        )
    )

    private val tblPath = Path.of("tables/orders.tbl")
    private val idxPath = Path.of("tables/orders_idx.bpt")
    private val tblCustomerPath = Path.of("tables/customer.tbl")
    private val idxCustomerPath = Path.of("tables/customer_idx.bpt")

    @BeforeTest
    fun buildIndexIfNeeded() {
        if (!idxPath.toFile().exists()) {
            BPlusTreeDiskBuilderInt.build(
                tbl = tblPath,
                schema = schema,
                keyCol = "o_orderkey",
                indexPath = idxPath,
                pageSize = 8192,
                order = 64
            )
        }
        if (!idxCustomerPath.toFile().exists()) {
            BPlusTreeDiskBuilderStr.build(
                tbl = tblCustomerPath,
                schema = customerSchema,
                keyCol = "c_name",
                indexPath = idxCustomerPath,
                keyLen = 25,
                pageSize = 8192,
                order = 64
            )
        }
    }

    @Test
    fun `test readTbl parses orders rows correctly`() {
        val rows = mutableListOf<Row>()

        TablesLoader.readTbl(tblPath, schema) { row ->
            if (row.columns["o_orderkey"] as Int <= 3)
                rows.add(row)
        }

        assertEquals(3, rows.size)
        assertEquals(36901, rows[0].columns["o_custkey"])
        assertEquals("O", rows[0].columns["o_status"])
        assertEquals(BigDecimal("173665.47"), rows[0].columns["o_totalprice"])
        assertEquals(LocalDate.parse("1996-01-02"), rows[0].columns["o_orderdate"])
        assertEquals("5-LOW", rows[0].columns["o_comment"])
    }

    @Test
    fun `test readTblWithOffsets returns correct RID offsets`() {
        val keys = mutableListOf<Any>()
        val rids = mutableListOf<Rid>()

        TablesLoader.readTblWithOffsets(tblPath, schema, "o_orderkey") { key, rid ->
            if (key as Int <= 3) {
                keys.add(key)
                rids.add(rid)
            }
        }

        assertEquals(listOf<Any>(1, 2, 3), keys)
        assertTrue(rids[0].pos < rids[1].pos && rids[1].pos < rids[2].pos)
    }

    @Test
    fun `test readRowByRid returns exact row`() {
        val rids = mutableListOf<Rid>()

        TablesLoader.readTblWithOffsets(tblPath, schema, "o_orderkey") { _, rid ->
            rids.add(rid)
        }

        val row = TablesLoader.readRowByRid(tblPath, rids[2], schema)

        assertEquals(3, row.columns["o_orderkey"])
        assertEquals("F", row.columns["o_status"])
        assertEquals(BigDecimal("193846.25"), row.columns["o_totalprice"])
        assertEquals("5-LOW", row.columns["o_comment"])
    }

    @Test
    fun `benchmark seq scan vs RID lookup`() {
        val keyToFind = 5656999
        var foundViaScan: Row? = null

        val startScan = System.nanoTime()
        TablesLoader.readTbl(tblPath, schema) { row ->
            if (row.columns["o_orderkey"] == keyToFind) {
                foundViaScan = row
            }
        }
        val timeScan = System.nanoTime() - startScan

        // Сначала получим RID этой строки
        var rid: Rid? = null
        TablesLoader.readTblWithOffsets(tblPath, schema, "o_orderkey") { key, r ->
            if (key == keyToFind) {
                rid = r
            }
        }

        val startSeek = System.nanoTime()
        val foundViaRid = TablesLoader.readRowByRid(tblPath, rid!!, schema)
        val timeSeek = System.nanoTime() - startSeek

        println("Seq scan took ${timeScan / 1_000_000.0} ms")
        println("RID seek took ${timeSeek / 1_000_000.0} ms")

        assertEquals(foundViaScan?.columns, foundViaRid.columns)
    }

    @Test
    fun `benchmark seq scan vs bpt seekEqual`() {
        val keyToFind = 5656999
        var foundViaScan: Row? = null

        val startScan = System.nanoTime()
        TablesLoader.readTbl(tblPath, schema) { row ->
            if (row.columns["o_orderkey"] == keyToFind) {
                foundViaScan = row
            }
        }
        val timeScan = System.nanoTime() - startScan

        val index = OnDiskBPlusTreeIndexInt.open(idxPath)
        val startIdx = System.nanoTime()
        val rids = index.seekEqual(keyToFind)
        val foundViaIndex = if (rids.isNotEmpty()) TablesLoader.readRowByRid(tblPath, rids.first(), schema) else null
        val timeIdx = System.nanoTime() - startIdx

        println("SeqScan: ${timeScan / 1_000_000.0} ms")
        println("B+Tree SeekEqual: ${timeIdx / 1_000_000.0} ms")

        assertEquals(foundViaScan?.columns, foundViaIndex?.columns)
    }

    @Test
    fun `benchmark seq scan vs bpt seekEqual string`() {
        val keyToFind = "Customer#000107443"
        var foundViaScan: Row? = null

        val startScan = System.nanoTime()
        TablesLoader.readTbl(tblCustomerPath, customerSchema) { row ->
            if (row.columns["c_name"] == keyToFind) {
                foundViaScan = row
            }
        }
        val timeScan = System.nanoTime() - startScan

        val index = OnDiskBPlusTreeIndexStr.open(idxCustomerPath, 25)
        val startIdx = System.nanoTime()
        val rids = index.seekEqual(keyToFind)
        val foundViaIndex = if (rids.isNotEmpty())
            TablesLoader.readRowByRid(tblCustomerPath, rids.first(), customerSchema) else null
        val timeIdx = System.nanoTime() - startIdx

        println("SeqScan: ${timeScan / 1_000_000.0} ms")
        println("B+Tree SeekEqual: ${timeIdx / 1_000_000.0} ms")

        assertEquals(foundViaScan?.columns, foundViaIndex?.columns)
    }

    @Test
    fun `test readTblWithRids returns row with correct RID`() {
        val rows = mutableListOf<Pair<Row, Rid>>()

        TablesLoader.readTblWithRids(tblPath, schema) { row, rid ->
            if ((row.columns["o_orderkey"] as Int) <= 3) {
                rows.add(row to rid)
            }
        }

        assertEquals(3, rows.size)
        assertTrue(rows[0].second.pos < rows[1].second.pos)
        assertEquals(1, rows[0].first.columns["o_orderkey"])
        assertEquals(2, rows[1].first.columns["o_orderkey"])
    }

}
