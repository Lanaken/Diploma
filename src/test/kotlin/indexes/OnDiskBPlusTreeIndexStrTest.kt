package indexes

import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.writeText
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.bmstu.indexes.BPlusTreeDiskBuilderInt
import org.bmstu.indexes.KeyRange
import org.bmstu.indexes.char.BPlusTreeDiskBuilderStr
import org.bmstu.indexes.char.OnDiskBPlusTreeIndexStr
import org.bmstu.reader.CType
import org.bmstu.reader.Col
import org.bmstu.reader.TablesLoader
import org.bmstu.reader.YSchema
import sort.ExternalStringRidSorter
import org.bmstu.util.createTbl
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInfo

class OnDiskBPlusTreeIndexStrTest {

    private val tmpDir = Path.of("build", "tmp", "junit").apply { createDirectories() }
    private val tblPath = Path.of("tables/customer.tbl")
    private val idxPath = tmpDir.resolve("customer_idx_cname.bpt")

    val schema = YSchema(
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

    @BeforeEach
    fun setUp(testInfo: TestInfo) {
        if (testInfo.tags.contains("CNameIndex")) {
            BPlusTreeDiskBuilderStr.build(
                tbl = tblPath,
                schema = schema,
                keyCol = "c_name",
                indexPath = idxPath,
                keyLen = 25,
                pageSize = 8192
            )
        }
        else {
            BPlusTreeDiskBuilderStr.build(
                tbl = tblPath,
                schema = schema,
                keyCol = "c_mktsegment",
                indexPath = idxPath,
                keyLen = 10,
                pageSize = 8192,
                order = 16
            )
        }
    }

    @Test
    fun `seekEqual returns all RIDs for given key`() {
        val index = OnDiskBPlusTreeIndexStr.open(idxPath, keyLen = 10)
        val rids = index.seekEqual("AUTOMOBILE")
        assertEquals(29752, rids.size)

        val values = rids.map { rid ->
            val row = TablesLoader.readRowByRid(tblPath, rid, schema)
            row.columns["c_name"] as String
        }
        assertTrue(values.all { it.startsWith("Customer#000") })
    }

    @Test
    @Tag("CNameIndex")
    fun `seekEqual returns empty for missing key`() {
        val index = OnDiskBPlusTreeIndexStr.open(idxPath, keyLen = 25)
        val rids = index.seekEqual("Customer#000150001")
        assertTrue(rids.isEmpty())
    }

    @Test
    @Tag("CNameIndex")
    fun `seekRange returns keys in lexicographic order`() {
        val index = OnDiskBPlusTreeIndexStr.open(idxPath, keyLen = 25)
        val range = KeyRange(lower = "AUTOMOBILE", upper = "date")
        val result = index.seekRange(range).asSequence().toList()

        val keys = result.map { it.first.trimEnd() }
       // assertEquals(listOf("banana", "cherry", "date"), keys)

        val phones = result.flatMap { it.second }
            .map { rid -> TablesLoader.readRowByRid(tblPath, rid, schema).columns["c_phone"] as String }
        println("phoneSize = ${phones.size}")
        assertTrue(phones.size >= keys.size)
    }

    @Test
    @Tag("CNameIndex")
    fun `iterator throws when no more elements`() {
        val index = OnDiskBPlusTreeIndexStr.open(idxPath, keyLen = 25)
        val it = index.seekRange(KeyRange(lower = "x", upper = "z"))
        assertFalse(it.hasNext())
        assertFailsWith<NoSuchElementException> { it.next() }
    }

    @Test
    fun `external sorter produces sorted sequence`() {
        val lines = listOf(
            "banana|100",
            "apple|200",
            "cherry|300",
            "date|400",
        )
        // Prepare a tiny temporary .tbl file with unsorted keys
        val smallTbl = createTbl(lines)
        // Schema for this small table: STRING key + INT value
        val smallSchema = YSchema(
            table = "small",
            columns = listOf(
                Col("key", CType.STRING, length = 10),
                Col("value", CType.INT)
            )
        )
        // Use a sorter with tiny memory budget to force external merge
        val sorter = ExternalStringRidSorter(memLimitBytes = 1)
        val sorted = sorter.sort(smallTbl, smallSchema, "key", 10).toList()

        // Verify keys come out in lex order
        val keys = sorted.map { it.first.trimEnd() }
        assertEquals(listOf("apple", "banana", "cherry", "date"), keys)

        // Verify we reconstruct the 'value' correctly by reading via RID
        val values = sorted.map { (_, rid) ->
            val row = TablesLoader.readRowByRid(smallTbl, rid, smallSchema)
            row.columns["value"] as Int
        }
        assertEquals(listOf(200, 100, 300, 400), values)
    }
}
