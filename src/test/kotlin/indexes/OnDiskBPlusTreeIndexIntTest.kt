package indexes
import java.math.BigDecimal
import java.nio.file.Path
import kotlin.io.path.*
import kotlin.test.*
import org.bmstu.indexes.BPlusTreeDiskBuilderInt
import org.bmstu.indexes.KeyRange
import org.bmstu.indexes.OnDiskBPlusTreeIndexInt
import org.bmstu.reader.*

class OnDiskBTreeTest {

    private val tmpDir = Path.of("build", "tmp", "junit").apply { createDirectories() }
    private val tblPath = Path.of("tables/orders.tbl")
    private val bptPath = tmpDir.resolve("orders_idx_orderkey.bpt")

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

    @BeforeTest
    fun createTbl() {
        // build index
        BPlusTreeDiskBuilderInt.build(
            tbl       = tblPath,
            schema    = schema,
            keyCol    = "o_orderkey",
            indexPath = bptPath,
            pageSize  = 8192,
            order     = 128
        )
    }

    @Test
    fun `seekEqual returns correct RIDs`() {
        val idx = OnDiskBPlusTreeIndexInt.open(bptPath)

        val ridList = idx.seekEqual(3440609)
        assertEquals(1, ridList.size)

        val row = TablesLoader.readRowByRid(tblPath, ridList.first(), schema)
        assertEquals(13750, row.columns["o_custkey"])
        assertEquals(BigDecimal("361588.68"), row.columns["o_totalprice"])
    }

    @Test
    fun `seekRange iterates ascending keys`() {
        val idx = OnDiskBPlusTreeIndexInt.open(bptPath)

        val range = KeyRange(lower = 1, upper = 2)
        val pairs = idx.seekRange(range).asSequence().toList()

        assertEquals(listOf(1, 2), pairs.map { it.first })
        assertEquals(1, pairs[0].second.size)  // по одному RID
        assertEquals(1, pairs[1].second.size)
    }

    @Test
    fun `index file has multiple pages`() {
        val size = bptPath.fileSize()
        assertTrue(size > 8192 + 32, "файл должен иметь >1 страницы")
    }
}
