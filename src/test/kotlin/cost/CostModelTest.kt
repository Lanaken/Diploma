package cost

import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.max
import logical.Group
import operators.*
import org.bmstu.joins.*
import org.bmstu.reader.CType
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import statistic.*
import java.nio.file.Paths

class CostModelTest {

    // Хелпер для создания Group с нужными метриками и статистикой
    private fun makeGroup(
        table: String,
        rows: Long,
        pages: Long,
        avgRowSize: Long,
        bestCost: Double,
        bestCard: Double
    ): Group {
        // 1) Зарегистрировать Stats для таблицы
        val stats = Stats(
            rows = rows,
            filePages = pages,
            totalBytes = rows * avgRowSize,    // не pages*8192, а именно rows*avgRowSize
            rowSizeBytes = avgRowSize
        )
        stats.finalizeStats() // теперь stats.avgRowSize == avgRowSize
        Catalog.putStats(table, stats)

        // 2) Создать реальный Group
        val grp = Group(setOf(table))
        grp.bestCost = bestCost
        grp.bestCard = bestCard
        grp.avgRowSize = avgRowSize
        return grp
    }

    @Test
    fun `scan cost = pages*C_IO + rows*C_CPU`() {
        val g = makeGroup("T", rows = 1000, pages = 10, avgRowSize = 100, bestCost = 0.0, bestCard = 0.0)
        val cost = CostModel.cost(ScanOp("T", card = 1000), listOf(g))
        // 10*1.0 + 1000*0.01 = 10 + 10 = 20
        assertEquals(20.0, cost, 1e-6)
    }

    @Test
    fun `generic join uses selectivity and COMPARE_CPU`() {
        val l = makeGroup("A", rows = 100, pages = 2, avgRowSize = 50, bestCost = 5.0, bestCard = 100.0)
        val r = makeGroup("B", rows = 200, pages = 4, avgRowSize = 50, bestCost = 8.0, bestCard = 200.0)
        // для EQUALS селективность = 1/max(NDV)=1/1=1, т.к. ndv по умолчанию=1
        val op = JoinOp(JoinCommand.INNER, "c1", "c2", ConditionOperator.EQUALS)
        val expected = 5.0 + 8.0 + (100.0 * 200.0 * 1.0) * 0.01
        Catalog.stats("A").col["c1"] = ColStats(kind = CType.INT, ndv = 1)
        Catalog.stats("B").col["c2"] = ColStats(kind = CType.INT, ndv = 1)
        val cost = CostModel.cost(op, listOf(l, r))
        assertEquals(expected, cost, 1e-6)
    }

    @Test
    fun `hash join includes build+probe CPU and no spill when fits in mem`() {
        val l = makeGroup("A", rows = 100, pages = 1, avgRowSize = 100, bestCost = 2.0, bestCard = 100.0)
        val r = makeGroup("B", rows = 50, pages = 1, avgRowSize = 100, bestCost = 3.0, bestCard = 50.0)
        // Создадим HashJoinOp(JoinCommand.INNER, outerKey, innerKey, buildSideLeft, partitioned)
        val op = HashJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "dummy",  // ключи не используются в costHashJoin
            innerKey = "dummy",
            buildSideLeft = true,
            partitioned = false
        )
        val cost = CostModel.cost(op, listOf(l, r))
        // CPU = build(100*3*0.01) + probe(50*2*0.01) = 3.0 + 1.0 = 4.0
        // IO spill=0, partIO=0
        // total = 2.0 + 3.0 + 4.0 = 9.0
        assertEquals(9.0, cost, 1e-6)
    }

    @Test
    fun `hash join accounts for spill when too big`() {
        val big = makeGroup("A", rows = 10_000_000, pages = 1000, avgRowSize = 100, bestCost = 100.0, bestCard = 10_000_000.0)
        val small = makeGroup("B", rows = 100, pages = 10, avgRowSize = 100, bestCost = 10.0, bestCard = 100.0)
        val op = HashJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "dummy",
            innerKey = "dummy",
            buildSideLeft = true,
            partitioned = false
        )
        val cost = CostModel.cost(op, listOf(big, small))
        val buildBytes = big.bestCard * big.avgRowSize
        val spillPages = buildBytes / PAGE_BYTES
        val expected = big.bestCost + small.bestCost +
                (big.bestCard * HASH_BUILD_CPU + small.bestCard * HASH_PROBE_CPU) +
                spillPages * 2 * C_IO
        assertEquals(expected, cost, 1e-1)
    }

    @Test
    fun `index nested loop uses index height and compare cost`() {
        val outer = makeGroup("R", rows = 100, pages = 10, avgRowSize = 50, bestCost = 5.0, bestCard = 100.0)
        val inner = makeGroup("S", rows = 1000, pages = 20, avgRowSize = 50, bestCost = 15.0, bestCard = 1000.0)
        // Зарегистрируем метаданные индекса в Catalog:
        Catalog.putIndex("S", "c2", IndexMeta(leafPages = 50, height = 3))

        // Создаём IndexNLJoinOp(JoinCommand.INNER, outerKey, innerKey, indexPath, condOp)
        val op = IndexNLJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "c1",               // наружный ключ (не важен для стоимости)
            innerKey = "c2",               // столбец, по которому построен индекс
            indexPath = Paths.get("unused"),// путь не используется в costIndexNL
            condOp = ConditionOperator.EQUALS
        )
        val cost = CostModel.cost(op, listOf(outer, inner))

        val seek = 3 * C_IO + COMPARE_CPU
        val sel = 1.0 / inner.bestCard // 1/NDV == 1/1000 (approx)
        val expected = outer.bestCost +
                inner.bestCost * 0.05 +
                outer.bestCard * (seek + COMPARE_CPU) +
                outer.bestCard * sel * C_IO

        assertEquals(expected, cost, 1e-6)
    }

    @Test
    fun `block nested loop includes IO passes and rough CPU`() {
        val outer = makeGroup("A", rows = 1000, pages = 100, avgRowSize = 50, bestCost = 50.0, bestCard = 1000.0)
        val inner = makeGroup("B", rows = 500, pages = 50, avgRowSize = 50, bestCost = 25.0, bestCard = 500.0)
        // Создаём BlockNLJoinOp(JoinCommand.INNER, outerKey, innerKey, outerLeft, condOp)
        val op = BlockNLJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "c1",
            innerKey = "c2",
            outerLeft = true,
            condOp = ConditionOperator.EQUALS
        )

        val cost = CostModel.cost(op, listOf(outer, inner))

        // Берём страницы из Catalog
        val outerPages = Catalog.stats("A").filePages
        val innerPages = Catalog.stats("B").filePages

        // MEM_PAGES и константы из CostModel
        val MEM_PAGES = (Runtime.getRuntime().maxMemory() -
                Runtime.getRuntime().totalMemory() +
                Runtime.getRuntime().freeMemory()) * 0.7 / 8192.0
        val io = outerPages * C_IO + ceil(outerPages / MEM_PAGES) * innerPages * C_IO
        val cpu = outer.bestCard * inner.bestCard * COMPARE_CPU

        val expected = outer.bestCost + inner.bestCost + io + cpu

        assertEquals(expected, cost, 1e-6)
    }

    @Test
    fun `sort merge join pays for sorts and merge CPU`() {
        val l = makeGroup("A", rows = 100, pages = 5, avgRowSize = 50, bestCost = 10.0, bestCard = 100.0)
        val r = makeGroup("B", rows = 200, pages = 10, avgRowSize = 50, bestCost = 20.0, bestCard = 200.0)
        // Создаём SortMergeJoinOp(JoinCommand.INNER, outerKey, innerKey, alreadySorted, condOp)
        val op = SortMergeJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "c1",
            innerKey = "c2",
            alreadySorted = false,
            condOp = ConditionOperator.EQUALS
        )
        val cost = CostModel.cost(op, listOf(l, r))

        // sortL + sortR + mergeCPU
        val sortL = invokeCostSort(l)
        val sortR = invokeCostSort(r)
        val mergeCPU = (l.bestCard + r.bestCard) * MERGE_CPU
        assertEquals(sortL + sortR + mergeCPU, cost, 1e-6)
    }

    // вспомогательный доступ к приватному costSort
    private fun invokeCostSort(g: Group): Double {
        val method = CostModel::class.java.getDeclaredMethod("costSort", Group::class.java)
        method.isAccessible = true
        return method.invoke(CostModel, g) as Double
    }
}
