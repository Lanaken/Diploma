package logical

import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import operators.ScanOp
import rules.Rule
import statistic.Catalog
import statistic.Stats

class JobTest {

    // "Логическое" правило: всегда матчит и порождает два скана X и Y
    private object DummyLogicalRule : Rule {
        override fun matches(e: Expression): Boolean = true
        override fun apply(e: Expression, memo: Memo): List<Expression> {
            val x = memo.insert(ScanOp("X", 1), emptyList())!!
            val y = memo.insert(ScanOp("Y", 2), emptyList())!!
            return listOf(x, y)
        }
    }

    @BeforeTest
    fun clearCatalog() {
        // Чтобы CostModel для ScanOp не уехал в Infinity
        listOf("A", "B", "X", "Y").forEach {
            val st = Stats(rows = 0, filePages = 0, totalBytes = 0, rowSizeBytes = 1)
            st.finalizeStats()
            Catalog.putStats(it, st)
        }
    }

    @Test
    fun `ExploreExprJob должен порождать ExploreExprJob для новых выражений и ImplementExprJob для исходного`() {
        val memo = Memo()
        // исходное выражение
        val seed = memo.insert(ScanOp("A", 10), emptyList())!!
        // Engine: одно логическое правило, физических — нет
        val engine = Engine(
            memo,
            logicalRules = listOf(DummyLogicalRule),
            physicalRules = emptyList()
        )
        engine.jobs.clear()

        ExploreExprJob(seed).run(engine)

        // в Memo появились группы {X} и {Y}
        assertNotNull(memo.groupOf(setOf("X")))
        assertNotNull(memo.groupOf(setOf("Y")))
        assertTrue(memo.groupOf(setOf("X")).expressions[0].op is ScanOp)
        assertTrue(memo.groupOf(setOf("Y")).expressions[0].op is ScanOp)

        // в очереди задач:
        //   2 × ExploreExprJob для X и Y
        // + 1 × ImplementExprJob для seed
        val exploreCount = engine.jobs.count { it is ExploreExprJob }
        val implementCount = engine.jobs.count { it is ImplementExprJob }
        assertEquals(2, exploreCount, "Должно быть ровно 2 ExploreExprJob (для X и Y)")
        assertEquals(1, implementCount, "Должно быть ровно 1 ImplementExprJob (для исходного scan A)")
    }

    @Test
    fun `OptimizeGroupJob для одиночного ScanOp должен установить bestCost и bestExpression`() {
        val memo = Memo()
        val tbl = "A"

        // (1) подставляем статистику: 5 строк, 0 страниц
        val st = Stats(rows = 5, filePages = 0, totalBytes = 0, rowSizeBytes = 1)
        st.finalizeStats()
        Catalog.putStats(tbl, st)

        // (2) создаём ScanOp и отмечаем группу как разведённую
        val exprA = memo.insert(ScanOp(tbl, 5), emptyList())!!
        val gA = memo.groupOf(setOf(tbl))
        gA.explored = true

        // (3) запускаем OptimizeGroupJob
        val engine = Engine(memo, logicalRules = emptyList(), physicalRules = emptyList())
        engine.jobs.clear()
        OptimizeGroupJob(gA).run(engine)

        // (4) проверяем: cost = rows * 0.01 = 5 * 0.01 = 0.05
        assertEquals(0.05, gA.bestCost, 1e-9)
        assertNotNull(gA.bestExpression)
        assertTrue(gA.bestExpression!!.op is ScanOp)
    }
}
