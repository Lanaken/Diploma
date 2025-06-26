package logical

import kotlin.test.*
import operators.JoinOp
import operators.ScanOp
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.bmstu.joins.Join
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator

class LogicalBuilderTest {

    private fun makeJoin(
        left: String,
        right: String,
        type: JoinCommand = JoinCommand.INNER,
        leftCol: String = "x",
        rightCol: String = "x",
        op: ConditionOperator = ConditionOperator.EQUALS
    ): Join = Join(
        leftTable  = left,
        rightTable = right,
        type       = type,
        leftColumn = leftCol,
        rightColumn= rightCol,
        operator   = op
    )

    @Test
    @DisplayName("Single join → две Scan-группы и одна Join-группа")
    fun `build single join`() {
        val memo = Memo()
        val joinAB = makeJoin("A", "B")

        val root = LogicalBuilder.build(listOf(joinAB), memo)

        // корневая группа должна быть {A, B}
        assertEquals(setOf("A","B"), root.tables)
        // в memo должны быть три группы: {A}, {B} и {A,B}
        assertNotNull(memo.groupOf(setOf("A")))
        assertNotNull(memo.groupOf(setOf("B")))
        assertNotNull(memo.groupOf(setOf("A","B")))

        // группа {A} содержит ровно один ScanOp
        val gA = memo.groupOf(setOf("A"))
        assertEquals(1, gA.expressions.size)
        assertTrue(gA.expressions[0].op is ScanOp)

        // группа {B} содержит ровно один ScanOp
        val gB = memo.groupOf(setOf("B"))
        assertEquals(1, gB.expressions.size)
        assertTrue(gB.expressions[0].op is ScanOp)

        // группа {A,B} содержит ровно одну Expression с JoinOp
        val gAB = memo.groupOf(setOf("A","B"))
        assertEquals(1, gAB.expressions.size)
        val expr = gAB.expressions.single()
        assertTrue(expr.op is JoinOp)
        // и его дети — именно группы {A} и {B}
        assertEquals(listOf(gA, gB), expr.children)
        // а build-функция вернула эту же группу
        assertSame(gAB, root)
    }

    @Test
    @DisplayName("Left-deep join: (A⋈B)⋈C")
    fun `build two joins left deep`() {
        val memo = Memo()
        val joinAB = makeJoin("A", "B", leftCol = "a", rightCol = "b")
        val joinBC = makeJoin("B", "C", leftCol = "b", rightCol = "c")

        val root = LogicalBuilder.build(listOf(joinAB, joinBC), memo)

        // группы {A}, {B}, {C}, {A,B}, {A,B,C} должны присутствовать
        val gA   = memo.groupOf(setOf("A"))
        val gB   = memo.groupOf(setOf("B"))
        val gC   = memo.groupOf(setOf("C"))
        val gAB  = memo.groupOf(setOf("A","B"))
        val gABC = memo.groupOf(setOf("A","B","C"))

        assertNotNull(gA)
        assertNotNull(gB)
        assertNotNull(gC)
        assertNotNull(gAB)
        assertNotNull(gABC)

        // корневая группа — это {A,B,C}
        assertSame(gABC, root)

        // в группе {A,B} — один JoinOp с детьми {A} и {B}
        run {
            assertEquals(1, gAB.expressions.size)
            val e = gAB.expressions.single()
            assertTrue(e.op is JoinOp)
            assertEquals(listOf(gA, gB), e.children)
        }

        // в группе {A,B,C} — один JoinOp, дети — {A,B} и {C}
        run {
            assertEquals(1, gABC.expressions.size)
            val e = gABC.expressions.single()
            assertTrue(e.op is JoinOp)
            assertEquals(listOf(gAB, gC), e.children)
        }

        // сканы вleaf-группах
        assertTrue(gA.expressions[0].op is ScanOp)
        assertTrue(gB.expressions[0].op is ScanOp)
        assertTrue(gC.expressions[0].op is ScanOp)
    }

    @Test
    @DisplayName("Complex chain: mixed INNER, LEFT, RIGHT, FULL, INNER")
    fun `build complex mixed joins`() {
        val memo = Memo()
        val joins = listOf(
            makeJoin("customer", "orders", JoinCommand.INNER, "c_custkey", "o_custkey", ConditionOperator.EQUALS),
            makeJoin("orders",   "lineitem", JoinCommand.LEFT,  "o_orderkey", "l_orderkey", ConditionOperator.EQUALS),
            makeJoin("lineitem","supplier",   JoinCommand.RIGHT, "l_suppkey",  "s_suppkey",  ConditionOperator.NOT_EQUALS),
            makeJoin("supplier","nation",     JoinCommand.FULL,  "s_nationkey","n_nationkey",ConditionOperator.LESS_THAN),
            makeJoin("nation",  "region",     JoinCommand.INNER, "n_regionkey","r_regionkey",ConditionOperator.GREATER_THAN_OR_EQUALS)
        )

        val root = LogicalBuilder.build(joins, memo)

        // Должна быть единственная выражение в корневой группе и она покрывает все 6 таблиц
        assertEquals(setOf("customer","orders","lineitem","supplier","nation","region"), root.tables)
        assertEquals(1, root.expressions.size)

        // Список ожидаемых операций снизу вверх
        data class Expected(
            val type: JoinCommand,
            val leftCol: String,
            val rightCol: String,
            val op: ConditionOperator,
            val rightTable: String
        )
        val expected = listOf(
            Expected(JoinCommand.INNER,   "n_regionkey", "r_regionkey", ConditionOperator.GREATER_THAN_OR_EQUALS, "region"),
            Expected(JoinCommand.FULL,    "s_nationkey", "n_nationkey", ConditionOperator.LESS_THAN,             "nation"),
            Expected(JoinCommand.RIGHT,   "l_suppkey",   "s_suppkey",    ConditionOperator.NOT_EQUALS,         "supplier"),
            Expected(JoinCommand.LEFT,    "o_orderkey",  "l_orderkey",   ConditionOperator.EQUALS,             "lineitem"),
            Expected(JoinCommand.INNER,   "c_custkey",   "o_custkey",    ConditionOperator.EQUALS,             "orders")
        )

        // Идём от корня по цепочке left-child
        var currentGroup = root
        for (exp in expected) {
            val expr = currentGroup.expressions.single()
            // Проверяем тип JOIN и колонки
            assertTrue(expr.op is JoinOp, "Ожидался JoinOp")
            val jop = expr.op as JoinOp
            assertEquals(exp.type, jop.type,     "Тип JOIN")
            assertEquals(exp.leftCol,  jop.leftCol,  "Левая колонка")
            assertEquals(exp.rightCol, jop.rightCol, "Правая колонка")
            assertEquals(exp.op,       jop.op,       "Оператор")

            // Проверяем, что правый ребёнок — Scan группы нужной таблицы
            val rightGroup = expr.children[1]
            assertEquals(setOf(exp.rightTable), rightGroup.tables)

            // Спускаемся в левый child, чтобы проверить предыдущий JOIN
            currentGroup = expr.children[0]
        }

        // В конце левое поддерево — это Scan(customer)
        val leafExpr = currentGroup.expressions.single()
        assertTrue(leafExpr.op is ScanOp)
        assertEquals(setOf("customer"), currentGroup.tables)
        val expectedGroups = listOf(
            setOf("customer"),
            setOf("orders"),
            setOf("lineitem"),
            setOf("supplier"),
            setOf("nation"),
            setOf("region"),
            setOf("customer", "orders"),
            setOf("customer", "orders", "lineitem"),
            setOf("customer", "orders", "lineitem", "supplier"),
            setOf("customer", "orders", "lineitem", "supplier", "nation"),
            setOf("customer", "orders", "lineitem", "supplier", "nation", "region")
        )
        for (grp in expectedGroups) {
            assertNotNull(memo.groupOf(grp), "Ожидалась группа $grp")
        }

    }
}
