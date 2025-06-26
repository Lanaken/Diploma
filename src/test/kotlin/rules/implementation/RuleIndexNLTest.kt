package rules.implementation

import logical.Memo
import logical.Group
import logical.Expression
import operators.IndexNLJoinOp
import operators.JoinOp
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import operators.ScanOp
import statistic.Catalog
import statistic.IndexMeta

class RuleIndexNLTest {

    private val rule = RuleIndexNL

    private fun makeJoinExpr(
        type: JoinCommand,
        leftCol: String,
        rightCol: String,
        op: ConditionOperator,
        leftTables: Set<String>,
        rightTables: Set<String>
    ): Expression {
        // создаём группы по одному имени (hasIndexFor сработает, если имя колонки оканчивается на "key")
        val leftG = Group(leftTables)
        val rightG = Group(rightTables)
        val joinOp = JoinOp(type, leftCol, rightCol, op)
        return Expression(joinOp, listOf(leftG, rightG))
    }

    @Test
    fun `matches false for non-JoinOp`() {
        val expr = Expression(ScanOp("T", 1), emptyList())
        assertFalse(rule.matches(expr))
    }

    @Test
    fun `matches true for left join`() {
        val expr = makeJoinExpr(
            JoinCommand.LEFT, "a_key", "b_key", ConditionOperator.EQUALS,
            setOf("L"), setOf("R")
        )
        assertTrue(rule.matches(expr))
    }

    @Test
    fun `matches true for non-equals operator`() {
        val expr = makeJoinExpr(
            JoinCommand.INNER, "a_key", "b_key", ConditionOperator.LESS_THAN,
            setOf("L"), setOf("R")
        )
        assertTrue(rule.matches(expr))
    }

    @Test
    fun `matches true for inner equals`() {
        val expr = makeJoinExpr(
            JoinCommand.INNER, "a_key", "b_key", ConditionOperator.EQUALS,
            setOf("L"), setOf("R")
        )
        assertTrue(rule.matches(expr))
    }

    @Test
    fun `apply yields no IndexNL when no indexes on either side`() {
        val memo = Memo()
        // колонки не оканчиваются на "key" → hasIndexFor = false
        val expr = memo.insert(
            JoinOp(JoinCommand.INNER, "a", "b", ConditionOperator.EQUALS),
            listOf(Group(setOf("L")), Group(setOf("R")))
        )!!
        val out = rule.apply(expr, memo)
        assertTrue(out.isEmpty())
    }

    @Test
    fun `apply yields one IndexNL when right has index`() {
        val memo = Memo()

        // 1) заводим в Catalog индекс на таблицу "R" по колонке "r_key":
        Catalog.putIndex("R", "r_key", IndexMeta(leafPages = 42, height = 3))

        // 2) строим Expr через Memo:
        val gL = memo.insert(ScanOp("L", 1), emptyList())!!.let { memo.groupOf(setOf("L")) }
        val gR = memo.insert(ScanOp("R", 1), emptyList())!!.let { memo.groupOf(setOf("R")) }
        val expr = memo.insert(
            JoinOp(JoinCommand.INNER, "x", "r_key", ConditionOperator.EQUALS),
            listOf(gL, gR)
        )!!

        // 3) теперь apply действительно найдёт один IndexNLJoinOp:
        val out = rule.apply(expr, memo)
        assertEquals(1, out.size)
        val idxExpr = out.single()
        assertTrue(idxExpr.op is IndexNLJoinOp)

        // Проверяем, что параметры оператора соответствуют ожиданиям:
        val idxOp = idxExpr.op as IndexNLJoinOp
        assertEquals(JoinCommand.INNER, idxOp.joinType)
        assertEquals("x", idxOp.outerKey)
        assertEquals("r_key", idxOp.innerKey)
        assertEquals(ConditionOperator.EQUALS, idxOp.condOp)

        // Дети остались теми же:
        assertEquals(expr.children, idxExpr.children)
    }



    @Test
    fun `apply yields one IndexNL when left has index`() {
        val memo = Memo()

        // 1) заводим индекс на левой таблице "L" по колонке "l_key"
        Catalog.putIndex("L", "l_key", IndexMeta(leafPages = 10, height = 2))

        // 2) создаём группы через Memo
        val gL = memo.insert(ScanOp("L", 1), emptyList())!!.let { memo.groupOf(setOf("L")) }
        val gR = memo.insert(ScanOp("R", 1), emptyList())!!.let { memo.groupOf(setOf("R")) }

        // 3) строим JoinExpr
        val expr = memo.insert(
            JoinOp(JoinCommand.INNER, "l_key", "y", ConditionOperator.EQUALS),
            listOf(gL, gR)
        )!!

        // 4) теперь apply найдёт один IndexNLJoinOp, причём он будет
        //    построен с outerKey="y", innerKey="l_key", т.к. мы "поменяем местами" при сохранении.
        val out = rule.apply(expr, memo)
        assertEquals(1, out.size)
        val idxExpr = out.single()
        assertTrue(idxExpr.op is IndexNLJoinOp)

        val idxOp = idxExpr.op as IndexNLJoinOp
        assertEquals(JoinCommand.INNER, idxOp.joinType)
        // Поскольку индекс был на левой стороне (l_key), то теперь l_key используется
        // как innerKey, а "y" как outerKey
        assertEquals("y", idxOp.outerKey)
        assertEquals("l_key", idxOp.innerKey)
        assertEquals(ConditionOperator.EQUALS, idxOp.condOp)
        assertTrue(idxOp.indexPath.toString().isNotEmpty())

        // Дети остались теми же
        assertEquals(expr.children, idxExpr.children)
    }

    @Test
    fun `apply yields two IndexNL when both sides have index`() {
        val memo = Memo()

        // 1) заводим сразу два индекса
        Catalog.putIndex("L", "l_key", IndexMeta(leafPages = 5, height = 2))
        Catalog.putIndex("R", "r_key", IndexMeta(leafPages = 7, height = 3))

        // 2) строим группы
        val gL = memo.insert(ScanOp("L", 1), emptyList())!!.let { memo.groupOf(setOf("L")) }
        val gR = memo.insert(ScanOp("R", 1), emptyList())!!.let { memo.groupOf(setOf("R")) }

        // 3) Join по l_key=r_key
        val expr = memo.insert(
            JoinOp(JoinCommand.INNER, "l_key", "r_key", ConditionOperator.EQUALS),
            listOf(gL, gR)
        )!!

        // 4) apply выдаст два варианта: index на правую и на левую стороны
        val out = rule.apply(expr, memo)
        assertEquals(2, out.size)

        // Проверяем множество (outerKey, innerKey) у каждого оператора:
        val pairs = out.map { (it.op as IndexNLJoinOp).outerKey to (it.op as IndexNLJoinOp).innerKey }.toSet()
        assertTrue(pairs.contains("l_key" to "r_key") || pairs.contains("r_key" to "l_key"))
        assertTrue(pairs.size == 2)
    }

}
