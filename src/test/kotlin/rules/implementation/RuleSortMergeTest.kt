package rules.implementation

import logical.Memo
import logical.Group
import logical.Expression
import operators.JoinOp
import operators.SortMergeJoinOp
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import statistic.Catalog
import statistic.Stats
import statistic.ColStats
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import java.lang.reflect.Field
import java.util.concurrent.ConcurrentHashMap
import operators.ScanOp
import org.bmstu.reader.CType

class RuleSortMergeTest {

    private val rule = RuleSortMerge

    @Suppress("UNCHECKED_CAST")
    @BeforeEach
    fun clearCatalog() {
        // достаём приватное поле tbl в объекте Catalog
        val tblField: Field = Catalog::class.java.getDeclaredField("tbl")
        tblField.isAccessible = true
        (tblField.get(Catalog) as ConcurrentHashMap<*, *>).clear()

        val idxField: Field = Catalog::class.java.getDeclaredField("idx")
        idxField.isAccessible = true
        (idxField.get(Catalog) as ConcurrentHashMap<*, *>).clear()
    }

    private fun makeStats(table: String, col: String, sorted: Boolean): Group {
        // заведём Stats и ColStats для одной колонки
        val cs = ColStats(kind = CType.INT, wasSorted = sorted)
        val st = Stats().apply { this.col[col] = cs }
        Catalog.putStats(table, st)
        // вернём пустую группу для удобства
        return Group(setOf(table))
    }

    @Test
    fun `matches false for non-JoinOp`() {
        val memo = Memo()
        // создаём произвольную Scan‐expr
        val g = memo.insert(ScanOp("T", 1), emptyList())!!
            .let { memo.groupOf(setOf("T")) }
        val expr = Expression(ScanOp("T",1), emptyList())
        assertFalse(rule.matches(expr))
    }

    @Test
    fun `matches false when left not sorted`() {
        val leftCol = "a"; val rightCol = "b"
        val leftG  = makeStats("L", leftCol, sorted = false)
        val rightG = makeStats("R", rightCol, sorted = true)

        // соберём Expression с JoinOp
        val memo = Memo()
        val expr = memo.insert(
            JoinOp(JoinCommand.INNER, leftCol, rightCol, ConditionOperator.EQUALS),
            listOf(leftG, rightG)
        )!!

        assertFalse(rule.matches(expr))
    }

    @Test
    fun `matches false when right not sorted`() {
        val leftCol = "a"; val rightCol = "b"
        val leftG  = makeStats("L", leftCol, sorted = true)
        val rightG = makeStats("R", rightCol, sorted = false)

        val memo = Memo()
        val expr = memo.insert(
            JoinOp(JoinCommand.INNER, leftCol, rightCol, ConditionOperator.EQUALS),
            listOf(leftG, rightG)
        )!!

        assertFalse(rule.matches(expr))
    }

    @Test
    fun `matches true when both sides sorted`() {
        val leftCol = "a"; val rightCol = "b"
        val leftG  = makeStats("L", leftCol, sorted = true)
        val rightG = makeStats("R", rightCol, sorted = true)

        val memo = Memo()
        val expr = memo.insert(
            JoinOp(JoinCommand.INNER, leftCol, rightCol, ConditionOperator.EQUALS),
            listOf(leftG, rightG)
        )!!

        assertTrue(rule.matches(expr))
    }

    @Test
    fun `apply inserts single SortMergeJoinOp with alreadySorted true`() {
        val leftCol = "x"; val rightCol = "y"
        val leftG  = makeStats("T1", leftCol, sorted = true)
        val rightG = makeStats("T2", rightCol, sorted = true)

        val memo = Memo()
        val expr = memo.insert(
            JoinOp(JoinCommand.INNER, leftCol, rightCol, ConditionOperator.EQUALS),
            listOf(leftG, rightG)
        )!!

        val newExprs = rule.apply(expr, memo)
        // должно быть ровно одно новое выражение
        assertEquals(1, newExprs.size)
        val smjExpr = newExprs.single()

        // оператор — SortMergeJoinOp(alreadySorted = true)
        smjExpr.op.let {
            assertTrue(it is SortMergeJoinOp)
            assertTrue((it as SortMergeJoinOp).alreadySorted)
        }

        // дети — те же группы
        assertEquals(listOf(leftG, rightG), smjExpr.children)

        // и в соответствующей группе оно должно оказаться
        val grp = memo.groupOf(setOf("T1", "T2"))
        assertTrue(grp.expressions.contains(smjExpr))
    }
}
