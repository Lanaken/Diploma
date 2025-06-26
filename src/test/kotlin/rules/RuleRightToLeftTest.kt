package rules

import kotlin.test.*
import logical.Memo
import operators.JoinOp
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import org.bmstu.util.scanGroup
import rules.transformation.RuleRightToLeft

class RuleRightToLeftTest {

    @Test
    fun `matches returns true only for RIGHT join`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)

        // RIGHT → должно совпадать
        val rightExpr = memo.insert(
            JoinOp(JoinCommand.RIGHT, "x", "y", ConditionOperator.EQUALS),
            listOf(gA, gB)
        )!!
        assertTrue(RuleRightToLeft.matches(rightExpr))

        // остальные типы → не должны совпадать
        for (cmd in listOf(
            JoinCommand.INNER,
            JoinCommand.LEFT,
            JoinCommand.FULL,
        )) {
            val expr = memo.insert(
                JoinOp(cmd, "x", "y", ConditionOperator.EQUALS),
                listOf(gA, gB)
            )!!
            assertFalse(RuleRightToLeft.matches(expr), "should not match $cmd")
        }
    }

    @Test
    fun `apply swaps children and flips operator for non-equals`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)

        // RIGHT join с LESS_THAN
        val orig = JoinOp(JoinCommand.RIGHT, "f1", "f2", ConditionOperator.LESS_THAN)
        val expr = memo.insert(orig, listOf(gA, gB))!!
        assertTrue(RuleRightToLeft.matches(expr))

        val results = RuleRightToLeft.apply(expr, memo)
        assertEquals(1, results.size)
        val swapped = results.single()

        // 1) Дети поменялись местами
        assertEquals(listOf(gB, gA), swapped.children)

        // 2) Тип JOIN стал LEFT
        val op2 = swapped.op as JoinOp
        assertEquals(JoinCommand.LEFT, op2.type)

        // 3) Оператор flipped: LESS_THAN → GREATER_THAN
        assertEquals(ConditionOperator.GREATER_THAN, op2.op)

        // 4) В группе {A,B} теперь два Expression
        val grpAB = memo.groupOf(setOf("A","B"))
        assertEquals(2, grpAB.expressions.size,
            "Group {A,B} should contain both original and right→left expr")
    }

    @Test
    fun `apply on EQUALS does not change condition operator`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)

        // RIGHT join с EQUALS
        val orig = JoinOp(JoinCommand.RIGHT, "col1", "col2", ConditionOperator.EQUALS)
        val expr = memo.insert(orig, listOf(gA, gB))!!
        val swapped = RuleRightToLeft.apply(expr, memo).single()

        val op2 = swapped.op as JoinOp
        // оператор EQUALS остаётся EQUALS
        assertEquals(ConditionOperator.EQUALS, op2.op)
        // тип JOIN стал LEFT
        assertEquals(JoinCommand.LEFT, op2.type)
        // порядок детей
        assertEquals(listOf(gB, gA), swapped.children)
    }

    @Test
    fun `scan groups remain unaffected`() {
        val memo = Memo()
        memo.scanGroup("A", 10)
        memo.scanGroup("B", 20)

        // После применения RuleRightToLeft группы {A} и {B} не должны измениться
        assertEquals(1, memo.groupOf(setOf("A")).expressions.size)
        assertEquals(1, memo.groupOf(setOf("B")).expressions.size)
    }
}
