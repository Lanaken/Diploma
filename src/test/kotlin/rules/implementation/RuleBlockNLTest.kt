package org.bmstu.util.rules.implementation

import kotlin.collections.get
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import logical.Memo
import logical.Expression
import operators.BlockNLJoinOp
import operators.JoinOp
import operators.ScanOp
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import org.bmstu.util.scanGroup
import rules.implementation.RuleBlockNL

class RuleBlockNLTest {

    @Test
    fun `matches returns true only for join expressions`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 10)
        val gB = memo.scanGroup("B", 20)

        val nonJoinExpr: Expression = memo.insert(
            ScanOp("C", 30),
            emptyList()
        )!!
        assertFalse(RuleBlockNL.matches(nonJoinExpr), "ScanOp should not match")

        val joinOp = JoinOp(
            type = JoinCommand.INNER,
            leftCol = "x",
            rightCol = "y",
            op = ConditionOperator.EQUALS
        )
        val joinExpr = memo.insert(joinOp, listOf(gA, gB))!!
        assertTrue(RuleBlockNL.matches(joinExpr), "JoinOp should match")
    }

    @Test
    fun `apply produces two BlockNL expressions with correct outerSide`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 5)
        val gB = memo.scanGroup("B", 8)

        val joinOp = JoinOp(
            type = JoinCommand.INNER,
            leftCol = "a_id",
            rightCol = "b_id",
            op = ConditionOperator.EQUALS
        )
        val joinExpr = memo.insert(joinOp, listOf(gA, gB))!!

        val results = RuleBlockNL.apply(joinExpr, memo)
        assertEquals(2, results.size, "Should generate two BlockNL expressions")

        val first = results[0]
        assertTrue(first.op is BlockNLJoinOp)
        val op1 = first.op
        assertTrue(op1.outerLeft, "First BlockNL should use left as outer")
        assertEquals(listOf(gA, gB), first.children)

        val second = results[1]
        assertTrue(second.op is BlockNLJoinOp)
        val op2 = second.op
        assertFalse(op2.outerLeft, "Second BlockNL should use right as outer")
        assertEquals(listOf(gA, gB), second.children)
    }

    @Test
    fun `apply registers new expressions in the memo group`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 3)
        val gB = memo.scanGroup("B", 7)

        val joinOp = JoinOp(
            type = JoinCommand.INNER,
            leftCol = "id1",
            rightCol = "id2",
            op = ConditionOperator.NOT_EQUALS
        )
        val joinExpr = memo.insert(joinOp, listOf(gA, gB))!!

        val gAB = memo.groupOf(setOf("A", "B"))
        val beforeCount = gAB.expressions.size

        val results = RuleBlockNL.apply(joinExpr, memo)

        val afterCount = gAB.expressions.size
        assertEquals(beforeCount + results.size, afterCount,
            "Memo group should include the new BlockNL expressions")
        results.forEach { expr ->
            assertTrue(gAB.expressions.contains(expr),
                "Memo must contain generated expression $expr")
        }
    }
}
