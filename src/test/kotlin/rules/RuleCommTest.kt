package rules

import kotlin.test.*
import logical.Memo
import operators.JoinOp
import operators.ScanOp
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import rules.transformation.RuleComm

class RuleCommTest {

    // хелпер для создания группы-скана
    private fun Memo.scanGroup(table: String, card: Long) =
        insert(ScanOp(table, card), emptyList())!!
            .let { groupOf(setOf(table)) }

    @Test
    fun `matches returns true only for INNER and FULL joins`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)

        for (cmd in listOf(JoinCommand.INNER, JoinCommand.FULL)) {
            val expr = memo.insert(
                JoinOp(cmd, "x", "y", ConditionOperator.EQUALS),
                listOf(gA, gB)
            )!!
            assertTrue(RuleComm.matches(expr), "should match $cmd")
        }

        for (cmd in listOf(
            JoinCommand.LEFT, JoinCommand.RIGHT,
        )) {
            val expr = memo.insert(
                JoinOp(cmd, "x", "y", ConditionOperator.EQUALS),
                listOf(gA, gB)
            )!!
            assertFalse(RuleComm.matches(expr), "should not match $cmd")
        }
    }

    @Test
    fun `apply swaps children and flips operator`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)

        val origOp = JoinOp(
            JoinCommand.INNER,
            leftCol = "x",
            rightCol = "y",
            op = ConditionOperator.LESS_THAN
        )
        val expr = memo.insert(origOp, listOf(gA, gB))!!
        assertTrue(RuleComm.matches(expr))

        val results = RuleComm.apply(expr, memo)
        assertEquals(1, results.size)
        val swapped = results.single()

        assertEquals(listOf(gB, gA), swapped.children)

        val op2 = swapped.op as JoinOp
        assertEquals("y", op2.leftCol)
        assertEquals("x", op2.rightCol)

        assertEquals(ConditionOperator.GREATER_THAN, op2.op)

        val grpAB = memo.groupOf(setOf("A", "B"))
        assertEquals(
            2, grpAB.expressions.size,
            "Group {A,B} should contain both original and commuted expr"
        )

        assertEquals(1, memo.groupOf(setOf("A")).expressions.size)
        assertEquals(1, memo.groupOf(setOf("B")).expressions.size)
    }

    @Test
    fun `apply on EQUALS does not change op`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)

        // EQUALS остался EQUALS
        val origOp = JoinOp(
            JoinCommand.INNER,
            leftCol = "f1",
            rightCol = "f2",
            op = ConditionOperator.EQUALS
        )
        val expr = memo.insert(origOp, listOf(gA, gB))!!
        val swapped = RuleComm.apply(expr, memo).single()

        val op2 = swapped.op as JoinOp
        assertEquals(
            ConditionOperator.EQUALS, op2.op,
            "EQUALS should remain unchanged when flipped"
        )
    }

    @Test
    fun `apply works for FULL join`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)

        val origOp = JoinOp(
            JoinCommand.FULL,
            leftCol = "a",
            rightCol = "b",
            op = ConditionOperator.NOT_EQUALS
        )
        val expr = memo.insert(origOp, listOf(gA, gB))!!
        assertTrue(RuleComm.matches(expr))

        val swapped = RuleComm.apply(expr, memo).single()
        val op2 = swapped.op as JoinOp

        assertEquals(ConditionOperator.NOT_EQUALS, op2.op)
        assertEquals(listOf(gB, gA), swapped.children)
    }
}
