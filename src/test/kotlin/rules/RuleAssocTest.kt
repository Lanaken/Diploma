package rules

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import logical.Memo
import operators.JoinOp
import operators.ScanOp
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand
import rules.transformation.RuleAssoc

class RuleAssocTest {

    private fun Memo.scanGroup(table: String, card: Long) =
        insert(ScanOp(table, card), emptyList())!!
            .let { groupOf(setOf(table)) }

    @Test
    fun `matches returns true for nested inner join`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 100)
        val gB = memo.scanGroup("B", 200)
        val gC = memo.scanGroup("C", 300)

        // (A ⋈ B)
        val opAB = JoinOp(JoinCommand.INNER, "id", "id", ConditionOperator.EQUALS)
        memo.insert(opAB, listOf(gA, gB))!!
        val gAB = memo.groupOf(setOf("A","B"))

        // ((A ⋈ B) ⋈ C)
        val opABC = JoinOp(JoinCommand.INNER, "id", "id", ConditionOperator.EQUALS)
        val exprABC = memo.insert(opABC, listOf(gAB, gC))!!

        assertTrue(RuleAssoc.matches(exprABC), "RuleAssoc should match nested INNER JOIN")
    }

    @Test
    fun `apply creates BcJoin and A_BcJoin for EQUALS`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)
        val gC = memo.scanGroup("C", 1)

        // (A ⋈ B) with EQUALS
        val opAB = JoinOp(JoinCommand.INNER, "x", "y", ConditionOperator.EQUALS)
        memo.insert(opAB, listOf(gA, gB))!!
        val gAB = memo.groupOf(setOf("A","B"))

        // ((A ⋈ B) ⋈ C) with EQUALS
        val opABC = JoinOp(JoinCommand.INNER, "z", "v", ConditionOperator.EQUALS)
        val exprABC = memo.insert(opABC, listOf(gAB, gC))!!

        val results = RuleAssoc.apply(exprABC, memo)
        assertEquals(1, results.size, "Should produce exactly one new Expr")
        val newTop = results.first()

        // Проверяем структуру A⋈(B⋈C)
        assertEquals(2, newTop.children.size)
        val leftChild = newTop.children[0]
        val rightChild = newTop.children[1]
        assertEquals(setOf("A"), leftChild.tables)
        assertEquals(setOf("B","C"), rightChild.tables)

        // Операторы для A⋈(B⋈C) и B⋈C должны быть EQUALS (не меняется)
        // Оп Expr для newTop — это исходный opAB
        val topOp = newTop.op as JoinOp
        assertEquals(opAB, topOp)

        // В группе {B,C} найдём Expr с этим opAB flipped, но flipped(EQUALS)=EQUALS
        val bcGroup = rightChild
        val bcExpr = bcGroup.expressions.firstOrNull {
            it.op is JoinOp && it.op.leftCol == "y" && it.children[0] == gB
        }
        assertNotNull(bcExpr, "Group {B,C} must contain the B⋈C Expr")
        assertEquals(ConditionOperator.EQUALS, (bcExpr.op as JoinOp).op)

        val abcGroup = memo.groupOf(setOf("A", "B", "C"))
        // там должен появиться новый верхний expr
        assertTrue(abcGroup.expressions.isNotEmpty(), "Group {A,B,C} must contain the new top expr")
        assertEquals(2, abcGroup.expressions.size)
        // среди них должен быть тот, что в results
        assertTrue(abcGroup.expressions.contains(results.single()))

        // 3) проверяем, что старые группы не испорчены
        assertEquals(1, memo.groupOf(setOf("A")).expressions.size)
        assertEquals(1, memo.groupOf(setOf("B")).expressions.size)
        assertEquals(1, memo.groupOf(setOf("C")).expressions.size)
    }

    @Test
    fun `apply flips operator for non-equals`() {
        val memo = Memo()
        val gA = memo.scanGroup("A", 1)
        val gB = memo.scanGroup("B", 1)
        val gC = memo.scanGroup("C", 1)

        // (A ⋈ B) with LESS_THAN
        val opAB = JoinOp(JoinCommand.INNER, "x", "y", ConditionOperator.LESS_THAN)
        memo.insert(opAB, listOf(gA, gB))!!
        val gAB = memo.groupOf(setOf("A","B"))

        // ((A ⋈ B) ⋈ C) with LESS_THAN
        val opABC = JoinOp(JoinCommand.INNER, "x", "y", ConditionOperator.LESS_THAN)
        val exprABC = memo.insert(opABC, listOf(gAB, gC))!!

        val results = RuleAssoc.apply(exprABC, memo)
        assertEquals(1, results.size)
        val newTop = results.first()
        val rightChild = newTop.children[1]

        // В группе {B,C} operator должен быть flipped()
        val bcExpr = rightChild.expressions.first { it.op is JoinOp }
        val bcOp = (bcExpr.op as JoinOp).op
        assertEquals(ConditionOperator.GREATER_THAN, bcOp)
    }

    @Test
    fun `matches returns false when no nested inner join`() {
        val memo = Memo()
        val gA = memo.scanGroup("A",1)
        val gB = memo.scanGroup("B",1)
        val gC = memo.scanGroup("C",1)

        // only single join: A ⋈ B
        val opAB = JoinOp(JoinCommand.INNER, "a","b", ConditionOperator.EQUALS)
        val exprAB = memo.insert(opAB, listOf(gA, gB))!!
        assertFalse(RuleAssoc.matches(exprAB))

        // non-inner join at top
        val opBC = JoinOp(JoinCommand.LEFT, "b","c", ConditionOperator.EQUALS)
        val exprBC = memo.insert(opBC, listOf(gB, gC))!!
        assertFalse(RuleAssoc.matches(exprBC))
    }
}
