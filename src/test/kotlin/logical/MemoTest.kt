package logical

import kotlin.test.*
import operators.JoinOp
import operators.ScanOp
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand

class MemoTest {

    @Test
    fun `groupOf creates empty group and reuses it`() {
        val memo = Memo()
        val g1 = memo.groupOf(setOf("X"))
        assertNotNull(g1)
        assertEquals(setOf("X"), g1.tables)
        assertTrue(g1.expressions.isEmpty(), "New group should start empty")

        // Subsequent calls with the same key return the same instance
        val g2 = memo.groupOf(setOf("X"))
        assertSame(g1, g2)
    }

    @Test
    fun `insert ScanOp into correct group and prevent duplicates`() {
        val memo = Memo()

        val scanOp = ScanOp("A", 42)
        // initially no expr in group {A}
        val gA = memo.groupOf(setOf("A"))
        assertTrue(gA.expressions.isEmpty())

        // insert first time
        val expr1 = memo.insert(scanOp, emptyList())
        assertNotNull(expr1, "First insert should produce an Expression")
        assertEquals(1, gA.expressions.size)
        assertSame(expr1, gA.expressions.first())

        // inserting equivalent ScanOp again returns null
        val exprDup = memo.insert(ScanOp("A", 42), emptyList())
        assertNull(exprDup, "Duplicate ScanOp should not be re-inserted")
        assertEquals(1, gA.expressions.size)
    }

    @Test
    fun `insert JoinOp into union-of-children group and prevent duplicates`() {
        val memo = Memo()
        // prepare scan groups
        val exprA = memo.insert(ScanOp("A", 10), emptyList())!!
        val exprB = memo.insert(ScanOp("B", 20), emptyList())!!
        val gA = memo.groupOf(setOf("A"))
        val gB = memo.groupOf(setOf("B"))

        val joinOp = JoinOp(
            type = JoinCommand.INNER,
            leftCol = "a1",
            rightCol = "b1",
            op = ConditionOperator.EQUALS
        )

        // insert join(A,B)
        val exprAB = memo.insert(joinOp, listOf(gA, gB))
        assertNotNull(exprAB)
        val gAB = memo.groupOf(setOf("A", "B"))
        assertEquals(1, gAB.expressions.size)
        assertSame(exprAB, gAB.expressions.first())

        // duplicate insert with same children order -> null
        val exprDup = memo.insert(joinOp, listOf(gA, gB))
        assertNull(exprDup)
        assertEquals(1, gAB.expressions.size)

        // inserting same op but swapped children is considered distinct
        val exprBA = memo.insert(joinOp, listOf(gB, gA))
        assertNotNull(exprBA, "Join with swapped children should be a new Expression")
        assertEquals(2, gAB.expressions.size)
        assertTrue(gAB.expressions.contains(exprBA))
    }

    @Test
    fun `insert multiple scan groups coexist without interference`() {
        val memo = Memo()

        val exprA = memo.insert(ScanOp("A", 1), emptyList())!!
        val exprB = memo.insert(ScanOp("B", 2), emptyList())!!
        val gA = memo.groupOf(setOf("A"))
        val gB = memo.groupOf(setOf("B"))

        assertEquals(1, gA.expressions.size)
        assertEquals(exprA, gA.expressions.first())

        assertEquals(1, gB.expressions.size)
        assertEquals(exprB, gB.expressions.first())
    }

    @Test
    fun `groupOf on new key does not pick up expressions from other groups`() {
        val memo = Memo()
        val exprA = memo.insert(ScanOp("A", 5), emptyList())!!
        val gA = memo.groupOf(setOf("A"))

        // now request a different group
        val gB = memo.groupOf(setOf("B"))
        assertNotSame(gA, gB)
        assertEquals(0, gB.expressions.size)
    }
}
