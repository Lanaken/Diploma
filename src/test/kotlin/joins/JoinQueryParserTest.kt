package joins

import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand
import org.bmstu.parser.Parser
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith

class JoinQueryParserTest {

    @Test
    fun `test single join parsing`() {
        val input = "table1 INNER table2 ON table1.id = table2.id"
        val joins = Parser.parseJoins(input)

        assertEquals(1, joins.size)
        assertEquals("table1", joins[0].leftTable)
        assertEquals("table2", joins[0].rightTable)
        assertEquals(JoinCommand.INNER, joins[0].type)
        assertEquals("id", joins[0].leftColumn)
        assertEquals("id", joins[0].rightColumn)
        assertEquals(ConditionOperator.EQUALS, joins[0].operator)
    }

    @Test
    fun `test multiple joins parsing`() {
        val input = """
            table1 INNER table2 ON table1.id = table2.id 
            LEFT table3 ON table2.id = table3.id 
            RIGHT table4 ON table3.id <> table4.id
        """.trimIndent()

        val joins = Parser.parseJoins(input)

        assertEquals(3, joins.size)

        assertEquals("table1", joins[0].leftTable)
        assertEquals("table2", joins[0].rightTable)
        assertEquals(JoinCommand.INNER, joins[0].type)
        assertEquals("id", joins[0].leftColumn)
        assertEquals("id", joins[0].rightColumn)
        assertEquals(ConditionOperator.EQUALS, joins[0].operator)

        assertEquals("table2", joins[1].leftTable)
        assertEquals("table3", joins[1].rightTable)
        assertEquals(JoinCommand.LEFT, joins[1].type)
        assertEquals("id", joins[1].leftColumn)
        assertEquals("id", joins[1].rightColumn)
        assertEquals(ConditionOperator.EQUALS, joins[1].operator)

        assertEquals("table3", joins[2].leftTable)
        assertEquals("table4", joins[2].rightTable)
        assertEquals(JoinCommand.RIGHT, joins[2].type)
        assertEquals("id", joins[2].leftColumn)
        assertEquals("id", joins[2].rightColumn)
        assertEquals(ConditionOperator.NOT_EQUALS, joins[2].operator)
    }

    @Test
    fun `test invalid join type`() {
        val input = "table1 UNKNOWN table2 ON table1.id = table2.id"

        assertFailsWith<IllegalArgumentException> {
            Parser.parseJoins(input)
        }
    }

    @Test
    fun `test empty input`() {
        val input = ""
        val joins = Parser.parseJoins(input)
        assertTrue(joins.isEmpty())
    }

    @Test
    fun `test input without join`() {
        val input = "table1"
        val joins = Parser.parseJoins(input)
        assertTrue(joins.isEmpty())
    }

    @Test
    fun `test unparsed text detection`() {
        val input = "table1 INNER table2 ON table1.id = table2.id AND more text"
        val joins = Parser.parseJoins(input)

        assertEquals(1, joins.size)
        assertEquals("table1", joins[0].leftTable)
        assertEquals("table2", joins[0].rightTable)
        assertEquals(JoinCommand.INNER, joins[0].type)
        assertEquals("id", joins[0].leftColumn)
        assertEquals("id", joins[0].rightColumn)
        assertEquals(ConditionOperator.EQUALS, joins[0].operator)
    }

}
