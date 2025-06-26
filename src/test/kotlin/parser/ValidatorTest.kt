package parser

import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.Join
import org.bmstu.joins.JoinCommand
import org.bmstu.reader.YSchema
import org.bmstu.reader.Col
import org.bmstu.reader.CType
import org.bmstu.tables.Table
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.assertFalse
import org.bmstu.parser.Validator

class ValidatorTest {

    // Утилита для создания схем
    private fun createSchema(tableName: String, columns: List<Col>) = YSchema(tableName, columns)

    // Утилита для создания таблиц
    private fun createTable(name: String) = Table(name)

    @Test
    fun `test valid join with EQUALS`() {
        val tables = setOf(createTable("table1"), createTable("table2"))
        val schemas = setOf(
            createSchema("table1", listOf(Col("id", CType.INT))),
            createSchema("table2", listOf(Col("id", CType.INT)))
        )

        val join = Join(
            leftTable = "table1",
            rightTable = "table2",
            type = JoinCommand.INNER,
            leftColumn = "id",
            rightColumn = "id",
            operator = ConditionOperator.EQUALS
        )

        val validator = Validator(tables, schemas)
        assertTrue(validator.validate(listOf(join)))
    }

    @Test
    fun `test valid join with LESS_THAN for numbers`() {
        val tables = setOf(createTable("table1"), createTable("table2"))
        val schemas = setOf(
            createSchema("table1", listOf(Col("value", CType.DECIMAL))),
            createSchema("table2", listOf(Col("value", CType.DECIMAL)))
        )

        val join = Join(
            leftTable = "table1",
            rightTable = "table2",
            type = JoinCommand.INNER,
            leftColumn = "value",
            rightColumn = "value",
            operator = ConditionOperator.LESS_THAN
        )

        val validator = Validator(tables, schemas)
        assertTrue(validator.validate(listOf(join)))
    }

    @Test
    fun `test invalid join with LESS_THAN for strings`() {
        val tables = setOf(createTable("table1"), createTable("table2"))
        val schemas = setOf(
            createSchema("table1", listOf(Col("name", CType.STRING))),
            createSchema("table2", listOf(Col("name", CType.STRING)))
        )

        val join = Join(
            leftTable = "table1",
            rightTable = "table2",
            type = JoinCommand.INNER,
            leftColumn = "name",
            rightColumn = "name",
            operator = ConditionOperator.LESS_THAN
        )

        val validator = Validator(tables, schemas)
        assertFalse(validator.validate(listOf(join)))
    }

    @Test
    fun `test missing left table`() {
        val tables = setOf(createTable("table1"))
        val schemas = setOf(
            createSchema("table1", listOf(Col("id", CType.INT)))
        )

        val join = Join(
            leftTable = "table2",
            rightTable = "table1",
            type = JoinCommand.INNER,
            leftColumn = "id",
            rightColumn = "id",
            operator = ConditionOperator.EQUALS
        )

        val validator = Validator(tables, schemas)
        assertFalse(validator.validate(listOf(join)))
    }

    @Test
    fun `test missing right table`() {
        val tables = setOf(createTable("table1"))
        val schemas = setOf(
            createSchema("table1", listOf(Col("id", CType.INT)))
        )

        val join = Join(
            leftTable = "table1",
            rightTable = "table2",
            type = JoinCommand.INNER,
            leftColumn = "id",
            rightColumn = "id",
            operator = ConditionOperator.EQUALS
        )

        val validator = Validator(tables, schemas)
        assertFalse(validator.validate(listOf(join)))
    }

    @Test
    fun `test different column types`() {
        val tables = setOf(createTable("table1"), createTable("table2"))
        val schemas = setOf(
            createSchema("table1", listOf(Col("id", CType.INT))),
            createSchema("table2", listOf(Col("id", CType.STRING)))
        )

        val join = Join(
            leftTable = "table1",
            rightTable = "table2",
            type = JoinCommand.INNER,
            leftColumn = "id",
            rightColumn = "id",
            operator = ConditionOperator.EQUALS
        )

        val validator = Validator(tables, schemas)
        assertFalse(validator.validate(listOf(join)))
    }

    @Test
    fun `test multiple joins with mixed validation`() {
        val tables = setOf(createTable("table1"), createTable("table2"), createTable("table3"))
        val schemas = setOf(
            createSchema("table1", listOf(Col("id", CType.INT))),
            createSchema("table2", listOf(Col("id", CType.INT))),
            createSchema("table3", listOf(Col("id", CType.STRING)))
        )

        val validJoin = Join(
            leftTable = "table1",
            rightTable = "table2",
            type = JoinCommand.INNER,
            leftColumn = "id",
            rightColumn = "id",
            operator = ConditionOperator.EQUALS
        )

        val invalidJoin = Join(
            leftTable = "table2",
            rightTable = "table3",
            type = JoinCommand.INNER,
            leftColumn = "id",
            rightColumn = "id",
            operator = ConditionOperator.EQUALS
        )

        val validator = Validator(tables, schemas)
        assertFalse(validator.validate(listOf(validJoin, invalidJoin)))
    }
}
