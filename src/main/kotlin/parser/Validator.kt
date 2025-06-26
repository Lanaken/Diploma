package org.bmstu.parser

import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.Join
import org.bmstu.reader.CType
import org.bmstu.reader.YSchema
import org.bmstu.tables.Table

class Validator(
    private val tables: Set<Table>,
    private val schemas: Set<YSchema>
) {
    fun validate(joins: List<Join>): Boolean {
        var response = true
        joins.forEach { join ->
            if (join.leftTable.equals(join.rightTable, ignoreCase = true)) {
                println("Self-join запрещён: ${join.leftTable} JOIN ${join.rightTable}")
                response = false
            }
            if (tables.none { it.name == join.leftTable }) {
                println("Таблица ${join.leftTable} не обнаружена")
                response = false
            }

            if (tables.none { it.name == join.rightTable }) {
                println("Таблица ${join.rightTable} не обнаружена")
                response = false
            }

            val leftTable = tables.find { it.name == join.leftTable }
            val leftTableSchema = schemas.find { it.table == leftTable?.name }
            val leftTableColumn = leftTableSchema?.columns?.find { it.name == join.leftColumn }
            if (leftTableColumn == null) {
                println("Колонки ${join.leftColumn} в таблице $leftTable не существует")
                response = false
            }

            val rightTable = tables.find { it.name == join.rightTable }
            val rightTableSchema = schemas.find { it.table == rightTable?.name }
            val rightTableColumn = rightTableSchema?.columns?.find { it.name == join.rightColumn }
            if (rightTableColumn == null) {
                println("Колонки ${join.rightColumn} в таблице $rightTable не существует")
                return false
            }

            if (leftTableColumn?.type != rightTableColumn.type) {
                println("${join.leftColumn} и ${join.rightColumn} имеют разные типы")
                return false
            }

            if (!validateCondition(leftTableColumn.type, rightTableColumn.type, join.operator)) {
                return false
            }
        }

        return response
    }

    private fun validateCondition(leftType: CType, rightType: CType, operator: ConditionOperator): Boolean {
        if (!operator.supportedTypes.contains(leftType)) {
            println("Оператор ${operator.symbol} не поддерживается для типа ${leftType}")
            return false
        }

        if (!operator.supportedTypes.contains(rightType)) {
            println("Оператор ${operator.symbol} не поддерживается для типа ${rightType}")
            return false
        }

        return true
    }
}