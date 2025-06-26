package org.bmstu.joins

import org.bmstu.reader.CType

enum class ConditionOperator(val symbol: String, val supportedTypes: Set<CType>) {
    EQUALS("=", setOf(CType.INT, CType.BIGINT, CType.DECIMAL, CType.DATE, CType.CHAR, CType.STRING)),
    NOT_EQUALS("<>", setOf(CType.INT, CType.BIGINT, CType.DECIMAL, CType.DATE, CType.CHAR, CType.STRING)),
    LESS_THAN("<", setOf(CType.INT, CType.BIGINT, CType.DECIMAL, CType.DATE)),
    GREATER_THAN(">", setOf(CType.INT, CType.BIGINT, CType.DECIMAL, CType.DATE)),
    LESS_THAN_OR_EQUALS("<=", setOf(CType.INT, CType.BIGINT, CType.DECIMAL, CType.DATE)),
    GREATER_THAN_OR_EQUALS(">=", setOf(CType.INT, CType.BIGINT, CType.DECIMAL, CType.DATE));

    fun flipped(): ConditionOperator = when (this) {
        EQUALS -> EQUALS
        NOT_EQUALS -> NOT_EQUALS
        LESS_THAN -> GREATER_THAN
        GREATER_THAN -> LESS_THAN
        LESS_THAN_OR_EQUALS -> GREATER_THAN_OR_EQUALS
        GREATER_THAN_OR_EQUALS -> LESS_THAN_OR_EQUALS
    }

    companion object {
        fun parse(operator: String): ConditionOperator {
            return entries.firstOrNull { it.symbol.equals(operator.trim(), ignoreCase = true) }
                ?: throw IllegalArgumentException("Неизвестный оператор условия: $operator")
        }

    }
}
