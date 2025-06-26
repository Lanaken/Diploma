package org.bmstu.joins

data class Join(
    val leftTable: String,
    val rightTable: String,
    val type: JoinCommand,
    val leftColumn: String,
    val rightColumn: String,
    val operator: ConditionOperator,
    val isMain: Boolean = false
)

