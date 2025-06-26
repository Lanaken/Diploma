package org.bmstu.parser

import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.Join
import org.bmstu.joins.JoinCommand

class Parser {
    companion object {
        private val joinRegex = Regex(
            "(\\S+)\\s+(\\S+)\\s+ON\\s+([^ ]+\\s*[=<>!]+\\s*[^ ]+)",
            RegexOption.IGNORE_CASE
        )
        private val conditionRegex = Regex(
            "([^ ]+)\\s*([=<>!]+)\\s*([^ ]+)"
        )

        fun parseJoins(input: String): List<Join> {
            val joins = mutableListOf<Join>()
            var remainingInput = input.trim()
            var currentLeftTable: String? = null

            while (true) {
                val match = joinRegex.find(remainingInput)
                if (match == null) {
                    break
                }

                val (joinType, rightTable, condition) = match.destructured
                if (currentLeftTable == null) {
                    currentLeftTable = remainingInput.substringBefore(match.value).trim().split("\\s+".toRegex()).first()
                    remainingInput = remainingInput.substringAfter(currentLeftTable).trim()
                }

                val conditionMatch = conditionRegex.find(condition)
                if (conditionMatch == null) {
                    println("Неверное условие: $condition")
                    return emptyList()
                }

                val (leftColumn, operator, rightColumn) = conditionMatch.destructured

                joins.add(
                    Join(
                        leftTable = currentLeftTable,
                        rightTable = rightTable,
                        type = JoinCommand.Companion.parse(joinType),
                        leftColumn = leftColumn.substringAfter("$currentLeftTable."),
                        rightColumn = rightColumn.substringAfter("$rightTable."),
                        operator = ConditionOperator.parse(operator)
                    )
                )

                remainingInput = remainingInput.removePrefix(match.value).trim()
                currentLeftTable = rightTable
            }

            if (remainingInput.isNotEmpty()) {
                println("Осталась неразобранная часть строки: $remainingInput")
            } else {
                println("Вся строка успешно разобрана.")
            }

            return joins
        }
    }
}