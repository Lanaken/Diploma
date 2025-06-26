package rules

import logical.Expression
import logical.Memo
import operators.JoinOp
import operators.SortOp
import statistic.Catalog
import org.bmstu.reader.CType

object RuleEnforceSort : Rule {
    override fun matches(expr: Expression): Boolean = when (val op = expr.op) {
        is JoinOp -> op.op !in setOf(
            org.bmstu.joins.ConditionOperator.EQUALS,
            org.bmstu.joins.ConditionOperator.NOT_EQUALS
        )
        else -> false
    }

    override fun apply(expr: Expression, memo: Memo): List<Expression> {
        val join = expr.op as JoinOp
        val leftGroup = expr.children[0]
        val rightGroup = expr.children[1]

        // Если группа содержит не более одной таблицы, можно проверить, был ли столбец уже отсортирован "на диске"
        fun isPhysicallySorted(group: logical.Group, colName: String): Boolean {
            // предполагаем, что group.tables.singleOrNull() != null, иначе – не можем проверить
            val tblName = group.tables.singleOrNull() ?: return false
            val colStats = Catalog.stats(tblName).col[colName]
            return colStats?.wasSorted == true
        }

        // Проверка: есть ли уже SortOp по нужному ключу на вершине любого expression в группе
        fun hasSortOpOnKey(group: logical.Group, key: String): Boolean {
            return group.expressions.any {
                it.op is SortOp && (it.op as SortOp).key == key
            }
        }

        val leftNeedsSort  = !isPhysicallySorted(leftGroup,  join.leftCol)  && !hasSortOpOnKey(leftGroup,  join.leftCol)
        val rightNeedsSort = !isPhysicallySorted(rightGroup, join.rightCol) && !hasSortOpOnKey(rightGroup, join.rightCol)

        // Если ни одна из сторон не нуждается в сортировке, больше нечего вставлять
        if (!leftNeedsSort && !rightNeedsSort) {
            return emptyList()
        }

        val newExprs = mutableListOf<Expression>()

        // Если обе стороны ещё не отсортированы, сразу порождаем вариант, где сортим обе:
        if (leftNeedsSort && rightNeedsSort) {
            // 1) Insert SortOp(join.leftCol) над левой группой
            val sortedLeftExpr = memo.insert(SortOp(join.leftCol), listOf(leftGroup))
            // 2) Insert SortOp(join.rightCol) над правой группой
            val sortedRightExpr = memo.insert(SortOp(join.rightCol), listOf(rightGroup))

            if (sortedLeftExpr != null && sortedRightExpr != null) {
                // обе новые группы уже есть в мемо
                val sortedLeftGroup  = memo.groupOf(sortedLeftExpr.tables)
                val sortedRightGroup = memo.groupOf(sortedRightExpr.tables)

                // теперь вставляем Join на две отсортированные группы
                memo.insert(join, listOf(sortedLeftGroup, sortedRightGroup))
                    ?.let { newExprs += it }
            }
            return newExprs
        }

        // Если сортировка нужна только левой стороне:
        if (leftNeedsSort) {
            val sortedLeftExpr = memo.insert(SortOp(join.leftCol), listOf(leftGroup))
            sortedLeftExpr?.let {
                val sortedLeftGroup = memo.groupOf(it.tables)
                // Join( Sort(left), right )
                memo.insert(join, listOf(sortedLeftGroup, rightGroup))
                    ?.let { newExprs += it }
            }
        }

        // Если сортировка нужна только правой стороне:
        if (rightNeedsSort) {
            val sortedRightExpr = memo.insert(SortOp(join.rightCol), listOf(rightGroup))
            sortedRightExpr?.let {
                val sortedRightGroup = memo.groupOf(it.tables)
                // Join( left, Sort(right) )
                memo.insert(join, listOf(leftGroup, sortedRightGroup))
                    ?.let { newExprs += it }
            }
        }

        return newExprs
    }
}
