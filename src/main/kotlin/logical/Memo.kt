package logical

import operators.LogicalOp
import operators.ScanOp
import util.estimateRowSize

class Memo {
    private val map = mutableMapOf<Set<String>, Group>()
    fun groupOf(tables: Set<String>): Group =
        map.computeIfAbsent(tables) { Group(tables) }

    fun insert(op: LogicalOp, kids: List<Group>): Expression? {
        val tables = when (op) {
            is ScanOp -> setOf(op.table)
            else -> kids.flatMap { it.tables }.toSet()
        }

        val group = groupOf(tables)
        val expr = Expression(op, kids)

        if (expr in group.expressions) return null

        group.expressions += expr

        group.avgRowSize = estimateRowSize(op, kids)

        return expr
    }

}