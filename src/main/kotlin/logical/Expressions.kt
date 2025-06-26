package logical

import operators.JoinOp
import operators.LogicalOp
import operators.ScanOp
import operators.SortOp
import rules.Rule


data class Expression(val op: LogicalOp, val children: List<Group>) {
    val tables: Set<String> = when (op) {
        is ScanOp -> setOf(op.table)
        else -> children.flatMap { it.tables }.toSet()
    }
    val appliedRules = mutableSetOf<Rule>()
    override fun toString(): String = toString("")

    internal fun toString(indent: String): String {
        val sb = StringBuilder()
        sb.append(indent).append(op.toString())
        sb.append("\n")
        // Печатаем дочерние группы по очереди
        for (child in children) {
            sb.append(child.toString("$indent  "))
        }
        return sb.toString()
    }

    fun makeLogicalPlanPretty(indent: String = "", sb: StringBuilder = StringBuilder()): String {
        when (val op = this.op) {
            is ScanOp -> sb.append(indent).append("Scan(${op.table})").append("\n")
            is SortOp -> {
                sb.append(indent).append("Sort(${op.key})").append("\n")
                children[0].expressions.first().makeLogicalPlanPretty("$indent  ", sb)
            }
            is JoinOp -> {
                sb.append(indent)
                    .append(op.type).append(' ')
                    .append(op.leftCol).append(op.op.symbol).append(op.rightCol)
                    .append("\n")
                children[0].expressions.first().makeLogicalPlanPretty("$indent  ", sb)
                children[1].expressions.first().makeLogicalPlanPretty("$indent  ", sb)
            }
            else -> {
                sb.append(indent).append(op.toString()).append("\n")
                children.forEach { child ->
                    child.expressions.first().makeLogicalPlanPretty("$indent  ", sb)
                }
            }
        }
        return sb.toString()
    }
}

class Group(val tables: Set<String>) {
    val sortedKeys = mutableSetOf<String>()
    val expressions = mutableListOf<Expression>()
    var bestCost = Double.POSITIVE_INFINITY
    var bestExpression: Expression? = null
    var explored: Boolean = false
    var bestCard: Double = Double.POSITIVE_INFINITY
    var avgRowSize: Long = 100

    override fun toString(): String = toString("")

    internal fun toString(indent: String): String {
        val sb = StringBuilder()
        sb.append(indent)
            .append("Group(tables=${tables.joinToString(", ")})")
            .append(" [expressions=${expressions.size}]")
            .append("\n")
        for ((idx, expr) in expressions.withIndex()) {
            sb.append(indent)
                .append("  Expr #${idx + 1}:\n")
            sb.append(expr.toString("$indent    "))
        }
        return sb.toString()
    }
}

