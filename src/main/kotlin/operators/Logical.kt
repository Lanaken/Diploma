package operators

import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand

sealed interface LogicalOp

data class ScanOp(val table: String, val card: Long) : LogicalOp {
    override fun toString(): String = "Scan($table, card=$card)"
}

data class JoinOp(
    val type: JoinCommand,
    val leftCol: String,
    val rightCol: String,
    val op: ConditionOperator
) : LogicalOp {
    override fun toString(): String = "${type.name} $leftCol${op.symbol}$rightCol"
}

data class SortOp(val key: String) : LogicalOp {
    override fun toString(): String = "Sort($key)"
}