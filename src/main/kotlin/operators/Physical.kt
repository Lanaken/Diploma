package operators

import java.nio.file.Path
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand

sealed interface PhysicalOp : LogicalOp

data class HashJoinOp(
    val joinType: JoinCommand,
    val outerKey: String,
    val innerKey: String,
    val buildSideLeft: Boolean,
    val partitioned: Boolean = false
) : PhysicalOp {
    override fun toString(): String =
        if (partitioned)
            "GraceHashJoin(${joinType.name}, ${if (buildSideLeft) "L" else "R"} on [$outerKey, $innerKey])"
        else
            "HashJoin(${joinType.name}, ${if (buildSideLeft) "L" else "R"} on [$outerKey, $innerKey])"
}

data class IndexNLJoinOp(
    val joinType: JoinCommand,
    val outerKey: String,
    val innerKey: String,
    val indexPath: Path,
    val condOp: ConditionOperator
) : PhysicalOp {
    override fun toString(): String =
        "IndexNL(${joinType.name} on [$outerKey ${condOp.symbol} $innerKey], index=$indexPath)"
}

data class BlockNLJoinOp(
    val joinType: JoinCommand,
    val outerKey: String,
    val innerKey: String,
    val outerLeft: Boolean,
    val condOp: ConditionOperator
) : PhysicalOp {
    override fun toString(): String =
        "BlockNL(${joinType.name}, ${if (outerLeft) "outer=left" else "outer=right"} on [$outerKey ${condOp.symbol} $innerKey])"
}

data class SortMergeJoinOp(
    val joinType: JoinCommand,
    val outerKey: String,
    val innerKey: String,
    val alreadySorted: Boolean,
    val condOp: ConditionOperator
) : PhysicalOp {
    override fun toString(): String =
        if (alreadySorted)
            "MergeJoin(${joinType.name} on [$outerKey ${condOp.symbol} $innerKey])"
        else
            "SortMergeJoin(${joinType.name} on [$outerKey ${condOp.symbol} $innerKey])"
}